import logging
import os

from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils.translation import gettext as _

from core.utils import date_utils
from core.utils.request_utils import _get_user
from config import celery_app
from collection.models import Collection
from log_manager_config import models as lmc_models

from . import (
    choices, 
    models, 
    utils,
)


LOGFILE_STAT_RESULT_CTIME_INDEX = 9

User = get_user_model()


@celery_app.task(bind=True, name=_('[Log Pipeline] 1. Search Logs (Manual)'), queue='load')
def task_search_log_files(self, collections=[], from_date=None, until_date=None, days_to_go_back=None, user_id=None, username=None, trigger_validation=False):
    """
    Task to search for log files in the directories defined in the CollectionLogDirectory model.

    Parameters:
        collections (list, optional): List of collection acronyms. Defaults to [].
        from_date (str, optional): The start date for log discovery in YYYY-MM-DD format. Defaults to None.
        until_date (str, optional): The end date for log discovery in YYYY-MM-DD format. Defaults to None.
        days_to_go_back (int, optional): The number of days to go back from today for log discovery. Defaults to None.
        user_id (int, optional): The ID of the user initiating the task. Defaults to None.
        username (str, optional): The username of the user initiating the task. Defaults to None.
    """
    user = _get_user(self.request, username=username, user_id=user_id)
    
    for col in collections or Collection.acron3_list():
        collection = Collection.objects.get(acron3=col)

        col_configs_dirs = lmc_models.CollectionLogDirectory.objects.filter(config__collection__acron3=col, active=True)
        if len(col_configs_dirs) == 0:
            logging.error(f'No CollectionLogDirectory found for collection {col}.')

        supported_logfile_extensions = settings.SUPPORTED_LOGFILE_EXTENSIONS
        if len(supported_logfile_extensions) == 0:
            logging.error('No SupportedLogFile found. Please, add a SupportedLogFile for each of the supported log file formats.')

        for cd in col_configs_dirs:
            for root, _sub_dirs, files in os.walk(cd.path):
                for name in files:
                    _name, extension = os.path.splitext(name)
                    if extension.lower() not in supported_logfile_extensions:
                        continue

                    visible_dates = _get_visible_dates(from_date, until_date, days_to_go_back)
                    logging.debug(f'Visible dates: {visible_dates}')

                    _add_log_file(collection, root, name, visible_dates)

    if trigger_validation:
        task_validate_log_files.apply_async(kwargs={
            "collections": collections,
            "from_date": from_date,
            "until_date": until_date,
            "days_to_go_back": days_to_go_back,
            "user_id": user_id,
            "username": username,
            "trigger_parse": True
        })


def _get_visible_dates(from_date, until_date, days_to_go_back):
    from_date_str, until_date_str = date_utils.get_date_range_str(from_date, until_date, days_to_go_back)
    return date_utils.get_date_objs_from_date_range(from_date_str, until_date_str)


def _add_log_file(collection, root, name, visible_dates):
    file_path = os.path.join(root, name)
    file_ctime = date_utils.get_date_obj_from_timestamp(os.stat(file_path).st_ctime)

    logging.debug(f'Checking file {file_path} with ctime {file_ctime}.')
    if file_ctime in visible_dates:
        models.LogFile.create_or_update(
            collection=collection,
            path=file_path,
            stat_result=os.stat(file_path),
            hash=utils.hash_file(file_path),
        )


@celery_app.task(bind=True, name=_('[Log Pipeline] 2. Validate Logs (Manual)'), timelimit=-1, queue='load')
def task_validate_log_files(self, collections=[], from_date=None, until_date=None, days_to_go_back=None, user_id=None, username=None, ignore_date=False, trigger_parse=False, revalidate=False, status_list=None):
    """
    Task to validate log files in the database.

    Parameters:
        collections (list, optional): List of collection acronyms. Defaults to [].
        from_date (str, optional): The start date for log discovery in YYYY-MM-DD format. Defaults to None.
        until_date (str, optional): The end date for log discovery in YYYY-MM-DD format. Defaults to None.
        days_to_go_back (int, optional): The number of days to go back from today for log discovery. Defaults to None.
        user_id (int, optional): The ID of the user initiating the task. Defaults to None.
        username (str, optional): The username of the user initiating the task. Defaults to None.
        ignore_date (bool, optional): If True, ignore the date of the log file. Defaults to False.
        revalidate (bool, optional): If True, also revalidate files in statuses from status_list. Defaults to False.
        status_list (list, optional): List of status codes to revalidate when revalidate=True. Defaults to [QUE, INV, ERR].
    """
    cols = collections or Collection.acron3_list()
    logging.info(f'Validating log files for collections: {cols}.')

    visible_dates = _get_visible_dates(from_date, until_date, days_to_go_back)
    if not ignore_date:
        if not visible_dates:
            logging.warning("No visible dates found for log validation.")
            return
        logging.info(f'Interval: {visible_dates[0]} to {visible_dates[-1]}.')

    status_filter = [choices.LOG_FILE_STATUS_CREATED]
    if revalidate:
        status_filter += status_list or [choices.LOG_FILE_STATUS_QUEUED, choices.LOG_FILE_STATUS_INVALIDATED, choices.LOG_FILE_STATUS_ERROR]

    tasks = []
    for col in cols:
        for log_file in models.LogFile.objects.filter(status__in=status_filter, collection__acron3=col):
            file_ctime = date_utils.get_date_obj_from_timestamp(log_file.stat_result[LOGFILE_STAT_RESULT_CTIME_INDEX])
            if file_ctime in visible_dates or ignore_date:
                tasks.append(task_validate_log_file.s(log_file.hash, user_id, username))

    if tasks:
        if trigger_parse:
            from celery import chord
            from metrics.tasks import task_parse_logs
            chord(tasks)(task_parse_logs.si(
                collections=collections,
                from_date=from_date,
                until_date=until_date,
                days_to_go_back=days_to_go_back,
                user_id=user_id,
                username=username,
            ))
        else:
            for task in tasks:
                task.apply_async()
    elif trigger_parse:
        from metrics.tasks import task_parse_logs
        task_parse_logs.apply_async(kwargs={
            "collections": collections,
            "from_date": from_date,
            "until_date": until_date,
            "days_to_go_back": days_to_go_back,
            "user_id": user_id,
            "username": username,
        })


@celery_app.task(bind=True, name=_('[Log Pipeline] Validate Single Log File (Auto)'), timelimit=-1, queue='load')
def task_validate_log_file(self, log_file_hash, user_id=None, username=None):
    """
    Task to validate a specific log file.

    Parameters:
        log_file_id (int): The ID of the log file to validate.
        user_id (int, optional): The ID of the user initiating the task. Defaults to None.
        username (str, optional): The username of the user initiating the task. Defaults to None.
    """
    user = _get_user(self.request, username=username, user_id=user_id)
    log_file = models.LogFile.objects.get(hash=log_file_hash)
    collection = log_file.collection.acron3

    buffer_size, sample_size = _fetch_validation_parameters(collection)

    logging.info(f'Validating log file {log_file.path}.')    
    val_result = utils.validate_file(path=log_file.path, buffer_size=buffer_size, sample_size=sample_size)
    if 'datetimes' in val_result.get('content', {}).get('summary', {}):
        del val_result['content']['summary']['datetimes']

    if 'probably_date' in val_result:
        if isinstance(val_result['probably_date'], dict):
            logging.error(f"Error determining probably_date: {val_result['probably_date'].get('error')}")
            val_result['probably_date'] = None
        else:
            try:
                val_result['probably_date'] = date_utils.get_date_str(val_result['probably_date'])
            except (ValueError, AttributeError) as e:
                logging.error(f'Error serializing probably_date: {e}')
                val_result['probably_date'] = None

    log_file.validation = val_result
    log_file.validation.update({'buffer_size': buffer_size, 'sample_size': sample_size})

    if val_result.get('is_valid', {}).get('all', False):
        log_file.date = val_result.get('probably_date') or None
        log_file.status = choices.LOG_FILE_STATUS_QUEUED

    else:
        log_file.status = choices.LOG_FILE_STATUS_INVALIDATED

    logging.info(f'Log file {log_file.path} ({log_file.collection.acron3}) has status {log_file.status}.')
    log_file.save()


def _fetch_validation_parameters(collection, default_buffer_size=0.1, default_sample_size=2048):
    col_configs = lmc_models.LogManagerCollectionConfig.objects.filter(collection__acron3=collection).first()
    if not col_configs:
        logging.warning(f'No LogManagerCollectionConfig found for collection {collection}. Using default values.')
        return default_buffer_size, default_sample_size
    return col_configs.buffer_size, col_configs.sample_size


@celery_app.task(bind=True, name=_('[Log Pipeline] Daily Routine (Auto)'), queue='load')
def task_daily_log_ingestion_pipeline(self):
    """
    Facade task for the daily log ingestion pipeline.
    It initiates the Search -> Validate -> Parse chain using default parameters.
    No arguments are required, making it easy to schedule periodically.
    """
    logging.info("Starting Daily Log Ingestion Pipeline")
    task_search_log_files.apply_async(kwargs={"trigger_validation": True})
