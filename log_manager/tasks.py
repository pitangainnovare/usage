import logging
import os

from celery import chord
from django.conf import settings

from collection.models import Collection
from config import celery_app
from core.utils import date_utils
from core.utils.request_utils import _get_user
from log_manager_config import models as lmc_models
from metrics.services.resources import extract_celery_queue_name
from metrics.tasks import task_parse_logs

from . import choices, models, utils

LOGFILE_STAT_RESULT_CTIME_INDEX = 9


@celery_app.task(
    bind=True, name="[Log Pipeline] 1. Search Logs (Manual)", queue="load"
)
def task_search_log_files(
    self,
    collections=None,
    from_date=None,
    until_date=None,
    days_to_go_back=None,
    user_id=None,
    username=None,
    trigger_validation=False,
):
    """
    Search for log files in configured collection directories.

    When trigger_validation=True, this starts the full Search -> Validate -> Parse
    chain. Parse callbacks are routed by collection size.
    """
    _get_user(self.request, username=username, user_id=user_id)

    from_date_str, until_date_str = date_utils.get_date_range_str(
        from_date, until_date, days_to_go_back
    )
    visible_dates = date_utils.get_date_objs_from_date_range(
        from_date_str, until_date_str
    )
    supported_extensions = settings.SUPPORTED_LOGFILE_EXTENSIONS
    if not supported_extensions:
        logging.error("No supported log file extensions configured.")

    for collection_code in collections or Collection.acron3_list():
        collection = Collection.objects.get(acron3=collection_code)
        directories = lmc_models.CollectionLogDirectory.objects.filter(
            config__collection__acron3=collection_code,
            active=True,
        )
        if not directories:
            logging.error(
                "No CollectionLogDirectory found for collection %s.", collection_code
            )

        for directory in directories:
            for root, _sub_dirs, files in os.walk(directory.path):
                for name in files:
                    _name, extension = os.path.splitext(name)
                    if extension.lower() not in supported_extensions:
                        continue

                    file_path = os.path.join(root, name)
                    file_stat = os.stat(file_path)
                    file_ctime = date_utils.get_date_obj_from_timestamp(
                        file_stat.st_ctime
                    )

                    logging.debug(
                        "Checking file %s with ctime %s.", file_path, file_ctime
                    )
                    if file_ctime in visible_dates:
                        models.LogFile.create_or_update(
                            collection=collection,
                            path=file_path,
                            stat_result=file_stat,
                            hash=utils.hash_file(file_path),
                        )

    if trigger_validation:
        task_validate_log_files.apply_async(
            kwargs={
                "collections": collections,
                "from_date": from_date,
                "until_date": until_date,
                "days_to_go_back": days_to_go_back,
                "user_id": user_id,
                "username": username,
                "trigger_parse": True,
            }
        )


@celery_app.task(
    bind=True,
    name="[Log Pipeline] 2. Validate Logs (Manual)",
    timelimit=-1,
    queue="load",
)
def task_validate_log_files(
    self,
    collections=None,
    from_date=None,
    until_date=None,
    days_to_go_back=None,
    user_id=None,
    username=None,
    ignore_date=False,
    trigger_parse=False,
    revalidate=False,
    status_list=None,
):
    """
    Validate cataloged log files.

    When trigger_parse=True, one parse orchestration task is enqueued per
    collection and routed to the proper parse_<size> queue.
    """
    collection_codes = collections or Collection.acron3_list()
    logging.info("Validating log files for collections: %s.", collection_codes)

    from_date_str, until_date_str = date_utils.get_date_range_str(
        from_date, until_date, days_to_go_back
    )
    visible_dates = date_utils.get_date_objs_from_date_range(
        from_date_str, until_date_str
    )
    if not ignore_date:
        if not visible_dates:
            logging.warning("No visible dates found for log validation.")
            return
        logging.info("Interval: %s to %s.", visible_dates[0], visible_dates[-1])

    status_filter = [choices.LOG_FILE_STATUS_CREATED]
    if revalidate:
        status_filter += status_list or [
            choices.LOG_FILE_STATUS_QUEUED,
            choices.LOG_FILE_STATUS_INVALIDATED,
            choices.LOG_FILE_STATUS_ERROR,
        ]

    tasks_by_collection = {}
    for collection_code in collection_codes:
        tasks_by_collection[collection_code] = []
        log_files = models.LogFile.objects.filter(
            status__in=status_filter,
            collection__acron3=collection_code,
        )
        for log_file in log_files:
            if not ignore_date:
                file_ctime = date_utils.get_date_obj_from_timestamp(
                    log_file.stat_result[LOGFILE_STAT_RESULT_CTIME_INDEX]
                )
                if file_ctime not in visible_dates:
                    continue

            tasks_by_collection[collection_code].append(
                task_validate_log_file.s(log_file.hash, user_id, username)
            )

    if trigger_parse:
        _enqueue_parse_after_validation(
            tasks_by_collection=tasks_by_collection,
            from_date=from_date,
            until_date=until_date,
            days_to_go_back=days_to_go_back,
            user_id=user_id,
            username=username,
        )
        return

    for collection_tasks in tasks_by_collection.values():
        for validation_task in collection_tasks:
            validation_task.apply_async()


@celery_app.task(
    bind=True,
    name="[Log Pipeline] Validate Single Log File (Auto)",
    timelimit=-1,
    queue="load",
)
def task_validate_log_file(self, log_file_hash, user_id=None, username=None):
    """Validate a single LogFile and update its status."""
    _get_user(self.request, username=username, user_id=user_id)
    log_file = models.LogFile.objects.get(hash=log_file_hash)
    collection = log_file.collection.acron3

    buffer_size, sample_size = _fetch_validation_parameters(collection)

    logging.info("Validating log file %s.", log_file.path)
    val_result = utils.validate_file(
        path=log_file.path, buffer_size=buffer_size, sample_size=sample_size
    )
    _clean_validation_result(val_result)

    log_file.validation = val_result
    log_file.validation.update({"buffer_size": buffer_size, "sample_size": sample_size})

    if val_result.get("is_valid", {}).get("all", False):
        log_file.date = val_result.get("probably_date") or None
        log_file.status = choices.LOG_FILE_STATUS_QUEUED
    else:
        log_file.status = choices.LOG_FILE_STATUS_INVALIDATED

    logging.info(
        "Log file %s (%s) has status %s.",
        log_file.path,
        log_file.collection.acron3,
        log_file.status,
    )
    log_file.save()


@celery_app.task(bind=True, name="[Log Pipeline] Daily Routine (Auto)", queue="load")
def task_daily_log_ingestion_pipeline(self):
    """
    Start the daily Search -> Validate -> Parse chain with default parameters.
    """
    logging.info("Starting Daily Log Ingestion Pipeline")
    task_search_log_files.apply_async(kwargs={"trigger_validation": True})


def _enqueue_parse_after_validation(
    tasks_by_collection, from_date, until_date, days_to_go_back, user_id, username
):
    for collection_code, validation_tasks in tasks_by_collection.items():
        if validation_tasks:
            chord(validation_tasks)(
                _build_parse_signature(
                    collection_code,
                    from_date,
                    until_date,
                    days_to_go_back,
                    user_id,
                    username,
                )
            )
        else:
            task_parse_logs.apply_async(
                **_build_parse_apply_kwargs(
                    collection_code,
                    from_date,
                    until_date,
                    days_to_go_back,
                    user_id,
                    username,
                )
            )


def _build_parse_signature(
    collection_code, from_date, until_date, days_to_go_back, user_id, username
):
    apply_kwargs = _build_parse_apply_kwargs(
        collection_code,
        from_date,
        until_date,
        days_to_go_back,
        user_id,
        username,
    )
    parse_callback = task_parse_logs.si(**apply_kwargs["kwargs"])
    if apply_kwargs.get("queue"):
        parse_callback.set(queue=apply_kwargs["queue"])
    return parse_callback


def _build_parse_apply_kwargs(
    collection_code, from_date, until_date, days_to_go_back, user_id, username
):
    collections = [collection_code]
    parse_queue = extract_celery_queue_name(collection_code)
    apply_kwargs = {
        "kwargs": {
            "collections": collections,
            "from_date": from_date,
            "until_date": until_date,
            "days_to_go_back": days_to_go_back,
            "queue_name": parse_queue,
            "user_id": user_id,
            "username": username,
        },
        "queue": parse_queue,
    }
    return apply_kwargs


def _fetch_validation_parameters(
    collection, default_buffer_size=0.1, default_sample_size=2048
):
    col_configs = lmc_models.LogManagerCollectionConfig.objects.filter(
        collection__acron3=collection
    ).first()
    if not col_configs:
        logging.warning(
            "No LogManagerCollectionConfig found for collection %s. Using default values.",
            collection,
        )
        return default_buffer_size, default_sample_size
    return col_configs.buffer_size, col_configs.sample_size


def _clean_validation_result(val_result):
    if "datetimes" in val_result.get("content", {}).get("summary", {}):
        del val_result["content"]["summary"]["datetimes"]

    if "probably_date" not in val_result:
        return

    probably_date = val_result["probably_date"]
    if isinstance(probably_date, dict):
        logging.error("Error determining probably_date: %s", probably_date.get("error"))
        val_result["probably_date"] = None
        return

    try:
        val_result["probably_date"] = date_utils.get_date_str(probably_date)
    except (ValueError, AttributeError) as exc:
        logging.error("Error serializing probably_date: %s", exc)
        val_result["probably_date"] = None
