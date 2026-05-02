import logging

from django.utils import timezone
from django.utils.translation import gettext as _

from config import celery_app
from core.utils.date_utils import get_date_obj, get_date_range_str
from core.utils.request_utils import _get_user
from log_manager import choices
from log_manager.models import LogFile
from metrics.models import DailyMetricJob

from metrics.services.jobs import create_or_update_daily_metric_job, release_stale_daily_metric_jobs
from metrics.services.resources import extract_celery_queue_name, get_log_files_for_collection_date
from metrics.services.parser import is_stale_parsing_log, requeue_stale_parsing_log
from metrics.counter import parser

from .parse import task_parse_logs
from .process import task_process_daily_metric_job


@celery_app.task(bind=True, name=_("[Metrics] Resume Log Exports"), timelimit=-1)
def task_resume_log_exports(
    self,
    collections=None,
    from_date=None,
    until_date=None,
    days_to_go_back=None,
    stale_after_minutes=60,
    queue_name=None,
    user_id=None,
    username=None,
    robots_source=None,
):
    _get_user(self.request, username=username, user_id=user_id)

    from_date, until_date = get_date_range_str(from_date, until_date, days_to_go_back)
    from_date_obj = get_date_obj(from_date)
    until_date_obj = get_date_obj(until_date)

    released_stale_jobs = release_stale_daily_metric_jobs(
        collections=collections,
        from_date=from_date_obj,
        until_date=until_date_obj,
        stale_after_minutes=stale_after_minutes,
    )
    queryset = DailyMetricJob.objects.filter(
        status__in=[DailyMetricJob.STATUS_PENDING, DailyMetricJob.STATUS_ERROR],
        access_date__gte=from_date_obj,
        access_date__lte=until_date_obj,
    ).select_related("collection").order_by("access_date", "collection__acron3")
    if collections:
        queryset = queryset.filter(collection__acron3__in=collections)

    resumed_jobs = 0
    for job in queryset:
        log_files = get_log_files_for_collection_date(
            collection=job.collection,
            access_date=job.access_date,
            status_filters=[
                choices.LOG_FILE_STATUS_QUEUED,
                choices.LOG_FILE_STATUS_ERROR,
            ],
        )
        if log_files:
            job = create_or_update_daily_metric_job(
                collection=job.collection,
                access_date=job.access_date,
                log_files=log_files,
            )
        elif not (job.storage_path and job.payload_hash):
            logging.warning(
                "Skipping daily metric job %s: no queued/error logs or stored payload.",
                job.pk,
            )
            continue

        if job.status == DailyMetricJob.STATUS_EXPORTED:
            continue

        task_process_daily_metric_job.apply_async(
            args=(job.pk, False, user_id, username, robots_source),
            queue=queue_name or extract_celery_queue_name(job.collection.acron3),
        )
        resumed_jobs += 1

    logging.info(
        "Resumed daily metric jobs for %s day(s); released %s stale job(s) at %s.",
        resumed_jobs,
        released_stale_jobs,
        timezone.now(),
    )
    return {
        "resumed_logs": resumed_jobs,
        "resumed_jobs": resumed_jobs,
        "released_stale_batches": released_stale_jobs,
        "released_stale_jobs": released_stale_jobs,
    }


@celery_app.task(bind=True, name=_("[Metrics] Resume Stale Parsing Logs"), timelimit=-1)
def task_resume_stale_parsing_logs(
    self,
    collections=None,
    batch_size=5000,
    track_errors=False,
    from_date=None,
    until_date=None,
    days_to_go_back=None,
    stale_after_minutes=60,
    max_log_files=None,
    queue_name=None,
    user_id=None,
    username=None,
    robots_source=None,
):
    from_date, until_date = get_date_range_str(from_date, until_date, days_to_go_back)
    from_date_obj = get_date_obj(from_date)
    until_date_obj = get_date_obj(until_date)

    queryset = (
        LogFile.objects.filter(status=choices.LOG_FILE_STATUS_PARSING)
        .select_related("collection")
        .order_by("validation__probably_date", "path", "hash")
    )
    if collections:
        queryset = queryset.filter(collection__acron3__in=collections)

    resumed_logs = 0
    for log_file in queryset:
        probably_date = parser.extract_date_from_validation_dict(log_file.validation)
        if not probably_date or probably_date < from_date_obj or probably_date > until_date_obj:
            continue
        if not is_stale_parsing_log(log_file, stale_after_minutes=stale_after_minutes):
            continue

        requeue_stale_parsing_log(log_file)
        resumed_logs += 1
        if max_log_files and resumed_logs >= max_log_files:
            break

    apply_kwargs = {
        "kwargs": {
            "collections": collections,
            "include_logs_with_error": True,
            "batch_size": batch_size,
            "max_log_files": max_log_files,
            "auto_reexecute": False,
            "replace": False,
            "track_errors": track_errors,
            "from_date": from_date,
            "until_date": until_date,
            "days_to_go_back": None,
            "queue_name": queue_name,
            "user_id": user_id,
            "username": username,
            "robots_source": robots_source,
        }
    }
    if queue_name:
        apply_kwargs["queue"] = queue_name
    task_parse_logs.apply_async(**apply_kwargs)
    return {
        "stale_logs_marked_for_retry": resumed_logs,
        "parse_logs_enqueued": True,
    }
