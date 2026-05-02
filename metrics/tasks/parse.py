import logging

from django.utils.translation import gettext as _

from config import celery_app
from core.utils.date_utils import get_date_obj, get_date_range_str
from core.utils.request_utils import _get_user
from collection.models import Collection
from log_manager import choices
from log_manager.models import LogFile
from metrics.models import DailyMetricJob

from metrics.services.resources import extract_celery_queue_name, get_log_files_for_collection_date
from metrics.services.jobs import create_or_update_daily_metric_job
from metrics.tasks.process import task_process_daily_metric_job

AUTO_REEXECUTE_POLL_INTERVAL_SECONDS = 30


@celery_app.task(bind=True, name=_("[Log Pipeline] 3. Parse Logs (Manual)"), timelimit=-1)
def task_parse_logs(
    self,
    collections=None,
    include_logs_with_error=True,
    batch_size=5000,
    max_log_files=None,
    auto_reexecute=False,
    replace=False,
    track_errors=False,
    from_date=None,
    until_date=None,
    days_to_go_back=None,
    queue_name=None,
    user_id=None,
    username=None,
    skip_log_hashes=None,
    robots_source=None,
):
    if replace:
        raise ValueError(
            "replace=True is not supported. Recompute requires deleting/recreating "
            "the affected day or period first."
        )

    from_date, until_date = get_date_range_str(from_date, until_date, days_to_go_back)
    from_date_obj = get_date_obj(from_date)
    until_date_obj = get_date_obj(until_date)
    enqueued_jobs = 0
    reached_max_log_files = False
    enqueued_wave_job_ids = []
    claimed_status_filters = list(_build_parse_status_filters(include_logs_with_error))
    skip_log_hashes = set(skip_log_hashes or [])

    for collection in collections or Collection.acron3_list():
        collection_obj = Collection.objects.filter(acron3=collection).first()
        if not collection_obj:
            continue

        access_dates = _find_access_dates(
            collection=collection_obj,
            from_date=from_date,
            until_date=until_date,
            from_date_obj=from_date_obj,
            until_date_obj=until_date_obj,
            status_filters=claimed_status_filters,
            skip_log_hashes=skip_log_hashes,
        )

        for access_date in access_dates:
            log_files = get_log_files_for_collection_date(
                collection=collection_obj,
                access_date=access_date,
                status_filters=claimed_status_filters,
            )
            log_files = [log_file for log_file in log_files if log_file.hash not in skip_log_hashes]
            if not log_files:
                continue

            job = create_or_update_daily_metric_job(
                collection=collection_obj,
                access_date=access_date,
                log_files=log_files,
            )
            if job.status == DailyMetricJob.STATUS_EXPORTED:
                continue

            task_process_daily_metric_job.apply_async(
                args=(job.pk, track_errors, user_id, username, robots_source),
                queue=queue_name or extract_celery_queue_name(collection),
            )
            enqueued_wave_job_ids.append(job.pk)
            enqueued_jobs += 1
            if max_log_files and enqueued_jobs >= max_log_files:
                reached_max_log_files = True
                break

        if reached_max_log_files:
            break

    auto_reexecution_enqueued = _schedule_parse_logs_reexecution(
        should_reexecute=auto_reexecute and reached_max_log_files and bool(enqueued_wave_job_ids),
        wave_job_ids=enqueued_wave_job_ids,
        collections=collections,
        include_logs_with_error=include_logs_with_error,
        batch_size=batch_size,
        max_log_files=max_log_files,
        auto_reexecute=auto_reexecute,
        replace=replace,
        track_errors=track_errors,
        from_date=from_date,
        until_date=until_date,
        days_to_go_back=days_to_go_back,
        queue_name=queue_name,
        user_id=user_id,
        username=username,
        skip_log_hashes=sorted(skip_log_hashes),
        robots_source=robots_source,
    )

    return {
        "enqueued_logs": enqueued_jobs,
        "enqueued_jobs": enqueued_jobs,
        "reached_max_log_files": reached_max_log_files,
        "auto_reexecution_enqueued": auto_reexecution_enqueued,
    }


def _build_parse_status_filters(include_logs_with_error):
    status_filters = [choices.LOG_FILE_STATUS_QUEUED]
    if include_logs_with_error:
        status_filters.append(choices.LOG_FILE_STATUS_ERROR)
    return tuple(status_filters)


def _find_access_dates(
    collection,
    from_date,
    until_date,
    from_date_obj,
    until_date_obj,
    status_filters,
    skip_log_hashes,
):
    date_queryset = (
        LogFile.objects.filter(
            status__in=status_filters,
            collection=collection,
            date__gte=from_date_obj,
            date__lte=until_date_obj,
        )
        .exclude(hash__in=skip_log_hashes)
        .values_list("date", flat=True)
        .distinct()
        .order_by("date")
    )

    access_dates = set()
    for value in list(date_queryset):
        access_date = value if hasattr(value, "isoformat") else get_date_obj(value)
        if access_date and from_date_obj <= access_date <= until_date_obj:
            access_dates.add(access_date)
    return sorted(access_dates)


def _schedule_parse_logs_reexecution(
    should_reexecute,
    wave_job_ids,
    collections,
    include_logs_with_error,
    batch_size,
    max_log_files,
    auto_reexecute,
    replace,
    track_errors,
    from_date,
    until_date,
    days_to_go_back,
    queue_name,
    user_id,
    username,
    skip_log_hashes,
    robots_source=None,
):
    if not should_reexecute:
        return False

    kwargs = {
        "wave_job_ids": wave_job_ids,
        "collections": collections,
        "include_logs_with_error": include_logs_with_error,
        "batch_size": batch_size,
        "max_log_files": max_log_files,
        "auto_reexecute": auto_reexecute,
        "replace": replace,
        "track_errors": track_errors,
        "from_date": from_date,
        "until_date": until_date,
        "days_to_go_back": days_to_go_back,
        "queue_name": queue_name,
        "user_id": user_id,
        "username": username,
        "skip_log_hashes": skip_log_hashes,
        "poll_interval_seconds": AUTO_REEXECUTE_POLL_INTERVAL_SECONDS,
    }
    if robots_source is not None:
        kwargs["robots_source"] = robots_source

    task_wait_parse_logs_wave.apply_async(kwargs=kwargs)
    return True


@celery_app.task(bind=True, name=_("[Metrics] Wait Parse Logs Wave"), timelimit=-1)
def task_wait_parse_logs_wave(
    self,
    wave_job_ids=None,
    collections=None,
    include_logs_with_error=True,
    batch_size=5000,
    max_log_files=None,
    auto_reexecute=False,
    replace=False,
    track_errors=False,
    from_date=None,
    until_date=None,
    days_to_go_back=None,
    queue_name=None,
    user_id=None,
    username=None,
    skip_log_hashes=None,
    poll_interval_seconds=AUTO_REEXECUTE_POLL_INTERVAL_SECONDS,
    robots_source=None,
    wave_log_hashes=None,
):
    wave_job_ids = wave_job_ids or wave_log_hashes or []
    if DailyMetricJob.objects.filter(
        pk__in=wave_job_ids,
        status__in=[DailyMetricJob.STATUS_PENDING, DailyMetricJob.STATUS_EXPORTING],
    ).exists():
        kwargs = {
            "wave_job_ids": wave_job_ids,
            "collections": collections,
            "include_logs_with_error": include_logs_with_error,
            "batch_size": batch_size,
            "max_log_files": max_log_files,
            "auto_reexecute": auto_reexecute,
            "replace": replace,
            "track_errors": track_errors,
            "from_date": from_date,
            "until_date": until_date,
            "days_to_go_back": days_to_go_back,
            "queue_name": queue_name,
            "user_id": user_id,
            "username": username,
            "skip_log_hashes": skip_log_hashes,
            "poll_interval_seconds": poll_interval_seconds,
        }
        if robots_source is not None:
            kwargs["robots_source"] = robots_source

        task_wait_parse_logs_wave.apply_async(
            kwargs=kwargs,
            countdown=poll_interval_seconds,
        )
        return {"wave_completed": False, "reexecution_enqueued": False}

    kwargs = {
        "collections": collections,
        "include_logs_with_error": include_logs_with_error,
        "batch_size": batch_size,
        "max_log_files": max_log_files,
        "auto_reexecute": auto_reexecute,
        "replace": replace,
        "track_errors": track_errors,
        "from_date": from_date,
        "until_date": until_date,
        "days_to_go_back": days_to_go_back,
        "queue_name": queue_name,
        "user_id": user_id,
        "username": username,
        "skip_log_hashes": skip_log_hashes,
    }
    if robots_source is not None:
        kwargs["robots_source"] = robots_source

    task_parse_logs.apply_async(kwargs=kwargs)
    return {"wave_completed": True, "reexecution_enqueued": True}
