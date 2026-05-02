import logging

from django.utils.translation import gettext as _

from config import celery_app
from core.utils.request_utils import _get_user
from metrics.models import DailyMetricJob

from metrics.services.jobs import acquire_daily_metric_job, mark_daily_metric_job_exported, mark_daily_metric_job_failed
from metrics.services.export import export_daily_metric_payload, load_daily_metric_payload
from metrics.services.resources import build_search_client, fetch_required_resources
from metrics.services.parser import process_daily_metric_job


@celery_app.task(bind=True, name=_("[Metrics] Process Daily Job"), timelimit=-1)
def task_process_daily_metric_job(
    self,
    job_id,
    track_errors=False,
    user_id=None,
    username=None,
    robots_source=None,
):
    user = _get_user(self.request, username=username, user_id=user_id)

    try:
        job = acquire_daily_metric_job(job_id)
    except DailyMetricJob.DoesNotExist:
        logging.error("Daily metric job %s does not exist.", job_id)
        return

    if not job:
        return

    try:
        payload = load_daily_metric_payload(job)
        if payload is None or not job.payload_hash:
            robots_list, mmdb = fetch_required_resources(robot_source=robots_source)
            if not robots_list or not mmdb:
                raise RuntimeError("Required parsing resources are not available.")
            payload = process_daily_metric_job(
                job=job,
                robots_list=robots_list,
                mmdb=mmdb,
                track_errors=track_errors,
            )
            job.refresh_from_db()

        search_client = build_search_client()
        if not search_client.ping():
            raise RuntimeError("OpenSearch client is not available.")

        export_daily_metric_payload(
            search_client=search_client,
            job=job,
            payload=payload,
        )
    except Exception as exc:
        logging.error("Failed to process daily metric job %s: %s", job_id, exc)
        mark_daily_metric_job_failed(job, exc)
        return

    mark_daily_metric_job_exported(job, user=user)
