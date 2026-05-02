import logging

from django.utils.translation import gettext as _

from config import celery_app
from core.utils.request_utils import _get_user
from metrics.services import daily_payloads


@celery_app.task(bind=True, name=_("[Metrics] Cleanup Daily Payloads"), timelimit=-1)
def task_cleanup_daily_payloads(
    self,
    collections=None,
    older_than_days=7,
    user_id=None,
    username=None,
):
    _get_user(self.request, username=username, user_id=user_id)

    deleted_count = daily_payloads.cleanup_exported_payloads(
        collections=collections or [],
        older_than_days=older_than_days,
    )

    logging.info(
        "Cleanup task completed: %s payload file(s) deleted (collections=%s, older_than_days=%s).",
        deleted_count,
        collections or "all",
        older_than_days,
    )
    return {"deleted_payloads": deleted_count}
