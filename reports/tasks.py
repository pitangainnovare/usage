import logging
from collections import defaultdict

from django.core.mail import send_mail
from django.conf import settings
from django.utils.translation import gettext as _

from config import celery_app
from core.utils import date_utils
from collection.models import Collection
from log_manager import choices
from log_manager.models import LogFile
from log_manager_config import models as lmc_models

from reports.models import WeeklyLogReport, MonthlyLogReport, YearlyLogReport


def _extract_date_from_log_file(lf):
    if lf.date:
        return lf.date

    probably_date = (lf.validation or {}).get("probably_date")
    if isinstance(probably_date, str) and probably_date:
        return date_utils.get_date_obj(probably_date)

    try:
        import re
        match = re.search(r"(\d{4}-\d{2}-\d{2})", lf.path)
        if match:
            return date_utils.get_date_obj(match.group(1))
    except Exception:
        pass

    return None


@celery_app.task(bind=True, name=_("[Reports] Populate All Reports"))
def task_populate_all_reports(self, year=None, collection_acron=None):
    qs = LogFile.objects.select_related("collection")
    if collection_acron:
        qs = qs.filter(collection__acron3=collection_acron)
    qs = qs.only(
        "id", "collection_id", "date", "path", "status", "summary", "validation"
    )

    weekly = defaultdict(lambda: defaultdict(int))
    monthly = defaultdict(lambda: defaultdict(int))
    yearly = defaultdict(lambda: defaultdict(int))

    for lf in qs.iterator(chunk_size=2000):
        extracted_date = _extract_date_from_log_file(lf)
        if not extracted_date:
            continue
        if year and extracted_date.year != int(year):
            continue

        iso_year, iso_week, _ = extracted_date.isocalendar()
        yr = extracted_date.year
        mo = extracted_date.month

        for agg, key in [
            (weekly, (lf.collection_id, iso_year, iso_week)),
            (monthly, (lf.collection_id, yr, mo)),
            (yearly, (lf.collection_id, yr)),
        ]:
            r = agg[key]
            r["total_files"] += 1
            st = lf.status
            if st == "CRE":
                r["created_files"] += 1
            elif st in ("QUE", "PAR", "PRO"):
                r["validated_files"] += 1
            elif st == "INV":
                r["invalidated_files"] += 1
            elif st == "ERR":
                r["errored_files"] += 1

            s = lf.summary or {}
            lp = s.get("lines_parsed", 0) or 0
            vl = s.get("valid_lines", 0) or 0
            r["lines_parsed"] += lp
            r["valid_lines"] += vl
            r["discarded_lines"] += max(lp - vl, 0)

            ips = (
                (lf.validation or {})
                .get("content", {})
                .get("summary", {})
                .get("ips", {})
            )
            r["ip_local_count"] += ips.get("local", 0) or 0
            r["ip_remote_count"] += ips.get("remote", 0) or 0
            r["ip_unknown_count"] += ips.get("unknown", 0) or 0

    w_count = _upsert_reports(WeeklyLogReport, weekly)
    m_count = _upsert_reports(MonthlyLogReport, monthly)
    y_count = _upsert_reports(YearlyLogReport, yearly)

    logging.info(
        "Reports populated: %s weekly, %s monthly, %s yearly.",
        w_count, m_count, y_count,
    )
    return f"Weekly: {w_count}, Monthly: {m_count}, Yearly: {y_count}"


def _upsert_reports(model_class, data):
    count = 0
    unique_fields = list(model_class._meta.unique_together[0])
    period_fields = unique_fields[1:]
    for key, fields in data.items():
        coll_id = key[0]
        period_values = key[1:]
        lookup = {"collection_id": coll_id}
        for idx, field_name in enumerate(period_fields):
            lookup[field_name] = period_values[idx]
        model_class.objects.update_or_create(defaults=fields, **lookup)
        count += 1
    return count


@celery_app.task(
    bind=True,
    name=_("[Reports] Generate Log Report Summary (Manual)"),
    queue="load",
)
def task_log_files_count_status_report(
    self,
    collections=None,
    from_date=None,
    until_date=None,
    days_to_go_back=None,
    user_id=None,
    username=None,
):
    from_date_str, until_date_str = date_utils.get_date_range_str(
        from_date, until_date, days_to_go_back
    )
    subject = _(
        "Usage Log Report Summary "
        f"({from_date_str} to {until_date_str})"
    )

    for collection_acron in (collections or Collection.acron3_list()):
        try:
            collection = Collection.objects.get(acron3=collection_acron)
        except Collection.DoesNotExist:
            logging.warning("Collection not found: %s", collection_acron)
            continue

        message = _build_report_message(
            collection,
            from_date_str,
            until_date_str,
        )

        if not message:
            continue

        logging.info(
            "Sending email to collection %s. Subject: %s.",
            collection.main_name, subject,
        )

        _send_collection_email(subject, message, collection_acron)


def _build_report_message(collection, from_date_str, until_date_str):
    monthly = MonthlyLogReport.objects.filter(
        collection=collection,
    ).order_by("-year", "-month")

    if not monthly.exists():
        return ""

    latest = monthly.first()
    message = _(
        f"Usage Log Report for {collection.acron3}\n"
        f"Period: {from_date_str} to {until_date_str}\n\n"
    )
    message += _("Latest month ({latest}):\n").format(latest=latest.period_label)
    message += (
        f"  Total files: {latest.total_files}\n"
        f"  Validated files: {latest.validated_files} ({latest.pct_validated}%)\n"
        f"  Invalidated files: {latest.invalidated_files}\n"
        f"  Errored files: {latest.errored_files}\n"
        f"  Lines parsed: {latest.lines_parsed}\n"
        f"  Valid lines: {latest.valid_lines} ({latest.pct_valid_lines}%)\n"
        f"  Discarded lines: {latest.discarded_lines}\n"
        f"  Remote IPs: {latest.ip_remote_count} ({latest.pct_remote_ip}%)\n"
        f"  Local IPs: {latest.ip_local_count}\n"
    )

    prev_month = latest
    if len(monthly) > 1:
        prev_month = monthly[1]
        message += _("\nPrevious month ({prev}):\n").format(prev=prev_month.period_label)
        message += (
            f"  Total files: {prev_month.total_files}\n"
            f"  Validated files: {prev_month.validated_files} ({prev_month.pct_validated}%)\n"
            f"  Valid lines: {prev_month.valid_lines} ({prev_month.pct_valid_lines}%)\n"
            f"  Remote IPs: {prev_month.ip_remote_count} ({prev_month.pct_remote_ip}%)\n"
        )

        if prev_month.total_files:
            file_diff = latest.total_files - prev_month.total_files
            line_diff = latest.lines_parsed - prev_month.lines_parsed
            message += _("\nMonth-over-month change:\n")
            message += f"  Files: {file_diff:+d}\n"
            message += f"  Lines: {line_diff:+d}\n"

    message += (
        f"\n---\n"
        f"This report is automatically generated by SciELO Usage.\n"
    )
    return message


def _send_collection_email(subject, message, collection):
    emails = lmc_models.CollectionEmail.objects.filter(
        config__collection__acron3=collection, active=True
    ).values_list("email", flat=True)

    if not emails:
        logging.error(
            "Error. Please, add an E-mail Configuration for the collection %s.",
            collection,
        )
        return

    try:
        send_mail(
            subject=subject,
            message=message,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=list(emails),
        )
    except Exception as e:
        logging.error("Error sending log files report for %s: %s", collection, e)
