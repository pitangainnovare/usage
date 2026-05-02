from django.core.management.base import BaseCommand
from django.db import transaction

from log_manager.models import LogFile
from metrics.models import DailyMetricJob
from metrics.services import daily_payloads
from reports.models import MonthlyLogReport, WeeklyLogReport, YearlyLogReport
from tracker.models import LogFileDiscardedLine


class Command(BaseCommand):
    help = (
        "Clear the log catalog stored in the database, including derived parsing "
        "records, daily metric payloads, and optionally reports, "
        "while preserving the source log files on disk."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--collection",
            action="append",
            dest="collections",
            help="Collection acronym to limit cleanup. Repeat the option for multiple collections.",
        )
        parser.add_argument(
            "--reports",
            action="store_true",
            default=False,
            help="Also clear Weekly/Monthly/Yearly log reports for the selected collections.",
        )

    def handle(self, *args, **options):
        collections = options.get("collections") or []
        clear_reports = options.get("reports")

        log_files = LogFile.objects.all()
        if collections:
            log_files = log_files.filter(collection__acron3__in=collections)

        log_file_ids = list(log_files.values_list("id", flat=True))
        if not log_file_ids:
            self.stdout.write(self.style.WARNING("No log catalog rows found for cleanup."))
            return

        daily_jobs = DailyMetricJob.objects.all()
        if collections:
            daily_jobs = daily_jobs.filter(collection__acron3__in=collections)
        payload_paths = list(daily_jobs.exclude(storage_path="").values_list("storage_path", flat=True))

        summary = {
            "log_files": len(log_file_ids),
            "discarded_lines": LogFileDiscardedLine.objects.filter(
                log_file_id__in=log_file_ids
            ).count(),
            "daily_metric_jobs": daily_jobs.count(),
        }

        for storage_path in payload_paths:
            daily_payloads.delete_payload(storage_path)

        with transaction.atomic():
            LogFileDiscardedLine.objects.filter(log_file_id__in=log_file_ids).delete()
            daily_jobs.delete()
            LogFile.objects.filter(id__in=log_file_ids).delete()

            if clear_reports:
                report_qs = WeeklyLogReport.objects.all()
                m_qs = MonthlyLogReport.objects.all()
                y_qs = YearlyLogReport.objects.all()
                if collections:
                    report_qs = report_qs.filter(collection__acron3__in=collections)
                    m_qs = m_qs.filter(collection__acron3__in=collections)
                    y_qs = y_qs.filter(collection__acron3__in=collections)
                summary["weekly_reports"] = report_qs.count()
                summary["monthly_reports"] = m_qs.count()
                summary["yearly_reports"] = y_qs.count()
                report_qs.delete()
                m_qs.delete()
                y_qs.delete()

        msg = (
            f"Cleared log catalog: "
            f"{summary['log_files']} log files, "
            f"{summary['discarded_lines']} discarded lines, "
            f"{summary['daily_metric_jobs']} daily metric jobs."
        )
        if clear_reports:
            msg += (
                f" Also cleared reports: "
                f"{summary['weekly_reports']} weekly, "
                f"{summary['monthly_reports']} monthly, "
                f"{summary['yearly_reports']} yearly."
            )
        self.stdout.write(self.style.SUCCESS(msg))
