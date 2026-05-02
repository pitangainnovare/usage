from datetime import date, timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone
from scielo_usage_counter.values import CONTENT_TYPE_FULL_TEXT, MEDIA_FORMAT_HTML

from collection.models import Collection
from log_manager import choices
from log_manager.models import LogFile
from metrics.models import DailyMetricJob
from metrics import services


class DailyMetricJobServiceTests(TestCase):
    def setUp(self):
        self.collection = Collection.objects.create(acron3="books", acron2="bk")

    def _log_file(self, hash_value, status=choices.LOG_FILE_STATUS_QUEUED):
        return LogFile.objects.create(
            hash=hash_value,
            path=f"/tmp/{hash_value}.log.gz",
            stat_result={},
            status=status,
            collection=self.collection,
            validation={"probably_date": "2012-03-10"},
        )

    def test_create_or_update_blocks_implicit_recompute_after_export(self):
        first = self._log_file("1" * 32)
        second = self._log_file("2" * 32)
        DailyMetricJob.objects.create(
            collection=self.collection,
            access_date=date(2012, 3, 10),
            status=DailyMetricJob.STATUS_EXPORTED,
            input_log_hashes=[first.hash],
            storage_path="books/2012/03/2012-03-10.json",
            payload_hash="abc",
        )

        with self.assertRaises(RuntimeError):
            services.create_or_update_daily_metric_job(
                collection=self.collection,
                access_date=date(2012, 3, 10),
                log_files=[first, second],
            )

    def test_create_or_update_keeps_payload_for_export_retry(self):
        log_file = self._log_file("1" * 32, status=choices.LOG_FILE_STATUS_ERROR)
        job = DailyMetricJob.objects.create(
            collection=self.collection,
            access_date=date(2012, 3, 10),
            status=DailyMetricJob.STATUS_ERROR,
            input_log_hashes=[log_file.hash],
            storage_path="books/2012/03/2012-03-10.json",
            payload_hash="abc",
            summary={"month_document_count": 1},
        )

        services.create_or_update_daily_metric_job(
            collection=self.collection,
            access_date=date(2012, 3, 10),
            log_files=[log_file],
        )

        job.refresh_from_db()
        self.assertEqual(job.status, DailyMetricJob.STATUS_PENDING)
        self.assertEqual(job.storage_path, "books/2012/03/2012-03-10.json")
        self.assertEqual(job.payload_hash, "abc")
        self.assertEqual(job.summary, {"month_document_count": 1})

    def test_create_or_update_clears_stale_payload_when_inputs_change_before_success(self):
        first = self._log_file("1" * 32)
        second = self._log_file("2" * 32)
        job = DailyMetricJob.objects.create(
            collection=self.collection,
            access_date=date(2012, 3, 10),
            status=DailyMetricJob.STATUS_ERROR,
            input_log_hashes=[first.hash],
            storage_path="books/2012/03/2012-03-10.json",
            payload_hash="abc",
            summary={"month_document_count": 1},
        )

        services.create_or_update_daily_metric_job(
            collection=self.collection,
            access_date=date(2012, 3, 10),
            log_files=[first, second],
        )

        job.refresh_from_db()
        self.assertEqual(job.input_log_hashes, sorted([first.hash, second.hash]))
        self.assertEqual(job.storage_path, "")
        self.assertEqual(job.payload_hash, "")
        self.assertEqual(job.summary, {})

    def test_release_stale_daily_metric_jobs_marks_logs_for_retry(self):
        log_file = self._log_file("1" * 32, status=choices.LOG_FILE_STATUS_PARSING)
        DailyMetricJob.objects.create(
            collection=self.collection,
            access_date=date(2012, 3, 10),
            status=DailyMetricJob.STATUS_EXPORTING,
            input_log_hashes=[log_file.hash],
            export_started_at=timezone.now() - timedelta(minutes=120),
        )

        released = services.release_stale_daily_metric_jobs(stale_after_minutes=60)

        log_file.refresh_from_db()
        self.assertEqual(released, 1)
        self.assertEqual(log_file.status, choices.LOG_FILE_STATUS_ERROR)
        self.assertIsNone(log_file.parse_heartbeat_at)

    def test_process_line_discards_invalid_local_datetime_without_raising(self):
        class FakeUtm:
            def translate(self, url):
                return {
                    "book_id": "q7gtd",
                    "pid_generic": "book:q7gtd",
                    "media_language": "en",
                    "media_format": MEDIA_FORMAT_HTML,
                    "content_type": CONTENT_TYPE_FULL_TEXT,
                }

        log_file = self._log_file("1" * 32)
        results = {}

        is_valid, error = services.process_line(
            results=results,
            line={
                "url": "/id/q7gtd",
                "client_name": "browser",
                "client_version": "1.0",
                "ip_address": "127.0.0.1",
                "country_code": "BR",
                "local_datetime": None,
            },
            utm=FakeUtm(),
            log_file=log_file,
        )

        self.assertFalse(is_valid)
        self.assertIsNone(error)
        self.assertEqual(results, {})

    def test_mark_daily_metric_job_exported_records_updated_by(self):
        user = get_user_model().objects.create_user(
            username="tester",
            email="tester@example.org",
            password="secret",
        )
        job = DailyMetricJob.objects.create(
            collection=self.collection,
            access_date=date(2012, 3, 10),
            status=DailyMetricJob.STATUS_EXPORTING,
        )

        services.mark_daily_metric_job_exported(job, user=user)

        job.refresh_from_db()
        self.assertEqual(job.status, DailyMetricJob.STATUS_EXPORTED)
        self.assertIsNotNone(job.exported_at)
