from unittest.mock import patch

from django.db import IntegrityError
from django.test import TestCase

from collection.models import Collection

from . import choices, tasks
from .models import LogFile


class LogFileTests(TestCase):
    def setUp(self):
        self.collection = Collection.objects.create(acron3="books", acron2="bk")

    def test_create_or_update_creates_log_file(self):
        log_file = LogFile.create_or_update(
            collection=self.collection,
            path="/tmp/new.log.gz",
            stat_result={"size": 10},
            hash="1" * 32,
        )

        self.assertEqual(log_file.collection, self.collection)
        self.assertEqual(log_file.path, "/tmp/new.log.gz")
        self.assertEqual(log_file.status, choices.LOG_FILE_STATUS_CREATED)

    def test_create_or_update_refetches_existing_log_after_integrity_error(self):
        existing = LogFile.objects.create(
            collection=self.collection,
            path="/tmp/existing.log.gz",
            stat_result={"size": 10},
            hash="1" * 32,
            status=choices.LOG_FILE_STATUS_CREATED,
        )

        with patch.object(LogFile.objects, "get_or_create", side_effect=IntegrityError):
            log_file = LogFile.create_or_update(
                collection=self.collection,
                path="/tmp/existing.log.gz",
                stat_result={"size": 10},
                hash=existing.hash,
            )

        self.assertEqual(log_file.pk, existing.pk)


class ValidateLogFilesTaskTests(TestCase):
    def test_validate_log_files_returns_for_empty_visible_date_range(self):
        with patch("log_manager.tasks.task_validate_log_file.s") as mocked_signature:
            result = tasks.task_validate_log_files.run(
                collections=["books"],
                from_date="2024-02-02",
                until_date="2024-02-01",
            )

        self.assertIsNone(result)
        mocked_signature.assert_not_called()
