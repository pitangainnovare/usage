import logging
from datetime import timedelta
from time import monotonic

from django.conf import settings
from django.utils import timezone

from scielo_usage_counter import log_handler, url_translator

from log_manager import choices
from log_manager.models import LogFile
from log_manager_config.models import CollectionLogDirectory
from source.models import Source
from document.models import Document
from tracker.choices import (
    LOG_FILE_DISCARDED_LINE_REASON_MISSING_DOCUMENT,
    LOG_FILE_DISCARDED_LINE_REASON_MISSING_SOURCE,
)
from tracker.models import LogFileDiscardedLine

from metrics.counter import access, documents as index_docs
from metrics.counter import parser

from .resources import get_log_files_for_collection_date
from . import daily_payloads


def process_daily_metric_job(job, robots_list, mmdb, track_errors=False):
    log_files = get_log_files_for_collection_date(
        collection=job.collection,
        access_date=job.access_date,
    )
    if not log_files:
        raise RuntimeError(f"No log files found for {job.collection.acron3} {job.access_date}.")

    results = {}
    summary = {
        "log_files": len(log_files),
        "input_log_hashes": sorted(log_file.hash for log_file in log_files if log_file.hash),
        "lines_parsed": 0,
        "valid_lines": 0,
        "discarded_lines": 0,
    }

    LogFile.objects.filter(pk__in=[log_file.pk for log_file in log_files]).update(
        status=choices.LOG_FILE_STATUS_PARSING,
        summary={},
        last_processed_line=0,
        parse_heartbeat_at=timezone.now(),
        updated=timezone.now(),
    )
    LogFileDiscardedLine.objects.filter(log_file_id__in=[log_file.pk for log_file in log_files]).delete()

    heartbeat_interval_seconds = getattr(settings, "METRICS_PARSE_HEARTBEAT_INTERVAL_SECONDS", 30)

    for log_file in log_files:
        log_parser, url_translator_manager = setup_parsing_environment(
            log_file=log_file,
            robots_list=robots_list,
            mmdb=mmdb,
        )
        line_count = 0
        valid_count = 0
        errors = []
        last_heartbeat_monotonic = monotonic()

        for line in log_parser.parse():
            line_count += 1
            if monotonic() - last_heartbeat_monotonic >= heartbeat_interval_seconds:
                touch_parse_heartbeat(log_file, log_parser.stats.lines_parsed)
                last_heartbeat_monotonic = monotonic()

            is_valid_line, error_obj = process_line(
                results=results,
                line=line,
                utm=url_translator_manager,
                log_file=log_file,
                track_errors=track_errors,
            )
            if is_valid_line:
                valid_count += 1
            else:
                summary["discarded_lines"] += 1
                if error_obj:
                    errors.append(error_obj)

        if errors:
            LogFileDiscardedLine.objects.bulk_create(errors)

        summary["lines_parsed"] += line_count
        summary["valid_lines"] += valid_count
        log_file.summary = {
            "parsing_completed": True,
            "lines_parsed": line_count,
            "valid_lines": valid_count,
        }
        log_file.last_processed_line = log_parser.stats.lines_parsed
        log_file.parse_heartbeat_at = timezone.now()
        log_file.save(
            update_fields=[
                "summary",
                "last_processed_line",
                "parse_heartbeat_at",
                "updated",
            ]
        )

    documents = index_docs.convert_raw_results_to_index_documents(results)
    storage_path = daily_payloads.build_daily_storage_path(job.collection, job.access_date)
    payload = {
        "collection": job.collection.acron3,
        "access_date": job.access_date.isoformat(),
        "input_log_hashes": summary["input_log_hashes"],
        "documents": documents,
        "summary": summary,
    }
    payload_hash = daily_payloads.write_payload(storage_path, payload)

    job.input_log_hashes = summary["input_log_hashes"]
    job.storage_path = storage_path.as_posix()
    job.payload_hash = payload_hash
    job.summary = {
        **summary,
        "month_document_count": len(documents.get("month", {})),
        "year_document_count": len(documents.get("year", {})),
    }
    job.save(
        update_fields=[
            "input_log_hashes",
            "storage_path",
            "payload_hash",
            "summary",
            "updated",
        ]
    )

    return payload


def setup_parsing_environment(log_file, robots_list, mmdb):
    lp = log_handler.LogParser(mmdb_data=mmdb.data, robots_list=robots_list, output_mode="dict")
    lp.logfile = log_file.path

    translator_class = None
    for cld in CollectionLogDirectory.objects.filter(config__collection=log_file.collection):
        if cld.path in log_file.path:
            if cld.translator_class:
                translator_class = parser.translator_class_name_to_obj(cld.translator_class)
                break

    if not translator_class:
        raise Exception(f"No URL translator class found for collection {log_file.collection}.")

    utm = url_translator.URLTranslationManager(
        documents_metadata=Document.metadata(collection=log_file.collection),
        sources_metadata=Source.metadata(collection=log_file.collection),
        translator=translator_class,
    )
    return lp, utm


def process_line(results, line, utm, log_file, track_errors=False):
    try:
        translated_url = utm.translate(line.get("url"))
    except Exception as exc:
        logging.error("Error translating URL %s: %s", line.get("url"), exc)
        return False, None

    try:
        item_access_data = access.extract_item_access_data(log_file.collection.acron3, translated_url)
    except Exception as exc:
        logging.error("Error extracting item access data from URL %s: %s", line.get("url"), exc)
        return False, None

    ignore_utm_validation = not track_errors
    is_valid, check_result = access.is_valid_item_access_data(
        item_access_data,
        utm,
        ignore_utm_validation,
    )

    if not is_valid:
        if track_errors:
            error_code = check_result.get("code")
            if error_code in {
                "invalid_scielo_issn",
                "invalid_source_id",
                "invalid_pid_v3",
                "invalid_pid_v2",
                "invalid_pid_generic",
            }:
                tracker_error_type = (
                    LOG_FILE_DISCARDED_LINE_REASON_MISSING_DOCUMENT
                    if "pid" in error_code
                    else LOG_FILE_DISCARDED_LINE_REASON_MISSING_SOURCE
                )

                return False, LogFileDiscardedLine.create(
                    log_file=log_file,
                    error_type=tracker_error_type,
                    message=check_result.get("message"),
                    data={"line": line, "item_access_data": item_access_data},
                    save=False,
                )

        return False, None

    try:
        access.update_results_with_item_access_data(results, item_access_data, line)
    except Exception as exc:
        logging.error("Error updating metrics results for URL %s: %s", line.get("url"), exc)
        return False, None

    return True, None


def touch_parse_heartbeat(log_file, last_processed_line=None):
    heartbeat_at = timezone.now()
    update_kwargs = {
        "parse_heartbeat_at": heartbeat_at,
        "updated": heartbeat_at,
    }
    if last_processed_line is not None:
        update_kwargs["last_processed_line"] = last_processed_line or 0
        log_file.last_processed_line = last_processed_line or 0
    LogFile.objects.filter(pk=log_file.pk).update(**update_kwargs)
    log_file.parse_heartbeat_at = heartbeat_at


def is_stale_parsing_log(log_file, stale_after_minutes=60):
    if log_file.status != choices.LOG_FILE_STATUS_PARSING:
        return False

    if not log_file.parse_heartbeat_at:
        return True

    cutoff = timezone.now() - timedelta(minutes=stale_after_minutes)
    return log_file.parse_heartbeat_at < cutoff


def requeue_stale_parsing_log(log_file):
    now = timezone.now()
    LogFile.objects.filter(pk=log_file.pk).update(
        status=choices.LOG_FILE_STATUS_ERROR,
        parse_heartbeat_at=None,
        updated=now,
    )
    log_file.status = choices.LOG_FILE_STATUS_ERROR
    log_file.parse_heartbeat_at = None
