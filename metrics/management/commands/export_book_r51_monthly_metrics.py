import csv
import json
from collections import defaultdict
from pathlib import Path

from device_detector import DeviceDetector
from django.core.management.base import BaseCommand, CommandError

from collection.models import Collection
from document.models import Document
from metrics.counter import access, documents as index_docs
from resources.models import MMDB, RobotUserAgent
from scielo_usage_counter import log_handler, url_translator
from scielo_usage_counter.translator.books import URLTranslatorBooksSite
from source.models import Source


class Command(BaseCommand):
    help = (
        "Generate COUNTER R5.1 monthly book metrics from one or more log files, "
        "writing item and title CSV outputs."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--input",
            dest="inputs",
            action="append",
            required=True,
            help="Input log file path. Repeat --input for multiple files.",
        )
        parser.add_argument(
            "--item-output",
            required=True,
            help="Output CSV path for item-level monthly metrics.",
        )
        parser.add_argument(
            "--title-output",
            required=True,
            help="Output CSV path for title-level monthly metrics.",
        )
        parser.add_argument(
            "--summary-output",
            help="Optional JSON path with parse and totals summary.",
        )
        parser.add_argument(
            "--collection",
            default="books",
            help="Collection acronym (default: books).",
        )
        parser.add_argument(
            "--robots-source",
            choices=sorted(RobotUserAgent.SOURCE_CHOICES),
            default=RobotUserAgent.SOURCE_ALL,
            help="Which active robot list to use: all, counter, or scielo.",
        )

    def handle(self, *args, **options):
        input_paths = [Path(value).expanduser() for value in options["inputs"]]
        item_output = Path(options["item_output"]).expanduser()
        title_output = Path(options["title_output"]).expanduser()
        summary_output = (
            Path(options["summary_output"]).expanduser()
            if options.get("summary_output")
            else None
        )

        for path in input_paths:
            if not path.exists():
                raise CommandError(f"Input file not found: {path}")

        collection = Collection.objects.filter(acron3=options["collection"]).first()
        if not collection:
            raise CommandError(f"Collection not found: {options['collection']}")

        robots_source = options["robots_source"]
        robots_list = RobotUserAgent.get_patterns(source=robots_source)
        if not robots_list:
            raise CommandError(
                f"No robot user agents found in database for source {robots_source}."
            )

        mmdb = MMDB.objects.order_by("-created").first()
        if not mmdb:
            raise CommandError("No MMDB found in database.")

        parser = log_handler.LogParser(
            mmdb_data=mmdb.data,
            robots_list=robots_list,
            output_mode="dict",
        )
        utm = url_translator.URLTranslationManager(
            documents_metadata=Document.metadata(collection=collection),
            sources_metadata=Source.metadata(collection=collection),
            translator=URLTranslatorBooksSite,
        )

        results = {}
        parse_summaries = []
        ua_cache = {}

        for path in input_paths:
            self.stdout.write(f"Processing {path}...")
            parse_summaries.append(
                self._parse_file(
                    path=path,
                    parser=parser,
                    utm=utm,
                    collection=collection,
                    ua_cache=ua_cache,
                    results=results,
                )
            )

        monthly_documents = self._build_monthly_documents(results)

        self._write_item_csv(item_output, monthly_documents["item"])
        self._write_title_csv(title_output, monthly_documents["title"])

        summary = {
            "robots_source": robots_source,
            "raw_result_count": len(results),
            "parse_summaries": parse_summaries,
            "totals": {
                "total_item_requests": sum(
                    doc.get("total_requests", 0) for doc in monthly_documents["item"]
                ),
                "total_item_investigations": sum(
                    doc.get("total_investigations", 0)
                    for doc in monthly_documents["item"]
                ),
                "unique_item_requests": sum(
                    doc.get("unique_requests", 0) for doc in monthly_documents["item"]
                ),
                "unique_item_investigations": sum(
                    doc.get("unique_investigations", 0)
                    for doc in monthly_documents["item"]
                ),
                "title_total_item_requests": sum(
                    doc.get("total_requests", 0) for doc in monthly_documents["title"]
                ),
                "title_total_item_investigations": sum(
                    doc.get("total_investigations", 0)
                    for doc in monthly_documents["title"]
                ),
                "unique_title_requests": sum(
                    doc.get("unique_requests", 0) for doc in monthly_documents["title"]
                ),
                "unique_title_investigations": sum(
                    doc.get("unique_investigations", 0)
                    for doc in monthly_documents["title"]
                ),
            },
        }

        if summary_output:
            summary_output.parent.mkdir(parents=True, exist_ok=True)
            summary_output.write_text(json.dumps(summary, indent=2, sort_keys=True))

        self.stdout.write(self.style.SUCCESS(f"Item CSV written to {item_output}"))
        self.stdout.write(self.style.SUCCESS(f"Title CSV written to {title_output}"))
        if summary_output:
            self.stdout.write(self.style.SUCCESS(f"Summary JSON written to {summary_output}"))

    def _parse_file(self, path, parser, utm, collection, ua_cache, results):
        stats = defaultdict(int)
        imported = 0

        with path.open("rb") as fh:
            for raw_line in fh:
                stats["lines_parsed"] += 1

                try:
                    line = raw_line.decode().strip()
                except UnicodeDecodeError:
                    line = raw_line.decode("utf-8", errors="ignore").strip()

                match, ip_value = parser.match_with_best_pattern(line)
                if not match:
                    stats["total_ignored_lines"] += 1
                    continue

                data = match.groupdict()
                is_bunny = "unix_ts" in data
                method = "GET" if is_bunny else data.get("method")
                status = data.get("status")
                user_agent = parser.format_user_agent(data.get("user_agent"))
                url = data.get("path")
                ip_address = ip_value

                if not parser.has_valid_method(method):
                    stats["ignored_lines_invalid_method"] += 1
                    stats["total_ignored_lines"] += 1
                    continue

                if not parser.has_valid_status(status):
                    if parser.status_is_redirect(status):
                        stats["ignored_lines_http_redirects"] += 1
                    elif parser.status_is_error(status):
                        stats["ignored_lines_http_errors"] += 1
                    stats["total_ignored_lines"] += 1
                    continue

                if parser.user_agent_is_bot(user_agent):
                    stats["ignored_lines_bot"] += 1
                    stats["total_ignored_lines"] += 1
                    continue

                if not parser.has_supported_url(url):
                    stats["ignored_lines_static_resources"] += 1
                    stats["total_ignored_lines"] += 1
                    continue

                if is_bunny:
                    local_datetime = parser.format_date(data.get("unix_ts"), None)
                    country_code = data.get("country") or parser.geoip.ip_to_country_code(
                        ip_address
                    )
                else:
                    local_datetime = parser.format_date(data.get("date"), data.get("timezone"))
                    country_code = parser.geoip.ip_to_country_code(ip_address)

                if not local_datetime:
                    stats["ignored_lines_invalid_local_datetime"] += 1
                    stats["total_ignored_lines"] += 1
                    continue

                if not country_code:
                    stats["ignored_lines_invalid_country_code"] += 1
                    stats["total_ignored_lines"] += 1
                    continue

                device = ua_cache.get(user_agent)
                if device is None:
                    try:
                        device = DeviceDetector(user_agent).parse()
                    except ZeroDivisionError:
                        stats["ignored_lines_invalid_user_agent"] += 1
                        stats["total_ignored_lines"] += 1
                        ua_cache[user_agent] = False
                        continue
                    ua_cache[user_agent] = device
                elif device is False:
                    stats["ignored_lines_invalid_user_agent"] += 1
                    stats["total_ignored_lines"] += 1
                    continue

                client_name = parser.format_client_name(device)
                client_version = parser.format_client_version(device)

                if not client_name:
                    stats["ignored_lines_invalid_client_name"] += 1
                    stats["total_ignored_lines"] += 1
                    continue

                if not client_version:
                    stats["ignored_lines_invalid_client_version"] += 1
                    stats["total_ignored_lines"] += 1
                    continue

                translated = utm.translate(url)
                item_access_data = access.extract_item_access_data(
                    collection.acron3,
                    translated,
                )
                is_valid, _ = access.is_valid_item_access_data(
                    item_access_data,
                    utm,
                    ignore_utm_validation=True,
                )
                if not is_valid:
                    stats["total_ignored_lines"] += 1
                    continue

                access.update_results_with_item_access_data(
                    results,
                    item_access_data,
                    {
                        "client_name": client_name,
                        "client_version": client_version,
                        "ip_address": ip_address,
                        "country_code": country_code,
                        "local_datetime": local_datetime,
                        "url": url,
                    },
                )
                imported += 1
                stats["total_imported_lines"] += 1

        return {"path": str(path), "valid_lines_used": imported, **stats}

    def _build_monthly_documents(self, results):
        documents = index_docs.convert_raw_results_to_index_documents(results)
        item_documents = {}
        title_documents = {}

        for doc in documents["month"].values():
            year_month = doc.get("access_month", "")
            scope = doc.get("metric_scope", "item")
            if scope == "title":
                key = (
                    year_month,
                    doc.get("title_pid_generic") or doc.get("pid_generic"),
                    doc.get("document_type"),
                )
                if key not in title_documents:
                    title_documents[key] = {
                        "year_month": year_month,
                        "title_pid_generic": doc.get("title_pid_generic")
                        or doc.get("pid_generic"),
                        "document_type": doc.get("document_type"),
                        "total_requests": 0,
                        "total_investigations": 0,
                        "unique_requests": 0,
                        "unique_investigations": 0,
                    }
                title_documents[key]["total_requests"] += doc.get("total_requests", 0)
                title_documents[key]["total_investigations"] += doc.get(
                    "total_investigations", 0
                )
                title_documents[key]["unique_requests"] += doc.get("unique_requests", 0)
                title_documents[key]["unique_investigations"] += doc.get(
                    "unique_investigations", 0
                )
                continue

            key = (
                year_month,
                doc.get("title_pid_generic"),
                doc.get("pid_generic"),
                doc.get("document_type"),
            )
            if key not in item_documents:
                item_documents[key] = {
                    "year_month": year_month,
                    "title_pid_generic": doc.get("title_pid_generic"),
                    "segment_pid_generic": doc.get("pid_generic"),
                    "document_type": doc.get("document_type"),
                    "total_requests": 0,
                    "total_investigations": 0,
                    "unique_requests": 0,
                    "unique_investigations": 0,
                }
            item_documents[key]["total_requests"] += doc.get("total_requests", 0)
            item_documents[key]["total_investigations"] += doc.get(
                "total_investigations", 0
            )
            item_documents[key]["unique_requests"] += doc.get("unique_requests", 0)
            item_documents[key]["unique_investigations"] += doc.get(
                "unique_investigations", 0
            )

        return {
            "item": list(item_documents.values()),
            "title": list(title_documents.values()),
        }

    @staticmethod
    def _write_item_csv(path, item_documents):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "year_month",
                    "title_pid_generic",
                    "segment_pid_generic",
                    "document_type",
                    "total_item_requests",
                    "total_item_investigations",
                    "unique_item_requests",
                    "unique_item_investigations",
                ],
            )
            writer.writeheader()
            for doc in sorted(
                item_documents,
                key=lambda item: (
                    item.get("year_month", ""),
                    item.get("title_pid_generic") or "",
                    item.get("segment_pid_generic") or "",
                ),
            ):
                writer.writerow(
                    {
                        "year_month": doc.get("year_month", ""),
                        "title_pid_generic": doc.get("title_pid_generic"),
                        "segment_pid_generic": doc.get("segment_pid_generic"),
                        "document_type": doc.get("document_type"),
                        "total_item_requests": doc.get("total_requests", 0),
                        "total_item_investigations": doc.get("total_investigations", 0),
                        "unique_item_requests": doc.get("unique_requests", 0),
                        "unique_item_investigations": doc.get("unique_investigations", 0),
                    }
                )

    @staticmethod
    def _write_title_csv(path, title_documents):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "year_month",
                    "title_pid_generic",
                    "document_type",
                    "total_item_requests",
                    "total_item_investigations",
                    "unique_title_requests",
                    "unique_title_investigations",
                ],
            )
            writer.writeheader()
            for doc in sorted(
                title_documents,
                key=lambda item: (
                    item.get("year_month", ""),
                    item.get("title_pid_generic") or "",
                ),
            ):
                writer.writerow(
                    {
                        "year_month": doc.get("year_month", ""),
                        "title_pid_generic": doc.get("title_pid_generic"),
                        "document_type": doc.get("document_type"),
                        "total_item_requests": doc.get("total_requests", 0),
                        "total_item_investigations": doc.get("total_investigations", 0),
                        "unique_title_requests": doc.get("unique_requests", 0),
                        "unique_title_investigations": doc.get("unique_investigations", 0),
                    }
                )
