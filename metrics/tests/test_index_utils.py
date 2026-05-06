import csv
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from scielo_usage_counter.values import (
    CONTENT_TYPE_ABSTRACT,
    CONTENT_TYPE_FULL_TEXT,
    CONTENT_TYPE_UNDEFINED,
    DEFAULT_SCIELO_ISSN,
    MEDIA_FORMAT_HTML,
    MEDIA_FORMAT_PDF,
    MEDIA_FORMAT_UNDEFINED,
)

from metrics.counter import access
from metrics.counter import documents as index_docs
from metrics.opensearch.names import generate_month_index_name, generate_year_index_name


class TestIndexUtils(unittest.TestCase):
    def test_is_valid_item_access_data_valid(self):
        data = {
            "scielo_issn": "1234-5678",
            "pid_v2": "S0102-67202020000100001",
            "pid_v3": "jGJccQ7bFdbz6wy3nfXGVdv",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_PDF,
            "content_type": CONTENT_TYPE_FULL_TEXT,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertTrue(result)

    def test_is_valid_item_access_data_missing_scielo_issn(self):
        data = {
            "scielo_issn": "",
            "pid_v2": "S0102-67202020000100001",
            "pid_v3": "jGJccQ7bFdbz6wy3nfXGVdv",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_PDF,
            "content_type": CONTENT_TYPE_FULL_TEXT,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertFalse(result)

    def test_is_valid_item_access_data_valid_book_source(self):
        data = {
            "source_type": "book",
            "source_id": "q7gtd",
            "scielo_issn": DEFAULT_SCIELO_ISSN,
            "pid_generic": "BOOK:Q7GTD",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_HTML,
            "content_type": CONTENT_TYPE_FULL_TEXT,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertTrue(result)

    def test_is_valid_item_access_data_undefined_media_format(self):
        data = {
            "scielo_issn": "1234-5678",
            "pid_v2": "S0102-67202020000100001",
            "pid_v3": "jGJccQ7bFdbz6wy3nfXGVdv",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_UNDEFINED,
            "content_type": CONTENT_TYPE_FULL_TEXT,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertFalse(result)

    def test_is_valid_item_access_data_undefined_content_type(self):
        data = {
            "scielo_issn": "1234-5678",
            "pid_v2": "S0102-67202020000100001",
            "pid_v3": "jGJccQ7bFdbz6wy3nfXGVdv",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_PDF,
            "content_type": CONTENT_TYPE_UNDEFINED,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertFalse(result)

    def test_is_valid_item_access_data_missing_pid_v2_and_pid_v3(self):
        data = {
            "scielo_issn": "1234-5678",
            "pid_v2": "",
            "pid_v3": "",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_PDF,
            "content_type": CONTENT_TYPE_FULL_TEXT,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertFalse(result)

    def test_is_valid_item_access_data_media_format_html(self):
        data = {
            "scielo_issn": "1234-5678",
            "pid_v2": "S0102-67202020000100001",
            "pid_v3": "jGJccQ7bFdbz6wy3nfXGVdv",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_HTML,
            "content_type": CONTENT_TYPE_FULL_TEXT,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertTrue(result)

    def test_is_valid_item_access_data_content_type_abstract(self):
        data = {
            "scielo_issn": "1234-5678",
            "pid_v2": "S0102-67202020000100001",
            "pid_v3": "jGJccQ7bFdbz6wy3nfXGVdv",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_PDF,
            "content_type": CONTENT_TYPE_ABSTRACT,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertTrue(result)

    def test_is_valid_item_access_data_dataset_without_source_or_language_is_valid(
        self,
    ):
        data = {
            "document_type": "dataset",
            "scielo_issn": DEFAULT_SCIELO_ISSN,
            "pid_v2": None,
            "pid_v3": None,
            "pid_generic": "DOI:10.48331/SCIELODATA.JLMAIY",
            "media_language": "un",
            "media_format": MEDIA_FORMAT_HTML,
            "content_type": CONTENT_TYPE_ABSTRACT,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertTrue(result)

    def test_is_valid_item_access_data_missing_media_language_is_invalid(self):
        data = {
            "scielo_issn": "1234-5678",
            "pid_v2": "S0102-67202020000100001",
            "pid_v3": "jGJccQ7bFdbz6wy3nfXGVdv",
            "media_language": "",
            "media_format": MEDIA_FORMAT_PDF,
            "content_type": CONTENT_TYPE_FULL_TEXT,
        }
        result, _ = access.is_valid_item_access_data(data)
        self.assertFalse(result)

    def test_extract_item_access_data_normalizes_source_fields_for_journal(self):
        data = access.extract_item_access_data(
            "scl",
            {
                "scielo_issn": "1234-5678",
                "pid_v2": "S0102-67202020000100001",
                "media_language": "en",
                "media_format": MEDIA_FORMAT_PDF,
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "publication_year": "2024",
                "journal_main_title": "Journal Title",
                "journal_subject_area_capes": ["Health Sciences"],
                "journal_subject_area_wos": ["Medicine"],
                "journal_acronym": "testjou",
                "journal_publisher_name": ["SciELO"],
            },
        )

        self.assertEqual(data["source_type"], "journal")
        self.assertEqual(data["source_id"], "1234-5678")
        self.assertEqual(data["source_main_title"], "Journal Title")
        self.assertEqual(data["source_acronym"], "testjou")

    def test_extract_item_access_data_normalizes_source_fields_for_books(self):
        data = access.extract_item_access_data(
            "books",
            {
                "book_id": "q7gtd",
                "book_title": "Book Title",
                "title_pid_generic": "book:q7gtd",
                "pid_generic": "book:q7gtd/chapter:03",
                "media_language": "en",
                "media_format": MEDIA_FORMAT_HTML,
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "publication_year": "2023",
            },
        )

        self.assertEqual(data["source_type"], "book")
        self.assertEqual(data["source_id"], "q7gtd")
        self.assertEqual(data["scielo_issn"], DEFAULT_SCIELO_ISSN)
        self.assertEqual(data["source_main_title"], "Book Title")
        self.assertEqual(data["title_pid_generic"], "BOOK:Q7GTD")

    def test_extract_item_access_data_preserves_access_url_and_free_to_read(self):
        data = access.extract_item_access_data(
            "books",
            {
                "book_id": "c2248",
                "book_title": "Book Title",
                "title_pid_generic": "book:c2248",
                "pid_generic": "book:c2248",
                "media_language": "pt",
                "media_format": MEDIA_FORMAT_PDF,
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "access_url": "/id/c2248/pdf/freitas-9788599662830.pdf",
                "source_access_type": "free_to_read",
            },
        )

        self.assertEqual(data["access_url"], "/id/c2248/pdf/freitas-9788599662830.pdf")
        self.assertEqual(data["counter_access_type"], "Free_To_Read")

    def test_extract_item_access_data_tolerates_malformed_media_language(self):
        data = access.extract_item_access_data(
            "books",
            {
                "book_id": "q7gtd",
                "pid_generic": "book:q7gtd",
                "media_language": "'",
                "media_format": MEDIA_FORMAT_HTML,
                "content_type": CONTENT_TYPE_FULL_TEXT,
            },
        )

        self.assertEqual(data["media_language"], "un")

    def test_extract_item_access_data_sets_document_title_by_type(self):
        chapter = access.extract_item_access_data(
            "books",
            {
                "book_id": "q7gtd",
                "chapter_id": "03",
                "pid_generic": "book:q7gtd/chapter:03",
                "book_title": "Book Title",
                "chapter_title": "Chapter Title",
                "media_format": MEDIA_FORMAT_HTML,
                "media_language": "en",
                "content_type": CONTENT_TYPE_FULL_TEXT,
            },
        )
        book = access.extract_item_access_data(
            "books",
            {
                "book_id": "q7gtd",
                "pid_generic": "book:q7gtd",
                "book_title": "Book Title",
                "media_format": MEDIA_FORMAT_HTML,
                "media_language": "en",
                "content_type": CONTENT_TYPE_FULL_TEXT,
            },
        )
        article = access.extract_item_access_data(
            "scl",
            {
                "scielo_issn": "1234-5678",
                "pid_v3": "jGJccQ7bFdbz6wy3nfXGVdv",
                "article_title": "Article Title",
                "media_format": MEDIA_FORMAT_HTML,
                "content_type": CONTENT_TYPE_FULL_TEXT,
            },
        )

        self.assertEqual(chapter["document_title"], "Chapter Title")
        self.assertEqual(book["document_title"], "Book Title")
        self.assertEqual(article["document_title"], "Article Title")

    def test_extract_item_access_data_normalizes_scielo_collection_document_types(self):
        preprint = access.extract_item_access_data(
            "preprints",
            {
                "pid_generic": "10.1590/SciELOPreprints.1234",
                "media_format": MEDIA_FORMAT_HTML,
                "content_type": CONTENT_TYPE_FULL_TEXT,
            },
        )
        dataset = access.extract_item_access_data(
            "data",
            {
                "pid_generic": "10.48331/scielodata.abc123",
                "media_format": MEDIA_FORMAT_HTML,
                "content_type": CONTENT_TYPE_ABSTRACT,
            },
        )
        article = access.extract_item_access_data(
            "scl",
            {
                "scielo_issn": "1234-5678",
                "pid_v3": "jGJccQ7bFdbz6wy3nfXGVdv",
                "media_format": MEDIA_FORMAT_HTML,
                "content_type": CONTENT_TYPE_FULL_TEXT,
            },
        )

        self.assertEqual(preprint["source_type"], "preprint_server")
        self.assertEqual(preprint["document_type"], "preprint")
        self.assertEqual(dataset["source_type"], "data_repository")
        self.assertEqual(dataset["document_type"], "dataset")
        self.assertEqual(article["source_type"], "journal")
        self.assertEqual(article["document_type"], "article")

    def test_update_results_with_item_access_data_stores_source_and_periods(self):
        results = {}
        item_access_data = {
            "collection": "books",
            "source_type": "book",
            "source_id": "q7gtd",
            "scielo_issn": DEFAULT_SCIELO_ISSN,
            "pid_v2": None,
            "pid_v3": None,
            "pid_generic": "BOOK:Q7GTD",
            "title_pid_generic": "BOOK:Q7GTD",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_HTML,
            "content_type": CONTENT_TYPE_FULL_TEXT,
            "publication_year": "2023",
            "document_title": "Book Title",
            "source_main_title": "Book Title",
            "source_subject_area_capes": [],
            "source_subject_area_wos": [],
            "source_acronym": None,
            "source_publisher_name": ["SciELO Books"],
        }
        line = {
            "client_name": "browser",
            "client_version": "1.0",
            "ip_address": "127.0.0.1",
            "country_code": "BR",
            "local_datetime": datetime(2024, 1, 15, 10, 0, 5),
        }

        access.update_results_with_item_access_data(results, item_access_data, line)

        self.assertEqual(len(results), 1)
        result = next(iter(results.values()))
        self.assertEqual(result["source"]["source_type"], "book")
        self.assertEqual(result["source"]["source_id"], "q7gtd")
        self.assertEqual(result["source"]["main_title"], "Book Title")
        self.assertEqual(result["access_date"], "2024-01-15")
        self.assertEqual(result["access_month"], "202401")
        self.assertEqual(result["access_year"], "2024")
        self.assertEqual(result["access_country_code"], "BR")
        self.assertEqual(result["content_language"], "en")
        self.assertEqual(result["title_pid_generic"], "BOOK:Q7GTD")
        self.assertEqual(result["document"], {"title": "Book Title"})
        self.assertIn("user_session_id", result)

    def test_update_results_with_item_access_data_rejects_invalid_local_datetime(self):
        results = {}
        item_access_data = {
            "collection": "books",
            "source_type": "book",
            "source_id": "q7gtd",
            "scielo_issn": DEFAULT_SCIELO_ISSN,
            "pid_generic": "BOOK:Q7GTD",
            "media_language": "en",
            "media_format": MEDIA_FORMAT_HTML,
            "content_type": CONTENT_TYPE_FULL_TEXT,
        }
        line = {
            "client_name": "browser",
            "client_version": "1.0",
            "ip_address": "127.0.0.1",
            "country_code": "BR",
            "local_datetime": None,
        }

        with self.assertRaises(ValueError):
            access.update_results_with_item_access_data(results, item_access_data, line)

        self.assertEqual(results, {})

    def test_update_results_with_item_access_data_does_not_expand_book_into_segments(
        self,
    ):
        results = {}
        item_access_data = {
            "collection": "books",
            "source_type": "book",
            "source_id": "c2248",
            "scielo_issn": DEFAULT_SCIELO_ISSN,
            "pid_v2": None,
            "pid_v3": None,
            "pid_generic": "BOOK:C2248",
            "title_pid_generic": "BOOK:C2248",
            "segment_pid_generics": [
                "BOOK:C2248/CHAPTER:00",
                "BOOK:C2248/CHAPTER:01",
                "BOOK:C2248/CHAPTER:02",
            ],
            "media_language": "pt",
            "media_format": MEDIA_FORMAT_PDF,
            "content_type": CONTENT_TYPE_FULL_TEXT,
            "publication_year": "2018",
            "source_main_title": "C2248 Book",
        }
        line = {
            "client_name": "browser",
            "client_version": "1.0",
            "ip_address": "127.0.0.1",
            "country_code": "BR",
            "local_datetime": datetime(2024, 1, 15, 10, 0, 5),
        }

        access.update_results_with_item_access_data(results, item_access_data, line)

        self.assertEqual(len(results), 1)
        result = list(results.values())[0]
        self.assertEqual(result["pid_generic"], "BOOK:C2248")

    def test_double_click_filter_uses_url_bucket_for_same_item(self):
        results = {}
        item_access_data = {
            "collection": "books",
            "source_type": "book",
            "source_id": "c2248",
            "scielo_issn": DEFAULT_SCIELO_ISSN,
            "pid_v2": None,
            "pid_v3": None,
            "pid_generic": "BOOK:C2248/CHAPTER:03",
            "title_pid_generic": "BOOK:C2248",
            "media_language": "pt",
            "media_format": MEDIA_FORMAT_HTML,
            "content_type": CONTENT_TYPE_FULL_TEXT,
            "publication_year": "2018",
            "source_main_title": "C2248 Book",
        }
        base_line = {
            "client_name": "browser",
            "client_version": "1.0",
            "ip_address": "127.0.0.1",
            "country_code": "BR",
        }

        access.update_results_with_item_access_data(
            results,
            item_access_data,
            {
                **base_line,
                "local_datetime": datetime(2024, 1, 15, 10, 0, 5),
                "url": "/id/c2248/03",
            },
        )
        access.update_results_with_item_access_data(
            results,
            item_access_data,
            {
                **base_line,
                "local_datetime": datetime(2024, 1, 15, 10, 0, 20),
                "url": "https://books.scielo.org/id/c2248/epub/03.html?x=1",
            },
        )

        raw = next(iter(results.values()))
        self.assertEqual(
            set(raw["click_timestamps_by_url"]),
            {"/id/c2248/03", "/id/c2248/epub/03.html"},
        )

        metrics_data = index_docs.convert_raw_results_to_index_documents(results)
        month_item = metrics_data["month"][
            "books|c2248|||BOOK:C2248/CHAPTER:03|2024-01|Open|Regular|2018"
        ]

        self.assertEqual(month_item["total_requests"], 2)
        self.assertEqual(month_item["unique_requests"], 1)

    def test_double_click_filter_collapses_same_url_within_30_seconds(self):
        results = {}
        item_access_data = {
            "collection": "books",
            "source_type": "book",
            "source_id": "c2248",
            "scielo_issn": DEFAULT_SCIELO_ISSN,
            "pid_v2": None,
            "pid_v3": None,
            "pid_generic": "BOOK:C2248/CHAPTER:03",
            "title_pid_generic": "BOOK:C2248",
            "media_language": "pt",
            "media_format": MEDIA_FORMAT_HTML,
            "content_type": CONTENT_TYPE_FULL_TEXT,
            "publication_year": "2018",
            "source_main_title": "C2248 Book",
        }
        base_line = {
            "client_name": "browser",
            "client_version": "1.0",
            "ip_address": "127.0.0.1",
            "country_code": "BR",
            "url": "/id/c2248/03?from=search",
        }

        access.update_results_with_item_access_data(
            results,
            item_access_data,
            {**base_line, "local_datetime": datetime(2024, 1, 15, 10, 0, 5)},
        )
        access.update_results_with_item_access_data(
            results,
            item_access_data,
            {**base_line, "local_datetime": datetime(2024, 1, 15, 10, 0, 20)},
        )

        raw = next(iter(results.values()))
        self.assertEqual(
            raw["click_timestamps_by_url"],
            {"/id/c2248/03": {"00:05": 1, "00:20": 1}},
        )

        metrics_data = index_docs.convert_raw_results_to_index_documents(results)
        month_item = metrics_data["month"][
            "books|c2248|||BOOK:C2248/CHAPTER:03|2024-01|Open|Regular|2018"
        ]

        self.assertEqual(month_item["total_requests"], 1)
        self.assertEqual(month_item["unique_requests"], 1)

    def test_generate_index_names_for_year_and_month(self):
        self.assertEqual(
            generate_year_index_name("usage", "scl", "2024-01-15"),
            "usage_yearly_scl_2024",
        )
        self.assertEqual(
            generate_month_index_name("usage", "scl", "2024-01-15"),
            "usage_monthly_scl_2024",
        )
        self.assertEqual(
            generate_year_index_name("usage", "books", "2024-01-15"),
            "usage_yearly_books",
        )
        self.assertEqual(
            generate_month_index_name("usage", "books", "2024-01-15"),
            "usage_monthly_books",
        )

    def test_convert_raw_results_to_index_documents_creates_month_and_year_views(self):
        data = {
            "books|q7gtd|||BOOK:Q7GTD/CHAPTER:03|browser|1.0|127.0.0.1|BR|en|html|full_text": {
                "collection": "books",
                "source_key": "q7gtd",
                "document_type": "chapter",
                "pid_v2": None,
                "pid_v3": None,
                "pid_generic": "BOOK:Q7GTD/CHAPTER:03",
                "document": {"title": "Chapter Title"},
                "title_pid_generic": "BOOK:Q7GTD",
                "user_session_id": "browser|1.0|127.0.0.1|2024-01-15|10",
                "click_timestamps": {"00:05": 1},
                "access_country_code": "BR",
                "content_language": "en",
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "access_date": "2024-01-15",
                "access_month": "202401",
                "access_year": "2024",
                "source": {
                    "source_type": "book",
                    "source_id": "q7gtd",
                    "scielo_issn": DEFAULT_SCIELO_ISSN,
                    "main_title": "Book Title",
                    "identifiers": {
                        "book_id": "q7gtd",
                        "isbn": "9788578791889",
                    },
                    "city": "Sao Paulo",
                    "country": "BR",
                    "subject_area_capes": [],
                    "subject_area_wos": [],
                    "acronym": None,
                    "publisher_name": ["SciELO Books"],
                },
                "publication_year": "2023",
            }
        }

        metrics_data = index_docs.convert_raw_results_to_index_documents(data)

        self.assertEqual(set(metrics_data.keys()), {"month", "year"})
        self.assertEqual(len(metrics_data["month"]), 2)
        self.assertEqual(len(metrics_data["year"]), 2)

        month_item = metrics_data["month"][
            "books|q7gtd|||BOOK:Q7GTD/CHAPTER:03|2024-01|Open|Regular|2023"
        ]
        self.assertEqual(month_item["access"], {"month": "2024-01"})
        self.assertIn("daily_metrics", month_item)
        self.assertNotIn("by_day", month_item)
        self.assertNotIn("access_country_code", month_item)
        self.assertNotIn("content_language", month_item)
        self.assertEqual(month_item["document"]["id"], "BOOK:Q7GTD/CHAPTER:03")
        self.assertEqual(month_item["document"]["type"], "chapter")
        self.assertEqual(month_item["document"]["title"], "Chapter Title")
        self.assertEqual(month_item["document"]["parent_id"], "BOOK:Q7GTD")
        self.assertEqual(month_item["document"]["publication_year"], "2023")
        self.assertEqual(month_item["document"]["identifiers"]["book_id"], "q7gtd")
        self.assertEqual(month_item["document"]["identifiers"]["chapter_id"], "03")
        self.assertEqual(month_item["document"]["identifiers"]["isbn"], "9788578791889")
        self.assertNotIn("pid_generic", month_item["document"]["identifiers"])
        self.assertEqual(month_item["counter"]["metric_scope"], "item")
        self.assertEqual(month_item["counter"]["data_type"], "Book_Segment")
        self.assertEqual(month_item["total_requests"], 1)
        self.assertEqual(month_item["unique_requests"], 1)
        self.assertNotIn("scielo_issn", month_item["source"])
        self.assertNotIn("book_id", month_item["source"]["identifiers"])
        self.assertEqual(month_item["source"]["publisher_name"], ["SciELO Books"])

        month_title = metrics_data["month"][
            "title|books|q7gtd|||BOOK:Q7GTD|2024-01|Open|Regular|2023"
        ]
        self.assertEqual(month_title["document"]["id"], "BOOK:Q7GTD")
        self.assertEqual(month_title["document"]["type"], "book")
        self.assertEqual(month_title["document"]["title"], "Book Title")
        self.assertNotIn("parent_id", month_title["document"])
        self.assertEqual(month_title["counter"]["metric_scope"], "title")
        self.assertEqual(month_title["counter"]["data_type"], "Book")
        self.assertEqual(month_title["total_requests"], 1)
        self.assertEqual(month_title["total_investigations"], 1)
        self.assertEqual(month_title["unique_requests"], 1)
        self.assertEqual(month_title["unique_investigations"], 1)

        year_item = metrics_data["year"][
            "books|q7gtd|||BOOK:Q7GTD/CHAPTER:03|en|BR|2024|Open|Regular|2023"
        ]
        self.assertEqual(
            year_item["access"],
            {"year": "2024", "country_code": "BR", "content_language": "en"},
        )
        self.assertNotIn("daily_metrics", year_item)
        self.assertNotIn("by_day", year_item)
        self.assertNotIn("access_month", year_item)
        self.assertEqual(year_item["document"]["title"], "Chapter Title")
        self.assertEqual(year_item["counter"]["metric_scope"], "item")
        self.assertEqual(year_item["total_requests"], 1)

        year_title = metrics_data["year"][
            "title|books|q7gtd|||BOOK:Q7GTD|en|BR|2024|Open|Regular|2023"
        ]
        self.assertEqual(year_title["counter"]["metric_scope"], "title")
        self.assertEqual(year_title["document"]["title"], "Book Title")
        self.assertNotIn("daily_metrics", year_title)
        self.assertNotIn("by_day", year_title)
        self.assertNotIn("access_month", year_title)
        self.assertEqual(year_title["total_requests"], 1)
        self.assertEqual(year_title["total_investigations"], 1)
        self.assertEqual(year_title["unique_requests"], 1)
        self.assertEqual(year_title["unique_investigations"], 1)

    def test_convert_raw_results_to_index_documents_maps_counter_data_types(self):
        data = {
            "preprints|scielo-preprints|||10.1590/SCIELOPREPRINTS.1234|sess|BR|un|html|full_text": {
                "collection": "preprints",
                "source_key": "scielo-preprints",
                "document_type": "preprint",
                "pid_generic": "10.1590/SCIELOPREPRINTS.1234",
                "user_session_id": "browser|1.0|127.0.0.1|2024-01-15|10",
                "click_timestamps": {"00:05": 1},
                "access_country_code": "BR",
                "content_language": "un",
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "access_date": "2024-01-15",
                "access_year": "2024",
                "source": {
                    "source_type": "preprint_server",
                    "source_id": "scielo-preprints",
                    "main_title": "SciELO Preprints",
                },
                "publication_year": "2024",
            },
            "data|scielo-data|||10.48331/SCIELODATA.ABC123|sess|BR|un|html|abstract": {
                "collection": "data",
                "source_key": "scielo-data",
                "document_type": "dataset",
                "pid_generic": "10.48331/SCIELODATA.ABC123",
                "user_session_id": "browser|1.0|127.0.0.1|2024-01-15|10",
                "click_timestamps": {"00:05": 1},
                "access_country_code": "BR",
                "content_language": "un",
                "content_type": CONTENT_TYPE_ABSTRACT,
                "access_date": "2024-01-15",
                "access_year": "2024",
                "source": {
                    "source_type": "data_repository",
                    "source_id": "scielo-data",
                    "main_title": "SciELO Data",
                },
                "publication_year": "2024",
            },
        }

        metrics_data = index_docs.convert_raw_results_to_index_documents(data)
        preprint_doc = metrics_data["month"][
            "preprints|scielo-preprints|||10.1590/SCIELOPREPRINTS.1234|2024-01|Open|Regular|2024"
        ]
        dataset_doc = metrics_data["month"][
            "data|scielo-data|||10.48331/SCIELODATA.ABC123|2024-01|Open|Regular|2024"
        ]

        self.assertEqual(preprint_doc["counter"]["data_type"], "Article")
        self.assertEqual(preprint_doc["document"]["type"], "preprint")
        self.assertEqual(preprint_doc["document"]["id"], "10.1590/SCIELOPREPRINTS.1234")
        self.assertNotIn("pid_generic", preprint_doc["document"].get("identifiers", {}))
        self.assertNotIn("scielo_document_type", preprint_doc)
        self.assertEqual(preprint_doc["counter"]["article_version"], "Preprint")
        self.assertEqual(dataset_doc["counter"]["data_type"], "Dataset")
        self.assertNotIn("article_version", dataset_doc["counter"])

    def test_convert_raw_results_to_index_documents_dedupes_book_unique_item_across_formats(
        self,
    ):
        data = {
            "books|c2248|||BOOK:C2248/CHAPTER:03|sess|BR|pt|html|full_text": {
                "collection": "books",
                "source_key": "c2248",
                "document_type": "chapter",
                "pid_v2": None,
                "pid_v3": None,
                "pid_generic": "BOOK:C2248/CHAPTER:03",
                "title_pid_generic": "BOOK:C2248",
                "user_session_id": "browser|1.0|127.0.0.1|2024-01-15|10",
                "click_timestamps": {"00:05": 1},
                "access_country_code": "BR",
                "content_language": "pt",
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "access_date": "2024-01-15",
                "access_month": "202401",
                "access_year": "2024",
                "source": {
                    "source_type": "book",
                    "source_id": "c2248",
                    "main_title": "C2248 Book",
                    "identifiers": {"book_id": "c2248", "isbn": "9788599662830"},
                    "publisher_name": ["SciELO Books"],
                },
                "publication_year": "2018",
            },
            "books|c2248|||BOOK:C2248/CHAPTER:03|sess|BR|pt|pdf|full_text": {
                "collection": "books",
                "source_key": "c2248",
                "document_type": "chapter",
                "pid_v2": None,
                "pid_v3": None,
                "pid_generic": "BOOK:C2248/CHAPTER:03",
                "title_pid_generic": "BOOK:C2248",
                "user_session_id": "browser|1.0|127.0.0.1|2024-01-15|10",
                "click_timestamps": {"00:45": 1},
                "access_country_code": "BR",
                "content_language": "pt",
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "access_date": "2024-01-15",
                "access_month": "202401",
                "access_year": "2024",
                "source": {
                    "source_type": "book",
                    "source_id": "c2248",
                    "main_title": "C2248 Book",
                    "identifiers": {"book_id": "c2248", "isbn": "9788599662830"},
                    "publisher_name": ["SciELO Books"],
                },
                "publication_year": "2018",
            },
        }

        metrics_data = index_docs.convert_raw_results_to_index_documents(data)

        month_item = metrics_data["month"][
            "books|c2248|||BOOK:C2248/CHAPTER:03|2024-01|Open|Regular|2018"
        ]
        month_title = metrics_data["month"][
            "title|books|c2248|||BOOK:C2248|2024-01|Open|Regular|2018"
        ]

        self.assertEqual(month_item["total_requests"], 2)
        self.assertEqual(month_item["total_investigations"], 2)
        self.assertEqual(month_item["unique_requests"], 1)
        self.assertEqual(month_item["unique_investigations"], 1)
        self.assertEqual(month_title["unique_requests"], 1)
        self.assertEqual(month_title["unique_investigations"], 1)

    def test_convert_raw_results_to_index_documents_skips_book_landing_page_from_item_scope(
        self,
    ):
        data = {
            "books|c2248|||BOOK:C2248|sess|BR|pt|html|abstract": {
                "collection": "books",
                "source_key": "c2248",
                "document_type": "book",
                "pid_v2": None,
                "pid_v3": None,
                "pid_generic": "BOOK:C2248",
                "document": {"title": "C2248 Book"},
                "title_pid_generic": "BOOK:C2248",
                "user_session_id": "browser|1.0|127.0.0.1|2024-01-15|10",
                "click_timestamps": {"00:05": 1},
                "access_country_code": "BR",
                "content_language": "pt",
                "content_type": CONTENT_TYPE_ABSTRACT,
                "access_date": "2024-01-15",
                "access_month": "202401",
                "access_year": "2024",
                "source": {
                    "source_type": "book",
                    "source_id": "c2248",
                    "main_title": "C2248 Book",
                    "identifiers": {"book_id": "c2248", "isbn": "9788599662830"},
                    "publisher_name": ["SciELO Books"],
                },
                "publication_year": "2018",
            },
        }

        metrics_data = index_docs.convert_raw_results_to_index_documents(data)

        self.assertEqual(
            set(metrics_data["month"].keys()),
            {"title|books|c2248|||BOOK:C2248|2024-01|Open|Regular|2018"},
        )
        self.assertEqual(
            set(metrics_data["year"].keys()),
            {"title|books|c2248|||BOOK:C2248|pt|BR|2024|Open|Regular|2018"},
        )

    def test_convert_raw_results_to_index_documents_counts_whole_book_without_segments_as_book_segment(
        self,
    ):
        data = {
            "books|c2248|||BOOK:C2248|sess|BR|pt|pdf|full_text": {
                "collection": "books",
                "source_key": "c2248",
                "document_type": "book",
                "pid_v2": None,
                "pid_v3": None,
                "pid_generic": "BOOK:C2248",
                "document": {"title": "C2248 Book"},
                "title_pid_generic": "BOOK:C2248",
                "user_session_id": "browser|1.0|127.0.0.1|2024-01-15|10",
                "click_timestamps": {"00:05": 1},
                "access_country_code": "BR",
                "content_language": "pt",
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "access_date": "2024-01-15",
                "access_month": "202401",
                "access_year": "2024",
                "source": {
                    "source_type": "book",
                    "source_id": "c2248",
                    "main_title": "C2248 Book",
                    "identifiers": {"book_id": "c2248"},
                    "publisher_name": ["SciELO Books"],
                },
                "publication_year": "2018",
            },
        }

        metrics_data = index_docs.convert_raw_results_to_index_documents(data)
        month_item = metrics_data["month"][
            "books|c2248|||BOOK:C2248|2024-01|Open|Regular|2018"
        ]
        month_title = metrics_data["month"][
            "title|books|c2248|||BOOK:C2248|2024-01|Open|Regular|2018"
        ]

        self.assertEqual(month_item["counter"]["data_type"], "Book_Segment")
        self.assertEqual(month_item["counter"]["metric_scope"], "item")
        self.assertEqual(month_item["document"]["id"], "BOOK:C2248")
        self.assertEqual(month_item["document"]["title"], "C2248 Book")
        self.assertNotIn("parent_id", month_item["document"])
        self.assertEqual(month_title["counter"]["data_type"], "Book")
        self.assertEqual(month_title["counter"]["metric_scope"], "title")
        self.assertEqual(month_title["document"]["id"], "BOOK:C2248")
        self.assertEqual(month_title["document"]["title"], "C2248 Book")

    def test_convert_raw_results_aggregates_multiple_chapters_correctly(self):
        """Test that accessing multiple chapters creates correct title-level totals"""
        data = {
            "books|q7gtd|||BOOK:Q7GTD/CHAPTER:01|session1|BR|en|html|full_text": {
                "collection": "books",
                "source_key": "q7gtd",
                "document_type": "chapter",
                "pid_generic": "BOOK:Q7GTD/CHAPTER:01",
                "title_pid_generic": "BOOK:Q7GTD",
                "user_session_id": "session1",
                "click_timestamps": {"00:05": 1},
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "access_date": "2024-01-15",
                "access_year": "2024",
                "source": {
                    "source_type": "book",
                    "source_id": "q7gtd",
                    "scielo_issn": DEFAULT_SCIELO_ISSN,
                    "main_title": "Book Title",
                    "identifiers": {"book_id": "q7gtd"},
                    "publisher_name": ["SciELO Books"],
                },
                "publication_year": "2023",
            },
            "books|q7gtd|||BOOK:Q7GTD/CHAPTER:02|session1|BR|en|html|full_text": {
                "collection": "books",
                "source_key": "q7gtd",
                "document_type": "chapter",
                "pid_generic": "BOOK:Q7GTD/CHAPTER:02",
                "title_pid_generic": "BOOK:Q7GTD",
                "user_session_id": "session1",  # SAME SESSION
                "click_timestamps": {"00:10": 1},
                "content_type": CONTENT_TYPE_FULL_TEXT,
                "access_date": "2024-01-15",
                "access_year": "2024",
                "source": {
                    "source_type": "book",
                    "source_id": "q7gtd",
                    "scielo_issn": DEFAULT_SCIELO_ISSN,
                    "main_title": "Book Title",
                    "identifiers": {"book_id": "q7gtd"},
                    "publisher_name": ["SciELO Books"],
                },
                "publication_year": "2023",
            },
        }

        metrics_data = index_docs.convert_raw_results_to_index_documents(data)

        # Should have 2 item documents (one per chapter) + 2 title documents (month and year)
        self.assertEqual(len(metrics_data["month"]), 3)  # 2 items + 1 title
        self.assertEqual(len(metrics_data["year"]), 3)  # 2 items + 1 title

        # Each item should have total=1, unique=1
        month_item_1 = metrics_data["month"][
            "books|q7gtd|||BOOK:Q7GTD/CHAPTER:01|2024-01|Open|Regular|2023"
        ]
        self.assertEqual(month_item_1["total_requests"], 1)
        self.assertEqual(month_item_1["unique_requests"], 1)

        month_item_2 = metrics_data["month"][
            "books|q7gtd|||BOOK:Q7GTD/CHAPTER:02|2024-01|Open|Regular|2023"
        ]
        self.assertEqual(month_item_2["total_requests"], 1)
        self.assertEqual(month_item_2["unique_requests"], 1)

        # Title should have total=2 (sum of both chapters)
        # Title unique should be 1 (same session accessed book, counted once)
        month_title = metrics_data["month"][
            "title|books|q7gtd|||BOOK:Q7GTD|2024-01|Open|Regular|2023"
        ]
        self.assertEqual(month_title["total_requests"], 2)
        self.assertEqual(month_title["total_investigations"], 2)
        self.assertEqual(month_title["unique_requests"], 1)
        self.assertEqual(month_title["unique_investigations"], 1)

    def test_export_book_r51_monthly_metrics_writes_counter_title_columns(self):
        from metrics.management.commands.export_book_r51_monthly_metrics import Command

        command = Command()
        monthly_documents = command._build_monthly_documents(
            {
                "books|c2248|||BOOK:C2248/CHAPTER:03|sess|BR|pt|pdf|full_text": {
                    "collection": "books",
                    "source_key": "c2248",
                    "document_type": "chapter",
                    "pid_v2": None,
                    "pid_v3": None,
                    "pid_generic": "BOOK:C2248/CHAPTER:03",
                    "title_pid_generic": "BOOK:C2248",
                    "user_session_id": "browser|1.0|127.0.0.1|2024-01-15|10",
                    "click_timestamps": {"00:05": 1},
                    "access_country_code": "BR",
                    "content_language": "pt",
                    "content_type": CONTENT_TYPE_FULL_TEXT,
                    "access_date": "2024-01-15",
                    "access_year": "2024",
                    "source": {
                        "source_type": "book",
                        "source_id": "c2248",
                        "main_title": "C2248 Book",
                        "identifiers": {"book_id": "c2248"},
                        "publisher_name": ["SciELO Books"],
                    },
                    "publication_year": "2018",
                }
            }
        )

        with TemporaryDirectory() as tmpdir:
            title_path = Path(tmpdir) / "title.csv"
            command._write_title_csv(title_path, monthly_documents["title"])

            with title_path.open(newline="") as fh:
                reader = csv.DictReader(fh)
                rows = list(reader)

        self.assertEqual(
            reader.fieldnames,
            [
                "year_month",
                "title_pid_generic",
                "document_type",
                "total_item_requests",
                "total_item_investigations",
                "unique_title_requests",
                "unique_title_investigations",
            ],
        )
        self.assertNotIn("total_title_requests", reader.fieldnames)
        self.assertEqual(rows[0]["year_month"], "2024-01")
        self.assertEqual(rows[0]["total_item_requests"], "1")
        self.assertEqual(rows[0]["unique_title_requests"], "1")
