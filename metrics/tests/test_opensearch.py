from unittest import TestCase
from unittest.mock import Mock, patch

from django.test import override_settings

from metrics import opensearch


class OpenSearchUsageClientTests(TestCase):
    @patch.object(opensearch.OpenSearchUsageClient, "get_opensearch_client")
    def test_create_index_sends_mappings_in_request_body(self, mock_get_client):
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        client = opensearch.OpenSearchUsageClient(url="https://example.org:9200")
        client.create_index(
            index_name="usage_monthly_books_202506",
            mappings=opensearch.MONTH_INDEX_MAPPINGS,
        )

        mock_client.indices.create.assert_called_once_with(
            index="usage_monthly_books_202506",
            body={
                "settings": {"index": {"number_of_replicas": 0}},
                "mappings": opensearch.MONTH_INDEX_MAPPINGS,
            },
        )

    @override_settings(
        OPENSEARCH_VERIFY_CERTS=True,
        OPENSEARCH_BASIC_AUTH=None,
        OPENSEARCH_API_KEY=None,
    )
    @patch("metrics.opensearch.client.OpenSearch")
    def test_verify_certs_false_explicitly_overrides_settings(self, mock_opensearch):
        opensearch.OpenSearchUsageClient(
            url="https://example.org:9200",
            verify_certs=False,
        )

        mock_opensearch.assert_called_once_with(
            "https://example.org:9200",
            verify_certs=False,
        )

    def test_get_index_mappings_returns_books_specific_mappings(self):
        self.assertIs(
            opensearch.get_index_mappings("books", "month"),
            opensearch.BOOKS_MONTH_INDEX_MAPPINGS,
        )
        self.assertIs(
            opensearch.get_index_mappings("books", "year"),
            opensearch.BOOKS_YEAR_INDEX_MAPPINGS,
        )
        self.assertIn("counter", opensearch.BOOKS_MONTH_INDEX_MAPPINGS["properties"])
        self.assertIn("access", opensearch.BOOKS_YEAR_INDEX_MAPPINGS["properties"])
        self.assertIn(
            "applied_jobs", opensearch.BOOKS_MONTH_INDEX_MAPPINGS["properties"]
        )
        for mappings in (
            opensearch.MONTH_INDEX_MAPPINGS,
            opensearch.YEAR_INDEX_MAPPINGS,
            opensearch.BOOKS_MONTH_INDEX_MAPPINGS,
            opensearch.BOOKS_YEAR_INDEX_MAPPINGS,
        ):
            for removed_field in (
                "document_type",
                "scielo_document_type",
                "pid",
                "pid_v2",
                "pid_v3",
                "pid_generic",
                "title_pid_generic",
                "counter_data_type",
                "access_month",
                "access_year",
            ):
                self.assertNotIn(removed_field, mappings["properties"])
            document_mapping = mappings["properties"]["document"]
            self.assertEqual(document_mapping["properties"]["id"]["type"], "keyword")
            self.assertEqual(document_mapping["properties"]["title"]["type"], "text")
            self.assertEqual(
                document_mapping["properties"]["title"]["fields"]["keyword"]["type"],
                "keyword",
            )
            self.assertEqual(
                mappings["properties"]["source"]["properties"]["id"]["type"],
                "keyword",
            )

    @patch("metrics.opensearch.client.helpers.bulk")
    @patch.object(opensearch.OpenSearchUsageClient, "get_opensearch_client")
    def test_increment_documents_for_daily_job_uses_applied_jobs(
        self,
        mock_get_client,
        mock_bulk,
    ):
        mock_get_client.return_value = Mock()
        client = opensearch.OpenSearchUsageClient(url="https://example.org:9200")

        client.increment_documents_for_daily_job(
            index_name="usage_monthly_books_202506",
            documents={
                "doc-1": {
                    "collection": "books",
                    "document": {"id": "BOOK:WD"},
                    "access": {"month": "2025-06"},
                    "total_requests": 3,
                    "total_investigations": 4,
                    "unique_requests": 2,
                    "unique_investigations": 3,
                }
            },
            job_id="books|2025-06-03|abc123",
        )

        actions = list(mock_bulk.call_args.args[1])
        self.assertEqual(len(actions), 1)
        action = actions[0]
        self.assertEqual(action["_op_type"], "update")
        self.assertEqual(
            action["script"]["params"]["job_id"], "books|2025-06-03|abc123"
        )
        self.assertEqual(action["upsert"], {"applied_jobs": []})
