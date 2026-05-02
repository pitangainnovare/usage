from django.test import TestCase
from unittest.mock import patch

from collection.models import Collection
from document import tasks as document_tasks
from source.services import books as source_books_service
from source.models import Source

from .models import Document
from .services import articles as article_service
from .services import books as books_service
from .services import datasets as dataset_service
from .services import preprints as preprint_service


class DocumentMetadataTests(TestCase):
    def test_metadata_includes_source_context_and_legacy_identifiers(self):
        collection = Collection.objects.create(acron3="scl", acron2="sc")
        source = Source.objects.create(
            collection=collection,
            source_type=Source.SOURCE_TYPE_JOURNAL,
            source_id="1234-5678",
            scielo_issn="1234-5678",
            title="Test Journal",
            identifiers={"scielo_issn": "1234-5678"},
        )
        Document.objects.create(
            collection=collection,
            source=source,
            document_type=Document.DOCUMENT_TYPE_ARTICLE,
            document_id="S123456782024000100001",
            scielo_issn="1234-5678",
            pid_v2="S123456782024000100001",
            pid_v3="abc123",
            title="Test Article",
            identifiers={"doi": "10.1590/example"},
            files={"pt": {"path": "/pdf/test.pdf"}},
            default_lang="en",
            text_langs=["en", "pt"],
            publication_date="2024-01-15",
            publication_year="2024",
        )

        metadata = list(Document.metadata(collection=collection))

        self.assertEqual(len(metadata), 1)
        self.assertEqual(metadata[0]["document_type"], Document.DOCUMENT_TYPE_ARTICLE)
        self.assertEqual(metadata[0]["document_id"], "S123456782024000100001")
        self.assertEqual(metadata[0]["source_type"], Source.SOURCE_TYPE_JOURNAL)
        self.assertEqual(metadata[0]["source_id"], "1234-5678")
        self.assertEqual(metadata[0]["scielo_issn"], "1234-5678")

    def test_upsert_monograph_and_part_documents_from_books_payload(self):
        collection = Collection.objects.create(acron3="books", acron2="bk")
        monograph_payload = {
            "TYPE": "Monograph",
            "id": "abcd1",
            "title": "Sample Book",
            "isbn": "9788578791889",
            "eisbn": "9788578791880",
            "doi_number": "10.1234/book",
            "language": "pt",
            "publication_date": "2024-05-20",
            "year": "2024",
            "publisher": "SciELO Books",
        }
        part_payload = {
            "TYPE": "Part",
            "id": "18",
            "monograph": "abcd1",
            "title": "Chapter 18",
            "text_language": "es",
            "order": "18",
        }

        source = source_books_service.upsert_monograph_source(
            monograph_payload,
            collection=collection,
        )
        parent_document = books_service.upsert_monograph_document(
            monograph_payload,
            collection=collection,
            source=source,
        )
        chapter = books_service.upsert_part_document(
            books_service.enrich_part_payload(part_payload, monograph_payload),
            collection=collection,
            source=source,
            parent_document=parent_document,
        )

        self.assertEqual(parent_document.document_type, Document.DOCUMENT_TYPE_BOOK)
        self.assertEqual(parent_document.document_id, "book:abcd1")
        self.assertEqual(parent_document.pid_generic, "book:abcd1")
        self.assertEqual(chapter.document_type, Document.DOCUMENT_TYPE_CHAPTER)
        self.assertEqual(chapter.document_id, "book:abcd1/chapter:18")
        self.assertEqual(chapter.parent_document, parent_document)
        self.assertEqual(chapter.identifiers["book_id"], "abcd1")
        self.assertEqual(chapter.default_lang, "es")

    def test_articlemeta_and_opac_upsert_same_document(self):
        collection = Collection.objects.create(acron3="scl", acron2="sc")
        source = Source.objects.create(
            collection=collection,
            source_type=Source.SOURCE_TYPE_JOURNAL,
            source_id="1234-5678",
            scielo_issn="1234-5678",
            acronym="testjou",
            title="Test Journal",
            identifiers={"scielo_issn": "1234-5678"},
        )

        first = article_service.upsert_article_document_from_articlemeta(
            {
                "code": "S123456782024000100001",
                "title": "Article Title",
                "pdfs": {"en": {"url": "/pdf/en.pdf"}},
                "processing_date": "2024-02-10",
                "publication_date": "2024-01-15",
                "publication_year": "2024",
                "default_language": "en",
                "text_langs": ["en", "pt"],
                "code_title": ["1234-5678"],
            },
            collection=collection,
            source=source,
        )
        second = article_service.upsert_article_document_from_opac(
            {
                "pid_v2": "S123456782024000100001",
                "pid_v3": "S1234-56782024000100001",
                "title": "Article Title",
                "journal_acronym": "testjou",
                "publication_date": "2024-01-15",
                "default_language": "en",
                "text_langs": ["en", "pt"],
            },
            collection=collection,
            source=source,
        )

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(Document.objects.count(), 1)
        second.refresh_from_db()
        self.assertEqual(second.pid_v3, "S1234-56782024000100001")
        self.assertEqual(second.identifiers["journal_acronym"], "testjou")

    def test_upsert_preprint_document_maps_metadata(self):
        collection = Collection.objects.create(acron3="preprints", acron2="pp")

        document = preprint_service.upsert_preprint_document(
            {
                "pid_generic": "preprint/123",
                "title": "Preprint Title",
                "text_langs": ["en", "pt"],
                "default_language": "en",
                "publication_date": "2024-01-20",
                "publication_year": "2024",
            },
            collection=collection,
        )

        self.assertEqual(document.document_type, Document.DOCUMENT_TYPE_PREPRINT)
        self.assertEqual(document.document_id, "preprint/123")
        self.assertEqual(document.pid_generic, "preprint/123")
        self.assertEqual(document.default_lang, "en")

    def test_upsert_dataset_document_accumulates_files(self):
        collection = Collection.objects.create(acron3="data", acron2="dt")

        dataset_service.upsert_dataset_document(
            {
                "title": "Dataset Title",
                "dataset_doi": "10.1234/dataset",
                "dataset_published": "2024-03-15",
                "file_id": "1",
                "file_name": "first.csv",
                "file_url": "https://example.org/first.csv",
                "file_persistent_id": "pid:first",
            },
            collection=collection,
        )
        document = dataset_service.upsert_dataset_document(
            {
                "title": "Dataset Title",
                "dataset_doi": "10.1234/dataset",
                "dataset_published": "2024-03-15",
                "file_id": "2",
                "file_name": "second.csv",
                "file_url": "https://example.org/second.csv",
                "file_persistent_id": "pid:second",
            },
            collection=collection,
        )

        self.assertEqual(document.document_type, Document.DOCUMENT_TYPE_DATASET)
        self.assertEqual(document.document_id, "10.1234/dataset")
        self.assertEqual(set(document.files.keys()), {"1", "2"})


class DocumentBooksSyncTests(TestCase):
    def test_get_latest_scielo_books_last_seq_uses_documents_and_sources(self):
        collection = Collection.objects.create(acron3="books", acron2="bk")
        source = Source.objects.create(
            collection=collection,
            source_type=Source.SOURCE_TYPE_BOOK,
            source_id="book-1",
            title="Book 1",
            extra_data={"last_seq": 120},
        )
        Document.objects.create(
            collection=collection,
            source=source,
            document_type=Document.DOCUMENT_TYPE_BOOK,
            document_id="book:book-1",
            extra_data={"last_seq": "135"},
        )

        self.assertEqual(document_tasks.get_latest_scielo_books_last_seq("books"), 135)

    def test_sync_documents_from_scielo_books_uses_computed_since(self):
        collection = Collection.objects.create(acron3="books", acron2="bk")
        source = Source.objects.create(
            collection=collection,
            source_type=Source.SOURCE_TYPE_BOOK,
            source_id="book-1",
            title="Book 1",
            extra_data={"last_seq": 120},
        )
        Document.objects.create(
            collection=collection,
            source=source,
            document_type=Document.DOCUMENT_TYPE_BOOK,
            document_id="book:book-1",
            extra_data={"last_seq": 135},
        )

        with patch("document.tasks.scielo_books.load_documents_from_scielo_books", return_value=True) as mocked:
            result = document_tasks.sync_documents_from_scielo_books(
                collection="books",
                db_name="scielobooks_1a",
                limit=500,
            )

        self.assertTrue(result)
        mocked.assert_called_once_with(
            collection="books",
            db_name="scielobooks_1a",
            since=135,
            limit=500,
            force_update=True,
            headers=None,
            base_url=None,
            user=None,
        )
