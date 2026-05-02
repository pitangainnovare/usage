from django.test import TestCase

from collection.models import Collection

from .models import Source
from .services import books as books_service
from .services import journals as journal_service


class SourceMetadataTests(TestCase):
    def test_source_type_choices_include_scielo_non_journal_sources(self):
        self.assertIn(
            (Source.SOURCE_TYPE_PREPRINT_SERVER, "Preprint Server"),
            [(value, str(label)) for value, label in Source.SOURCE_TYPE_CHOICES],
        )
        self.assertIn(
            (Source.SOURCE_TYPE_DATA_REPOSITORY, "Data Repository"),
            [(value, str(label)) for value, label in Source.SOURCE_TYPE_CHOICES],
        )

    def test_metadata_exposes_generic_and_journal_fields(self):
        collection = Collection.objects.create(acron3="scl", acron2="sc")
        Source.objects.create(
            collection=collection,
            source_type=Source.SOURCE_TYPE_JOURNAL,
            source_id="1234-5678",
            scielo_issn="1234-5678",
            acronym="testjou",
            title="Test Journal",
            identifiers={
                "electronic_issn": "1234-5678",
                "print_issn": "8765-4321",
                "doi": "10.1590/example",
            },
            publisher_name=["SciELO"],
            subject_areas=["Health Sciences"],
            wos_subject_areas=["Medicine"],
            default_lang="en",
            publication_date="2024-01-15",
            publication_year="2024",
            extra_data={"country": "BR"},
        )

        metadata = list(Source.metadata(collection=collection))

        self.assertEqual(len(metadata), 1)
        self.assertEqual(metadata[0]["source_type"], Source.SOURCE_TYPE_JOURNAL)
        self.assertEqual(metadata[0]["source_id"], "1234-5678")
        self.assertEqual(metadata[0]["scielo_issn"], "1234-5678")
        self.assertEqual(metadata[0]["issns"], {"1234-5678", "8765-4321"})
        self.assertEqual(metadata[0]["title"], "Test Journal")

    def test_upsert_monograph_source_maps_scielo_books_payload(self):
        collection = Collection.objects.create(acron3="books", acron2="bk")

        source = books_service.upsert_monograph_source(
            {
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
                "is_comercial": False,
                "visible": True,
            },
            collection=collection,
        )

        self.assertEqual(source.source_type, Source.SOURCE_TYPE_BOOK)
        self.assertEqual(source.source_id, "abcd1")
        self.assertEqual(source.identifiers["isbn"], "9788578791889")
        self.assertEqual(source.default_lang, "pt")
        self.assertEqual(source.publication_year, "2024")
        self.assertEqual(source.access_type, Source.ACCESS_TYPE_OPEN_ACCESS)

    def test_upsert_monograph_source_accepts_long_real_world_title(self):
        collection = Collection.objects.create(acron3="books", acron2="bk")
        title = (
            "O Estado da Arte sobre Refugiados, Deslocados Internos, "
            "Deslocados Ambientais e Apatridas no Brasil: atualizacao do "
            "Diretorio Nacional do ACNUR de teses, dissertacoes, trabalhos "
            "de conclusao de curso de graduacao em Joao Pessoa (Paraiba) e "
            "artigos (2007 a 2017)"
        )

        source = books_service.upsert_monograph_source(
            {
                "TYPE": "Monograph",
                "id": "9zzts",
                "title": title,
            },
            collection=collection,
        )

        self.assertEqual(source.title, title)

    def test_upsert_journal_source_maps_articlemeta_payload(self):
        collection = Collection.objects.create(acron3="scl", acron2="sc")

        source = journal_service.upsert_journal_source(
            {
                "collection_acronym": "scl",
                "scielo_issn": "1234-5678",
                "electronic_issn": "1234-5678",
                "print_issn": "8765-4321",
                "acronym": "testjou",
                "title": "Test Journal",
                "publisher_name": "SciELO",
                "subject_areas": ["Health Sciences"],
                "wos_subject_areas": ["Medicine"],
            },
            collection=collection,
            load_mode="thrift",
        )

        self.assertEqual(source.source_type, Source.SOURCE_TYPE_JOURNAL)
        self.assertEqual(source.source_id, "1234-5678")
        self.assertEqual(source.identifiers["electronic_issn"], "1234-5678")
        self.assertEqual(source.publisher_name, ["SciELO"])
        self.assertEqual(source.extra_data["load_mode"], "thrift")
        self.assertEqual(
            journal_service.find_journal_source_by_issns(collection, ["8765-4321"]).pk,
            source.pk,
        )
        self.assertEqual(
            journal_service.find_journal_source_by_acronym(collection, "testjou").pk,
            source.pk,
        )
