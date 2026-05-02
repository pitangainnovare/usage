from django.db.models import Q

from collection.models import Collection
from source.models import Source


def get_collection(acronym):
    return Collection.objects.filter(acron3=acronym).first()


def upsert_journal_source(
    journal,
    collection,
    user=None,
    force_update=True,
    load_mode=None,
):
    scielo_issn = _value(journal, "scielo_issn")
    if not scielo_issn:
        return None

    source, created = Source.objects.get_or_create(
        collection=collection,
        source_type=Source.SOURCE_TYPE_JOURNAL,
        source_id=scielo_issn,
    )

    if created and user:
        source.creator = user

    if created or force_update:
        source.scielo_issn = scielo_issn
        source.acronym = _value(journal, "acronym") or ""
        source.title = _value(journal, "title") or scielo_issn
        source.identifiers = _build_source_identifiers(journal)
        source.publisher_name = _as_list(_value(journal, "publisher_name"))
        source.subject_areas = _as_list(_value(journal, "subject_areas"))
        source.wos_subject_areas = _as_list(_value(journal, "wos_subject_areas"))
        source.default_lang = None
        source.publication_date = None
        source.publication_year = None
        source.extra_data = _compact_dict(
            {
                "collection_acronym": _value(journal, "collection_acronym"),
                "load_mode": load_mode,
            }
        )

    if user:
        source.updated_by = user

    source.save()
    return source


def find_journal_source_by_issns(collection, issns):
    for issn in filter(None, issns or []):
        source = (
            Source.objects.filter(
                collection=collection,
                source_type=Source.SOURCE_TYPE_JOURNAL,
            )
            .filter(
                Q(scielo_issn=issn)
                | Q(source_id=issn)
                | Q(identifiers__electronic_issn=issn)
                | Q(identifiers__print_issn=issn)
                | Q(identifiers__scielo_issn=issn)
            )
            .first()
        )
        if source:
            return source
    return None


def find_journal_source_by_acronym(collection, acronym):
    if not acronym:
        return None

    return Source.objects.filter(
        collection=collection,
        source_type=Source.SOURCE_TYPE_JOURNAL,
        acronym=acronym,
    ).first()


def _build_source_identifiers(journal):
    identifiers = {
        "electronic_issn": _value(journal, "electronic_issn"),
        "print_issn": _value(journal, "print_issn"),
        "scielo_issn": _value(journal, "scielo_issn"),
    }
    return _compact_dict(identifiers)


def _as_list(value):
    if not value:
        return []

    if isinstance(value, list):
        return value

    return [value]


def _value(data, key, default=None):
    if isinstance(data, dict):
        return data.get(key, default)
    return getattr(data, key, default)


def _compact_dict(data):
    return {
        key: value
        for key, value in data.items()
        if value not in (None, "", [], {}, ())
    }
