import logging

from collection.models import Collection
from document.models import Document
from source.models import Source


def _get_collection(acronym):
    if not acronym:
        return None
    return Collection.objects.filter(acron3=acronym).first()


def get_latest_scielo_books_last_seq(collection="books"):
    document_last_seq = _get_latest_last_seq_from_queryset(
        Document.objects.filter(collection__acron3=collection).only("extra_data")
    )
    source_last_seq = _get_latest_last_seq_from_queryset(
        Source.objects.filter(
            collection__acron3=collection,
            source_type=Source.SOURCE_TYPE_BOOK,
        ).only("extra_data")
    )
    return max(document_last_seq, source_last_seq)


def _get_latest_last_seq_from_queryset(queryset):
    latest = 0
    for item in queryset.iterator():
        value = _coerce_last_seq((item.extra_data or {}).get("last_seq"))
        if value is not None and value > latest:
            latest = value
    return latest


def _coerce_last_seq(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        logging.warning("Ignoring invalid SciELO Books last_seq value: %r", value)
        return None
