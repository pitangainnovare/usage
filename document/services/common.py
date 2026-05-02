from document.models import Document


def build_document_id(*values):
    for value in values:
        if value not in (None, ""):
            return str(value)
    return None


def get_existing_document(collection, document_type, *identifiers):
    identifiers = [str(value) for value in identifiers if value not in (None, "")]
    if not identifiers:
        return None

    queryset = Document.objects.filter(
        collection=collection,
        document_type=document_type,
    )

    for field_name in ("document_id", "pid_v2", "pid_v3", "pid_generic"):
        for identifier in identifiers:
            document = queryset.filter(**{field_name: identifier}).first()
            if document:
                return document

    return None


def normalize_langs(value):
    if not value:
        return []

    if isinstance(value, list):
        return [item for item in value if item not in (None, "")]

    if isinstance(value, dict):
        return [key for key, enabled in value.items() if enabled]

    return [value]


def normalize_year(value, fallback_date=None):
    if value not in (None, ""):
        return str(value)[:4]

    if fallback_date not in (None, ""):
        return str(fallback_date)[:4]

    return None


def compact_dict(data):
    return {
        key: value
        for key, value in data.items()
        if value not in (None, "", [], {}, ())
    }
