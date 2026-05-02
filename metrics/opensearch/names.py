from django.conf import settings


def _validate_index_inputs(index_prefix: str, collection: str, date: str):
    if not date or not isinstance(date, str):
        raise ValueError("Date must be a non-empty string in 'YYYY-MM-DD' format.")
    if not collection or not isinstance(collection, str):
        raise ValueError("Collection must be a non-empty string.")
    if not index_prefix or not isinstance(index_prefix, str):
        raise ValueError("Index prefix must be a non-empty string.")


def _get_collection_size(collection: str) -> str:
    return getattr(settings, "COLLECTION_ACRON3_SIZE_MAP", {}).get(collection, "small")


def extract_access_year(date: str) -> str:
    _validate_index_inputs("usage", "tmp", date)
    return date.split("-")[0]


def extract_access_month(date: str) -> str:
    _validate_index_inputs("usage", "tmp", date)
    year, month, _ = date.split("-")
    return f"{year}{month}"


def generate_month_index_name(index_prefix: str, collection: str, date: str) -> str:
    _validate_index_inputs(index_prefix, collection, date)
    size = _get_collection_size(collection)
    if size in ("xlarge", "large"):
        return f"{index_prefix}_monthly_{collection}_{extract_access_year(date)}"
    return f"{index_prefix}_monthly_{collection}"


def generate_year_index_name(index_prefix: str, collection: str, date: str) -> str:
    _validate_index_inputs(index_prefix, collection, date)
    size = _get_collection_size(collection)
    if size in ("xlarge", "large"):
        return f"{index_prefix}_yearly_{collection}_{extract_access_year(date)}"
    return f"{index_prefix}_yearly_{collection}"
