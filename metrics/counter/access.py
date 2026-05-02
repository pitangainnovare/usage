import re
from urllib.parse import unquote, urlparse

from scielo_usage_counter.values import (
    CONTENT_TYPE_UNDEFINED,
    DEFAULT_SCIELO_ISSN,
    MEDIA_LANGUAGE_UNDEFINED,
    MEDIA_FORMAT_UNDEFINED,
)

from core.utils.standardizer import (
    standardize_language_code,
    standardize_pid_generic,
    standardize_pid_v2,
    standardize_pid_v3,
    standardize_year_of_publication,
)
from core.utils.date_utils import extract_minute_second_key, truncate_datetime_to_hour
from metrics.counter.identifiers import generate_item_access_id, generate_user_session_id


def extract_item_access_data(collection_acron3: str, translated_url: dict):
    if not translated_url or not isinstance(translated_url, dict):
        return {}

    source_type = _extract_source_type(collection_acron3, translated_url)
    source_id = _extract_source_id(collection_acron3, translated_url, source_type)
    scielo_issn = _extract_scielo_issn(translated_url, source_type, source_id)
    document_type = _extract_document_type(collection_acron3, translated_url, source_type)
    publication_year = _safe_standardize(
        standardize_year_of_publication,
        translated_url.get("year_of_publication"),
    )
    source_access_type = translated_url.get("source_access_type")

    return {
        "collection": collection_acron3,
        "source_type": source_type,
        "source_id": source_id,
        "scielo_issn": scielo_issn,
        "document_type": document_type,
        "pid_v2": _safe_standardize(standardize_pid_v2, translated_url.get("pid_v2")),
        "pid_v3": _safe_standardize(standardize_pid_v3, translated_url.get("pid_v3")),
        "pid_generic": _safe_standardize(
            standardize_pid_generic,
            translated_url.get("pid_generic"),
        ),
        "title_pid_generic": _safe_standardize(
            standardize_pid_generic,
            translated_url.get("title_pid_generic"),
        ),
        "segment_pid_generics": _standardize_pid_generic_list(
            translated_url.get("segment_pid_generics"),
        ),
        "media_language": _safe_standardize(
            standardize_language_code,
            translated_url.get("media_language"),
            default="un",
        ),
        "media_format": translated_url.get("media_format"),
        "content_type": translated_url.get("content_type"),
        "access_url": translated_url.get("access_url") or translated_url.get("normalized_url"),
        "publication_year": publication_year,
        "counter_access_type": _counter_access_type(source_access_type),
        "access_method": "Regular",
        "source_main_title": _extract_source_title(translated_url),
        "source_subject_area_capes": translated_url.get("source_subject_area_capes")
        or translated_url.get("journal_subject_area_capes"),
        "source_subject_area_wos": translated_url.get("source_subject_area_wos")
        or translated_url.get("journal_subject_area_wos"),
        "source_acronym": translated_url.get("source_acronym")
        or translated_url.get("journal_acronym"),
        "source_publisher_name": translated_url.get("source_publisher_name")
        or translated_url.get("journal_publisher_name"),
        "source_access_type": source_access_type,
        "source_identifiers": _extract_source_identifiers(translated_url, source_id, source_type),
        "source_city": translated_url.get("source_city"),
        "source_country": translated_url.get("source_country"),
    }


def is_valid_item_access_data(data: dict, utm=None, ignore_utm_validation=False):
    if not isinstance(data, dict):
        return False, {"message": "Invalid data format. Expected a dictionary.", "code": "invalid_format"}

    scielo_issn = data.get("scielo_issn")
    source_id = data.get("source_id")
    source_type = data.get("source_type")
    document_type = data.get("document_type") or "article"
    media_format = data.get("media_format")
    media_language = data.get("media_language")
    content_type = data.get("content_type")
    pid_v2 = data.get("pid_v2")
    pid_v3 = data.get("pid_v3")
    pid_generic = data.get("pid_generic")
    has_source_identity = bool(source_id) or bool(
        scielo_issn and scielo_issn != DEFAULT_SCIELO_ISSN
    )
    has_media_language = bool(media_language and media_language != MEDIA_LANGUAGE_UNDEFINED)
    has_pid = bool(pid_v2 or pid_v3 or pid_generic)

    if not all([media_format and media_format != MEDIA_FORMAT_UNDEFINED, content_type and content_type != CONTENT_TYPE_UNDEFINED, has_pid]):
        return False, {"message": "Missing required fields in item access data.", "code": "missing_fields"}

    if document_type in {"article", "book", "chapter"} and not has_media_language:
        return False, {"message": "Missing media language in item access data.", "code": "missing_fields"}

    if document_type == "article" and not has_source_identity:
        return False, {"message": "Missing article source identity.", "code": "missing_fields"}

    if document_type in {"book", "chapter"} and not source_id:
        return False, {"message": "Missing book source identity.", "code": "missing_fields"}

    if document_type in {"preprint", "dataset"} and not pid_generic:
        return False, {"message": "Missing generic PID in item access data.", "code": "missing_fields"}

    if utm and not ignore_utm_validation:
        if (
            source_type == "journal"
            and scielo_issn
            and scielo_issn != DEFAULT_SCIELO_ISSN
            and not utm.is_valid_code(scielo_issn, utm.sources_metadata["issn_set"])
        ):
            return False, {"message": f"Invalid scielo_issn: {scielo_issn}", "code": "invalid_scielo_issn"}

        if (
            source_type
            and source_type != "journal"
            and source_id
            and source_id not in utm.sources_metadata.get("source_id_to_type", {})
        ):
            return False, {"message": f"Invalid source_id: {source_id}", "code": "invalid_source_id"}

        if pid_v2 and not utm.is_valid_code(pid_v2, utm.documents_metadata["pid_set"]):
            return False, {"message": f"Invalid pid_v2: {pid_v2}", "code": "invalid_pid_v2"}

        if pid_v3 and not utm.is_valid_code(pid_v3, utm.documents_metadata["pid_set"]):
            return False, {"message": f"Invalid pid_v3: {pid_v3}", "code": "invalid_pid_v3"}

        if pid_generic and not utm.is_valid_code(pid_generic, utm.documents_metadata["pid_set"]):
            return False, {"message": f"Invalid pid_generic: {pid_generic}", "code": "invalid_pid_generic"}

    return True, {"message": "Item access data is valid.", "code": "valid"}


def update_results_with_item_access_data(results: dict, item_access_data: dict, line: dict):
    col_acron3 = item_access_data.get("collection")
    source_key = (
        item_access_data.get("source_id")
        or item_access_data.get("scielo_issn")
        or item_access_data.get("source_type")
        or col_acron3
    )
    pid_v2 = item_access_data.get("pid_v2")
    pid_v3 = item_access_data.get("pid_v3")
    media_format = item_access_data.get("media_format")
    content_language = item_access_data.get("media_language")
    content_type = item_access_data.get("content_type")
    access_url = item_access_data.get("access_url") or _normalize_access_url(line.get("url"))

    client_name = line.get("client_name")
    client_version = line.get("client_version")
    local_datetime = line.get("local_datetime")
    access_country_code = line.get("country_code")
    ip_address = line.get("ip_address")

    truncated_datetime = truncate_datetime_to_hour(local_datetime)
    ms_key = extract_minute_second_key(local_datetime)
    if truncated_datetime is None or ms_key is None:
        raise ValueError("Invalid local_datetime in parsed log line.")

    access_date = truncated_datetime.strftime("%Y-%m-%d")
    access_year = access_date[:4]
    access_month = access_date[:7].replace("-", "")

    user_session_id = generate_user_session_id(
        client_name,
        client_version,
        ip_address,
        truncated_datetime,
    )

    for access_target in _iter_access_targets(item_access_data):
        item_access_id = generate_item_access_id(
            user_session_id=user_session_id,
            col_acron3=col_acron3,
            source_key=source_key,
            pid_v2=pid_v2,
            pid_v3=pid_v3,
            pid_generic=access_target.get("pid_generic"),
            content_language=content_language,
            access_country_code=access_country_code,
            media_format=media_format,
            content_type=content_type,
        )

        if item_access_id not in results:
            results[item_access_id] = {
                "collection": col_acron3,
                "source_key": source_key,
                "document_type": access_target.get("document_type"),
                "pid_v2": pid_v2,
                "pid_v3": pid_v3,
                "pid_generic": access_target.get("pid_generic"),
                "title_pid_generic": (
                    item_access_data.get("title_pid_generic")
                    or access_target.get("pid_generic")
                ),
                "user_session_id": user_session_id,
                "click_timestamps": {ms_key: 0},
                "click_timestamps_by_url": {},
                "access_url": access_url,
                "media_format": media_format,
                "content_language": content_language,
                "content_type": content_type,
                "access_country_code": access_country_code,
                "access_date": access_date,
                "access_year": access_year,
                "access_month": access_month,
                "publication_year": item_access_data.get("publication_year"),
                "counter_access_type": item_access_data.get("counter_access_type") or "Open",
                "access_method": item_access_data.get("access_method") or "Regular",
                "source": {
                    "source_type": item_access_data.get("source_type"),
                    "source_id": item_access_data.get("source_id"),
                    "scielo_issn": item_access_data.get("scielo_issn"),
                    "main_title": item_access_data.get("source_main_title"),
                    "identifiers": item_access_data.get("source_identifiers"),
                    "access_type": item_access_data.get("source_access_type"),
                    "city": item_access_data.get("source_city"),
                    "country": item_access_data.get("source_country"),
                    "subject_area_capes": item_access_data.get("source_subject_area_capes"),
                    "subject_area_wos": item_access_data.get("source_subject_area_wos"),
                    "acronym": item_access_data.get("source_acronym"),
                    "publisher_name": item_access_data.get("source_publisher_name"),
                },
            }

        if ms_key not in results[item_access_id]["click_timestamps"]:
            results[item_access_id]["click_timestamps"][ms_key] = 0

        results[item_access_id]["click_timestamps"][ms_key] += 1

        access_url_key = access_url or _fallback_access_url_key(
            access_target.get("pid_generic"),
            media_format,
            content_type,
        )
        timestamps_by_url = results[item_access_id].setdefault("click_timestamps_by_url", {})
        url_timestamps = timestamps_by_url.setdefault(access_url_key, {})
        if ms_key not in url_timestamps:
            url_timestamps[ms_key] = 0
        url_timestamps[ms_key] += 1


def _extract_source_type(collection_acron3, translated_url):
    source_type = translated_url.get("source_type")
    if source_type:
        return source_type

    if collection_acron3 == "preprints":
        return "preprint_server"

    if collection_acron3 == "data":
        return "data_repository"

    if collection_acron3 == "books":
        return "book"

    if translated_url.get("book_id"):
        return "book"

    if (
        translated_url.get("scielo_issn")
        and translated_url.get("scielo_issn") != DEFAULT_SCIELO_ISSN
    ):
        return "journal"

    if translated_url.get("journal_acronym") or translated_url.get("journal_main_title"):
        return "journal"

    return "other"


def _extract_source_id(collection_acron3, translated_url, source_type):
    source_id = translated_url.get("source_id")
    if source_id:
        return source_id

    if source_type == "preprint_server":
        return translated_url.get("preprint_server_id") or "scielo-preprints"

    if source_type == "data_repository":
        return translated_url.get("repository_id") or "scielo-data"

    if source_type == "book":
        return (
            translated_url.get("book_id")
            or _extract_book_id_from_pid(translated_url.get("title_pid_generic"))
            or _extract_book_id_from_pid(translated_url.get("pid_generic"))
        )

    if source_type == "journal":
        return translated_url.get("scielo_issn")

    return None


def _extract_scielo_issn(translated_url, source_type, source_id):
    scielo_issn = translated_url.get("scielo_issn")
    if scielo_issn:
        return scielo_issn

    if source_type == "journal" and source_id:
        return source_id

    if source_type in {"book", "other"} or translated_url.get("book_id"):
        return DEFAULT_SCIELO_ISSN

    return None


def _extract_source_title(translated_url):
    return (
        translated_url.get("source_main_title")
        or translated_url.get("journal_main_title")
        or translated_url.get("book_title")
    )


def _extract_document_type(collection_acron3, translated_url, source_type):
    document_type = translated_url.get("document_type")
    if document_type:
        return document_type

    if collection_acron3 == "preprints":
        return "preprint"

    if collection_acron3 == "data":
        return "dataset"

    if collection_acron3 == "books" or source_type == "book":
        pid_generic = translated_url.get("pid_generic") or ""
        if translated_url.get("chapter_id") or "/CHAPTER:" in pid_generic.upper():
            return "chapter"
        if translated_url.get("book_id"):
            return "book"
        return "book"

    if source_type == "journal":
        return "article"

    return "article"


def _extract_source_identifiers(translated_url, source_id, source_type):
    identifiers = translated_url.get("source_identifiers")
    if isinstance(identifiers, dict):
        compact = {key: value for key, value in identifiers.items() if value not in (None, "", [], {}, ())}
        if compact:
            return compact

    if source_type != "book":
        return None

    compact = {
        "book_id": source_id or translated_url.get("book_id"),
        "isbn": translated_url.get("isbn"),
        "eisbn": translated_url.get("eisbn"),
        "doi": translated_url.get("doi"),
    }
    compact = {key: value for key, value in compact.items() if value not in (None, "", [], {}, ())}
    return compact or None


def _extract_book_id_from_pid(value):
    if not value:
        return None
    normalized = str(value).upper()
    if not normalized.startswith("BOOK:"):
        return None
    return normalized.split("BOOK:", 1)[1].split("/", 1)[0] or None


def _counter_access_type(source_access_type):
    normalized = str(source_access_type or "").strip().lower()
    if normalized == "commercial":
        return "Controlled"
    if normalized in {"free_to_read", "free-to-read", "free"}:
        return "Free_To_Read"
    return "Open"


def _safe_standardize(func, value, default=""):
    try:
        return func(value)
    except Exception:
        return default


def _standardize_pid_generic_list(values):
    if not isinstance(values, (list, tuple, set)):
        return []
    items = []
    for value in values:
        item = _safe_standardize(standardize_pid_generic, value)
        if item and item not in items:
            items.append(item)
    return items


def _iter_access_targets(item_access_data):
    return [
        {
            "pid_generic": item_access_data.get("pid_generic"),
            "document_type": item_access_data.get("document_type"),
        }
    ]


def _normalize_access_url(url):
    if not url:
        return None
    parsed_url = urlparse(str(url).strip())
    path = parsed_url.path if parsed_url.scheme or parsed_url.netloc else str(url).strip()
    path = unquote(path or "")
    path = path.split("?", 1)[0].split("#", 1)[0].split()[0]
    path = re.sub(r"/+", "/", path)
    path = path.rstrip(".,;:")
    return path or None


def _fallback_access_url_key(pid_generic, media_format, content_type):
    return "|".join([
        str(pid_generic or ""),
        str(media_format or ""),
        str(content_type or ""),
    ])
