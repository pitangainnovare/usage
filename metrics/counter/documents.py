from scielo_usage_counter.counter import is_request

from metrics.counter.aggregation import (
    apply_unique_metrics,
    article_version,
    counter_data_type,
    extract_title_pid_generic,
    increment_document_totals,
    parent_data_type,
    should_create_book_item_document,
)
from metrics.counter.identifiers import (
    generate_month_document_id,
    generate_year_document_id,
)


def convert_to_month_index_documents(data: dict):
    if not isinstance(data, dict):
        return {}

    metrics_data = {}
    unique_state = _initialize_unique_state()

    for value in data.values():
        _accumulate_documents(
            data=metrics_data,
            unique_state=unique_state,
            value=value,
            granularity="month",
        )

    return metrics_data


def convert_to_year_index_documents(data: dict):
    if not isinstance(data, dict):
        return {}

    metrics_data = {}
    unique_state = _initialize_unique_state()

    for value in data.values():
        _accumulate_documents(
            data=metrics_data,
            unique_state=unique_state,
            value=value,
            granularity="year",
        )

    return metrics_data


def convert_raw_results_to_index_documents(data: dict):
    return {
        "month": convert_to_month_index_documents(data),
        "year": convert_to_year_index_documents(data),
    }


def _initialize_unique_state():
    return {
        "item_investigations": set(),
        "item_requests": set(),
        "title_investigations": set(),
        "title_requests": set(),
    }


def _accumulate_documents(data, unique_state, value, granularity):
    if not isinstance(value, dict):
        return

    if value.get("collection") == "books":
        _accumulate_books_documents(data, unique_state, value, granularity)
        return

    _accumulate_standard_documents(data, unique_state, value, granularity)


def _accumulate_standard_documents(data, unique_state, value, granularity):
    document_id = _generate_document_id(value, granularity)
    document = data.setdefault(
        document_id,
        _build_base_document(value=value, granularity=granularity),
    )

    increment_document_totals(
        document=document,
        click_timestamps=value.get("click_timestamps"),
        click_timestamps_by_url=value.get("click_timestamps_by_url"),
        content_type=value.get("content_type"),
    )
    apply_unique_metrics(
        document=document,
        unique_state=unique_state,
        scope="item",
        document_id=document_id,
        user_session_id=value.get("user_session_id"),
        is_request_event=is_request(value.get("content_type")),
    )


def _accumulate_books_documents(data, unique_state, value, granularity):
    if should_create_book_item_document(value):
        item_document_id = _generate_document_id(
            value,
            granularity,
            metric_scope="item",
        )
        item_document = data.setdefault(
            item_document_id,
            _build_base_document(
                value=value,
                granularity=granularity,
                metric_scope="item",
            ),
        )
        increment_document_totals(
            document=item_document,
            click_timestamps=value.get("click_timestamps"),
            click_timestamps_by_url=value.get("click_timestamps_by_url"),
            content_type=value.get("content_type"),
        )
        apply_unique_metrics(
            document=item_document,
            unique_state=unique_state,
            scope="item",
            document_id=item_document_id,
            user_session_id=value.get("user_session_id"),
            is_request_event=is_request(value.get("content_type")),
        )

    title_pid_generic = extract_title_pid_generic(value)
    if not title_pid_generic:
        return

    title_document_id = _generate_document_id(
        value,
        granularity,
        metric_scope="title",
        pid_generic=title_pid_generic,
    )
    title_document = data.setdefault(
        title_document_id,
        _build_base_document(
            value=value,
            granularity=granularity,
            metric_scope="title",
            pid_generic=title_pid_generic,
            document_type="book",
        ),
    )
    increment_document_totals(
        document=title_document,
        click_timestamps=value.get("click_timestamps"),
        click_timestamps_by_url=value.get("click_timestamps_by_url"),
        content_type=value.get("content_type"),
    )
    apply_unique_metrics(
        document=title_document,
        unique_state=unique_state,
        scope="title",
        document_id=title_document_id,
        user_session_id=value.get("user_session_id"),
        is_request_event=is_request(value.get("content_type")),
    )


def _generate_document_id(value, granularity, metric_scope=None, pid_generic=None):
    pid_generic = pid_generic or value.get("pid_generic")
    publication_year = str(value.get("publication_year") or "0001")
    if granularity == "month":
        access_month = (
            value.get("access_date", "")[:7] if value.get("access_date") else ""
        )
        return generate_month_document_id(
            collection=value.get("collection"),
            source_key=value.get("source_key"),
            pid_v2=value.get("pid_v2"),
            pid_v3=value.get("pid_v3"),
            pid_generic=pid_generic,
            access_month=access_month,
            counter_access_type=value.get("counter_access_type") or "Open",
            access_method=value.get("access_method") or "Regular",
            publication_year=publication_year,
            metric_scope="title" if metric_scope == "title" else None,
        )

    return generate_year_document_id(
        collection=value.get("collection"),
        source_key=value.get("source_key"),
        pid_v2=value.get("pid_v2"),
        pid_v3=value.get("pid_v3"),
        pid_generic=pid_generic,
        content_language=value.get("content_language"),
        access_country_code=value.get("access_country_code"),
        access_year=value.get("access_year"),
        counter_access_type=value.get("counter_access_type") or "Open",
        access_method=value.get("access_method") or "Regular",
        publication_year=publication_year,
        metric_scope="title" if metric_scope == "title" else None,
    )


def _build_base_document(
    value, granularity, metric_scope=None, pid_generic=None, document_type=None
):
    collection = value.get("collection")
    scope = metric_scope or "item"
    if collection == "books":
        document_id = pid_generic or value.get("pid_generic")
        parent_id = extract_title_pid_generic(value, fallback=document_id)
        if parent_id == document_id or scope == "title":
            parent_id = None
        raw_source = value.get("source") or {}
        source = _build_source(raw_source)
        base_document = {
            "collection": collection,
            "source": source,
            "document": _build_document(
                value=value,
                document_id=document_id,
                document_type=document_type or value.get("document_type"),
                parent_id=parent_id,
                source_identifiers=raw_source.get("identifiers"),
                metric_scope=scope,
            ),
            "counter": _compact_dict(
                {
                    "metric_scope": scope,
                    "data_type": "Book" if scope == "title" else "Book_Segment",
                    "parent_data_type": "Book" if scope != "title" else None,
                    "access_type": value.get("counter_access_type") or "Open",
                    "access_method": value.get("access_method") or "Regular",
                }
            ),
            "total_requests": 0,
            "total_investigations": 0,
            "unique_requests": 0,
            "unique_investigations": 0,
        }
        base_document["access"] = _build_access(value, granularity)
        if granularity == "month":
            base_document["daily_metrics"] = _build_daily_metrics(value)
        return base_document

    document_type = value.get("document_type")
    document_id = value.get("pid_v3") or value.get("pid_v2") or value.get("pid_generic")
    base_document = {
        "collection": collection,
        "source": _build_source(value.get("source")),
        "document": _build_document(
            value=value,
            document_id=document_id,
            document_type=document_type,
        ),
        "counter": _compact_dict(
            {
                "metric_scope": "item",
                "data_type": counter_data_type(document_type),
                "parent_data_type": parent_data_type(
                    document_type,
                    (value.get("source") or {}).get("source_type"),
                ),
                "article_version": article_version(document_type),
                "access_type": value.get("counter_access_type") or "Open",
                "access_method": value.get("access_method") or "Regular",
            }
        ),
        "total_requests": 0,
        "total_investigations": 0,
        "unique_requests": 0,
        "unique_investigations": 0,
    }
    base_document["access"] = _build_access(value, granularity)
    if granularity == "month":
        base_document["daily_metrics"] = _build_daily_metrics(value)
    return base_document


def _build_access(value, granularity):
    if granularity == "month":
        return {
            "month": value.get("access_date", "")[:7]
            if value.get("access_date")
            else ""
        }

    return _compact_dict(
        {
            "year": value.get("access_year"),
            "country_code": value.get("access_country_code"),
            "content_language": value.get("content_language"),
        }
    )


def _build_daily_metrics(value):
    day = value.get("access_date", "")[-2:] if value.get("access_date") else "01"
    return {
        day: {
            "total_requests": 0,
            "total_investigations": 0,
            "unique_requests": 0,
            "unique_investigations": 0,
        }
    }


def _build_document(
    value,
    document_id,
    document_type,
    parent_id=None,
    source_identifiers=None,
    metric_scope="item",
):
    document = value.get("document") or {}
    title = document.get("title")
    if metric_scope == "title":
        title = (value.get("source") or {}).get("main_title") or title

    identifiers = _document_identifiers(
        value=value,
        document_id=document_id,
        source_identifiers=source_identifiers,
        metric_scope=metric_scope,
    )

    return _compact_dict(
        {
            "id": document_id,
            "type": document_type,
            "title": title,
            "parent_id": parent_id,
            "publication_year": value.get("publication_year"),
            "identifiers": identifiers,
        }
    )


def _document_identifiers(
    value, document_id, source_identifiers=None, metric_scope="item"
):
    if value.get("collection") == "books" and metric_scope == "title":
        identifiers = _book_identifiers_from_pid(document_id)
        identifiers.update(source_identifiers or {})
        return _compact_identifiers(identifiers, canonical_id=document_id)

    document_identifiers = (value.get("document") or {}).get("identifiers") or {}
    identifiers = {
        "pid_v2": value.get("pid_v2"),
        "pid_v3": value.get("pid_v3"),
        "pid_generic": value.get("pid_generic"),
    }
    identifiers.update(document_identifiers)

    if value.get("collection") == "books":
        identifiers.update(_book_identifiers_from_pid(value.get("pid_generic")))
        identifiers.update(source_identifiers or {})

    return _compact_identifiers(identifiers, canonical_id=document_id)


def _book_identifiers_from_pid(pid_generic):
    value = str(pid_generic or "")
    if not value.upper().startswith("BOOK:"):
        return {}

    identifiers = {}
    parts = value.split("/", 1)
    book_id = parts[0].split(":", 1)[1] if ":" in parts[0] else ""
    if book_id:
        identifiers["book_id"] = book_id

    if len(parts) > 1 and parts[1].upper().startswith("CHAPTER:"):
        chapter_id = parts[1].split(":", 1)[1] if ":" in parts[1] else ""
        if chapter_id:
            identifiers["chapter_id"] = chapter_id

    return identifiers


def _build_source(source):
    source = source or {}
    source_id = source.get("source_id")
    source_type = source.get("source_type")
    identifiers = _compact_identifiers(
        source.get("identifiers") or {}, canonical_id=source_id
    )

    return _compact_dict(
        {
            "id": source_id,
            "type": source_type,
            "title": source.get("main_title"),
            "scielo_issn": None if source_type == "book" else source.get("scielo_issn"),
            "acronym": source.get("acronym"),
            "publisher_name": source.get("publisher_name"),
            "subject_area_capes": source.get("subject_area_capes"),
            "subject_area_wos": source.get("subject_area_wos"),
            "access_type": source.get("access_type"),
            "city": source.get("city"),
            "country": source.get("country"),
            "identifiers": identifiers,
        }
    )


def _compact_identifiers(identifiers, canonical_id=None):
    compact = {}
    canonical_value = str(canonical_id or "").strip().upper()
    for key, value in (identifiers or {}).items():
        if value in (None, "", [], {}, ()):
            continue
        if canonical_value and str(value).strip().upper() == canonical_value:
            continue
        compact[key] = value
    return compact


def _compact_dict(data):
    return {
        key: value for key, value in data.items() if value not in (None, "", [], {}, ())
    }
