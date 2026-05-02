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
from metrics.counter.identifiers import generate_month_document_id, generate_year_document_id


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
        access_month = value.get("access_date", "")[:7] if value.get("access_date") else ""
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


def _build_base_document(value, granularity, metric_scope=None, pid_generic=None, document_type=None):
    collection = value.get("collection")
    if collection == "books":
        normalized_pid_generic = pid_generic or value.get("pid_generic")
        title_pid_generic = extract_title_pid_generic(value, fallback=normalized_pid_generic)
        base_document = {
            "collection": collection,
            "source": _build_books_source(value.get("source")),
            "document_type": document_type or value.get("document_type"),
            "scielo_document_type": document_type or value.get("document_type"),
            "metric_scope": metric_scope or "item",
            "counter_data_type": "Book" if metric_scope == "title" else "Book_Segment",
            "parent_data_type": "Book" if metric_scope != "title" else None,
            "title_pid_generic": title_pid_generic,
            "pid": normalized_pid_generic,
            "pid_generic": normalized_pid_generic,
            "publication_year": value.get("publication_year"),
            "counter_access_type": value.get("counter_access_type") or "Open",
            "access_method": value.get("access_method") or "Regular",
            "total_requests": 0,
            "total_investigations": 0,
            "unique_requests": 0,
            "unique_investigations": 0,
        }
        _apply_access_fields(base_document, value, granularity)
        if granularity == "year":
            base_document["content_language"] = value.get("content_language")
            base_document["access_country_code"] = value.get("access_country_code")
        return base_document

    base_document = {
        "collection": collection,
        "source": _build_standard_source(value.get("source")),
        "document_type": value.get("document_type"),
        "scielo_document_type": value.get("document_type"),
        "metric_scope": "item",
        "counter_data_type": counter_data_type(value.get("document_type")),
        "parent_data_type": parent_data_type(
            value.get("document_type"),
            (value.get("source") or {}).get("source_type"),
        ),
        "article_version": article_version(value.get("document_type")),
        "pid": value.get("pid_v3") or value.get("pid_v2") or value.get("pid_generic"),
        "pid_v2": value.get("pid_v2"),
        "pid_v3": value.get("pid_v3"),
        "pid_generic": value.get("pid_generic"),
        "publication_year": value.get("publication_year"),
        "counter_access_type": value.get("counter_access_type") or "Open",
        "access_method": value.get("access_method") or "Regular",
        "total_requests": 0,
        "total_investigations": 0,
        "unique_requests": 0,
        "unique_investigations": 0,
    }
    _apply_access_fields(base_document, value, granularity)
    if granularity == "year":
        base_document["content_language"] = value.get("content_language")
        base_document["access_country_code"] = value.get("access_country_code")
    return base_document


def _apply_access_fields(base_document, value, granularity):
    if granularity == "month":
        base_document["access_month"] = value.get("access_date", "")[:7] if value.get("access_date") else ""
        day = value.get("access_date", "")[-2:] if value.get("access_date") else "01"
        base_document["daily_metrics"] = {
            day: {
                "total_requests": 0,
                "total_investigations": 0,
                "unique_requests": 0,
                "unique_investigations": 0,
            }
        }
        return

    base_document["access_year"] = value.get("access_year")


def _build_books_source(source):
    source = source or {}
    identifiers = source.get("identifiers") or {}
    compact_identifiers = {
        key: value
        for key, value in identifiers.items()
        if key in {"book_id", "isbn", "eisbn", "doi"} and value not in (None, "", [], {}, ())
    }

    return {
        "source_type": source.get("source_type"),
        "source_id": source.get("source_id"),
        "main_title": source.get("main_title"),
        "access_type": source.get("access_type"),
        "publisher": source.get("publisher_name"),
        "city": source.get("city"),
        "country": source.get("country"),
        "identifiers": compact_identifiers,
    }


def _build_standard_source(source):
    source = source or {}
    identifiers = source.get("identifiers") or {}
    compact_identifiers = {
        key: value
        for key, value in identifiers.items()
        if value not in (None, "", [], {}, ())
    }

    return {
        "source_type": source.get("source_type"),
        "source_id": source.get("source_id"),
        "scielo_issn": source.get("scielo_issn"),
        "main_title": source.get("main_title"),
        "acronym": source.get("acronym"),
        "publisher_name": source.get("publisher_name"),
        "subject_area_capes": source.get("subject_area_capes"),
        "subject_area_wos": source.get("subject_area_wos"),
        "access_type": source.get("access_type"),
        "city": source.get("city"),
        "country": source.get("country"),
        "identifiers": compact_identifiers,
    }
