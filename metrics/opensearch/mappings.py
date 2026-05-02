YEAR_INDEX_MAPPINGS = {
    "properties": {
        "collection": {"type": "keyword"},
        "source": {
            "properties": {
                "source_type": {"type": "keyword"},
                "source_id": {"type": "keyword"},
                "scielo_issn": {"type": "keyword"},
                "main_title": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 512
                        }
                    }
                },
                "subject_area_capes": {"type": "keyword"},
                "subject_area_wos": {"type": "keyword"},
                "acronym": {"type": "keyword"},
                "publisher_name": {"type": "keyword"},
                "access_type": {"type": "keyword"},
                "city": {"type": "keyword"},
                "country": {"type": "keyword"},
                "identifiers": {"type": "object"},
            }
        },
        "document_type": {"type": "keyword"},
        "scielo_document_type": {"type": "keyword"},
        "metric_scope": {"type": "keyword"},
        "counter_data_type": {"type": "keyword"},
        "parent_data_type": {"type": "keyword"},
        "article_version": {"type": "keyword"},
        "pid": {"type": "keyword"},
        "pid_v2": {"type": "keyword"},
        "pid_v3": {"type": "keyword"},
        "pid_generic": {"type": "keyword"},
        "publication_year": {"type": "integer"},
        "counter_access_type": {"type": "keyword"},
        "access_method": {"type": "keyword"},
        "access_year": {"type": "date", "format": "yyyy"},
        "access_country_code": {"type": "keyword"},
        "content_language": {"type": "keyword"},
        "applied_jobs": {"type": "keyword", "index": False},
        "total_requests": {"type": "integer"},
        "total_investigations": {"type": "integer"},
        "unique_requests": {"type": "integer"},
        "unique_investigations": {"type": "integer"},
    }
}


MONTH_INDEX_MAPPINGS = {
    "properties": {
        "collection": {"type": "keyword"},
        "source": YEAR_INDEX_MAPPINGS["properties"]["source"],
        "document_type": {"type": "keyword"},
        "scielo_document_type": {"type": "keyword"},
        "metric_scope": {"type": "keyword"},
        "counter_data_type": {"type": "keyword"},
        "parent_data_type": {"type": "keyword"},
        "article_version": {"type": "keyword"},
        "pid": {"type": "keyword"},
        "pid_v2": {"type": "keyword"},
        "pid_v3": {"type": "keyword"},
        "pid_generic": {"type": "keyword"},
        "publication_year": {"type": "integer"},
        "counter_access_type": {"type": "keyword"},
        "access_method": {"type": "keyword"},
        "access_month": {"type": "date", "format": "yyyy-MM"},
        "applied_jobs": {"type": "keyword", "index": False},
        "daily_metrics": {"type": "object", "dynamic": True},
        "total_requests": {"type": "integer"},
        "total_investigations": {"type": "integer"},
        "unique_requests": {"type": "integer"},
        "unique_investigations": {"type": "integer"},
    }
}


BOOKS_YEAR_INDEX_MAPPINGS = {
    "properties": {
        "collection": {"type": "keyword"},
        "source": {
            "properties": {
                "source_type": {"type": "keyword"},
                "source_id": {"type": "keyword"},
                "main_title": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 512
                        }
                    }
                },
                "access_type": {"type": "keyword"},
                "publisher": {"type": "keyword"},
                "city": {"type": "keyword"},
                "country": {"type": "keyword"},
                "identifiers": {
                    "properties": {
                        "book_id": {"type": "keyword"},
                        "isbn": {"type": "keyword"},
                        "eisbn": {"type": "keyword"},
                        "doi": {"type": "keyword"},
                    }
                },
            }
        },
        "document_type": {"type": "keyword"},
        "scielo_document_type": {"type": "keyword"},
        "metric_scope": {"type": "keyword"},
        "counter_data_type": {"type": "keyword"},
        "parent_data_type": {"type": "keyword"},
        "article_version": {"type": "keyword"},
        "pid": {"type": "keyword"},
        "pid_generic": {"type": "keyword"},
        "title_pid_generic": {"type": "keyword"},
        "publication_year": {"type": "integer"},
        "counter_access_type": {"type": "keyword"},
        "access_method": {"type": "keyword"},
        "access_year": {"type": "date", "format": "yyyy"},
        "access_country_code": {"type": "keyword"},
        "content_language": {"type": "keyword"},
        "applied_jobs": {"type": "keyword", "index": False},
        "total_requests": {"type": "integer"},
        "total_investigations": {"type": "integer"},
        "unique_requests": {"type": "integer"},
        "unique_investigations": {"type": "integer"},
    }
}


BOOKS_MONTH_INDEX_MAPPINGS = {
    "properties": {
        "collection": {"type": "keyword"},
        "source": BOOKS_YEAR_INDEX_MAPPINGS["properties"]["source"],
        "document_type": {"type": "keyword"},
        "scielo_document_type": {"type": "keyword"},
        "metric_scope": {"type": "keyword"},
        "counter_data_type": {"type": "keyword"},
        "parent_data_type": {"type": "keyword"},
        "article_version": {"type": "keyword"},
        "pid": {"type": "keyword"},
        "pid_generic": {"type": "keyword"},
        "title_pid_generic": {"type": "keyword"},
        "publication_year": {"type": "integer"},
        "counter_access_type": {"type": "keyword"},
        "access_method": {"type": "keyword"},
        "access_month": {"type": "date", "format": "yyyy-MM"},
        "applied_jobs": {"type": "keyword", "index": False},
        "daily_metrics": {"type": "object", "dynamic": True},
        "total_requests": {"type": "integer"},
        "total_investigations": {"type": "integer"},
        "unique_requests": {"type": "integer"},
        "unique_investigations": {"type": "integer"},
    }
}


METRIC_FIELDS = (
    "total_requests",
    "total_investigations",
    "unique_requests",
    "unique_investigations",
)


def get_index_mappings(collection, granularity):
    if granularity not in {"month", "year"}:
        raise ValueError("Granularity must be 'month' or 'year'.")

    if collection == "books":
        return BOOKS_MONTH_INDEX_MAPPINGS if granularity == "month" else BOOKS_YEAR_INDEX_MAPPINGS

    return MONTH_INDEX_MAPPINGS if granularity == "month" else YEAR_INDEX_MAPPINGS
