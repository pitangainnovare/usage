from scielo_usage_counter.counter import get_valid_clicks, is_request


def apply_unique_metrics(
    document,
    unique_state,
    scope,
    document_id,
    user_session_id,
    is_request_event,
):
    if not user_session_id:
        return

    inv_bucket = unique_state[f"{scope}_investigations"]
    inv_key = (document_id, user_session_id)
    add_investigation = inv_key not in inv_bucket
    if add_investigation:
        inv_bucket.add(inv_key)

    add_request = False
    if is_request_event:
        req_bucket = unique_state[f"{scope}_requests"]
        req_key = (document_id, user_session_id)
        add_request = req_key not in req_bucket
        if add_request:
            req_bucket.add(req_key)

    increment_document_uniques(
        document=document,
        add_investigation=add_investigation,
        add_request=add_request,
    )


def increment_document_totals(document, click_timestamps, content_type, click_timestamps_by_url=None):
    number_of_clicks = _count_valid_clicks(
        click_timestamps=click_timestamps,
        click_timestamps_by_url=click_timestamps_by_url,
    )

    document["total_investigations"] += number_of_clicks
    if is_request(content_type):
        document["total_requests"] += number_of_clicks

    if "daily_metrics" in document:
        day_key = list(document["daily_metrics"].keys())[0]
        document["daily_metrics"][day_key]["total_investigations"] += number_of_clicks
        if is_request(content_type):
            document["daily_metrics"][day_key]["total_requests"] += number_of_clicks


def _count_valid_clicks(click_timestamps, click_timestamps_by_url=None):
    if isinstance(click_timestamps_by_url, dict) and click_timestamps_by_url:
        return sum(
            get_valid_clicks(timestamps or {})
            for timestamps in click_timestamps_by_url.values()
        )
    return get_valid_clicks(click_timestamps or {})


def increment_document_uniques(document, add_investigation=False, add_request=False):
    if add_investigation:
        document["unique_investigations"] += 1
    if add_request:
        document["unique_requests"] += 1

    if "daily_metrics" in document:
        day_key = list(document["daily_metrics"].keys())[0]
        if add_investigation:
            document["daily_metrics"][day_key]["unique_investigations"] += 1
        if add_request:
            document["daily_metrics"][day_key]["unique_requests"] += 1


def counter_data_type(document_type):
    if document_type == "dataset":
        return "Dataset"
    if document_type in {"article", "preprint"}:
        return "Article"
    if document_type == "book":
        return "Book"
    if document_type == "chapter":
        return "Book_Segment"
    return "Other"


def parent_data_type(document_type, source_type=None):
    if document_type == "chapter":
        return "Book"
    if document_type == "article" and source_type == "journal":
        return "Journal"
    return None


def article_version(document_type):
    if document_type == "preprint":
        return "Preprint"
    return None


def should_create_book_item_document(value):
    if not value.get("pid_generic"):
        return False
    if value.get("document_type") == "book" and not is_request(value.get("content_type")):
        return False
    return True


def extract_title_pid_generic(value, fallback=None):
    title_pid_generic = value.get("title_pid_generic")
    if title_pid_generic:
        return title_pid_generic

    pid_generic = value.get("pid_generic")
    if "/CHAPTER:" in (pid_generic or "").upper():
        return pid_generic.upper().split("/CHAPTER:")[0]

    source = value.get("source") or {}
    source_id = source.get("source_id")
    if source_id:
        return f"BOOK:{str(source_id).upper()}"

    return fallback
