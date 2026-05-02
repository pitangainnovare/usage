def generate_user_session_id(client_name, client_version, ip_address, datetime, sep="|"):
    dt_year_month_day = datetime.strftime("%Y-%m-%d")
    dt_hour = datetime.strftime("%H")

    return sep.join(
        [
            str(client_name),
            str(client_version),
            str(ip_address),
            str(dt_year_month_day),
            str(dt_hour),
        ]
    )


def generate_item_access_id(
    col_acron3,
    source_key,
    pid_v2,
    pid_v3,
    pid_generic,
    user_session_id,
    access_country_code,
    content_language,
    media_format,
    content_type,
    sep="|",
):
    return sep.join(
        [
            col_acron3,
            str(source_key or ""),
            pid_v2 or "",
            pid_v3 or "",
            pid_generic or "",
            str(user_session_id or ""),
            str(access_country_code or ""),
            str(content_language or ""),
            str(media_format or ""),
            str(content_type or ""),
        ]
    )


def generate_month_document_id(
    collection: str,
    source_key: str,
    pid_v2: str,
    pid_v3: str,
    pid_generic: str,
    access_month: str,
    counter_access_type: str,
    access_method: str,
    publication_year: str,
    metric_scope: str = None,
) -> str:
    parts = []
    if metric_scope:
        parts.append(metric_scope)

    parts.extend(
        [
            str(collection or ""),
            str(source_key or ""),
            pid_v2 or "",
            pid_v3 or "",
            pid_generic or "",
            str(access_month or ""),
            str(counter_access_type or ""),
            str(access_method or ""),
            str(publication_year or ""),
        ]
    )
    return "|".join(parts)


def generate_year_document_id(
    collection: str,
    source_key: str,
    pid_v2: str,
    pid_v3: str,
    pid_generic: str,
    content_language: str,
    access_country_code: str,
    access_year: str,
    counter_access_type: str,
    access_method: str,
    publication_year: str,
    metric_scope: str = None,
) -> str:
    parts = []
    if metric_scope:
        parts.append(metric_scope)

    parts.extend(
        [
            str(collection or ""),
            str(source_key or ""),
            pid_v2 or "",
            pid_v3 or "",
            pid_generic or "",
            content_language or "",
            access_country_code or "",
            str(access_year or ""),
            str(counter_access_type or ""),
            str(access_method or ""),
            str(publication_year or ""),
        ]
    )
    return "|".join(parts)
