import logging

from django.conf import settings

from log_manager.models import LogFile
from resources.models import MMDB, RobotUserAgent

from metrics import opensearch


def extract_celery_queue_name(collection_acronym):
    return f"parse_{settings.COLLECTION_ACRON3_SIZE_MAP.get(collection_acronym, 'small')}"


def fetch_required_resources(robot_source=None):
    robots_list = RobotUserAgent.get_patterns(source=robot_source)
    if not robots_list:
        logging.error(
            "There are no robots available in the database for source %s.",
            RobotUserAgent.normalize_source(robot_source),
        )
        return None, None

    try:
        mmdb = MMDB.objects.latest("created")
    except MMDB.DoesNotExist:
        logging.error("There are no MMDB files available in the database.")
        return None, None

    return robots_list, mmdb


def build_search_client():
    return opensearch.OpenSearchUsageClient(
        settings.OPENSEARCH_URL,
        settings.OPENSEARCH_BASIC_AUTH,
        settings.OPENSEARCH_API_KEY,
        settings.OPENSEARCH_VERIFY_CERTS,
    )


def get_log_files_for_collection_date(collection, access_date, status_filters=None):
    queryset = (
        LogFile.objects.filter(
            collection=collection,
            date=access_date,
        )
        .select_related("collection")
        .order_by("path", "hash")
    )
    if status_filters:
        queryset = queryset.filter(status__in=status_filters)

    return list(queryset)
