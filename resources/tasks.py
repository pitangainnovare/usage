import logging

from django.conf import settings

from config import celery_app

from . import models, utils


@celery_app.task(bind=True, name='[Resources] Load Robots Data')
def task_load_robots(self, url_robots=None):
    """
    Load robots from a given URL and save them to the database.
    This function fetches robot data from a specified URL (or a default URL if none is provided),
    cleans the data, and saves it to the database. If the robots already exist in the database,
    their information is updated.
    Args:
        url_robots (str, optional): The URL to fetch the robots data from. Defaults to None.
        user_id (int, optional): The ID of the user performing the task. Defaults to None.
        username (str, optional): The username of the user performing the task. Defaults to None.
    Returns:
        bool: True if the robots were successfully loaded and saved, False otherwise.
    Raises:
        Exception: If there is an error fetching or saving the robots data.
    Logs:
        - Warning if no robots URL is provided.
        - Error if there is an issue downloading or saving the robots.
        - Debug information for each robot saved.
    """
    if not url_robots:
        url_robots = settings.COUNTER_ROBOTS_URL
        logging.warning(f'No robots URL provided. Using default: {url_robots}')

    try:
        robots_data = utils.fetch_data(url_robots, data_type='json')
    except Exception as e:
        logging.error(f'Error downloading robots: {e}')
        return False

    cleaned_robots_data = utils.clean_robots_list(robots_data)
    fetched_patterns = set()

    try:
        for r_str in cleaned_robots_data:
            pattern = r_str.get('pattern')
            last_changed = r_str.get('last_changed')
            fetched_patterns.add(pattern)

            r_obj = models.RobotUserAgent.objects.filter(pattern=pattern).first()
            created = r_obj is None

            if created:
                r_obj = models.RobotUserAgent(
                    pattern=pattern,
                    source_counter=True,
                    source_scielo=False,
                )
            r_obj.source_counter = True
            r_obj.is_active = True
            r_obj.source_url = url_robots
            r_obj.last_changed = last_changed

            r_obj.save()
            logging.debug(f'Robot saved: {r_obj}')

        stale_counter_patterns = models.RobotUserAgent.objects.filter(
            source_counter=True
        ).exclude(pattern__in=fetched_patterns)

        for r_obj in stale_counter_patterns:
            r_obj.source_counter = False
            r_obj.source_url = None
            r_obj.last_changed = None
            if not r_obj.source_scielo:
                r_obj.is_active = False
            r_obj.save()
            logging.debug(f'Robot deactivated or detached from COUNTER source: {r_obj}')

        return True

    except Exception as e:
        logging.error(f'Error saving robots: {e}')
        return False


@celery_app.task(bind=True, name='[Resources] Load Geolocation Data')
def task_load_geoip(self, url_geoip=None, validate=True):
    """
    Load GeoIP data from a specified URL, validate it, and save it to the database.

    When ``url_geoip`` is not provided the task resolves the URL automatically:
    it tries the current month first and, if the file is not yet available,
    falls back to the previous month.

    Args:
        url_geoip (str, optional): Explicit URL to download. Defaults to None
            (auto-resolved for the current/previous month).
        validate (bool, optional): Whether to validate the GeoIP data. Defaults to True.
    Returns:
        bool: True if the GeoIP data was successfully loaded and saved, False otherwise.
    """
    if url_geoip:
        candidates = [url_geoip]
    else:
        candidates = utils.resolve_mmdb_url()
        logging.info('No GeoIP URL provided. Will try candidates: %s', candidates)

    data = None
    resolved_url = None
    for url in candidates:
        try:
            data = utils.fetch_data(url, data_type='content')
            resolved_url = url
            logging.info('GeoIP data downloaded from: %s', url)
            break
        except Exception as e:
            logging.warning(
                'Failed to download GeoIP from %s: %s. Trying next candidate.', url, e
            )

    if data is None:
        logging.error(
            'Could not download GeoIP data from any candidate URL: %s', candidates
        )
        return False

    try:
        mmdb_data = utils.decompress_gzip(data)
    except Exception as e:
        logging.error(f'Error decompressing GeoIP data: {e}')
        return False

    if validate:
        try:
            utils.validate_geoip_data(mmdb_data)
        except Exception as e:
            logging.error(f'Error validating GeoIP data: {e}')
            return False

    mmdb_hash = models.MMDB.compute_hash(mmdb_data)

    try:
        mmdb_obj = models.MMDB.objects.get(id=mmdb_hash)
        logging.debug(f'GeoIP data already exists: {mmdb_obj}')

    except models.MMDB.DoesNotExist:
        mmdb_obj = models.MMDB.objects.create(id=mmdb_hash, data=mmdb_data)
        mmdb_obj.url = resolved_url

    mmdb_obj.save()
    logging.info('GeoIP data saved (url=%s, hash=%s)', resolved_url, mmdb_hash)

    return True
