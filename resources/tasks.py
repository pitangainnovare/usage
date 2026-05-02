import logging

from django.utils.translation import gettext as _

from config import celery_app

from . import constants, models, utils

@celery_app.task(bind=True, name=_('[Resources] Load Robots Data'))
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
        url_robots = constants.DEFAULT_COUNTER_ROBOTS_URL
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


@celery_app.task(bind=True, name=_('[Resources] Load Geolocation Data'))
def task_load_geoip(self, url_geoip=None, validate=True):
    """
    Load GeoIP data from a specified URL, validate it, and save it to the database.
    Args:
        url_geoip (str, optional): The URL to download the GeoIP data from. Defaults to None.
        validate (bool, optional): Whether to validate the GeoIP data. Defaults to True.
    Returns:
        bool: True if the GeoIP data was successfully loaded and saved, False otherwise.
    Raises:
        Exception: If there is an error downloading, decompressing, or validating the GeoIP data.
    """

    if not url_geoip:
        url_geoip = constants.DEFAULT_MMDB_URL
        logging.warning(f'No GeoIP URL provided. Using default: {url_geoip}')
                                                                  
    try:
        data = utils.fetch_data(url_geoip, data_type='content')
    except Exception as e:
        logging.error(f'Error downloading GeoIP: {e}')
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
        mmdb_obj.url = url_geoip or constants.DEFAULT_MMDB_URL

    mmdb_obj.save()
    logging.debug(f'GeoIP data has been saved: {mmdb_obj}')
    
    return True
