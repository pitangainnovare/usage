from datetime import datetime, timedelta

import hashlib
import logging

from scielo_log_validator import validator
from scielo_usage_counter import log


def get_date_offset_from_today(days=1):
    return (datetime.now() - timedelta(days=days)).date()


def formatted_text_to_datetime(text, format="%Y-%m-%d"):
    try:
        return datetime.strptime(text, format)
    except ValueError:
        raise


def date_range(start, end):
    if not isinstance(start, datetime):
        start = datetime.strptime(start, "%Y-%m-%d").date()
        
    if not isinstance(end, datetime):
        end = datetime.strptime(end, "%Y-%m-%d").date()

    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
        

def timestamp_to_date(timestamp):
    return datetime.fromtimestamp(timestamp).date()


def hash_file(path, num_lines=25):
    """
    Calculates the MD5 hash of a file using a combination of its first and last `num_lines` lines, 
    as well as its size.
    
    Args:
        path (str): The path to the file.
        num_lines (int): The number of lines to consider from the beginning and end of the file. Default is 25.

    Returns:
        The MD5 hash digest as a hexadecimal string.
    """
    md5_hash = hashlib.md5()

    with open(path, 'rb') as file:
        # Read the first `num_lines` lines of the file
        first_lines = b''.join([file.readline() for _ in range(num_lines)])
        md5_hash.update(first_lines)

        # Move the file pointer to the end of the file
        file.seek(0, 2)

        # Get the size of the file
        size = file.tell()
        md5_hash.update(str(size).encode())

        # Move the file pointer to the start of the file
        file.seek(-size, 2)

        # Read the last `num_lines` lines of the file
        last_lines = file.readlines()[-num_lines:]
        md5_hash.update(b''.join(last_lines))

    return md5_hash.hexdigest()


def validate_file(path, sample_size=0.05, buffer_size=2048, days_delta=5, apply_path_validation=True, apply_content_validation=True):
    return validator.pipeline_validate(
        path=path, 
        sample_size=sample_size,
        buffer_size=buffer_size,
        days_delta=days_delta,
        apply_path_validation=apply_path_validation,
        apply_content_validation=apply_content_validation,
    )


def parse_file(path_file, mmdb_data, robots_list):
    lp = log.LogParser(
        mmdb_data=mmdb_data, 
        robots_list=robots_list,
    )
    lp.logfile = path_file

    logging.info(f'INFO. LogParser has been started processing {lp.logfile}')
    for row in lp.parse():
        '''
        hit.local_datetime
        hit.client_name
        hit.client_version
        hit.ip
        hit.geolocation
        hit.action
        '''
        yield row

    # FIXME: It could be interesting to obtain the summary and other information from the logparser object.
    #  The idea is to save these information specifically in some new tracker database table.
