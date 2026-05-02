import gzip
import hashlib
from collections import deque

from scielo_log_validator import validator


def hash_file(path, num_lines=500):
    """
    Calculates the MD5 hash of a file using a combination of its first and last
    `num_lines` lines.

    For gzip-compressed files, the content is decompressed before hashing,
    so that different compressions of the same data produce the same hash.
    File size is intentionally NOT included because it varies between
    compressions and between growing log files, causing false duplicates.

    Args:
        path (str): The path to the file.
        num_lines (int): The number of lines to consider from the beginning
            and end of the file. Default is 500.

    Returns:
        The MD5 hash digest as a hexadecimal string.
    """
    md5_hash = hashlib.md5()

    opener = gzip.open if _is_gzip(path) else open

    with opener(path, 'rb') as file:
        first_lines = b''.join([file.readline() for _ in range(num_lines)])
        md5_hash.update(first_lines)

        tail = deque(maxlen=num_lines)
        for line in file:
            tail.append(line)
        md5_hash.update(b''.join(tail))

    return md5_hash.hexdigest()


def _is_gzip(path):
    with open(path, 'rb') as f:
        return f.read(2) == b'\x1f\x8b'

def validate_file(path, sample_size=0.1, buffer_size=2048, days_delta=5, apply_path_validation=True, apply_content_validation=True):
    return validator.pipeline_validate(
        path=path, 
        sample_size=sample_size,
        buffer_size=buffer_size,
        days_delta=days_delta,
        apply_path_validation=apply_path_validation,
        apply_content_validation=apply_content_validation,
    )
