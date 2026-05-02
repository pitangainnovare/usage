import logging

import requests
from django.conf import settings

from core.utils import standardizer


def _request_json(url):
    try:
        response = requests.get(url, timeout=settings.DATAVERSE_SLEEP_TIME)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        logging.error("Error fetching %s: %s", url, exc)
        return {}


def _get_subdataverses():
    url = f"{settings.DATAVERSE_ENDPOINT}/dataverses/{settings.DATAVERSE_ROOT_COLLECTION}/contents"
    return _request_json(url).get("data", [])


def _get_datasets(subdataverse_id):
    url = f"{settings.DATAVERSE_ENDPOINT}/dataverses/{subdataverse_id}/contents"
    return _request_json(url).get("data", [])


def _get_files(dataset_id):
    url = f"{settings.DATAVERSE_ENDPOINT}/datasets/{dataset_id}/versions/:latest/files"
    return _request_json(url).get("data", [])


def iter_dataset_metadata(from_date=None, until_date=None):
    for subdataverse in _get_subdataverses():
        if subdataverse.get("type") != "dataverse":
            continue

        subdataverse_id = subdataverse["id"]
        subdataverse_title = subdataverse["title"]

        for dataset in _get_datasets(subdataverse_id):
            if dataset.get("type") != "dataset":
                continue

            dataset_id = dataset["id"]
            doi = standardizer.standardize_doi(dataset.get("persistentUrl"))
            if not doi:
                logging.warning("Dataset %s does not have a DOI.", dataset_id)
                continue

            publication_date = dataset.get("publicationDate")
            if publication_date:
                if (from_date and publication_date < from_date) or (
                    until_date and publication_date > until_date
                ):
                    continue

            for file_data in _get_files(dataset_id):
                file_persistent_id = file_data["dataFile"].get("persistentId")
                standardized_persistent_id = (
                    standardizer.standardize_pid_generic(file_persistent_id)
                    if file_persistent_id
                    else None
                )

                yield {
                    "title": subdataverse_title,
                    "dataset_doi": doi,
                    "dataset_published": publication_date,
                    "file_id": file_data["dataFile"]["id"],
                    "file_name": file_data["label"],
                    "file_url": f"{settings.DATAVERSE_ENDPOINT}/access/datafile/{file_data['dataFile']['id']}",
                    "file_persistent_id": standardized_persistent_id,
                }
