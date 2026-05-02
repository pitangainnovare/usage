from django.conf import settings
from sickle import Sickle

from core.utils import standardizer


def iter_records(from_date, until_date):
    oai_client = Sickle(
        endpoint=settings.OAI_PMH_PREPRINT_ENDPOINT,
        max_retries=settings.OAI_PMH_MAX_RETRIES,
        verify=False,
    )
    records = oai_client.ListRecords(
        **{
            "metadataPrefix": settings.OAI_METADATA_PREFIX,
            "from": from_date,
            "until": until_date,
        }
    )

    for record in records:
        yield record


def extract_record_data(record):
    pid_generic = _extract_compatible_identifier(record.header.identifier)
    text_langs = [
        standardizer.standardize_language_code(language)
        for language in record.metadata.get("language", [])
    ]
    publication_date = record.metadata.get("date", [""])[0]
    default_language = text_langs[0] if text_langs else ""
    publication_year = _extract_publication_year_from_date(publication_date)

    return {
        "pid_generic": pid_generic,
        "text_langs": text_langs,
        "publication_date": publication_date,
        "default_language": default_language,
        "publication_year": publication_year,
    }


def _extract_compatible_identifier(identifier):
    try:
        return identifier.split(":")[-1].split("/")[1]
    except IndexError:
        return ""


def _extract_publication_year_from_date(date_str):
    try:
        return date_str[:4]
    except IndexError:
        return ""
