import logging

from django.utils.translation import gettext as _
from django.conf import settings

from collection.models import Collection
from config import celery_app
from core.collectors import articlemeta as articlemeta_collector
from core.collectors import scielo_books as scielo_books_collector
from core.utils.request_utils import _get_user
from source.services import books as books_service
from source.services import journals as journal_service


def load_sources_from_article_meta(
    collections=None,
    force_update=True,
    user=None,
    mode="thrift",
):
    collection_codes = collections or Collection.acron3_list()

    for collection_code in collection_codes:
        logging.info(
            "Loading sources from Article Meta. Collection: %s, Mode: %s",
            collection_code,
            mode,
        )

        for journal in articlemeta_collector.iter_journals(
            collection=collection_code,
            mode=mode,
        ):
            collection = journal_service.get_collection(journal.collection_acronym)
            if not collection:
                logging.error(
                    "Collection %s does not exist",
                    journal.collection_acronym,
                )
                continue

            source = journal_service.upsert_journal_source(
                journal,
                collection=collection,
                user=user,
                force_update=force_update,
                load_mode=mode,
            )
            logging.info(
                "Source %s upserted for collection %s",
                source.source_id if source else None,
                collection.acron3,
            )

    return True


def load_sources_from_scielo_books(
    collection="books",
    db_name=settings.SCIELO_BOOKS_DB_NAME,
    since=0,
    limit=settings.SCIELO_BOOKS_LIMIT,
    force_update=True,
    headers=None,
    base_url=None,
    user=None,
):
    collection_obj = books_service.get_books_collection(collection)

    logging.info(
        "Loading sources from SciELO Books. Collection: %s, DB: %s, Since: %s, Limit: %s",
        collection,
        db_name,
        since,
        limit,
    )

    for item in scielo_books_collector.iter_change_documents(
        base_url=base_url,
        db_name=db_name,
        since=since,
        limit=limit,
        headers=headers,
    ):
        change = item["change"]

        if item["deleted"]:
            books_service.delete_book_source(collection_obj, change.get("id"))
            continue

        payload = item["payload"] or {}
        if payload.get("TYPE") != "Monograph":
            continue

        books_service.upsert_monograph_source(
            payload,
            collection=collection_obj,
            user=user,
            force_update=force_update,
            source_url=item.get("source_url"),
            last_seq=change.get("seq"),
        )

    return True


@celery_app.task(bind=True, name=_("[Metadata] Sync Sources (Article Meta)"), queue="load")
def task_load_sources_from_article_meta(
    self,
    collections=None,
    force_update=True,
    user_id=None,
    username=None,
    mode="thrift",
):
    user = _get_user(self.request, username=username, user_id=user_id)
    return load_sources_from_article_meta(
        collections=collections,
        force_update=force_update,
        user=user,
        mode=mode,
    )


@celery_app.task(bind=True, name=_("[Metadata] Sync Sources (SciELO Books)"), queue="load")
def task_load_sources_from_scielo_books(
    self,
    collection="books",
    db_name=settings.SCIELO_BOOKS_DB_NAME,
    since=0,
    limit=settings.SCIELO_BOOKS_LIMIT,
    force_update=True,
    headers=None,
    base_url=None,
    user_id=None,
    username=None,
):
    user = _get_user(self.request, username=username, user_id=user_id)
    return load_sources_from_scielo_books(
        collection=collection,
        db_name=db_name,
        since=since,
        limit=limit,
        force_update=force_update,
        headers=headers,
        base_url=base_url,
        user=user,
    )
