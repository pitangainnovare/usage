from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.utils.translation import gettext as _

from core.utils.utils import _get_user
from config import celery_app
from tracker.models import Top100ArticlesFileEvent, ArticleLangByCountryFileEvent

from .exceptions import (
    Top100ArticlesFileNotFoundError, 
    Top100ArticlesFileAttachmentNotFoundError,
    Top100ArticlesFileAttachmentInvalidFormatError,
    ArticleLangByCountryFileAttachmentInvalidFormatError,
    ArticleLangByCountryFileAttachmentNotFoundError,
    ArticleLangByCountryFileNotFoundError,
)
from .models import Top100Articles, Top100ArticlesFile, ArticleLangByCountry, ArticleLangByCountryFile
from .utils import get_load_data_function


User = get_user_model()


@celery_app.task(bind=True, name=_('Process File for Top100 Article Metrics'), timelimit=-1)
def task_process_top100_file(self, file_id=None, bulk_size=2500, user_id=None, username=None):
    """
    Process a file to create or update `Top100Articles`.

    Parameters:
        file_id (int, optional): Specific file ID to process.
        bulk_size (int): Number of records to process per batch.
        user_id (int, optional): User ID for context.
        username (str, optional): Username for context.
    """    
    top100_files = Top100ArticlesFile.objects.filter(
        pk=file_id) if file_id else Top100ArticlesFile.objects.filter(status=Top100ArticlesFile.Status.QUEUED).order_by('-created')

    for obj_file in top100_files:
        obj_file.status = Top100ArticlesFile.Status.PARSING
        obj_file.save()
        task_process_top100_file_item.apply_async(args=(obj_file.pk, bulk_size, user_id, username))


@celery_app.task(bind=True, name=_('Process File for ArticleLang by Country Metrics'), timelimit=-1)
def task_process_article_lang_country_file(self, file_id=None, bulk_size=10000, user_id=None, username=None):
    """
    Process a file to create or update `ArticleLangByCountry`.

    Parameters:
        file_id (int, optional): Specific file ID to process.
        bulk_size (int): Number of records to process per batch.
        user_id (int, optional): User ID for context.
        username (str, optional): Username for context.
    """    
    files = ArticleLangByCountryFile.objects.filter(
        pk=file_id) if file_id else ArticleLangByCountryFile.objects.filter(status=ArticleLangByCountryFile.Status.QUEUED).order_by('-created')

    for obj_file in files:
        obj_file.status = ArticleLangByCountryFile.Status.PARSING
        obj_file.save()
        task_process_article_lang_country_file_item.apply_async(args=(obj_file.pk, bulk_size, user_id, username))


@celery_app.task(bind=True, name=_('Process File Item for ArticleLangByCountry Metrics'), timelimit=-1)
def task_process_article_lang_country_file_item(self, file_id, bulk_size=2500, user_id=None, username=None):
    """
    Process items in a file to create or update `ArticleLangByCountry`.

    Parameters:
        file_id (int): ID of the file to process.
        bulk_size (int): Number of records per batch.
        user_id (int, optional): ID of the user performing the action.
        username (str, optional): Username of the user performing the action.
    """
    user = _get_user(self.request, username=username, user_id=user_id)

    try:
        obj_file = ArticleLangByCountryFile.objects.get(pk=file_id)
    except ArticleLangByCountryFile.DoesNotExist:
        obj_file.status = ArticleLangByCountryFile.Status.ERROR
        obj_file.save()
        raise ArticleLangByCountryFileNotFoundError(f'ArticleLangByCountryFile with id {file_id} does not exist.')

    try:
        file_path = obj_file.attachment.file.path
    except AttributeError:
        obj_file.status = ArticleLangByCountryFile.Status.ERROR
        obj_file.save()
        ArticleLangByCountryFileEvent.create_or_update(
            user=user,
            file=obj_file,
            status=obj_file.status,
            lines=0,
            message=f'Attachment related to {file_id} does not exist.',
        )
        raise ArticleLangByCountryFileAttachmentNotFoundError(f'Attachment related to {file_id} does not exist.')

    load_data_function = get_load_data_function(file_path)
    if not load_data_function:
        obj_file.status = ArticleLangByCountryFile.Status.ERROR
        obj_file.save()
        ArticleLangByCountryFileEvent.create_or_update(
            user=user,
            file=obj_file,
            status=obj_file.status,
            lines=0,
            message=f'File {file_id} does not have a valid format.',
        )
        raise ArticleLangByCountryFileAttachmentInvalidFormatError(f'File {file_id} does not have a valid format.')
    
    _process_article_lang_by_country_file_item(user, obj_file, bulk_size, file_path, load_data_function)


@celery_app.task(bind=True, name=_('Process File Item for Top100 Article Metrics'), timelimit=-1)
def task_process_top100_file_item(self, file_id, bulk_size=2500, user_id=None, username=None):
    """
    Process items in a file to create or update `Top100Articles`.

    Parameters:
        file_id (int): ID of the file to process.
        bulk_size (int): Number of records per batch.
        user_id (int, optional): ID of the user performing the action.
        username (str, optional): Username of the user performing the action.
    """
    user = _get_user(self.request, username=username, user_id=user_id)

    try:
        obj_file = Top100ArticlesFile.objects.get(pk=file_id)
    except Top100ArticlesFile.DoesNotExist:
        obj_file.status = Top100ArticlesFile.Status.ERROR
        obj_file.save()
        raise Top100ArticlesFileNotFoundError(f'Top100ArticlesFile with id {file_id} does not exist.')

    try:
        file_path = obj_file.attachment.file.path
    except AttributeError:
        obj_file.status = Top100ArticlesFile.Status.ERROR
        obj_file.save()
        Top100ArticlesFileEvent.create_or_update(
            user=user,
            file=obj_file,
            status=obj_file.status,
            lines=0,
            message=f'Attachment related to {file_id} does not exist.',
        )
        raise Top100ArticlesFileAttachmentNotFoundError(f'Attachment related to {file_id} does not exist.')

    load_data_function = get_load_data_function(file_path)
    if not load_data_function:
        obj_file.status = Top100ArticlesFile.Status.ERROR
        obj_file.save()
        Top100ArticlesFileEvent.create_or_update(
            user=user,
            file=obj_file,
            status=obj_file.status,
            lines=0,
            message=f'File {file_id} does not have a valid format.',
        )
        raise Top100ArticlesFileAttachmentInvalidFormatError(f'File {file_id} does not have a valid format.')
    
    _process_top100_file_item(user, obj_file, bulk_size, file_path, load_data_function)


def _process_article_lang_by_country_file_item(user, obj_file, bulk_size, file_path, load_data_function):
    objs_create, objs_update = [], []
    lines = 0

    try:
        for row in load_data_function(file_path):
            obj, created = ArticleLangByCountry.create_or_update(user=user, save=False, **row)
            if created:
                objs_create.append(obj)
            else:
                objs_update.append(obj)

            if len(objs_create) >= bulk_size:
                ArticleLangByCountry.bulk_create(objs_create)
                objs_create = []
                lines += len(objs_create)

            if len(objs_update) >= bulk_size:
                ArticleLangByCountry.bulk_update(objs_update)
                objs_update = []

        if objs_create:
            ArticleLangByCountry.bulk_create(objs_create)
            lines += len(objs_create)
    
        if objs_update:
            ArticleLangByCountry.bulk_update(objs_update)
    
    except Exception as e:
        obj_file.status = ArticleLangByCountryFile.Status.ERROR
        ArticleLangByCountryFileEvent.create_or_update(
            user=user,
            file=obj_file,
            status=obj_file.status,
            lines=lines,
            message=str(e),
        )        
    else:
        obj_file.status = ArticleLangByCountryFile.Status.PROCESSED
        ArticleLangByCountryFileEvent.create_or_update(
            user=user,
            file=obj_file,
            status=obj_file.status,
            lines=lines,
            message='File processed successfully.',
        )
    finally:
        obj_file.save()


def _process_top100_file_item(user, obj_file, bulk_size, file_path, load_data_function):
    objs_create, objs_update = [], []
    lines = 0

    try:
        for row in load_data_function(file_path):
            obj_top100, created = Top100Articles.create_or_update(user=user, save=False, **row)
            if created:
                objs_create.append(obj_top100)
            else:
                objs_update.append(obj_top100)

            if len(objs_create) >= bulk_size:
                Top100Articles.bulk_create(objs_create)
                objs_create = []
                lines += len(objs_create)

            if len(objs_update) >= bulk_size:
                Top100Articles.bulk_update(objs_update)
                objs_update = []

        if objs_create:
            Top100Articles.bulk_create(objs_create)
            lines += len(objs_create)
    
        if objs_update:
            Top100Articles.bulk_update(objs_update)
    
    except Exception as e:
        obj_file.status = Top100ArticlesFile.Status.ERROR
        Top100ArticlesFileEvent.create_or_update(
            user=user,
            file=obj_file,
            status=obj_file.status,
            lines=lines,
            message=str(e),
        )        
    else:
        obj_file.status = Top100ArticlesFile.Status.PROCESSED
        Top100ArticlesFileEvent.create_or_update(
            user=user,
            file=obj_file,
            status=obj_file.status,
            lines=lines,
            message='File processed successfully.',
        )
    finally:
        obj_file.save()


@celery_app.task(bind=True, name=_('Rebuild Index'), timelimit=-1)
def rebuild_index(self, index_name, user_id=None, username=None):
    """Celery task to rebuild the informed index (metrics)."""
    
    user = _get_user(self.request, username=username, user_id=user_id)
    call_command('rebuild_index', f'--using={index_name}', '--noinput')


@celery_app.task(bind=True, name=_('Update Index'), timelimit=-1)
def update_index(self, index_name, model_name, user_id=None, username=None):
    """Celery task to update the informed index (metrics) with respect the model_name (Top100Articles)."""
    
    user = _get_user(self.request, username=username, user_id=user_id)
    call_command('update_index', f'--using={index_name}', f'{index_name}.{model_name}')
