import logging

from django.db import IntegrityError, models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from wagtail.admin.panels import FieldPanel
from wagtailautocomplete.edit_handlers import AutocompletePanel

from collection.models import Collection

from . import choices


class LogFile(models.Model):
    created = models.DateTimeField(verbose_name=_("Creation date"), auto_now_add=True)
    updated = models.DateTimeField(verbose_name=_("Last update date"), auto_now=True)
    date = models.DateField(verbose_name=_("Date"), null=True, blank=True, db_index=True)
    hash = models.CharField(_("Hash MD5"), max_length=32, null=True, blank=True, unique=True)

    path = models.CharField(_("Name"), max_length=255, null=False, blank=False)

    stat_result = models.JSONField(_("OS Stat Result"), null=False, blank=False)

    status = models.CharField(
        _("Status"), 
        choices=choices.LOG_FILE_STATUS, 
        max_length=3, 
        blank=False, 
        null=False,
    )

    validation = models.JSONField(
        _("Validation"), 
        null=True, 
        blank=True,
        default=dict,
    )

    summary = models.JSONField(
        _("Summary"),
        null=True,
        blank=True,
        default=dict,
    )

    collection = models.ForeignKey(
        Collection,
        verbose_name=_("Collection"),
        on_delete=models.DO_NOTHING,
        null=False,
        blank=False,
    )

    last_processed_line = models.IntegerField(
        _("Last Processed Line"),
        blank=True,
        default=0,
    )

    parse_heartbeat_at = models.DateTimeField(
        _("Parse Heartbeat At"),
        null=True,
        blank=True,
    )

    panels = [
        FieldPanel('hash'),
        FieldPanel('date'),
        FieldPanel('path'),
        FieldPanel('stat_result'),
        FieldPanel('status'),
        FieldPanel('validation'),
        FieldPanel('summary'),
        FieldPanel('last_processed_line'),
        FieldPanel('parse_heartbeat_at'),
        AutocompletePanel('collection'),
    ]

    class Meta:
        verbose_name = _("Log File")
        verbose_name_plural = _("Log Files")

    @classmethod
    def get(cls, hash):
        return cls.objects.get(hash=hash)

    @classmethod
    def create_or_update(cls, collection, path, stat_result, hash, status=None):
        try:
            obj, created = cls.objects.get_or_create(
                hash=hash,
                defaults={
                    "collection": collection,
                    "path": path,
                    "stat_result": stat_result,
                    "status": status or choices.LOG_FILE_STATUS_CREATED,
                },
            )
        except IntegrityError:
            obj = cls.get(hash=hash)
            created = False

        if created:
            logging.info(f'File {path} added to the database.')
        else:
            obj.updated = timezone.now()
            obj.save(update_fields=["updated"])
            logging.info(f'File {path} already exists in the database.')

        return obj
        
    def __str__(self):
        return f'{self.path}'
