import os

from datetime import datetime, timezone

from django.db import models, transaction
from django.utils.translation import gettext_lazy as _
from wagtail.admin.panels import FieldPanel

from core.models import CommonControlField


class Top100Articles(CommonControlField):
    pid_issn = models.CharField('PID ISSN', max_length=9, null=False, blank=False)
    year_month_day = models.DateField('Date of access', null=False, blank=False)

    print_issn = models.CharField('Print ISSN', max_length=9, null=True, blank=True)
    online_issn = models.CharField('Online ISSN', max_length=9, null=True, blank=True)

    collection = models.CharField('Collection Acronym 3', max_length=3, null=False, blank=False)
    pid = models.CharField('Publication ID', null=False, blank=False)
    yop = models.PositiveSmallIntegerField('Year of Publication', null=False, blank=False)
    
    total_item_requests = models.IntegerField('Total Item Requests', null=False, blank=False)
    total_item_investigations = models.IntegerField('Total Item Investigations', null=False, blank=False)
    unique_item_requests = models.IntegerField('Unique Item Requests', null=False, blank=False)
    unique_item_investigations = models.IntegerField('Unique Item Investigations', null=False, blank=False)

    panels = [
        FieldPanel('pid_issn'),
        FieldPanel('year_month_day'),
        FieldPanel('print_issn'),
        FieldPanel('online_issn'),
        FieldPanel('collection'),
        FieldPanel('pid'),
        FieldPanel('yop'),
        FieldPanel('total_item_requests'),
        FieldPanel('total_item_investigations'),
        FieldPanel('unique_item_requests'),
        FieldPanel('unique_item_investigations'),
    ]

    class Meta:
        unique_together = (
            'collection',
            'pid_issn',
            'pid',
            'year_month_day',
        )
        verbose_name_plural = _('Top 100 Articles')
        indexes = [
            models.Index(fields=['pid_issn']),
            models.Index(fields=['year_month_day']),
        ]

    @classmethod
    def create_or_update(cls, user, save=True, **data):
        with transaction.atomic():
            now = datetime.now(timezone.utc)

            obj, created = cls.objects.get_or_create(
                collection=data.get('collection'),
                pid_issn=data.get('pid_issn'),
                pid=data.get('pid'),
                year_month_day=data.get('year_month_day'),
                defaults={
                    **data, 
                    'creator': user, 
                    'created': now,
                    'updated_by': user,
                    'updated': now
                }
            )
            if not created:
                for key, value in data.items():
                    setattr(obj, key, value)
                obj.updated_by = user
                obj.updated = now

        if save:
            obj.save()

        return obj, created
    
    @classmethod
    def bulk_create(cls, objects, ignore_conflicts=True):
        cls.objects.bulk_create(objs=objects, ignore_conflicts=ignore_conflicts)

    @classmethod
    def bulk_update(cls, objects, fields=['print_issn', 'online_issn', 'yop', 'total_item_requests', 'total_item_investigations', 'unique_item_requests', 'unique_item_investigations', 'updated', 'updated_by']):
        cls.objects.bulk_update(objs=objects, fields=fields)

    def __str__(self):
        return f'{self.pid_issn}, {self.pid}, {self.total_item_requests}'


class ArticleLangByCountry(CommonControlField):
    collection = models.CharField('Collection', max_length=3, null=False, blank=False)
    issn = models.CharField('ISSN', max_length=9, null=False, blank=False)
    pid = models.CharField('Publication ID', null=False, blank=False)
    language = models.CharField('Article Language', max_length=2, null=False, blank=False)

    publication_year = models.PositiveSmallIntegerField('Year of Publication (Site)', null=False, blank=False)
    publication_month = models.CharField('Month of Publication (Site)', max_length=2)

    access_country = models.CharField('Country of Access', max_length=2, null=False, blank=False)
    access_year = models.PositiveSmallIntegerField('Year of Access', null=False, blank=False)
    total_item_requests = models.IntegerField('Total Item Requests', null=False, blank=False)

    panels = [
        FieldPanel('collection'),
        FieldPanel('issn'),
        FieldPanel('pid'),
        FieldPanel('language'),
        FieldPanel('publication_year'),
        FieldPanel('publication_month'),
        FieldPanel('access_country'),
        FieldPanel('access_year'),
        FieldPanel('total_item_requests'),
    ]

    class Meta:
        unique_together = (
            'collection',
            'issn',
            'pid',
            'language',
            'publication_year',
            'publication_month',
            'access_country',
        )
        verbose_name_plural = _('ArticleLangByCountries')
        indexes = [
            models.Index(fields=['collection']),
            models.Index(fields=['issn']),
            models.Index(fields=['pid']),
            models.Index(fields=['language']),
            models.Index(fields=['access_country']),
            models.Index(fields=['access_year']),
        ]

    @classmethod
    def create_or_update(cls, user, save=True, **data):
        with transaction.atomic():
            now = datetime.now(timezone.utc)

            try:
                obj, created = cls.objects.get_or_create(
                    collection=data.get('collection'),
                    issn=data.get('issn'),
                    pid=data.get('pid'),
                    language=data.get('language'),
                    publication_year=data.get('publication_year'),
                    publication_month=data.get('publication_month'),
                    access_country=data.get('access_country'),
                    access_year=data.get('access_year'),
                    defaults={
                        **data, 
                        'creator': user, 
                        'created': now,
                        'updated_by': user,
                        'updated': now
                    }
                )
                if not created:
                    for key, value in data.items():
                        setattr(obj, key, value)
                    obj.updated_by = user
                    obj.updated = now
            except ValueError as e:
                print(f'Line has been ignored due to ValueError Exception: {data}', e)

        if save:
            obj.save()

        return obj, created
    
    @classmethod
    def bulk_create(cls, objects, ignore_conflicts=True):
        cls.objects.bulk_create(objs=objects, ignore_conflicts=ignore_conflicts)

    @classmethod
    def bulk_update(cls, objects, fields=['total_item_requests', 'updated', 'updated_by']):
        cls.objects.bulk_update(objs=objects, fields=fields)

    def __str__(self):
        return f'{self.collection}, {self.issn}, {self.pid}, {self.language}, {self.total_item_requests}'


class BaseArticleFile(CommonControlField):
    class Meta:
        abstract = True

    class Status(models.TextChoices):
        QUEUED = "QUE", _("Queued")
        PARSING = "PAR", _("Parsing")
        PROCESSED = "PRO", _("Processed")
        ERROR = "ERR", _("Error")
        INVALIDATED = "INV", _("Invalidated")
    
    attachment = models.ForeignKey(
        "wagtaildocs.Document",
        verbose_name=_("Attachment"),
        null=True,
        blank=False,
        on_delete=models.SET_NULL,
        related_name="+",
    )

    status = models.CharField(max_length=5, choices=Status.choices, default=Status.QUEUED)

    def get_status_display(self):
        return self.Status(self.status).label
    
    get_status_display.admin_order_field = "status"
    get_status_display.short_description = "Status"

    @property
    def filename(self):
        if self.attachment:
            return os.path.basename(self.attachment.filename)
        return _('File not available')

    panels = [
        FieldPanel("attachment"),
        FieldPanel("status"),
    ]

    def __str__(self):
        return f'{self.filename}'


class ArticleLangByCountryFile(BaseArticleFile):
    class Meta:
        verbose_name_plural = _("ArticleLangByCountry Files")
        verbose_name = _("ArticleLangByCountry File")


class Top100ArticlesFile(BaseArticleFile):
    class Meta:
        verbose_name_plural = _("Top 100 Articles Files")
        verbose_name = _("Top 100 Articles File")
