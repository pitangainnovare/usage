import hashlib

from django.db import models
from django.utils.translation import gettext_lazy as _

from core.models import CommonControlField
from collection.models import Collection


class RobotUserAgent(CommonControlField):
    pattern = models.CharField(
        verbose_name=_('Pattern'),
        max_length=255,
        null=False,
        blank=False,
        primary_key=True,
    )
    last_changed = models.DateField(
        verbose_name=_('Last Changed'),
        null=False,
        blank=False,
    )

    @classmethod
    def get_all_patterns(cls):
        return cls.objects.values_list('pattern', flat=True)

    def __str__(self):
        return self.pattern


class MMDB(CommonControlField):
    id = models.CharField(
        verbose_name=_('ID (HASH)'),
        max_length=64, 
        primary_key=True,
    )
    data = models.BinaryField(
        verbose_name=_('MMDB Data'),
    )
    url = models.URLField(
        verbose_name=_('URL'),
        max_length=255,
        null=True,
        blank=True,
    )
    
    def save(self, *args, **kwargs):
        if self.data:
            self.id = MMDB.compute_hash(self.data)
        super().save(*args, **kwargs)

    @classmethod
    def compute_hash(cls, data):
        return hashlib.sha256(data).hexdigest()

    def __str__(self):
        return f'{self.id}'


# class ISSNToAcronym(CommonControlField):
#     collection = models.ForeignKey(
#         Collection,
#         verbose_name=_('Collection'), 
#         on_delete=models.CASCADE,
#         blank=False, 
#         null=False,
#     )

#     issn = models.CharField(
#         verbose_name=_('ISSN'), 
#         max_length=9, 
#         blank=False, 
#         null=False,
#     )

#     acronym = models.CharField(
#         verbose_name=_('Journal Acronym'), 
#         max_length=16, 
#         blank=False, 
#         null=False
#     )

#     def __str__(self):
#         return f'{self.collection.acron2} - {self.issn} - {self.acronym}'


# class PDFPathToPIDv2(CommonControlField):
#     collection = models.ForeignKey(
#         Collection,
#         verbose_name=_('Collection'), 
#         on_delete=models.CASCADE,
#         blank=False, 
#         null=False,
#     )

#     pdf_path = models.CharField(
#         verbose_name=_('PDF Path'), 
#         max_length=255, 
#         blank=False, 
#         null=False,
#     )

#     pid_v2 = models.CharField(
#         verbose_name=_('PID V2'), 
#         max_length=23, 
#         blank=False, 
#         null=False
#     )

#     def __str__(self):
#         return f'{self.collection.acron2} - {self.pdf_path} - {self.pid_v2}'


# class PIDToDates(CommonControlField):
#     collection = models.ForeignKey(
#         Collection,
#         verbose_name=_('Collection'), 
#         on_delete=models.CASCADE,
#         blank=False, 
#         null=False,
#     )

#     pid = models.CharField(
#         verbose_name=_('PID'),
#         max_length=23,
#         blank=False,
#         null=False,
#     )

#     pid_type = models.CharField(
#         verbose_name=_('PID Type'),
#         max_length=2,
#         blank=False,
#         null=False,
#     )

#     processing_date = models.CharField(verbose_name=_('Processing Date'), max_length=32)
#     publication_date = models.CharField(verbose_name=_('Publication Date'), max_length=32)
#     publication_year = models.CharField(verbose_name=_('Publication Year'), max_length=4)
#     created_at = models.DateTimeField(verbose_name=_('Created At'))
#     updated_at = models.DateTimeField(verbose_name=_('Updated At'))

#     def __str__(self):
#         return f'{self.collection.acron2} - {self.pid} ({self.pid_type}) - {self.publication_year}'


# class PIDV3ToISSN(CommonControlField):
#     collection = models.ForeignKey(
#         Collection,
#         verbose_name=_('Collection'), 
#         on_delete=models.CASCADE,
#         blank=False, 
#         null=False,
#     )

#     pid_v3 = models.CharField(
#         verbose_name=_('PID V3'),
#         max_length=23,
#         blank=False,
#         null=False,
#     )

#     issn = models.CharField(verbose_name=_('ISSN'), max_length=9, blank=False, null=False)

#     def __str__(self):
#         return f'{self.collection.acron2} - {self.pid_v3} - {self.issn}'


# class PIDToFormatAndLang(CommonControlField):
#     collection = models.ForeignKey(
#         Collection,
#         verbose_name=_('Collection'), 
#         on_delete=models.CASCADE,
#         blank=False, 
#         null=False,
#     )

#     pid = models.CharField(
#         verbose_name=_('PID'),
#         max_length=23,
#         blank=False,
#         null=False,
#     )

#     pid_type = models.CharField(
#         verbose_name=_('PID Type'),
#         max_length=2,
#         blank=False,
#         null=False,
#     )

#     default_language = models.CharField(
#         verbose_name=_('Default Language'),
#         max_length=2,
#         blank=False,
#         null=False,
#     )

#     document_languages = models.JSONField(
#         verbose_name=_('Document Languages'),
#         blank=False,
#         null=False,
#     )

#     document_formats = models.JSONField(
#         verbose_name=_('Document Formats'),
#         blank=False,
#         null=False,
#     )

#     def __str__(self):
#         return f'{self.collection.acron2} - {self.pid} ({self.pid_type}) - {self.default_language}'
