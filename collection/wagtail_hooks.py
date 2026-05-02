from django.utils.translation import gettext as _
from wagtail.snippets.views.snippets import SnippetViewSet

from .models import Collection


class CollectionSnippetViewSet(SnippetViewSet):
    model = Collection
    icon = "folder-open-inverse"
    menu_label = _("Collection")
    menu_order = 100

    list_display = (
        "main_name",
        "acron3",
        "acron2",
        "code",
        "status",
        "collection_type",
        "is_active",
        "updated",
        "created",
    )
    list_filter = (
        "status",
        "collection_type",
        "is_active",
        "has_analytics",
    )
    search_fields = (
        "acron3",
        "acron2",
        "code",
        "domain",
        "name__text",
        "main_name",
    )
    list_export = (
        "acron3",
        "acron2",
        "code",
        "domain",
        "main_name",
        "status",
        "has_analytics",
        "collection_type",
        "is_active",
        "foundation_date",
        "creator",
        "updated",
        "created",
        "updated_by",
    )
    export_filename = "collections"
