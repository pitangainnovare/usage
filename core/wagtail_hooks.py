"""File: core/wagtail_hooks.py."""

from django.templatetags.static import static
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.snippets.models import register_snippet
from wagtail.snippets.views.snippets import SnippetViewSetGroup

from collection.wagtail_hooks import CollectionSnippetViewSet
from config.menu import get_menu_order
from document.wagtail_hooks import DocumentSnippetViewSet
from source.wagtail_hooks import SourceSnippetViewSet


@hooks.register("insert_global_admin_css", order=100)
def global_admin_css():
    """Add /static/css/custom.css to the admin."""
    """Add /static/admin/css/custom.css to the admin."""
    return format_html(
        '<link rel="stylesheet" href="{}">', static("admin/css/custom.css")
    )


@hooks.register("insert_global_admin_js", order=100)
def global_admin_js():
    """Add /static/css/custom.js to the admin."""
    """Add /static/admin/css/custom.js to the admin."""
    return format_html('<script src="{}"></script>', static("admin/js/custom.js"))


@hooks.register("construct_homepage_summary_items", order=1)
def remove_all_summary_items(request, items):
    items.clear()


@hooks.register("construct_main_menu")
def reorder_main_menu(request, menu_items):
    hidden_items = {
        "documents",
        "explorer",
        "help",
        "images",
        "reports",
        "snippets",
    }
    preferred_order = {
        "metadata": 0,
        "resources": 1,
        "log_manager": 2,
        "tracker": 3,
    }

    menu_items[:] = [
        item
        for item in menu_items
        if getattr(item, "name", "") not in hidden_items
    ]

    menu_items.sort(
        key=lambda item: (
            preferred_order.get(getattr(item, "name", ""), 100),
            getattr(item, "order", 1000),
            getattr(item, "name", ""),
        )
    )


class MetadataSnippetViewSetGroup(SnippetViewSetGroup):
    menu_name = "metadata"
    menu_label = _("Metadata")
    menu_icon = "folder-open-inverse"
    menu_order = get_menu_order("metadata")
    items = (
        CollectionSnippetViewSet,
        SourceSnippetViewSet,
        DocumentSnippetViewSet,
    )


register_snippet(MetadataSnippetViewSetGroup)
