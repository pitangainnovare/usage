from django.utils.translation import gettext_lazy as _
from wagtail.snippets.views.snippets import SnippetViewSet, SnippetViewSetGroup
from wagtail.snippets.models import register_snippet

from config.menu import get_menu_order
from log_manager_config.wagtail_hooks import LogManagerCollectionConfigSnippetViewSet
from metrics.wagtail_hooks import DailyMetricJobSnippetViewSet

from log_manager.models import LogFile


class LogFileSnippetViewSet(SnippetViewSet):
    model = LogFile
    menu_label = _("Log File Status")
    icon = "folder"
    menu_order = 500
    list_display = (
        "path",
        "collection", 
        "status", 
        "date",
        "validation",
        "summary",
        "last_processed_line",
        "parse_heartbeat_at",
        "hash"
    )
    list_filter = ("status", "collection", "date")
    search_fields = ("path", "hash", "collection__acron3", "collection__main_name")


class LogSnippetViewSetGroup(SnippetViewSetGroup):
    menu_name = 'log_manager'
    menu_label = _("Log Manager")
    menu_icon = "folder-open-inverse"
    menu_order = get_menu_order("log_manager")
    items = (
        LogManagerCollectionConfigSnippetViewSet,
        LogFileSnippetViewSet, 
        DailyMetricJobSnippetViewSet,
    )


register_snippet(LogSnippetViewSetGroup)
