import inspect
import json

from celery import current_app
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse
from django.utils.translation import gettext as _
from wagtail.admin import messages

from django_celery_beat import models


def task_run(request):
    """
    View funciton to run the task by PeriodicTask id.
    """

    task_id = int(request.GET.get("task_id", None))

    p_task = get_object_or_404(models.PeriodicTask, pk=task_id)

    current_app.loader.import_default_modules()

    task = current_app.tasks.get(p_task.task)

    if task is None:
        messages.error(
            request,
            _("Task '{0}' not found in the Celery registry.").format(p_task.task),
        )
        return redirect(request.META.get("HTTP_REFERER") or reverse("wagtailadmin_home"))

    kwargs = json.loads(p_task.kwargs)
    sig = inspect.signature(task.run)
    if "user_id" in sig.parameters:
        kwargs["user_id"] = request.user.id

    task.apply_async(
        args=json.loads(p_task.args),
        kwargs=kwargs,
        queue=p_task.queue,
        periodic_task_name=p_task.name,
    )

    messages.success(request, _("Task {0} was successfully run").format(p_task.name))

    return redirect(request.META.get("HTTP_REFERER") or reverse("wagtailadmin_home"))
