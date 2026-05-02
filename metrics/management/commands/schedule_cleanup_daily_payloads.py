from django.core.management.base import BaseCommand

from core.utils.scheduler import schedule_task
from metrics.tasks import task_cleanup_daily_payloads


class Command(BaseCommand):
    help = (
        "Schedule the periodic cleanup of exported daily metric payload files. "
        "Runs weekly on Sunday at 03:00 UTC by default, deleting payload files "
        "for jobs that were exported more than 7 days ago."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--day-of-week",
            default="0",
            help="Crontab day of week (0=Sunday, 6=Saturday). Default: 0",
        )
        parser.add_argument(
            "--hour",
            default="3",
            help="Crontab hour (0-23). Default: 3",
        )
        parser.add_argument(
            "--minute",
            default="0",
            help="Crontab minute (0-59). Default: 0",
        )
        parser.add_argument(
            "--older-than-days",
            type=int,
            default=7,
            help="Only delete payloads exported more than N days ago. Default: 7",
        )
        parser.add_argument(
            "--collection",
            action="append",
            dest="collections",
            help="Limit cleanup to a specific collection acronym. Repeat for multiple.",
        )

    def handle(self, *args, **options):
        celery_task_name = task_cleanup_daily_payloads.name

        kwargs = {
            "older_than_days": options["older_than_days"],
            "collections": options.get("collections") or [],
        }

        schedule_task(
            task=celery_task_name,
            name=celery_task_name,
            kwargs=kwargs,
            description="Weekly cleanup of exported daily payload files from disk.",
            day_of_week=options["day_of_week"],
            hour=options["hour"],
            minute=options["minute"],
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"Scheduled periodic task '{celery_task_name}' "
                f"(day_of_week={options['day_of_week']}, hour={options['hour']}, "
                f"minute={options['minute']}, older_than_days={kwargs['older_than_days']}, "
                f"collections={kwargs['collections'] or 'all'})."
            )
        )
