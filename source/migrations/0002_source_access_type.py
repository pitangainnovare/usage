from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("source", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="source",
            name="access_type",
            field=models.CharField(
                blank=True,
                choices=[
                    ("open_access", "Open Access"),
                    ("commercial", "Commercial"),
                ],
                db_index=True,
                max_length=32,
                null=True,
                verbose_name="Access Type",
            ),
        ),
    ]
