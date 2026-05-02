from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("source", "0002_source_access_type"),
    ]

    operations = [
        migrations.AlterField(
            model_name="source",
            name="title",
            field=models.CharField(max_length=500, verbose_name="Source Title"),
        ),
    ]
