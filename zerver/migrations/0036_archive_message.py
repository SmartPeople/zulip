# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import zerver.lib.str_utils
from django.conf import settings
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('zerver', '0035_realm_message_retention_period_days'),
    ]

    operations = [
        migrations.CreateModel(
            name='ArchiveMessage',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, verbose_name='ID', serialize=False)),
                ('subject', models.CharField(db_index=True, max_length=60)),
                ('content', models.TextField()),
                ('rendered_content', models.TextField(null=True)),
                ('rendered_content_version', models.IntegerField(null=True)),
                ('pub_date', models.DateTimeField(verbose_name='date published', db_index=True)),
                ('last_edit_time', models.DateTimeField(null=True)),
                ('edit_history', models.TextField(null=True)),
                ('has_attachment', models.BooleanField(db_index=True, default=False)),
                ('has_image', models.BooleanField(db_index=True, default=False)),
                ('has_link', models.BooleanField(db_index=True, default=False)),
                ('archived_date', models.DateTimeField(default=django.utils.timezone.now, verbose_name='data archived', db_index=True)),
                ('recipient', models.ForeignKey(to='zerver.Recipient')),
                ('sender', models.ForeignKey(to=settings.AUTH_USER_MODEL)),
                ('sending_client', models.ForeignKey(to='zerver.Client')),
            ],
            options={
                'abstract': False,
            },
            bases=(zerver.lib.str_utils.ModelReprMixin, models.Model),
        ),
    ]
