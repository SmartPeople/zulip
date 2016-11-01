# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.utils.timezone
from django.conf import settings
import bitfield.models
import zerver.lib.str_utils


class Migration(migrations.Migration):

    dependencies = [
        ('zerver', '0036_archive_message'),
    ]

    operations = [
        migrations.CreateModel(
            name='ArchiveUserMessage',
            fields=[
                ('id', models.AutoField(serialize=False, auto_created=True, primary_key=True, verbose_name='ID')),
                ('flags', bitfield.models.BitField(['read', 'starred', 'collapsed', 'mentioned', 'wildcard_mentioned', 'summarize_in_home', 'summarize_in_stream', 'force_expand', 'force_collapse', 'has_alert_word', 'historical', 'is_me_message'], default=0)),
                ('archived_date', models.DateTimeField(default=django.utils.timezone.now, db_index=True, verbose_name='data archived')),
                ('message', models.ForeignKey(to='zerver.ArchiveMessage')),
                ('user_profile', models.ForeignKey(to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'abstract': False,
            },
            bases=(zerver.lib.str_utils.ModelReprMixin, models.Model),
        ),
        migrations.AlterUniqueTogether(
            name='archiveusermessage',
            unique_together=set([('user_profile', 'message')]),
        ),
    ]
