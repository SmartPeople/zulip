from __future__ import absolute_import
from __future__ import print_function

from datetime import timedelta

from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone
from zerver.lib.upload import delete_message_image
from zerver.models import (Message, UserMessage, ArchivedMessage, ArchivedUserMessage,
                           Attachment, ArchivedAttachment)

from typing import Any


@transaction.atomic
def move_expired_rows(src_model, raw_query):
    # type: (Any, str) -> None
    src_db_table = src_model._meta.db_table
    src_fields = ["{}.{}".format(src_db_table, field.column) for field in src_model._meta.fields]
    dst_fields = [field.column for field in src_model._meta.fields]
    with connection.cursor() as cursor:
        cursor.execute(
            raw_query.format(
                src_fields=','.join(src_fields),
                dst_fields=','.join(dst_fields),
                archive_timestamp=timezone.now()
            )
        )


def move_expired_messages_to_archive():
    # type: () -> None
    query = """
    INSERT INTO zerver_archivedmessage ({dst_fields}, archive_timestamp)
    SELECT {src_fields}, '{archive_timestamp}'
    FROM zerver_message
    INNER JOIN zerver_usermessage ON zerver_message.id = zerver_usermessage.message_id
    INNER JOIN zerver_userprofile ON zerver_usermessage.user_profile_id = zerver_userprofile.id
    INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
    WHERE zerver_realm.message_retention_days IS NOT NULL
          AND EXTRACT(DAY FROM (CURRENT_DATE - zerver_message.pub_date)) >= zerver_realm.message_retention_days
          AND zerver_message.id NOT IN (SELECT ID FROM zerver_archivedmessage)
    GROUP BY zerver_message.id
    """
    move_expired_rows(Message, query)


def move_expired_user_messages_to_archive():
    # type: () -> None
    query = """
    INSERT INTO zerver_archivedusermessage ({dst_fields}, archive_timestamp)
    SELECT {src_fields}, '{archive_timestamp}'
    FROM zerver_usermessage
    INNER JOIN zerver_message ON zerver_message.id = zerver_usermessage.message_id
    INNER JOIN zerver_userprofile ON zerver_usermessage.user_profile_id = zerver_userprofile.id
    INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
    WHERE zerver_realm.message_retention_days IS NOT NULL
         AND EXTRACT(DAY FROM (CURRENT_DATE - zerver_message.pub_date)) >= zerver_realm.message_retention_days
         AND zerver_usermessage.id NOT IN (SELECT id FROM zerver_archivedusermessage)
    """
    move_expired_rows(UserMessage, query)


def move_expired_attachments_to_archive():
    # type: () -> None
    query = """
       INSERT INTO zerver_archivedattachment ({dst_fields}, archive_timestamp)
       SELECT {src_fields}, '{archive_timestamp}'
       FROM zerver_attachment
       INNER JOIN zerver_attachment_messages
           ON zerver_attachment_messages.attachment_id = zerver_attachment.id
       INNER JOIN zerver_message ON zerver_message.id = zerver_attachment_messages.message_id
       INNER JOIN zerver_usermessage ON zerver_message.id = zerver_usermessage.message_id
       INNER JOIN zerver_userprofile ON zerver_usermessage.user_profile_id = zerver_userprofile.id
       INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
       WHERE zerver_realm.message_retention_days IS NOT NULL
            AND EXTRACT(DAY FROM (CURRENT_DATE - zerver_message.pub_date)) >= zerver_realm.message_retention_days
            AND zerver_attachment.id NOT IN (SELECT id FROM zerver_archivedattachment)
       GROUP BY zerver_attachment.id
       """
    move_expired_rows(Attachment, query)


def move_expired_attachments_message_rows_to_archive():
    # type: () -> None
    query = """
       INSERT INTO zerver_archivedattachment_messages (id, archivedattachment_id, archivedmessage_id)
       SELECT zerver_attachment_messages.id, zerver_attachment_messages.attachment_id,
           zerver_attachment_messages.message_id
       FROM zerver_attachment_messages
       INNER JOIN zerver_message ON zerver_message.id = zerver_attachment_messages.message_id
       INNER JOIN zerver_usermessage ON zerver_message.id = zerver_usermessage.message_id
       INNER JOIN zerver_userprofile ON zerver_usermessage.user_profile_id = zerver_userprofile.id
       INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
       WHERE zerver_realm.message_retention_days IS NOT NULL
            AND EXTRACT(DAY FROM (CURRENT_DATE - zerver_message.pub_date)) >= zerver_realm.message_retention_days
            AND zerver_attachment_messages.id NOT IN (SELECT id FROM zerver_archivedattachment_messages)
       GROUP BY zerver_attachment_messages.id
       """
    with connection.cursor() as cursor:
        cursor.execute(query)


def delete_expired_messages():
    # type: () -> None
    removing_messages = Message.objects.filter(
        usermessage__isnull=True, id__in=ArchivedMessage.objects.all())
    removing_messages.delete()


def delete_expired_user_messages():
    # type: () -> None
    removing_user_messages = UserMessage.objects.filter(
        id__in=ArchivedUserMessage.objects.all()
    )
    removing_user_messages.delete()


def delete_expired_attachments():
    # type: () -> None
    attachments_to_remove = Attachment.objects.filter(
        messages__isnull=True, id__in=ArchivedAttachment.objects.all())
    attachments_to_remove.delete()


def archive_messages():
    # type: () -> None
    move_expired_messages_to_archive()
    move_expired_user_messages_to_archive()
    move_expired_attachments_to_archive()
    move_expired_attachments_message_rows_to_archive()
    delete_expired_user_messages()
    delete_expired_messages()
    delete_expired_attachments()


def delete_expired_archived_attachments():
    # type: () -> None
    expired_date = timezone.now() - timedelta(days=settings.ARCHIVED_DATA_RETENTION_DAYS)
    arc_attachments = ArchivedAttachment.objects \
        .filter(archived_date__lt=expired_date, messages__isnull=True) \
        .exclude(id__in=Attachment.objects.all())
    for arc_att in arc_attachments:
        delete_message_image(arc_att.path_id)
    arc_attachments.delete()


def delete_expired_archived_data():
    # type: () -> None
    arc_expired_date = timezone.now() - timedelta(days=settings.ARCHIVED_DATA_RETENTION_DAYS)
    ArchivedUserMessage.objects.filter(archived_date__lt=arc_expired_date).delete()
    ArchivedMessage.objects.filter(archived_date__lt=arc_expired_date,
                                   archivedusermessage__isnull=True).delete()
    delete_expired_archived_attachments()
