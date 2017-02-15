from __future__ import absolute_import
from __future__ import print_function

from datetime import timedelta

from django.conf import settings
from django.db import connection, transaction, models
from django.utils import timezone
from zerver.lib.upload import delete_message_image
from zerver.models import (Message, UserMessage, ArchivedMessage, ArchivedUserMessage,
                           Attachment, ArchivedAttachment, Realm)

from typing import Any, List


@transaction.atomic
def move_rows(src_model, fields, raw_query, **kwargs):
    # type: (models.Model, List[models.fields.Field], str, **Any) -> None
    src_db_table = src_model._meta.db_table
    src_fields = ["{}.{}".format(src_db_table, field.column) for field in fields]
    dst_fields = [field.column for field in fields]
    sql_args = {
        'src_fields': ','.join(src_fields),
        'dst_fields': ','.join(dst_fields),
    }
    sql_args.update(kwargs)
    with connection.cursor() as cursor:
        cursor.execute(
            raw_query.format(**sql_args)
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
    move_rows(Message, Message._meta.fields, query, archive_timestamp=timezone.now())


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
    move_rows(UserMessage, UserMessage._meta.fields, query, archive_timestamp=timezone.now())


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
    move_rows(Attachment, Attachment._meta.fields, query, archive_timestamp=timezone.now())


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
    # Delete messages after retention period
    # if they already have been moved to archive.
    removing_messages = Message.objects.filter(
        usermessage__isnull=True, id__in=ArchivedMessage.objects.all())
    removing_messages.delete()


def delete_expired_user_messages():
    # type: () -> None
    # Delete user_messages after retention period
    # if they are already moved to archive.
    removing_user_messages = UserMessage.objects.filter(
        id__in=ArchivedUserMessage.objects.all()
    )
    removing_user_messages.delete()


def delete_expired_attachments():
    # type: () -> None
    # Delete attachments after retention period
    # if they already have been moved to archive.
    attachments_to_remove = Attachment.objects.filter(
        messages__isnull=True, id__in=ArchivedAttachment.objects.all())
    attachments_to_remove.delete()


def archive_messages():
    # type: () -> None
    # The main function for archiving messages' data.
    move_expired_messages_to_archive()
    move_expired_user_messages_to_archive()
    move_expired_attachments_to_archive()
    move_expired_attachments_message_rows_to_archive()
    delete_expired_user_messages()
    delete_expired_messages()
    delete_expired_attachments()


def delete_expired_archived_attachments():
    # type: () -> None
    # Delete old archived attachments from archive table
    # after retention period for archived data.
    expired_date = timezone.now() - timedelta(days=settings.ARCHIVED_DATA_RETENTION_DAYS)
    arc_attachments = ArchivedAttachment.objects \
        .filter(archive_timestamp__lt=expired_date, messages__isnull=True) \
        .exclude(id__in=Attachment.objects.all())
    for arc_att in arc_attachments:
        delete_message_image(arc_att.path_id)
    arc_attachments.delete()


def delete_expired_archived_data():
    # type: () -> None
    # Delete old archived messages and user_messages from archive tables
    # after retention period for archived data.
    arc_expired_date = timezone.now() - timedelta(days=settings.ARCHIVED_DATA_RETENTION_DAYS)
    ArchivedUserMessage.objects.filter(archive_timestamp__lt=arc_expired_date).delete()
    ArchivedMessage.objects.filter(archive_timestamp__lt=arc_expired_date,
                                   archivedusermessage__isnull=True).delete()
    delete_expired_archived_attachments()


def restore_archived_messages_by_realm(realm_id):
    # type: (int) -> None
    # Function for restoring archived messages by realm for emergency cases.
    query = """
        INSERT INTO zerver_message ({dst_fields})
        SELECT {src_fields}
        FROM zerver_archivedmessage
        INNER JOIN zerver_archivedusermessage ON zerver_archivedmessage.id = zerver_archivedusermessage.message_id
        INNER JOIN zerver_userprofile ON zerver_archivedusermessage.user_profile_id = zerver_userprofile.id
        INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
        WHERE zerver_realm.id = {realm_id}
              AND zerver_archivedmessage.id NOT IN (SELECT ID FROM zerver_message)
        GROUP BY zerver_archivedmessage.id
    """
    move_rows(ArchivedMessage, Message._meta.fields, query, realm_id=realm_id)


def restore_archived_usermessages_by_realm(realm_id):
    # type: (int) -> None
    # Function for restoring archived user_messages by realm for emergency cases.
    query = """
        INSERT INTO zerver_usermessage ({dst_fields})
        SELECT {src_fields}
        FROM zerver_archivedusermessage
        INNER JOIN zerver_userprofile ON zerver_archivedusermessage.user_profile_id = zerver_userprofile.id
        INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
        WHERE zerver_realm.id = {realm_id}
             AND zerver_archivedusermessage.id NOT IN (SELECT id FROM zerver_usermessage)
             AND zerver_archivedusermessage.message_id IN (SELECT id from zerver_message)
        """
    move_rows(ArchivedUserMessage, UserMessage._meta.fields, query, realm_id=realm_id)


def restore_archived_attachments_by_realm(realm_id):
    # type: (int) -> None
    # Function for restoring archived attachments by realm for emergency cases.
    query = """
       INSERT INTO zerver_attachment ({dst_fields})
       SELECT {src_fields}
       FROM zerver_archivedattachment
       INNER JOIN zerver_archivedattachment_messages
           ON zerver_archivedattachment_messages.archivedattachment_id = zerver_archivedattachment.id
       INNER JOIN zerver_archivedmessage ON zerver_archivedmessage.id = zerver_archivedattachment_messages.archivedmessage_id
       INNER JOIN zerver_archivedusermessage ON zerver_archivedmessage.id = zerver_archivedusermessage.message_id
       INNER JOIN zerver_userprofile ON zerver_archivedusermessage.user_profile_id = zerver_userprofile.id
       INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
       WHERE zerver_realm.id = {realm_id}
            AND zerver_archivedattachment.id NOT IN (SELECT id FROM zerver_attachment)
       GROUP BY zerver_archivedattachment.id
       """
    move_rows(ArchivedAttachment, Attachment._meta.fields, query, realm_id=realm_id)


def restore_archived_attachments_message_rows_by_realm(realm_id):
    # type: (int) -> None
    # Function for restoring archived data in many-to-many attachment_messages
    # table by realm for emergency cases.
    query = """
       INSERT INTO zerver_attachment_messages (id, attachment_id, message_id)
       SELECT zerver_archivedattachment_messages.id, zerver_archivedattachment_messages.archivedattachment_id,
           zerver_archivedattachment_messages.archivedmessage_id
       FROM zerver_archivedattachment_messages
       INNER JOIN zerver_archivedmessage ON zerver_archivedmessage.id = zerver_archivedattachment_messages.archivedmessage_id
       INNER JOIN zerver_archivedusermessage ON zerver_archivedmessage.id = zerver_archivedusermessage.message_id
       INNER JOIN zerver_userprofile ON zerver_archivedusermessage.user_profile_id = zerver_userprofile.id
       INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
       WHERE zerver_realm.id = {realm_id}
            AND zerver_archivedattachment_messages.id NOT IN (SELECT id FROM zerver_attachment_messages)
       GROUP BY zerver_archivedattachment_messages.id
       """

    with connection.cursor() as cursor:
        cursor.execute(query.format(realm_id=realm_id))


def restore_realm_archived_data(realm_id):
    # type: (int) -> None
    # The main function for restoring archived messages' data by realm.
    restore_archived_messages_by_realm(realm_id)
    restore_archived_usermessages_by_realm(realm_id)
    restore_archived_attachments_by_realm(realm_id)
    restore_archived_attachments_message_rows_by_realm(realm_id)
    realm = Realm.objects.get(id=realm_id)
    realm.message_retention_days = None
    realm.save()
