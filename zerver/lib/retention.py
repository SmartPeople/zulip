from __future__ import absolute_import
from __future__ import print_function

from datetime import timedelta

from django.conf import settings
from django.db import connection, transaction, models
from django.db.models import QuerySet
from django.utils import timezone
from zerver.lib.upload import delete_message_image
from zerver.models import (Message, UserMessage, ArchivedMessage, ArchivedUserMessage,
                           Attachment, ArchivedAttachment, Realm)

from typing import Any, Dict, List, Optional, Text, Tuple


@transaction.atomic
def move_rows(select_query, fields, insert_query, **kwargs):
    # type: (str, List[models.fields.Field], str, **Any) -> None
    dst_fields = [field.column for field in fields]
    sql_args = {
        'dst_fields': ','.join(dst_fields),
        'select_query': select_query
    }
    sql_args.update(kwargs)
    with connection.cursor() as cursor:
        cursor.execute(
            insert_query.format(**sql_args)
        )


@transaction.atomic
def execute_select_query(query):
    # type: (str) -> Optional[List[Tuple[Any]]]
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.cursor.fetchall()

def fill_select_query(select_query, src_model, fields, **kwargs):
    # type: (str, models.Model, List[models.fields.Field], **Any) -> str
    src_db_table = src_model._meta.db_table
    src_fields = ["{}.{}".format(src_db_table, field.column) for field in fields]
    sql_args = {
        'src_fields': ','.join(src_fields),
    }
    sql_args.update(kwargs)
    return select_query.format(**sql_args)


def move_expired_messages_to_archive(dry_run=False):
    # type: (bool) -> Optional[List[Tuple[Any]]]
    select_query_template = """
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
    select_query = fill_select_query(select_query_template, Message, Message._meta.fields,
                                     archive_timestamp=timezone.now())
    if dry_run:
        return execute_select_query(select_query)
    insert_query = """
        INSERT INTO zerver_archivedmessage ({dst_fields}, archive_timestamp)
        {select_query}
    """
    move_rows(select_query, Message._meta.fields, insert_query)
    return None


def move_expired_user_messages_to_archive(dry_run=False):
    # type: (bool) -> Optional[List[Tuple[Any]]]
    select_query_template = """
        SELECT {src_fields}, '{archive_timestamp}'
        FROM zerver_usermessage
        INNER JOIN zerver_message ON zerver_message.id = zerver_usermessage.message_id
        INNER JOIN zerver_userprofile ON zerver_usermessage.user_profile_id = zerver_userprofile.id
        INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
        WHERE zerver_realm.message_retention_days IS NOT NULL
             AND EXTRACT(DAY FROM (CURRENT_DATE - zerver_message.pub_date)) >= zerver_realm.message_retention_days
             AND zerver_usermessage.id NOT IN (SELECT id FROM zerver_archivedusermessage)
    """
    select_query = fill_select_query(select_query_template, UserMessage, UserMessage._meta.fields,
                                     archive_timestamp=timezone.now())
    if dry_run:
        return execute_select_query(select_query)
    insert_query = """
        INSERT INTO zerver_archivedusermessage ({dst_fields}, archive_timestamp)
        {select_query}
    """
    move_rows(select_query, UserMessage._meta.fields, insert_query)
    return None


def move_expired_attachments_to_archive(dry_run=False):
    # type: (bool) -> Optional[List[Tuple[Any]]]
    select_query_template = """
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
    select_query = fill_select_query(select_query_template, Attachment, Attachment._meta.fields,
                                     archive_timestamp=timezone.now())
    if dry_run:
        return execute_select_query(select_query)
    insert_query = """
       INSERT INTO zerver_archivedattachment ({dst_fields}, archive_timestamp)
        {select_query}
    """
    move_rows(select_query, Attachment._meta.fields, insert_query)
    return None

def move_expired_attachments_message_rows_to_archive(dry_run=False):
    # type: (bool) -> Optional[List[Tuple[Any]]]
    select_query = """
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
    if dry_run:
        return execute_select_query(select_query)
    insert_query = """
        INSERT INTO zerver_archivedattachment_messages (id, archivedattachment_id, archivedmessage_id)
        {select_query}
    """
    with connection.cursor() as cursor:
        cursor.execute(insert_query.format(select_query=select_query))
    return None


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


def archive_messages(dry_run=False):
    # type: (bool) -> Optional[Dict[Text, int]]
    # The main function for archiving messages' data.
    exp_messages = move_expired_messages_to_archive(dry_run)
    exp_user_messages = move_expired_user_messages_to_archive(dry_run)
    exp_attachments = move_expired_attachments_to_archive(dry_run)
    exp_attachments_message = move_expired_attachments_message_rows_to_archive(dry_run)
    if dry_run:
        return {
            "exp_messages": len(exp_messages),
            "exp_user_messages": len(exp_user_messages),
            "exp_attachments": len(exp_attachments),
            "exp_attachments_messages": len(exp_attachments_message)

        }
    else:
        delete_expired_user_messages()
        delete_expired_messages()
        delete_expired_attachments()
    return None


def delete_expired_archived_attachments(query):
    # type: (QuerySet) -> None
    # Delete old archived attachments from archive table
    # after retention period for archived data.
    arc_attachments = query.filter(messages__isnull=True)
    for arc_att in arc_attachments:
        delete_message_image(arc_att.path_id)
    arc_attachments.delete()


def delete_expired_archived_data(dry_run=False):
    # type: (bool) -> Dict[str, int]
    # Delete old archived messages and user_messages from archive tables
    # after retention period for archived data.
    arc_expired_date = timezone.now() - timedelta(days=settings.ARCHIVED_DATA_RETENTION_DAYS)
    del_arc_user_messages = ArchivedUserMessage.objects.filter(archive_timestamp__lt=arc_expired_date)
    del_arc_messages = ArchivedMessage.objects.filter(archive_timestamp__lt=arc_expired_date)
    del_arc_attachments = ArchivedAttachment.objects.filter(archive_timestamp__lt=arc_expired_date) \
        .exclude(id__in=Attachment.objects.all())
    if dry_run:
        return {
            "del_arc_user_messages": del_arc_user_messages.count(),
            "del_arc_messages": del_arc_messages.count(),
            "del_arc_attachments": del_arc_attachments.count()

        }
    del_arc_user_messages.delete()
    del_arc_messages.filter(archivedusermessage__isnull=True).delete()
    delete_expired_archived_attachments(del_arc_attachments)
    return None


def restore_archived_messages_by_realm(realm_id, dry_run=False):
    # type: (int, bool) -> Optional[Dict[Text, int]]
    # Function for restoring archived messages by realm for emergency cases.
    select_query_template = """
        SELECT {src_fields}
        FROM zerver_archivedmessage
        INNER JOIN zerver_archivedusermessage ON zerver_archivedmessage.id = zerver_archivedusermessage.message_id
        INNER JOIN zerver_userprofile ON zerver_archivedusermessage.user_profile_id = zerver_userprofile.id
        INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
        WHERE zerver_realm.id = {realm_id}
              AND zerver_archivedmessage.id NOT IN (SELECT ID FROM zerver_message)
        GROUP BY zerver_archivedmessage.id
    """
    select_query = fill_select_query(select_query_template, ArchivedMessage, Message._meta.fields,
                                     realm_id=realm_id)
    if dry_run:
        return execute_select_query(select_query)
    insert_query = """
        INSERT INTO zerver_message ({dst_fields})
        {select_query}
    """
    move_rows(select_query, Message._meta.fields, insert_query)
    return None

def restore_archived_usermessages_by_realm(realm_id, dry_run=False):
    # type: (int, bool) -> QuerySet
    # Function for restoring archived user_messages by realm for emergency cases.
    select_query_template = """
        SELECT {src_fields}
        FROM zerver_archivedusermessage
        INNER JOIN zerver_userprofile ON zerver_archivedusermessage.user_profile_id = zerver_userprofile.id
        INNER JOIN zerver_realm ON zerver_userprofile.realm_id = zerver_realm.id
        WHERE zerver_realm.id = {realm_id}
             AND zerver_archivedusermessage.id NOT IN (SELECT id FROM zerver_usermessage)
             AND zerver_archivedusermessage.message_id IN (SELECT id from zerver_message)
    """
    select_query = fill_select_query(select_query_template, ArchivedUserMessage,
                                     UserMessage._meta.fields, realm_id=realm_id)
    if dry_run:
        return ArchivedUserMessage.objects.filter(user_profile__realm_id=realm_id)
    insert_query = """
        INSERT INTO zerver_usermessage ({dst_fields})
        {select_query}
    """
    move_rows(select_query, UserMessage._meta.fields, insert_query)
    return None

def restore_archived_attachments_by_realm(realm_id, dry_run=False):
    # type: (int, bool) -> Optional[Dict[Text, int]]
    # Function for restoring archived attachments by realm for emergency cases.
    select_query_template = """
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
    select_query = fill_select_query(select_query_template, ArchivedAttachment, Attachment._meta.fields,
                                     realm_id=realm_id)
    if dry_run:
        return execute_select_query(select_query)
    insert_query = """
        INSERT INTO zerver_attachment ({dst_fields})
        {select_query}
    """
    move_rows(select_query, Attachment._meta.fields, insert_query)
    return None

def restore_archived_attachments_message_rows_by_realm(realm_id, dry_run=False):
    # type: (int, bool) -> Optional[Dict[Text, int]]
    # Function for restoring archived data in many-to-many attachment_messages
    # table by realm for emergency cases.
    select_query_template = """
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
    select_query = select_query_template.format(realm_id=realm_id)
    if dry_run:
        return execute_select_query(select_query)
    insert_query = """
        INSERT INTO zerver_attachment_messages (id, attachment_id, message_id)
        {select_query}
    """
    with connection.cursor() as cursor:
        cursor.execute(insert_query.format(select_query=select_query))
    return None


def restore_realm_archived_data(realm_id, dry_run=False):
    # type: (int, bool) -> Dict[str, int]
    # The main function for restoring archived messages' data by realm.
    restoring_arc_messages = restore_archived_messages_by_realm(realm_id, dry_run)
    restoring_arc_user_messages = restore_archived_usermessages_by_realm(realm_id, dry_run)
    restoring_arc_attachemnts = restore_archived_attachments_by_realm(realm_id, dry_run)
    rest_arc_attachments_message = restore_archived_attachments_message_rows_by_realm(realm_id,
                                                                                      dry_run)
    if dry_run:
        return {
            "restoring_arc_messages": len(restoring_arc_messages),
            "restoring_arc_user_messages": len(restoring_arc_user_messages),
            "restoring_arc_attachemnts": len(restoring_arc_attachemnts),
            "rest_arc_attachments_messages": len(rest_arc_attachments_message)
        }
    realm = Realm.objects.get(id=realm_id)
    realm.message_retention_days = None
    realm.save()
    return None
