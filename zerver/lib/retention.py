from __future__ import absolute_import
from __future__ import print_function


from django.db import connection, transaction
from django.utils import timezone
from zerver.models import Message, UserMessage, ArchivedMessage, ArchivedUserMessage

from typing import Any, Optional


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

def archive_messages():
    # type: () -> None
    move_expired_messages_to_archive()
    move_expired_user_messages_to_archive()
    delete_expired_user_messages()
    delete_expired_messages()
