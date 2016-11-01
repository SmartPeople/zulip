# -*- coding: utf-8 -*-
from __future__ import absolute_import
from datetime import datetime, timedelta

from django.conf import settings
from django.utils import timezone

from zerver.lib.test_helpers import ZulipTestCase
from zerver.models import (Message, Realm, Recipient, UserProfile, ArchiveUserMessage,
                           ArchiveMessage, UserMessage, get_user_profile_by_email)
from zerver.lib.retention import (move_expired_messages_to_archive,
                                  move_expired_user_messages_to_archive, delete_expired_messages,
                                  delete_expired_user_messages, archive_messages)

from six.moves import range

from typing import Any


class TestRetentionLib(ZulipTestCase):
    """
        Test receiving expired messages retention tool.
    """

    def setUp(self):
        # type: () -> None
        super(TestRetentionLib, self).setUp()
        self.zulip_realm = self._set_realm_message_retention_value('zulip.com', 30)
        self.mit_realm = self._set_realm_message_retention_value('mit.edu', 100)

    @staticmethod
    def _set_realm_message_retention_value(domain, retention_period):
        # type: (str, int) -> Realm
        # Change retention period for certain realm.
        realm = Realm.objects.filter(domain=domain).first()
        realm.message_retention_days = retention_period
        realm.save()
        return realm

    @staticmethod
    def _change_msgs_pub_date(msgs_ids, pub_date):
        # type: (List[int], datetime) -> Any
        # Update message pud_date value.
        msgs = Message.objects.filter(id__in=msgs_ids).order_by('id')
        msgs.update(pub_date=pub_date)
        return msgs

    def _make_mit_msgs(self, msg_qauntity, pub_date):
        # type: (int, datetime) -> Any
        # Send messages from mit.edu realm and change messages pub_date.
        sender = UserProfile.objects.filter(email='espuser@mit.edu').first()
        recipient = UserProfile.objects.filter(email='starnine@mit.edu').first()
        msgs_ids = [self.send_message(sender.email, recipient.email, Recipient.PERSONAL) for i in
                    range(msg_qauntity)]
        mit_msgs = self._change_msgs_pub_date(msgs_ids, pub_date)
        return mit_msgs

    def _send_cross_realm_message(self):
        # type: () -> int
        # Send message from bot to users from different realm.
        settings.CROSS_REALM_BOT_EMAILS.add('test-og-bot@zulip.com')
        user1_email = 'test-og-bot@zulip.com'
        self.create_user(user1_email)
        zulip_user = UserProfile.objects.filter(realm=self.zulip_realm).first()
        mit_user = UserProfile.objects.filter(realm=self.mit_realm).first()
        return self.send_message(user1_email, [zulip_user.email, mit_user.email],
                                 Recipient.PERSONAL)

    def create_user(self, email):
        # type: (str) -> UserProfile
        # Create user by email
        username, domain = email.split('@')
        self.register(username, 'test', domain=domain)
        return get_user_profile_by_email(email)

    def test_no_expired_messages(self):
        # type: () -> None
        move_expired_messages_to_archive()
        move_expired_user_messages_to_archive()
        self.assertEqual(ArchiveUserMessage.objects.count(), 0)
        self.assertEqual(ArchiveMessage.objects.count(), 0)

    def test_expired_msgs_in_each_realm(self):
        # type: () -> None
        # Check result realm messages order and result content
        # when all realm has expired messages.
        exp_messages_ids = []
        exp_mit_msgs = self._make_mit_msgs(3, timezone.now() - timedelta(days=101))
        exp_messages_ids.extend(list(exp_mit_msgs.order_by('id').values_list('id', flat=True)))
        self._make_mit_msgs(4, timezone.now() - timedelta(days=50))
        zulip_msgs_ids = list(Message.objects.order_by('id').filter(
            sender__realm=self.zulip_realm).values_list('id', flat=True)[3:10])
        exp_messages_ids.extend(zulip_msgs_ids)
        exp_zulip_msgs = self._change_msgs_pub_date(zulip_msgs_ids,
                                                    timezone.now() - timedelta(days=31))
        move_expired_messages_to_archive()
        move_expired_user_messages_to_archive()
        archived_messages = ArchiveMessage.objects.all()
        archived_user_messages = ArchiveUserMessage.objects.all()
        self.assertEqual(archived_messages.count(), len(exp_messages_ids))
        self.assertEqual(
            archived_user_messages.count(),
            UserMessage.objects.filter(message_id__in=exp_messages_ids).count()
        )
        # Compare expected messages ids with archived messages by realm.
        self.assertEqual(
            list(exp_mit_msgs.order_by('id').values_list('id', flat=True)),
            list(
                archived_messages
                    .filter(archiveusermessage__user_profile__realm=self.mit_realm)
                    .order_by('id')
                    .distinct('id')
                    .values_list('id', flat=True)
            )
        )
        self.assertEqual(
            list(exp_zulip_msgs.order_by('id').values_list('id', flat=True)),
            list(
                archived_messages
                    .filter(archiveusermessage__user_profile__realm=self.zulip_realm)
                    .order_by('id')
                    .distinct('id')
                    .values_list('id', flat=True))
        )
        self.assertEqual(
            list(
                UserMessage.objects
                    .filter(message__in=exp_mit_msgs)
                    .order_by('id')
                    .values_list('id', flat=True)
            ),
            list(
                ArchiveUserMessage.objects
                    .filter(user_profile__realm=self.mit_realm)
                    .order_by('id')
                    .values_list('id', flat=True)
            )
        )
        self.assertEqual(
            list(
                UserMessage.objects
                    .filter(message__in=exp_zulip_msgs)
                    .order_by('id')
                    .values_list('id', flat=True)
            ),
            list(
                ArchiveUserMessage.objects
                    .filter(message__in=archived_messages, user_profile__realm=self.zulip_realm)
                    .order_by('id')
                    .values_list('id', flat=True)
            )
        )

    def test_expired_messages_in_one_realm(self):
        # type: () -> None
        # Check realm with expired messages and messages
        # with one day to expiration data.
        exp_mit_msgs = self._make_mit_msgs(5, timezone.now() - timedelta(days=101))
        move_expired_messages_to_archive()
        move_expired_user_messages_to_archive()
        archived_messages = ArchiveMessage.objects.all()
        archived_user_messages = ArchiveUserMessage.objects.all()
        self.assertEqual(archived_messages.count(), 5)
        self.assertEqual(archived_user_messages.count(), 10)
        # Compare expected messages ids with archived messages in mit realm
        self.assertEqual(
            list(exp_mit_msgs.order_by('id').values_list('id', flat=True)),
            list(
                archived_messages
                    .filter(archiveusermessage__user_profile__realm=self.mit_realm)
                    .order_by('id')
                    .distinct('id')
                    .values_list('id', flat=True)
            )
        )
        self.assertEqual(
            list(
                UserMessage.objects
                    .filter(message__in=exp_mit_msgs)
                    .order_by('id')
                    .values_list('id', flat=True)
            ),
            list(
                ArchiveUserMessage.objects
                    .filter(user_profile__realm=self.mit_realm)
                    .order_by('id')
                    .values_list('id', flat=True)
            )
        )
        # Check no archive messages for zulip realm.
        self.assertEqual(
            archived_messages
                .filter(archiveusermessage__user_profile__realm=self.zulip_realm)
                .distinct('id')
                .count()
            ,
            0
        )
        self.assertEqual(
            ArchiveUserMessage.objects
                .filter(user_profile__realm=self.zulip_realm)
                .count(),
            0
        )

    def test_cross_realm_messages_archiving_one_realm_expired(self):
        # type: () -> None
        # Check archiving messages which is sent to different realms
        # and expired just on on one of them.
        sended_message_id = self._send_cross_realm_message()
        all_user_messages_qty = UserMessage.objects.count()
        self._change_msgs_pub_date([sended_message_id], timezone.now() - timedelta(days=31))
        move_expired_messages_to_archive()
        move_expired_user_messages_to_archive()
        user_messages_sended = UserMessage.objects.filter(message_id=sended_message_id)
        archived_messages = ArchiveMessage.objects.all()
        archived_user_messages = ArchiveUserMessage.objects.all()
        self.assertEqual(user_messages_sended.count(), 3)
        # Compare archived messages and user messages
        # with expired sended messages.
        self.assertEqual(archived_messages.count(), 1)
        self.assertEqual(archived_user_messages.count(), 2)
        self.assertEqual(
            list(ArchiveUserMessage.objects.order_by('id').values_list('id', flat=True)),
            list(
                user_messages_sended
                    .filter(user_profile__realm=self.zulip_realm)
                    .order_by('id')
                    .values_list('id', flat=True))
        )
        delete_expired_user_messages()
        delete_expired_messages()
        # Check messages and user messages after deleting expired messages
        # from the main tables.
        self.assertTrue(Message.objects.filter(id=sended_message_id).exists())
        self.assertEqual(user_messages_sended.count(), 1)
        self.assertEqual(UserMessage.objects.count(), all_user_messages_qty - 2)

    def test_cross_realm_messages_archiving_two_realm_expired(self):
        # type: () -> None
        # Check archiving cross realm message wich is expired on both realms.
        sended_message_id = self._send_cross_realm_message()
        all_user_messages_qty = UserMessage.objects.count()
        self._change_msgs_pub_date([sended_message_id], timezone.now() - timedelta(days=101))
        move_expired_messages_to_archive()
        move_expired_user_messages_to_archive()
        user_messages_sended = UserMessage.objects.filter(message_id=sended_message_id)
        archived_messages = ArchiveMessage.objects.all()
        archived_user_messages = ArchiveUserMessage.objects.all()
        self.assertEqual(user_messages_sended.count(), 3)
        # Compare archived messages and user messages
        # with expired sended messages.
        self.assertEqual(archived_messages.count(), 1)
        self.assertEqual(archived_user_messages.count(), 3)
        self.assertEqual(
            list(ArchiveUserMessage.objects.order_by('id').values_list('id', flat=True)),
            list(user_messages_sended.order_by('id').values_list('id', flat=True))
        )
        delete_expired_user_messages()
        delete_expired_messages()
        # Check messages and user messages after deleting expired messages
        # from the main tables.
        self.assertFalse(Message.objects.filter(id=sended_message_id).exists())
        self.assertEqual(user_messages_sended.count(), 0)
        self.assertEqual(UserMessage.objects.count(), all_user_messages_qty - 3)

    def test_archive_message_tool(self):
        # type: () -> None
        # Check archiving tool.
        exp_messages_ids = []  # List of expired messages ids.
        exp_mit_msgs = self._make_mit_msgs(3, timezone.now() - timedelta(days=101))
        exp_mit_msgs_ids = list(exp_mit_msgs.order_by('id').values_list('id', flat=True))
        exp_messages_ids.extend(exp_mit_msgs_ids)
        self._make_mit_msgs(4, timezone.now() - timedelta(days=50))
        zulip_msgs_ids = list(Message.objects.order_by('id').filter(
            sender__realm=self.zulip_realm).values_list('id', flat=True)[3:10])
        # Add expired zulip mesages ids.
        exp_messages_ids.extend(zulip_msgs_ids)
        self._change_msgs_pub_date(zulip_msgs_ids,
                                   timezone.now() - timedelta(days=31))
        sended_cross_realm_message_id = self._send_cross_realm_message()
        exp_mit_msgs_ids.append(sended_cross_realm_message_id)
        # Add cross realm message id.
        exp_messages_ids.append(sended_cross_realm_message_id)
        self._change_msgs_pub_date(
            [sended_cross_realm_message_id],
            timezone.now() - timedelta(days=101)
        )
        # Get expired user messages by message ids
        exp_user_messages_ids = list(UserMessage.objects.filter(
            message_id__in=exp_messages_ids).order_by('id').values_list('id', flat=True))
        msgs_qty = Message.objects.count()
        user_msgs_qty = UserMessage.objects.count()
        archive_messages()
        # Compare archived messages and user messages with expired messages
        self.assertEqual(ArchiveMessage.objects.count(), len(exp_messages_ids))
        self.assertEqual(ArchiveUserMessage.objects.count(), len(exp_user_messages_ids))
        # Check left messages after removing expired messages from main tables
        self.assertEqual(Message.objects.count(), msgs_qty - ArchiveMessage.objects.count())
        self.assertEqual(UserMessage.objects.count(), user_msgs_qty - len(exp_user_messages_ids))
        self.assertEqual(exp_mit_msgs.count(), 0)
        self.assertEqual(Message.objects.filter(id__in=zulip_msgs_ids).count(), 0)
        zulip_msgs_ids.append(sended_cross_realm_message_id)
        # Check archived messages by realm
        self.assertEqual(
            zulip_msgs_ids,
            list(
                ArchiveMessage.objects
                    .filter(archiveusermessage__user_profile__realm=self.zulip_realm)
                    .distinct('id')
                    .values_list('id', flat=True))
        )
        self.assertEqual(
            exp_mit_msgs_ids,
            list(
                ArchiveMessage.objects
                    .filter(archiveusermessage__user_profile__realm=self.mit_realm)
                    .distinct('id')
                    .values_list('id', flat=True))
        )
