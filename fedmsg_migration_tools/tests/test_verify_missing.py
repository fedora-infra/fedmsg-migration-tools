# This file is part of fedmsg_migration_tools.
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import datetime
import json
import unittest

from fedmsg_migration_tools import verify_missing


class ZmqConsumerTestCase(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.consumer = verify_missing.ZmqConsumer(self.store, [])

    def test_year_prefix(self):
        """Assert the year prefix on fedmsg is removed."""
        year = datetime.datetime.utcnow().year
        msg = {
            "msg_id": "{}-dummy-msgid".format(year),
            "body": "dummy-body",
        }
        self.consumer.on_message(json.dumps(msg), b"dummy.topic")
        self.assertEqual(len(self.store), 1)
        self.assertIn("dummy-msgid", self.store)
        self.assertEqual(self.store["dummy-msgid"][1], msg)

    def test_without_year_prefix(self):
        """Assert it handles messages without the year prefix."""
        msg = {
            "msg_id": "dummy-msgid",
            "body": "dummy-body",
        }
        self.consumer.on_message(json.dumps(msg), b"dummy.topic")
        self.assertEqual(len(self.store), 1)
        self.assertIn("dummy-msgid", self.store)
        self.assertEqual(self.store["dummy-msgid"][1], msg)
