# This file is part of fedmsg_migration_tools.
# Copyright (C) 2019 Red Hat, Inc.
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

import logging
import unittest

from fedmsg_migration_tools import filters


test_log = logging.Logger(__name__)


class RateLimiterTests(unittest.TestCase):
    """Tests for the :class:`filters.RateLimiter` class."""

    def test_filter_new_record(self):
        """Assert a new record is not limited."""
        record = test_log.makeRecord(
            "test_name", logging.INFO, "/my/file.py", 3, "beep boop", tuple(), None
        )
        rate_filter = filters.RateLimiter()

        self.assertTrue(rate_filter.filter(record))

    def test_filter_false(self):
        """Assert if the filename:lineno entry exists and is new, it's filtered out."""
        record = test_log.makeRecord(
            "test_name", logging.INFO, "/my/file.py", 3, "beep boop", tuple(), None
        )
        rate_filter = filters.RateLimiter(rate=2)
        rate_filter._sent["/my/file.py:3"] = record.created - 1

        self.assertFalse(rate_filter.filter(record))

    def test_rate_is_used(self):
        """Assert custom rates are respected."""
        record = test_log.makeRecord(
            "test_name", logging.INFO, "/my/file.py", 3, "beep boop", tuple(), None
        )
        rate_filter = filters.RateLimiter(rate=2)
        rate_filter._sent["/my/file.py:3"] = record.created - 2

        self.assertTrue(rate_filter.filter(record))

    def test_rate_limited(self):
        """Assert the first call is allowed and the subsequent one is not."""
        record = test_log.makeRecord(
            "test_name", logging.INFO, "/my/file.py", 3, "beep boop", tuple(), None
        )
        rate_filter = filters.RateLimiter(rate=60)

        self.assertTrue(rate_filter.filter(record))
        self.assertFalse(rate_filter.filter(record))

    def test_different_lines(self):
        """Assert rate limiting is line-dependent."""
        record1 = test_log.makeRecord(
            "test_name", logging.INFO, "/my/file.py", 3, "beep boop", tuple(), None
        )
        record2 = test_log.makeRecord(
            "test_name", logging.INFO, "/my/file.py", 4, "beep boop", tuple(), None
        )
        rate_filter = filters.RateLimiter()

        self.assertTrue(rate_filter.filter(record1))
        self.assertTrue(rate_filter.filter(record2))
