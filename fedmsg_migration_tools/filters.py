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
"""Standard library logging filters."""

import logging


class RateLimiter(logging.Filter):
    """
    Log filter that rate-limits logs based on time.

    The rate limit is applied to records by filename and line number.

    Filters can be applied to handlers and loggers. Configuring this via
    dictConfig is possible, but has somewhat odd syntax::

        log_config = {
            "filters": {
                "60_second_filter": {
                    "()": "fedmsg_migration_tools.filters.RateLimiter",
                    "rate": "60"
                }
            }
            "handlers": {
                "rate_limited": {
                    "filters": ["60_second_filter"],
                    ...
                }
            }
            "loggers": {
                "fedmsg_migration_tools": {
                    "filters": ["60_second_filter"],
                    ...
                }
            }
        }

    Args:
        rate (int): How often, in seconds, to allow records. Defaults to hourly.
    """

    def __init__(self, rate=3600):
        self.rate = rate
        self._sent = {}

    def filter(self, record):
        """Record call sites and filter based on time."""
        key = "{}:{}".format(record.pathname, record.lineno)
        try:
            if self.rate > record.created - self._sent[key]:
                return False
        except KeyError:
            pass
        self._sent[key] = record.created
        return True
