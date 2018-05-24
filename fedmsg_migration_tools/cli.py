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
"""
The ``fedmsg-migration-tools`` `Click`_ CLI.

.. _Click: http://click.pocoo.org/
"""
from __future__ import absolute_import

import logging
import logging.config

import click
import zmq

from . import (
    bridges as bridges_module,
    config,
    verify_missing as verify_missing_module,
)

try:
    from twisted import logger as tw_logger
except ImportError:
    tw_logger = None


_log = logging.getLogger(__name__)


@click.group()
@click.option('--conf', envvar='FEDMSG_MIGRATION_TOOLS_CONFIG')
def cli(conf):
    """
    The fedmsg-migration-tools command line interface.

    This can be used to run AMQP <-> ZMQ bridge services.
    """
    if conf:
        try:
            config.conf.load_config(filename=conf)
        except ValueError as e:
            raise click.exceptions.BadParameter(e)


@cli.command()
@click.option('--topic', multiple=True)
@click.option('--zmq-endpoint', multiple=True, help='A ZMQ socket to subscribe to')
@click.option('--exchange')
def zmq_to_amqp(exchange, zmq_endpoint, topic):
    """Bridge ZeroMQ messages to an AMQP exchange."""
    topics = topic or config.conf['zmq_to_amqp']['topics']
    exchange = exchange or config.conf['zmq_to_amqp']['exchange']
    topics = [t.encode('utf-8') for t in topics]

    zmq_endpoints = zmq_endpoint or config.conf['zmq_to_amqp']['zmq_endpoints']
    if not zmq_endpoints:
        raise click.exceptions.UsageError(
            'No ZeroMQ endpoints defined, please provide one or more endpoints '
            'using the --zmq-endpoint flag or by setting endpoints in the '
            '"zmq_to_amqp" section of your configuration.')

    try:
        bridges_module.zmq_to_amqp(exchange, zmq_endpoints, topics)
    except Exception:
        _log.exception('An unexpected error occurred, please file a bug report')


@cli.command()
@click.option('--zmq-endpoint', multiple=True, help='A ZMQ socket to subscribe to')
def verify_missing(zmq_endpoint):
    """Check that all messages go through AMQP and ZeroMQ."""
    if tw_logger is None:
        raise click.exceptions.UsageError(
            "You need to install Twisted to use this command."
        )
    tw_logger.globalLogPublisher.addObserver(
        tw_logger.STDLibLogObserver(name="verify_missing")
    )
    zmq_endpoints = zmq_endpoint or config.conf['zmq_to_amqp']['zmq_endpoints']
    if not zmq_endpoints:
        raise click.exceptions.UsageError(
            'No ZeroMQ endpoints defined, please provide one or more endpoints '
            'using the --zmq-endpoint flag or by setting endpoints in the '
            '"zmq_to_amqp" section of your configuration.')
    try:
        verify_missing_module.main(zmq_endpoints)
    except zmq.error.ZMQError as e:
        _log.exception(e)
    except Exception:
        _log.exception('An unexpected error occurred, please file a bug report')
