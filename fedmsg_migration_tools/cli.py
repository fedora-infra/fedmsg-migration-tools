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
    config,
    bridges as bridges_module,
    verify_missing as verify_missing_module,
)

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
@click.option('--amqp-url')
def zmq_to_amqp(amqp_url, exchange, zmq_endpoint, topic):
    """Bridge ZeroMQ messages to an AMQP exchange."""
    amqp_url = amqp_url or config.conf['amqp_url']
    topics = topic or config.conf['zmq_to_amqp']['topics']
    exchange = exchange or config.conf['zmq_to_amqp']['exchange']
    zmq_endpoints = zmq_endpoint or config.conf['zmq_to_amqp']['zmq_endpoints']
    topics = [t.encode('utf-8') for t in topics]
    try:
        bridges_module.zmq_to_amqp(amqp_url, exchange, zmq_endpoints, topics)
    except Exception:
        _log.exception('An unexpected error occurred, please file a bug report')


@cli.command()
@click.option('--publish-endpoint')
@click.option('--bindings')
@click.option('--queue-name')
@click.option('--amqp-url')
def amqp_to_zmq(amqp_url, queue_name, bindings, publish_endpoint):
    """Bridge an AMQP queue to a ZeroMQ PUB socket."""
    amqp_url = amqp_url or config.conf['amqp_url']
    queue_name = queue_name or config.conf['amqp_to_zmq']['queue_name']
    bindings = bindings or config.conf['amqp_to_zmq']['bindings']
    publish_endpoint = publish_endpoint or config.conf['amqp_to_zmq']['publish_endpoint']
    try:
        bridges_module.amqp_to_zmq(amqp_url, queue_name, bindings, publish_endpoint)
    except zmq.error.ZMQError as e:
        _log.error(str(e))
    except Exception:
        _log.exception('An unexpected error occurred, please file a bug report')


@cli.command()
@click.option('--zmq-endpoint', multiple=True, help='ZMQ socket where messages are published')
@click.option('--exchange', multiple=True, help="AMQP exchanges to bind to")
@click.option('--amqp-url')
def verify_missing(amqp_url, exchange, zmq_endpoint):
    """Check that all messages go through AMQP and ZeroMQ."""
    amqp_url = amqp_url or config.conf['amqp_url']
    zmq_endpoints = zmq_endpoint or config.conf['zmq_to_amqp']['zmq_endpoints']
    exchanges = exchange
    if not exchanges:
        exchanges = [b["exchange"] for b in config.conf['amqp_to_zmq']['bindings']]
        exchanges.append(config.conf['zmq_to_amqp']['exchange'])
    try:
        verify_missing_module.main(amqp_url, zmq_endpoints, exchanges)
    except zmq.error.ZMQError as e:
        _log.exception(e)
    except Exception:
        _log.exception('An unexpected error occurred, please file a bug report')
