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

import json
import logging

from fedora_messaging import api, config as fm_config
from fedora_messaging.message import Message
from fedora_messaging.exceptions import Nack
import zmq

_log = logging.getLogger(__name__)


def zmq_to_amqp(exchange, zmq_endpoints, topics):
    """
    Connect to a set of ZeroMQ PUB sockets and re-publish the messages to an AMQP
    exchange.
    """
    context = zmq.Context.instance()
    sub_socket = context.socket(zmq.SUB)
    for endpoint in zmq_endpoints:
        sub_socket.connect(endpoint)
        _log.info('Connecting ZeroMQ subscription socket to %s', endpoint)
    for topic in topics:
        sub_socket.setsockopt(zmq.SUBSCRIBE, topic)
        _log.info('Configuring ZeroMQ subscription socket with the "%s" topic', topic)

    while True:
        try:
            topic, body = sub_socket.recv_multipart()
        except zmq.ZMQError as e:
            _log.error('Failed to receive message from subscription socket: %s', e)
            continue
        except ValueError as e:
            _log.error('Unable to unpack message from pair socket: %s', e)
            continue

        message = Message(
            body=json.loads(body), topic=topic.decode("utf-8"),
        )
        _log.debug('Publishing %r to %r', body, topic)
        try:
            api.publish(message, exchange=exchange)
        except Exception as e:
            _log.exception('Publishing "%r" to exchange "%r" on topic "%r" failed (%r)',
                           body, exchange, topic, e)


class AmqpToZmq(object):
    """
    A fedora-messaging consumer that publishes messages it consumes to ZeroMQ.

    A single configuration key is used from fedora-messaging's "consumer_config"
    key, "publish_endpoint", which should be a ZeroMQ socket. For example, to
    bind to just the IPv4 local host interface, place the following in your
    fedora-messaging configuration file::

        [consumer_config]
        publish_endpoint = "tcp://127.0.0.1:9940"

    The default is to bind to all available interfaces on port 9940.
    """

    def __init__(self):
        try:
            self.publish_endpoint = fm_config.conf['consumer_config']['publish_endpoint']
        except KeyError:
            self.publish_endpoint = 'tcp://*:9940'

        context = zmq.Context.instance()
        self.pub_socket = context.socket(zmq.PUB)
        self.pub_socket.bind(self.publish_endpoint)
        _log.info('Bound to %s for ZeroMQ publication', self.publish_endpoint)

    def __call__(self, message):
        """
        Invoked when a message is received by the consumer.

        Args:
            message (fedora_messaging.api.Message): The message from AMQP.
        """
        try:
            _log.debug('Publishing message on "%s" to the ZeroMQ PUB socket "%s"',
                       message.topic, self.publish_endpoint)
            zmq_message = [message.topic.encode("utf-8"), json.dumps(message.body).encode('utf-8')]
            self.pub_socket.send_multipart(zmq_message)
        except zmq.ZMQError as e:
            _log.error('Message delivery failed: %r', e)
            raise Nack()
