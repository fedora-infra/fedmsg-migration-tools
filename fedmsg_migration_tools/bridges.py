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

import fedora_messaging.api
import fedora_messaging.message
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

        message = fedora_messaging.message.Message(
            body=json.loads(body), topic=topic.decode("utf-8"),
        )
        _log.debug('Publishing %r to %r', body, topic)
        try:
            fedora_messaging.api.publish(message, exchange=exchange)
        except Exception as e:
            _log.exception('Publishing "%r" to exchange "%r" on topic "%r" failed (%r)',
                           body, exchange, topic, e)


def amqp_to_zmq(queue_name, bindings, publish_endpoint):
    """
    Publish messages from an AMQP queue to ZeroMQ.

    Args:
        queue_name (str): The queue name to use. If it doesn't exist it will be created.
        bindings (dict): A list of dictionaries with "exchange" and "routing_key" keys.
        publish_endpoint: The ZeroMQ socket to bind to.
    """
    # ZMQ
    context = zmq.Context.instance()
    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(publish_endpoint)
    _log.info('Bound to %s for ZeroMQ publication', publish_endpoint)

    # AMQP
    def on_message(ch, method, properties, body):
        try:
            topic = method.routing_key
            _log.debug('Publishing message on "%s" to the ZeroMQ PUB socket "%s"',
                       topic, publish_endpoint)
            zmq_message = [topic.encode("utf-8"), body]
            pub_socket.send_multipart(zmq_message)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except zmq.ZMQError as e:
            _log.error('Message delivery failed: %r', e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    fedora_messaging.api.consume(on_message, bindings)
