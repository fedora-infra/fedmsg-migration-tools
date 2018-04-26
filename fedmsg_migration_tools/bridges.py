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

import logging

import pika
import zmq

_log = logging.getLogger(__name__)


def zmq_to_amqp(amqp_url, exchange, zmq_endpoints, topics):
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

    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)
    while True:
        try:
            topic, body = sub_socket.recv_multipart()
        except zmq.ZMQError as e:
            _log.error('Failed to receive message from subscription socket: %s', e)
            continue
        except ValueError as e:
            _log.error('Unable to unpack message from pair socket: %s', e)
            continue

        _log.debug('Publishing %r to %r', body, topic)
        try:
            channel.basic_publish(
                exchange=exchange,
                routing_key=topic,
                body=body,
                properties=pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=1,
                )
            )
        except Exception as e:
            _log.exception('Publishing "%r" to exchange "%r" on topic "%r" failed (%r)',
                           body, exchange, topic, e)
    connection.close()


def amqp_to_zmq(amqp_url, queue_name, bindings, publish_endpoint):
    """
    Publish messages from an AMQP queue to ZeroMQ.

    Args:
        amqp_url (str): The URL of the AMQP server to connect to.
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
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    _log.info('AMQP consumer ready on the %r queue', queue_name)
    for bind in bindings:
        channel.exchange_declare(
            exchange=bind['exchange'],
            exchange_type='topic',
            durable=True,
        )
        channel.queue_bind(
            queue=queue_name,
            exchange=bind['exchange'],
            routing_key=bind['routing_key'],
            arguments=bind['arguments'],
        )
        _log.info('Binding "%s" to %r', queue_name, bind)

    # Start relaying
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
    channel.basic_consume(on_message, queue=queue_name)
    channel.start_consuming()
