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
from __future__ import absolute_import

import logging
import logging.config

from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin
from kombu.pools import connections
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

    retry_policy = {
        'interval_start': 0,
        'interval_step': 2,
        'interval_max': 15,
        'max_retries': 60,
    }
    with connections[Connection(amqp_url)].acquire(block=True, timeout=60) as conn:
        exchange = Exchange(name=exchange, type='topic', durable=True)
        producer = conn.Producer(auto_declare=True, exchange=exchange)
        _log.info('Successfully connected to the AMQP broker; publishing to the "%s" '
                  'exchange', exchange.name)
        while True:
            try:
                topic, body = sub_socket.recv_multipart()
            except zmq.ZMQError as e:
                _log.error('Failed to receive message from subscription socket: %s', str(e))
                continue
            except ValueError as e:
                _log.error('Unable to unpack message from pair socket: %s', e)
                continue

            _log.debug('Publishing %r to %r', body, topic)
            try:
                producer.publish(body, routing_key=topic, retry=True, retry_policy=retry_policy)
            except Exception as e:
                _log.exception('Publishing "%r" to exchange "%r" on topic "%r" failed (%r)',
                               body, exchange, topic, e)


def amqp_to_zmq(amqp_url, queue_name, bindings, publish_endpoint):
    """
    Publish messages from an AMQP queue to ZeroMQ.

    Args:
        amqp_url (str): The URL of the AMQP server to connect to.
        queue_name (str): The queue name to use. If it doesn't exist it will be created.
        bindings (dict): A list of dictionaries with "exchange" and "routing_key" keys.
        publish_endpoint: The ZeroMQ socket to bind to.
    """
    consumer = AmqpConsumer(amqp_url, queue_name, bindings, publish_endpoint)
    consumer.run()


class AmqpConsumer(ConsumerMixin):
    """Consumes messages from AMQP and publishes them to ZeroMQ."""

    def __init__(self, amqp_url, queue_name, bindings, publish_endpoint):
        self.connection = Connection(amqp_url)
        self.bindings = bindings
        self.queues = [Queue(queue_name)]
        context = zmq.Context.instance()
        self.publish_endpoint = publish_endpoint
        self.pub_socket = context.socket(zmq.PUB)
        self.pub_socket.bind(publish_endpoint)
        _log.info('Bound to %s for ZeroMQ publication', publish_endpoint)

    def get_consumers(self, consumer_class, channel):
        """
        Get the list of Consumers this class uses.

        This is part of the :class:`kombu.mixins.ConsumerMixin` API.

        Args:
            consumer_class (class): The class to use when creating consumers.
            channel (kombu.Channel): Unused.
        """
        return [
            consumer_class(self.queues, callbacks=[self.on_message], accept=['json']),
        ]

    def on_consume_ready(self, connection, channel, consumers, **kwargs):  # pragma: no cover
        """
        Implement the ConsumerMixin API.

        Args:
            connection (kombu.Connection): Unused.
            channel (kombu.Channel): Unused.
            consumers (list): List of :class:`kombu.Consumer`. Unused.
        """
        _log.info('AMQP consumer ready on the %r queues', self.queues)
        for consumer in consumers:
            for queue in consumer.queues:
                for bind in self.bindings:
                    queue.bind_to(
                        exchange=bind['exchange'], routing_key=bind['routing_key'],
                        arguments=bind['arguments'])
                    _log.info('Binding "%s" to %r', queue.name, bind)
        super(AmqpConsumer, self).on_consume_ready(connection, channel, consumers, **kwargs)

    def on_consume_end(self, connection, channel):  # pragma: no cover
        """
        Implement the ConsumerMixin API.

        Args:
            connection (kombu.Connection): Unused.
            channel (kombu.Channel): Unused.
        """
        _log.info('Successfully canceled the AMQP consumer')
        super(AmqpConsumer, self).on_consume_end(connection, channel)

    def on_message(self, body, message):
        """
        The callback for the Consumer, called when a message is received.

        As this consumer must run outside the reactor thread (since it uses blocking APIs)
        this simply uses the Twisted API to call the delivery service's message handler inside
        the reactor thread.

        Args:
            body (dict): The decoded message body.
            message (kombu.Message): The Kombu message object.
        """
        try:
            topic = message.delivery_info['routing_key']
            _log.debug('Publishing message on "%s" to the ZeroMQ PUB socket "%s"',
                       topic, self.publish_endpoint)

            zmq_message = [topic.encode('utf-8'), body.encode('utf-8')]
            self.pub_socket.send_multipart(zmq_message)
            message.ack()
        except zmq.ZMQError as e:
            _log.error('Message delivery failed: %r', e)
            message.requeue()
