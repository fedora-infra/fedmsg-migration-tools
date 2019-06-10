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
import logging
import socket
import time

from fedmsg import config as fedmsg_config
from fedora_messaging import api, config as fm_config
from fedora_messaging.message import Message
from fedora_messaging.exceptions import Nack, HaltConsumer
import fedmsg
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
        _log.info("Connecting ZeroMQ subscription socket to %s", endpoint)
    for topic in topics:
        sub_socket.setsockopt(zmq.SUBSCRIBE, topic)
        _log.info('Configuring ZeroMQ subscription socket with the "%s" topic', topic)

    while True:
        try:
            topic, zmq_message = sub_socket.recv_multipart()
        except zmq.ZMQError as e:
            _log.error("Failed to receive message from subscription socket: %s", e)
            continue
        except ValueError as e:
            _log.error("Unable to unpack message from pair socket: %s", e)
            continue

        _convert_and_maybe_publish(topic, zmq_message, exchange)


def _convert_and_maybe_publish(topic, zmq_message, exchange):
    """
    Try to convert a fedmsg to a valid fedora-messaging AMQP message and
    publish it.  If something is wrong, no exception will be raised and the
    message will just be dropped.

    Args:
        topic (bytes): The ZeroMQ message topic. Assumed to be UTF-8 encoded.
        zmq_message (bytes): The ZeroMQ message. Assumed to be UTF-8 encoded.
        exchange (str): The name of the AMQP exchange to publish to.
    """
    try:
        zmq_message = json.loads(zmq_message)
    except (ValueError, TypeError):
        _log.error("Failed to parse %r as a json message", repr(zmq_message))
        return

    try:
        if zmq_message["username"] == "amqp-bridge":
            _log.info(
                "Dropping message %s as it's from the AMQP->ZMQ bridge",
                zmq_message["msg_id"],
            )
            return
    except KeyError:
        # Some messages aren't coming from fedmsg so they lack the username key
        if "msg_id" in zmq_message:
            _log.info(
                'Publishing %s despite it missing the normal "username" key',
                zmq_message["msg_id"],
            )
        else:
            _log.error("Message is missing a message id, dropping it")
            return

    if fedmsg_config.conf["validate_signatures"] and not fedmsg.crypto.validate(
        zmq_message, **fedmsg_config.conf
    ):
        _log.error("Message on topic %r failed validation", topic)
        return

    try:
        body = zmq_message["msg"]
    except KeyError:
        _log.error(
            "The zeromq message %r didn't have a 'msg' key; dropping", zmq_message
        )
        return

    try:
        headers = zmq_message["headers"]
    except KeyError:
        headers = None

    message = Message(body=body, headers=headers, topic=topic.decode("utf-8"))
    message.id = zmq_message["msg_id"]

    _log.debug("Publishing %r to %r", body, topic)
    try:
        api.publish(message, exchange=exchange)
    except Exception as e:
        _log.exception(
            'Publishing "%r" to exchange "%r" on topic "%r" failed (%r)',
            body,
            exchange,
            topic,
            e,
        )


class AmqpToZmq(object):
    """
    A fedora-messaging consumer that publishes messages it consumes to ZeroMQ.

    A configuration key is used from fedora-messaging's "consumer_config"
    key, "publish_endpoint", which should be a ZeroMQ socket. For example, to
    bind to just the IPv4 local host interface, place the following in your
    fedora-messaging configuration file::

        [consumer_config]
        publish_endpoint = "tcp://127.0.0.1:9940"

    The default is to bind to all available interfaces on port 9940.

    If you need to connect to a remote relay to publish the ZeroMQ messages,
    set the "remote_publish" configuration value to ``true``. For example::

        [consumer_config]
        publish_endpoint = "tcp://gateway.example.com:9941"
        remote_publish = true

    Additionally, this consumer can optionally sign messages if they don't have
    a signature already. This happens if the published message originates from
    an AMQP publisher. The ZMQ -> AMQP bridge can be configured to validate
    messages signatures before publishing, so we rely on the AMQP broker's
    authentication and authorization to ensure the message is legitimate. To
    enable this, set "sign_messages" to true in the fedmsg configuration.
    """

    def __init__(self):
        try:
            self.publish_endpoint = fm_config.conf["consumer_config"][
                "publish_endpoint"
            ]
        except KeyError:
            self.publish_endpoint = "tcp://*:9940"

        context = zmq.Context.instance()
        self.pub_socket = context.socket(zmq.PUB)
        if fm_config.conf["consumer_config"].get("remote_publish", False):
            self.pub_socket.connect(self.publish_endpoint)
            _log.info("Connected to %s for ZeroMQ publication", self.publish_endpoint)
        else:
            self.pub_socket.bind(self.publish_endpoint)
            _log.info("Bound to %s for ZeroMQ publication", self.publish_endpoint)
        self._message_counter = 0

    def __call__(self, message):
        """
        Invoked when a message is received by the consumer.

        Args:
            message (fedora_messaging.api.Message): The message from AMQP.
        """
        # fedmsg wraps message bodies in the following dictionary. We need to
        # wrap messages bridged back into ZMQ with it so old consumers don't
        # explode with KeyErrors.
        self._message_counter += 1
        wrapped_body = {
            "topic": message.topic,
            "msg": message._body,
            "timestamp": int(time.time()),
            "msg_id": "{y}-{id}".format(
                y=datetime.datetime.utcnow().year, id=message.id
            ),
            "i": self._message_counter,
            "username": "amqp-bridge",
        }
        message._body = wrapped_body

        if fedmsg_config.conf["sign_messages"]:
            # Find the cert name
            if not fedmsg_config.conf.get("certname"):
                hostname = socket.gethostname().split(".", 1)[0]
                if "cert_prefix" in fedmsg_config.conf:
                    cert_index = "%s.%s" % (fedmsg_config.conf["cert_prefix"], hostname)
                else:
                    cert_index = fedmsg_config.conf["name"]
                    if cert_index == "relay_inbound":
                        cert_index = "shell.%s" % hostname
                fedmsg_config.conf["certname"] = fedmsg_config.conf["certnames"][
                    cert_index
                ]
            # Sign the message
            try:
                message._body = fedmsg.crypto.sign(message._body, **fedmsg_config.conf)
            except ValueError as e:
                _log.error("Unable to sign message with fedmsg: %s", str(e))
                raise HaltConsumer(exit_code=1, reason=e)

        try:
            _log.debug(
                'Publishing message on "%s" to the ZeroMQ PUB socket "%s"',
                message.topic,
                self.publish_endpoint,
            )
            zmq_message = [
                message.topic.encode("utf-8"),
                json.dumps(message._body).encode("utf-8"),
            ]
            self.pub_socket.send_multipart(zmq_message)
        except zmq.ZMQError as e:
            _log.error("Message delivery failed: %r", e)
            raise Nack()
