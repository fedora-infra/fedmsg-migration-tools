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
import re
from datetime import datetime, timedelta

from fedmsg_migration_tools import config
from fedora_messaging.twisted.service import FedoraMessagingService

from twisted.internet import reactor, task
from twisted.application import service

# twisted.logger is available with Twisted 15+
from twisted.python import log
from txzmq import ZmqEndpoint, ZmqEndpointType, ZmqFactory, ZmqSubConnection


class AmqpConsumer(FedoraMessagingService):

    name = "AmqpConsumer"

    def __init__(self, store):
        self.store = store
        kwargs = config.conf["verify_missing"].copy()
        kwargs["queues"] = [kwargs.pop("queue")]
        kwargs["consumers"] = {
            config.conf["verify_missing"]["queue"]["queue"]: self.on_message
        }
        FedoraMessagingService.__init__(self, **kwargs)

    def on_message(self, message):
        log.msg(
            "Received from AMQP on topic {topic}: {msgid}".format(
                topic=message.topic, msgid=message.id
            ),
            system=self.name,
            logLevel=logging.DEBUG,
        )
        self.store[message.id] = (
            datetime.utcnow(),
            {"msg_id": message.id, "topic": message.topic, "msg": str(message)},
        )


class ZmqConsumer(service.Service):

    year_prefix_re = re.compile("^[0-9]{4}-")

    def __init__(self, store, zmq_endpoints):
        self.store = store
        self.endpoints = zmq_endpoints
        self._socket = None
        self._factory = None

    def startService(self):
        self._factory = ZmqFactory()
        endpoints = [
            ZmqEndpoint(ZmqEndpointType.connect, endpoint)
            for endpoint in self.endpoints
        ]
        log.msg("Configuring ZeroMQ subscription socket", logLevel=logging.DEBUG)
        for endpoint in endpoints:
            log.msg(
                "Connecting to the {endpoint} ZeroMQ endpoint".format(endpoint=endpoint)
            )
            s = ZmqSubConnection(self._factory, endpoint)
            s.subscribe(b"")
            s.gotMessage = self.on_message
        log.msg("ZeroMQ consumer is ready")

    def on_message(self, body, topic):
        topic = topic.decode("utf-8")
        msg = json.loads(body)
        if "msg_id" not in msg:
            log.msg(
                "Received a message without a msg_id from ZeroMQ on topic {topic}".format(
                    topic=topic
                ),
                logLevel=logging.INFO,
            )
            return
        msg_id = msg["msg_id"]
        log.msg(
            "Received from ZeroMQ on topic {topic}: {msgid}".format(
                topic=topic, msgid=msg["msg_id"]
            ),
            logLevel=logging.DEBUG,
        )
        msg_id = self.year_prefix_re.sub("", msg_id)
        self.store[msg_id] = (datetime.utcnow(), msg)

    def stopService(self):
        log.msg("Stopping ZmqConsumer", logLevel=logging.DEBUG)
        if self._factory.connections is not None:
            self._factory.shutdown()


class Comparator(service.Service):

    MATCH_WINDOW = 60

    def __init__(self, amqp_store, zmq_store):
        self.amqp_store = amqp_store
        self.zmq_store = zmq_store
        self._rm_loop = task.LoopingCall(self.remove_matching)
        self._cm_loop = task.LoopingCall(self.check_missing)

    def startService(self):
        self._rm_loop.start(1)
        self._cm_loop.start(10)

    def stopService(self):
        log.msg("Stopping Comparator", logLevel=logging.DEBUG)
        for loop in (self._rm_loop, self._cm_loop):
            if loop.running:
                loop.stop()

    def remove_matching(self):
        log.msg(
            "Checking for matching messages ({amqplen}, {zmqlen})".format(
                amqplen=len(self.amqp_store), zmqlen=len(self.zmq_store)
            ),
            logLevel=logging.DEBUG,
        )
        for msg_id in list(self.amqp_store.keys()):
            if msg_id in self.zmq_store:
                log.msg(
                    (
                        "Successfully received message (id {msgid}) via " "ZMQ and AMQP"
                    ).format(msgid=msg_id)
                )
                del self.amqp_store[msg_id]
                del self.zmq_store[msg_id]

    def check_missing(self):
        log.msg("Checking for missing messages", logLevel=logging.DEBUG)
        self._check_store(self.amqp_store, "AMQP")
        self._check_store(self.zmq_store, "ZeroMQ")

    def _check_store(self, store, source_name):
        threshold = datetime.utcnow() - timedelta(seconds=self.MATCH_WINDOW)
        for msg_id, value in list(store.items()):
            time, msg = value
            if time < threshold:
                log.msg(
                    (
                        "Message {msgid} was only received in {source} "
                        "(at {time}, with topic {topic})"
                    ).format(
                        msgid=msg_id,
                        source=source_name,
                        time=time.isoformat(),
                        topic=msg.get("topic", "NO TOPIC"),
                    ),
                    logLevel=logging.WARNING,
                )
                del store[msg_id]


def get_main_service(zmq_endpoints):
    amqp_store = {}
    zmq_store = {}
    verify_service = service.MultiService()
    comparator = Comparator(amqp_store, zmq_store)
    comparator.setServiceParent(verify_service)
    zmq_consumer = ZmqConsumer(zmq_store, zmq_endpoints)
    zmq_consumer.setServiceParent(verify_service)
    amqp_consumer = AmqpConsumer(amqp_store)
    amqp_consumer.setServiceParent(verify_service)
    return verify_service


def main(zmq_endpoints):
    verify_service = get_main_service(zmq_endpoints)
    verify_service.startService()
    try:
        reactor.run()
    except KeyboardInterrupt:
        verify_service.stopService()
        reactor.run()
