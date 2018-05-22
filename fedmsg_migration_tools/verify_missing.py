import json
import logging
from datetime import datetime, timedelta

from fedmsg_migration_tools import config
from fedora_messaging.twisted.producer import MessageProducer

from twisted.internet import defer, reactor, task
from twisted.application import service
from twisted.logger import Logger
from txzmq import (
    ZmqEndpoint, ZmqEndpointType, ZmqFactory, ZmqSubConnection,
)


_log = logging.getLogger(__name__)
_txlog = Logger()


class MasterService(service.MultiService):
    """Multi service that stops the reactor with itself."""

    def stopService(self):
        d = super(MasterService, self).stopService()
        d.addCallback(lambda _: reactor.stop())
        return d


class AmqpConsumer(service.Service):

    def __init__(self, store):
        self.store = store
        self.producer = MessageProducer(
            self.on_message, self._get_bindings())

    def _get_bindings(self):
        exchanges = [b["exchange"] for b in config.conf['amqp_to_zmq']['bindings']]
        exchanges.append(config.conf['zmq_to_amqp']['exchange'])
        return [
            dict(
                exchange=exchange,
                routing_key="#",
                queue_name=config.conf['verify_missing']['queue_name'],
                # We don't want to store messages when not running.
                queue_auto_delete=True,
            )
            for exchange in exchanges
        ]

    @defer.inlineCallbacks
    def startService(self):
        try:
            yield self.producer.resumeProducing()
        except Exception:
            _txlog.failure("Error in the AMQP consumer")
        # Stop when the producer stops (on error probably).
        self.producer.producing.addCallback(
            lambda x: self.parent.stopService()
        )

    def on_message(self, message):
        _log.debug('Received from AMQP on topic %s: %s', message.topic, message.body["msg_id"])
        self.store[message.body["msg_id"]] = (
            datetime.utcnow(),
            message.body,
        )

    def stopService(self):
        _log.debug("Stopping AmqpConsumer")
        return self.producer.stopProducing()


class ZmqConsumer(service.Service):

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
        _log.debug('Configuring ZeroMQ subscription socket')
        for endpoint in endpoints:
            _log.info('Connecting to the %s ZeroMQ endpoint', endpoint)
            s = ZmqSubConnection(self._factory, endpoint)
            s.subscribe(b"")
            s.gotMessage = self.on_message
        _log.info('ZeroMQ consumer is ready')

    def on_message(self, body, topic):
        topic = topic.decode("utf-8")
        msg = json.loads(body)
        _log.debug('Received from ZeroMQ on topic %s: %s',
                   topic, msg["msg_id"])
        self.store[msg["msg_id"]] = (
            datetime.utcnow(),
            msg,
        )

    def stopService(self):
        _log.debug("Stopping ZmqConsumer")
        self._factory.shutdown()


class Comparator(service.Service):

    MATCH_WINDOW = 20

    def __init__(self, amqp_store, zmq_store):
        self.amqp_store = amqp_store
        self.zmq_store = zmq_store
        self._rm_loop = task.LoopingCall(self.remove_matching)
        self._cm_loop = task.LoopingCall(self.check_missing)

    def startService(self):
        self._rm_loop.start(1)
        self._cm_loop.start(10)

    def stopService(self):
        _log.debug("Stopping Comparator")
        self._rm_loop.stop()
        self._cm_loop.stop()

    def remove_matching(self):
        _log.debug("Checking for matching messages (%i, %i)",
                   len(self.amqp_store), len(self.zmq_store))
        for msg_id in list(self.amqp_store.keys()):
            if msg_id in self.zmq_store:
                _log.info('Successfully received message (id %s) via ZMQ and '
                          'AMQP', msg_id)
                del self.amqp_store[msg_id]
                del self.zmq_store[msg_id]

    def check_missing(self):
        _log.debug("Checking for missing messages")
        self._check_store(self.amqp_store, "AMQP")
        self._check_store(self.zmq_store, "ZeroMQ")

    def _check_store(self, store, source_name):
        threshold = datetime.utcnow() - timedelta(seconds=self.MATCH_WINDOW)
        for msg_id, value in list(store.items()):
            time, msg = value
            if time < threshold:
                _log.warning("Message %s was only received in %s",
                             msg_id, source_name)
                del store[msg_id]


def main(zmq_endpoints):
    amqp_store = {}
    zmq_store = {}
    verify_service = MasterService()
    comparator = Comparator(amqp_store, zmq_store)
    comparator.setServiceParent(verify_service)
    zmq_consumer = ZmqConsumer(zmq_store, zmq_endpoints)
    zmq_consumer.setServiceParent(verify_service)
    amqp_consumer = AmqpConsumer(amqp_store)
    amqp_consumer.setServiceParent(verify_service)
    verify_service.startService()
    try:
        reactor.run()
    except KeyboardInterrupt:
        verify_service.stopService()
        reactor.run()
