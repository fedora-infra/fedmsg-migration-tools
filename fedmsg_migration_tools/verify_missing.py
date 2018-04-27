import asyncio
import json
import logging
from datetime import datetime, timedelta

import zmq
import zmq.asyncio
from .amqp import Connection, Channel, Exchange, Queue, Consumer

_log = logging.getLogger(__name__)


class AmqpConsumer:

    def __init__(self, store, url, exchanges):
        self.store = store
        self.url = url
        self.exchanges = exchanges
        self._connection = None
        self._channel = None
        self._consumer = None

    @asyncio.coroutine
    def run(self):
        self._connection = Connection(self.url)
        yield from self._connection.connect()
        self._channel = Channel(self._connection)
        yield from self._channel.open()
        queue = Queue(self._channel, args=dict(exclusive=True, auto_delete=True))
        yield from queue.declare()
        for exchange_name in self.exchanges:
            exchange = Exchange(self._channel, exchange_name, "topic")
            yield from exchange.declare()
            yield from queue.bind(exchange, routing_key="#")
        self._consumer = Consumer(self._channel, queue, self.on_message)
        self._consumer.start()
        _log.info('AMQP consumer is ready')

    @asyncio.coroutine
    def stop(self):
        if not self._consumer:
            return
        yield from self._consumer.stop()
        yield from self._channel.close()
        yield from self._connection.close()

    def on_message(self, ch, method, properties, body):
        topic = method.routing_key
        _log.debug('Received message on "%s"', topic)
        try:
            msg = json.loads(body)
        except ValueError as e:
            _log.warning("Invalid message: %r", body)
            return
        _log.debug('Received from AMQP on topic %s: %s', topic, msg["msg_id"])
        self.store[msg["msg_id"]] = (
            datetime.utcnow(),
            msg,
        )


class ZmqConsumer:

    def __init__(self, store, endpoints, topics):
        self.store = store
        self.endpoints = endpoints
        self.topics = topics
        self._socket = None

    @asyncio.coroutine
    def run(self):
        context = zmq.asyncio.Context()
        self._socket = context.socket(zmq.SUB)
        for endpoint in self.endpoints:
            self._socket.connect(endpoint)
            _log.debug('Connecting ZeroMQ subscription socket to %s', endpoint)
        for topic in self.topics:
            self._socket.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
            _log.debug('Configuring ZeroMQ subscription socket with the "%s" topic', topic)
        _log.info('ZeroMQ consumer is ready')
        while True:
            try:
                topic, body = yield from self._socket.recv_multipart()
            except zmq.ZMQError as e:
                _log.error('Failed to receive message from subscription socket: %s', e)
                continue
            except ValueError as e:
                _log.error('Unable to unpack message from pair socket: %s', e)
                continue
            msg = json.loads(body)
            _log.debug('Received from ZeroMQ on topic %s: %s',
                       topic.decode("utf-8"), msg["msg_id"])
            self.store[msg["msg_id"]] = (
                datetime.utcnow(),
                msg,
            )

    @asyncio.coroutine
    def stop(self):
        self._socket.close()


class Comparator:

    MATCH_WINDOW = 20

    def __init__(self, loop, amqp_store, zmq_store):
        self.loop = loop
        self.amqp_store = amqp_store
        self.zmq_store = zmq_store

    def run(self):
        self.remove_matching()
        self.check_missing()

    def remove_matching(self):
        _log.debug("Checking for matching messages (%i, %i)",
                   len(self.amqp_store), len(self.zmq_store))
        for msg_id in list(self.amqp_store.keys()):
            if msg_id in self.zmq_store:
                del self.amqp_store[msg_id]
                del self.zmq_store[msg_id]
        return self.loop.call_later(1, self.remove_matching)

    def check_missing(self):
        _log.debug("Checking for missing messages")
        self._check_store(self.amqp_store, "AMQP")
        self._check_store(self.zmq_store, "ZeroMQ")
        return self.loop.call_later(10, self.check_missing)

    def _check_store(self, store, source_name):
        threshold = datetime.utcnow() - timedelta(seconds=self.MATCH_WINDOW)
        for msg_id, value in list(store.items()):
            time, msg = value
            if time < threshold:
                _log.warning("Message %s was only received in %s",
                             msg_id, source_name)
                del store[msg_id]


def main(amqp_url, zmq_endpoints, exchanges):
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    amqp_store = {}
    zmq_store = {}
    comparator = Comparator(loop, amqp_store, zmq_store)
    comparator.run()
    zmq_consumer = ZmqConsumer(
        zmq_store, zmq_endpoints, [""])
    asyncio.ensure_future(zmq_consumer.run())
    amqp_consumer = AmqpConsumer(amqp_store, amqp_url, exchanges)
    asyncio.ensure_future(amqp_consumer.run())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        stop_future = asyncio.gather(
            amqp_consumer.stop(),
            zmq_consumer.stop(),
        )
        stop_future.add_done_callback(lambda f: loop.stop())
        loop.run_forever()
    finally:
        loop.close()
