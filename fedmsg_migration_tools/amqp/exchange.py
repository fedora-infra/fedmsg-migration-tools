import asyncio
import logging


_log = logging.getLogger(__name__)


class Exchange(object):

    def __init__(self, channel, name, exchange_type):
        self._channel = channel
        self.name = name
        self._args = dict(
            exchange=self.name, durable=True, exchange_type=exchange_type)
        self._declare_future = None

    def declare(self):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        """
        _log.debug('Declaring exchange %s', self.name)
        self._declare_future = asyncio.Future()
        self._channel.get_channel().exchange_declare(
            self.on_declareok, **self._args)
        return self._declare_future

    def on_declareok(self, frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method frame: Exchange.DeclareOk response frame

        """
        _log.debug('Exchange %s declared', self.name)
        self._declare_future.set_result(frame)
