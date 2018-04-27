import asyncio
import logging

_log = logging.getLogger(__name__)


class Queue(object):

    def __init__(self, channel, name=None, args=None):
        self._channel = channel
        self.name = name
        self._args = args or {}
        self._declare_future = None
        self._bind_future = None

    def declare(self):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        """
        _log.debug('Declaring queue %s', self.name)
        self._declare_future = asyncio.Future()
        args = self._args.copy()
        if self.name is not None:
            args["queue"] = self.name
        self._channel.get_channel().queue_declare(
            self.on_declareok, **args)
        return self._declare_future

    def on_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        declare has completed.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame
        """
        self.name = method_frame.method.queue
        _log.debug('Queue %s declared', self.name)
        self._declare_future.set_result(method_frame)

    def bind(self, exchange, routing_key):
        """In this method we will bind the queue and exchange together with the
        routing key by issuing the Queue.Bind RPC command. When this command is
        complete, the on_bindok method will be invoked by pika.

        :param str exchange: The AMQP exchange name to bind to
        :param str routing_key: The routing key
        """
        _log.debug('Binding %s to %s with %s',
                   exchange.name, self.name, routing_key)
        self._bind_future = asyncio.Future()
        self._channel.get_channel().queue_bind(
            self.on_bindok, self.name, exchange.name, routing_key)
        return self._bind_future

    def on_bindok(self, frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method frame: The Queue.BindOk response frame

        """
        _log.debug('Queue bound')
        self._bind_future.set_result(frame)
