import asyncio
import logging


_log = logging.getLogger(__name__)


class Consumer(object):

    def __init__(self, channel, queue, on_message):
        """Create a new instance of the consumer class.
        :param str queue: The AMQP queue to connect to
        :param callable on_message: Callback to handle messages

        """
        self._channel = channel
        self._queue = queue
        self._message_callback = on_message
        self._consumer_tag = None
        self._stop_future = None

    def start(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        _log.debug('Issuing consumer related RPC commands')
        channel = self._channel.get_channel()
        channel.add_on_cancel_callback(self.on_cancelled)
        self._consumer_tag = channel.basic_consume(
            self.on_message, self._queue.name)

    @asyncio.coroutine
    def on_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        _log.debug('Consumer was cancelled remotely, shutting down: %r',
                   method_frame)
        yield from self._channel.close()

    def on_message(self, channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        # _log.debug('Received message # %s from %s: %s',
        #           basic_deliver.delivery_tag, properties.app_id, body)
        try:
            self._message_callback(channel, basic_deliver, properties, body)
        except Exception as e:
            _log.error("Failed processing message: %s", e)
            channel.basic_nack(basic_deliver.delivery_tag, requeue=True)
        else:
            channel.basic_ack(basic_deliver.delivery_tag)

    def stop(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        self._stop_future = asyncio.Future()
        _log.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
        self._channel.get_channel().basic_cancel(self.on_cancelok, self._consumer_tag)
        return self._stop_future

    def on_cancelok(self, frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we should close the channel.

        :param pika.frame.Method frame: The Basic.CancelOk frame

        """
        _log.debug('RabbitMQ acknowledged the cancellation of the consumer')
        self._stop_future.set_result(frame)
