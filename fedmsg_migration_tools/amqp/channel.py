import asyncio
import logging


_log = logging.getLogger(__name__)


class Channel:

    def __init__(self, connection):
        self._connection = connection
        self._open_future = None
        self._close_future = None

    def open(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_open callback will be invoked by pika.
        """
        _log.debug('Creating a new channel')
        self._open_future = asyncio.Future()
        self._connection.get_connection().channel(on_open_callback=self.on_open)
        return self._open_future

    def on_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        :param pika.channel.Channel channel: The channel object
        """
        _log.debug('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_closed)
        self._open_future.set_result(channel)

    def close(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        _log.debug('Closing the channel')
        self._close_future = asyncio.Future()
        self._channel.close()
        return self._close_future

    def on_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        self._channel = None
        if self._close_future and not self._close_future.done():
            self._close_future.set_result((reply_code, reply_text))
        else:
            _log.warning('Channel %i was closed: (%s) %s',
                         channel, reply_code, reply_text)

    def get_channel(self):
        return self._channel
