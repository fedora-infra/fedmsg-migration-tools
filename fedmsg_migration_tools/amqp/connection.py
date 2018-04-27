import asyncio
import logging

import pika
from pika import adapters


_log = logging.getLogger(__name__)


class Connection:

    def __init__(self, url):
        self._url = url
        self._connection = None
        self._closing = False
        self._connect_future = None
        self._close_future = None

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_open method
        will be invoked by pika.
        """
        _log.debug('AMQP connecting to %s', self._url)
        self._closing = False
        self._connect_future = asyncio.Future()
        adapters.AsyncioConnection(
            pika.URLParameters(self._url),
            self.on_open
        )
        return self._connect_future

    def on_open(self, connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object.
        """
        _log.debug('AMQP connection opened')
        connection.add_on_close_callback(self.on_closed)
        self._connection = connection
        self._connect_future.set_result(connection)

    def close(self):
        """This method closes the connection to RabbitMQ."""
        _log.debug('Closing connection')
        self._closing = True
        self._close_future = asyncio.Future()
        self._connection.close()
        return self._close_future

    def on_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        if self._closing:
            _log.debug('Connection closed: (%s) %s',
                       reply_code, reply_text)
            self._close_future.set_result((reply_code, reply_text))
        else:
            _log.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                         reply_code, reply_text)
            asyncio.get_event_loop().call_later(10, self.connect)

    def get_connection(self):
        return self._connection
