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
import unittest
import json
import socket

from fedora_messaging import message
import mock

from fedmsg_migration_tools import bridges
from fedmsg_migration_tools.tests import FIXTURES_DIR


@mock.patch("fedmsg_migration_tools.bridges.time.time", mock.Mock(return_value=101))
class AmqpToZmqTests(unittest.TestCase):
    @mock.patch("fedmsg_migration_tools.bridges.zmq.Context", mock.Mock())
    def test_unsigned(self):
        """Assert fedmsg's "sign_messages" config option is honored."""
        year = datetime.datetime.utcnow().year
        zmq_bridge = bridges.AmqpToZmq()
        msg = message.Message(topic="my.topic", body={"my": "message"})
        expected = [
            "my.topic".encode("utf-8"),
            json.dumps(
                {
                    "topic": "my.topic",
                    "msg": {"my": "message"},
                    "timestamp": 101,
                    "msg_id": "{}-{}".format(year, msg.id),
                    "i": 1,
                    "username": "amqp-bridge",
                }
            ).encode("utf-8"),
        ]

        zmq_bridge(msg)
        zmq_bridge.pub_socket.send_multipart.assert_called_once_with(expected)

    @mock.patch("fedmsg_migration_tools.bridges.zmq.Context", mock.Mock())
    def test_signed(self):
        """Assert messages are signed if fedmsg is configured for signatures."""
        year = datetime.datetime.utcnow().year
        zmq_bridge = bridges.AmqpToZmq()
        msg = message.Message(topic="my.topic", body={"my": "message"})
        expected = {
            "topic": "my.topic",
            "msg": {"my": "message"},
            "timestamp": 101,
            "msg_id": "{}-{}".format(year, msg.id),
            "i": 1,
            "username": "amqp-bridge",
            "crypto": "x509",
        }
        conf = {"sign_messages": True, "ssldir": FIXTURES_DIR, "certname": "fedmsg"}

        with mock.patch.dict("fedmsg_migration_tools.bridges.fedmsg_config.conf", conf):
            zmq_bridge(msg)

        body = json.loads(
            zmq_bridge.pub_socket.send_multipart.call_args_list[0][0][0][1].decode(
                "utf-8"
            )
        )
        self.assertIn("signature", body)
        self.assertIn("certificate", body)
        del body["signature"]
        del body["certificate"]

        self.assertEqual(body, expected)

    @mock.patch("fedmsg_migration_tools.bridges.zmq.Context", mock.Mock())
    def test_signed_implicit_cert(self):
        """Assert signing certificate is properly autodetected."""
        zmq_bridge = bridges.AmqpToZmq()
        msg = message.Message(topic="my.topic", body={"my": "message"})
        hostname = socket.gethostname().split(".", 1)[0]
        base_conf = {"sign_messages": True, "ssldir": FIXTURES_DIR}
        sign_configs = [
            {"name": "fedmsg", "certnames": {"fedmsg": "fedmsg"}},
            {
                "cert_prefix": "fedmsg",
                "certnames": {"fedmsg.{}".format(hostname): "fedmsg"},
            },
        ]
        for sign_config in sign_configs:
            conf = base_conf.copy()
            conf.update(sign_config)
            with mock.patch.dict(
                "fedmsg_migration_tools.bridges.fedmsg_config.conf", conf
            ):
                with mock.patch(
                    "fedmsg_migration_tools.bridges.fedmsg.crypto.sign"
                ) as mock_sign:
                    mock_sign.side_effect = lambda *a, **kw: a[0]
                    zmq_bridge(msg)
            sign_call_kw = mock_sign.call_args_list[-1][1]
            self.assertIn("certname", sign_call_kw)
            self.assertEqual(sign_call_kw["certname"], "fedmsg")

    @mock.patch("fedmsg_migration_tools.bridges.zmq.Context", mock.Mock())
    def test_local_or_remote_publish(self):
        """Assert the proper method is used for local or remote endpoints."""
        # Local
        zmq_bridge = bridges.AmqpToZmq()
        zmq_bridge.pub_socket.bind.assert_called_with("tcp://*:9940")
        # Remote
        conf = {
            "consumer_config": {
                "publish_endpoint": "dummy_endpoint",
                "remote_publish": True,
            }
        }
        with mock.patch.dict("fedmsg_migration_tools.bridges.fm_config.conf", conf):
            zmq_bridge = bridges.AmqpToZmq()
        zmq_bridge.pub_socket.connect.assert_called_with("dummy_endpoint")
