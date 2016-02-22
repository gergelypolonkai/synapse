# -*- coding: utf-8 -*-
# Copyright 2016 OpenMarket Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from synapse.replication.resource import ReplicationResource
from synapse.types import Requester, UserID

from twisted.internet import defer
from tests import unittest
from tests.utils import setup_test_homeserver
from mock import Mock, NonCallableMock
import json
import contextlib


class ReplicationResourceCase(unittest.TestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.hs = yield setup_test_homeserver(
            "red",
            http_client=None,
            replication_layer=Mock(),
            ratelimiter=NonCallableMock(spec_set=[
                "send_message",
            ]),
        )

        self.hs.get_ratelimiter().send_message.return_value = (True, 0)

        self.resource = ReplicationResource(self.hs)

    @unittest.DEBUG
    @defer.inlineCallbacks
    def test_streams(self):
        code, body = yield self.get(streams="-1")
        self.assertEquals(code, 200)
        self.assertEquals(body["streams"]["field_names"], ["name", "position"])

    @defer.inlineCallbacks
    def test_events_timeout(self):
        get = self.get(events="-1", timeout="0")
        self.hs.clock.advance_time_msec(1)
        code, body = yield get
        self.assertEquals(body, {})

    @defer.inlineCallbacks
    def test_events(self):
        get = self.get(events="-1", timeout="0")
        yield self.hs.get_handlers().room_creation_handler.create_room(
            Requester(UserID.from_string("@seeing:red"), "", False), {}
        )
        code, body = yield get
        self.assertEquals(body["events"]["field_names"], [
            "position", "internal", "json"
        ])

    @defer.inlineCallbacks
    def get(self, **params):
        request = NonCallableMock(spec_set=[
            "write", "finish", "setResponseCode", "setHeader", "args",
            "method", "processing"
        ])

        request.method = "GET"
        request.args = {k: [v] for k, v in params.items()}

        @contextlib.contextmanager
        def processing():
            yield
        request.processing = processing

        yield self.resource._async_render_GET(request)
        self.assertTrue(request.finish.called)

        if request.setResponseCode.called:
            response_code = request.setResponseCode.call_args[0][0]
        else:
            response_code = 200

        response_json = "".join(
            call[0][0] for call in request.write.call_args_list
        )
        response_body = json.loads(response_json)

        defer.returnValue((response_code, response_body))
