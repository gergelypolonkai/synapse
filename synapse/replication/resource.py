# -*- coding: utf-8 -*-
# Copyright 2015 OpenMarket Ltd
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

from synapse.http.servlet import parse_integer, parse_string
from synapse.http.server import request_handler
from synapse.types import StreamToken

from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import defer

import logging

logger = logging.getLogger(__name__)

REPLICATION_PREFIX = "/_synapse/replication"


def encode_item(item):
    try:
        return item.encode("UTF-8")
    except:
        return str(item)


class ReplicationResource(Resource):
    isLeaf = True

    def __init__(self, hs):
        Resource.__init__(self)  # Resource is old-style, so no super()

        self.version_string = hs.version_string
        self.store = hs.get_datastore()
        self.sources = hs.get_event_sources()

    def render_GET(self, request):
        self._async_render_GET(request)
        return NOT_DONE_YET

    @request_handler
    @defer.inlineCallbacks
    def _async_render_GET(self, request):

        current_token = yield self.sources.get_current_token()

        logger.info("Replicating up to %r", current_token)

        def write_row(row):
            row_bytes = b"\xFE".join(encode_item(item) for item in row)
            request.write(row_bytes + b"\xFF")

        def write_header(name, stream_id, count, fields):
            write_row((name, stream_id, count, len(fields)) + fields)

        def write_header_and_rows(name, rows, fields, stream_id=None):
            if not rows:
                return 0
            if stream_id is None:
                stream_id = rows[-1][0]
            write_header(name, stream_id, len(rows), fields)
            for row in rows:
                write_row(row)
            return len(rows)

        limit = parse_integer(request, "limit", 100)

        request.setHeader(b"Content-Type", b"application/x-synapse")
        total = 0
        total += yield self.account_data(
            request, write_header_and_rows, current_token, limit
        )
        total += yield self.streams(
            request, write_header_and_rows, current_token
        )
        logger.info("Replicated %d rows", total)

        request.finish()

    def streams(self, request, write_header_and_rows, current_token):
        request_token = parse_string(request, "streams")
        streams = []

        if request_token is not None:
            if request_token != "-1":
                request_token = StreamToken.from_string(request_token)
            streams.extend(self.account_data_streams(
                current_token, request_token
            ))
            if streams:
                write_header_and_rows(
                    "streams", streams, ("name", "stream_id"),
                    stream_id=current_token.to_string()
                )
        return len(streams)

    def account_data_streams(self, current_token, request_token="-1"):
        current_stream_id = int(current_token.account_data_key)

        stream_updated = (
            request_token == "-1"
            or int(request_token.account_data_key) < current_stream_id
        )

        if stream_updated:
            return (
                ("user_account_data", current_stream_id),
                ("room_account_data", current_stream_id),
                ("tag_account_data", current_stream_id),
            )
        else:
            return ()

    @defer.inlineCallbacks
    def account_data(self, request, write_header_and_rows, current_token,
                     limit):
        current_stream_id = int(current_token.account_data_key)

        user_account_data = parse_integer(request, "user_account_data")
        room_account_data = parse_integer(request, "room_account_data")
        tag_account_data = parse_integer(request, "tag_account_data")

        total = 0
        if user_account_data is not None or room_account_data is not None:
            if user_account_data is None:
                user_account_data = current_stream_id
            if room_account_data is None:
                room_account_data = current_stream_id
            user_rows, room_rows = yield self.store.get_all_updated_account_data(
                user_account_data, room_account_data, current_stream_id, limit
            )
            total += write_header_and_rows(
                "user_account_data", user_rows,
                ("stream_id", "user_id", "type", "content")
            )
            total += write_header_and_rows(
                "room_account_data", room_rows,
                ("stream_id", "user_id", "room_id", "type", "content")
            )

        if tag_account_data is not None:
            tag_rows = yield self.store.get_all_updated_tags(
                tag_account_data, current_stream_id, limit
            )
            total += write_header_and_rows(
                "tag_account_data", tag_rows,
                ("stream_id", "user_id", "room_id", "tag", "content")
            )

        defer.returnValue(total)
