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

from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import defer

import ujson as json

import collections
import logging

logger = logging.getLogger(__name__)

REPLICATION_PREFIX = "/_synapse/replication"


class _Writer(object):
    def __init__(self, request):
        self.request = request
        self.sep = b"[["
        self.total = 0

    def write_row(self, row):
        row_bytes = b",".join(json.dumps(item) for item in row)
        self.request.write(self.sep)
        self.request.write(row_bytes)
        self.sep = b"]\n,["

    def write_header(self, name, stream_id, count, fields):
        self.write_row((name, stream_id, count, len(fields)) + fields)

    def write_header_and_rows(self, name, rows, fields, stream_id=None):
        if not rows:
            return 0
        if stream_id is None:
            stream_id = rows[-1][0]
        self.write_header(name, stream_id, len(rows), fields)
        for row in rows:
            self.write_row(row)
        self.total += len(rows)

    def finish(self):
        if self.sep == b"[[":
            self.request.write(b"[]")
        else:
            self.request.write(b"]\n]")
        self.request.finish()


class _ReplicationToken(collections.namedtuple("_ReplicationToken", (
    "events", "presence", "typing", "receipts", "account_data", "backfill",
))):
    __slots__ = []

    def __new__(cls, *args):
        if len(args) == 1:
            return cls(*(int(value) for value in args[0].split("/")))
        else:
            return super(_ReplicationToken, cls).__new__(cls, *args)

    def __str__(self):
        return "/".join(str(value) for value in self)


STREAM_NAMES = (
    ("events",),
    ("presence",),
    ("typing",),
    ("receipts",),
    ("user_account_data", "room_account_data", "tag_account_data",),
    ("backfill",),
)


class ReplicationResource(Resource):
    isLeaf = True

    def __init__(self, hs):
        Resource.__init__(self)  # Resource is old-style, so no super()

        self.version_string = hs.version_string
        self.store = hs.get_datastore()
        self.sources = hs.get_event_sources()
        self.presence_handler = hs.get_handlers().presence_handler
        self.typing_handler = hs.get_handlers().typing_notification_handler
        self.notifier = hs.notifier

    def render_GET(self, request):
        self._async_render_GET(request)
        return NOT_DONE_YET

    @defer.inlineCallbacks
    def current_replication_token(self):
        stream_token = yield self.sources.get_current_token()
        backfill_token = yield self.store.get_current_backfill_token()

        defer.returnValue(_ReplicationToken(
            stream_token.room_stream_id,
            int(stream_token.presence_key),
            int(stream_token.typing_key),
            int(stream_token.receipt_key),
            int(stream_token.account_data_key),
            backfill_token,
        ))

    @request_handler
    @defer.inlineCallbacks
    def _async_render_GET(self, request):
        limit = parse_integer(request, "limit", 100)
        timeout = parse_integer(request, "timeout", 10 * 1000)

        request.setHeader(b"Content-Type", b"application/json")
        writer = _Writer(request)

        @defer.inlineCallbacks
        def replicate():
            current_token = yield self.current_replication_token()
            logger.info("Replicating up to %r", current_token)

            yield self.account_data(writer, current_token, limit)
            yield self.events(writer, current_token, limit)
            yield self.presence(writer, current_token, limit)
            yield self.typing(writer, current_token, limit)
            yield self.receipts(writer, current_token, limit)
            self.streams(writer, current_token)

            logger.info("Replicated %d rows", writer.total)
            defer.returnValue(writer.total)

        yield self.notifier.wait_for_replication(replicate, timeout)

        writer.finish()

    def streams(self, writer, current_token):
        request_token = parse_string(writer.request, "streams")

        streams = []

        if request_token is not None:
            if request_token == "-1":
                for names, stream_id in zip(STREAM_NAMES, current_token):
                    streams.extend((name, stream_id) for name in names)
            else:
                items = zip(
                    STREAM_NAMES,
                    current_token,
                    map(int, request_token.split("_"))
                )
                for names, current_id, last_id in items:
                    if last_id < current_id:
                        streams.extend((name, current_id) for name in names)

            if streams:
                writer.write_header_and_rows(
                    "streams", streams, ("name", "stream_id"),
                    stream_id="_".join(str(x) for x in current_token)
                )

    @defer.inlineCallbacks
    def events(self, writer, current_token, limit):
        request_events = parse_integer(writer.request, "events")
        request_backfill = parse_integer(writer.request, "backfill")

        if request_events is not None or request_backfill is not None:
            if request_events is None:
                request_events = current_token.events
            if request_backfill is None:
                request_backfill = current_token.backfill
            events_rows, backfill_rows = yield self.store.get_all_new_events(
                request_backfill, request_events,
                current_token.backfill, current_token.events,
                limit
            )
            writer.write_header_and_rows(
                "events", events_rows, ("stream_id", "internal", "json")
            )
            writer.write_header_and_rows(
                "backfill", backfill_rows, ("stream_id", "internal", "json")
            )

    @defer.inlineCallbacks
    def presence(self, writer, current_token, limit):
        current_stream_id = current_token.presence

        request_presence = parse_integer(writer.request, "presence")

        if request_presence is not None:
            presence_rows = yield self.presence_handler.get_all_presence_updates(
                request_presence, current_stream_id, limit
            )
            writer.write_header_and_rows(
                "presence", presence_rows, ("stream_id", "user_id", "status")
            )

    @defer.inlineCallbacks
    def typing(self, writer, current_token, limit):
        current_stream_id = current_token.presence

        request_typing = parse_integer(writer.request, "typing")

        if request_typing is not None:
            typing_rows = yield self.typing_handler.get_all_typing_updates(
                request_typing, current_stream_id, limit
            )
            writer.write_header_and_rows("typing", typing_rows, (
                "stream_id", "room_id", "typing"
            ))

    @defer.inlineCallbacks
    def receipts(self, writer, current_token, limit):
        current_stream_id = current_token.receipts

        request_receipts = parse_integer(writer.request, "receipts")

        if request_receipts is not None:
            receipts_rows = yield self.store.get_all_updated_receipts(
                request_receipts, current_stream_id, limit
            )
            writer.write_header_and_rows("receipts", receipts_rows, (
                "stream_id", "room_id", "receipt_type", "user_id", "event_id", "data"
            ))

    @defer.inlineCallbacks
    def account_data(self, writer, current_token, limit):
        current_stream_id = current_token.account_data

        user_account_data = parse_integer(writer.request, "user_account_data")
        room_account_data = parse_integer(writer.request, "room_account_data")
        tag_account_data = parse_integer(writer.request, "tag_account_data")

        if user_account_data is not None or room_account_data is not None:
            if user_account_data is None:
                user_account_data = current_stream_id
            if room_account_data is None:
                room_account_data = current_stream_id
            user_rows, room_rows = yield self.store.get_all_updated_account_data(
                user_account_data, room_account_data, current_stream_id, limit
            )
            writer.write_header_and_rows("user_account_data", user_rows, (
                "stream_id", "user_id", "type", "content"
            ))
            writer.write_header_and_rows("room_account_data", room_rows, (
                "stream_id", "user_id", "room_id", "type", "content"
            ))

        if tag_account_data is not None:
            tag_rows = yield self.store.get_all_updated_tags(
                tag_account_data, current_stream_id, limit
            )
            writer.write_header_and_rows("tag_account_data", tag_rows, (
                "stream_id", "user_id", "room_id", "tags"
            ))
