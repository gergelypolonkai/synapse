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

import collections
import logging

logger = logging.getLogger(__name__)

REPLICATION_PREFIX = "/_synapse/replication"


def encode_item(item):
    try:
        return item.encode("UTF-8")
    except:
        return str(item)


def write_row(request, row):
    row_bytes = b"\xFE".join(encode_item(item) for item in row)
    request.write(row_bytes + b"\xFF")


def write_header(request, name, stream_id, count, fields):
    write_row(request, (name, stream_id, count, len(fields)) + fields)


def write_header_and_rows(request, name, rows, fields, stream_id=None):
    if not rows:
        return 0
    if stream_id is None:
        stream_id = rows[-1][0]
    write_header(request, name, stream_id, len(rows), fields)
    for row in rows:
        write_row(request, row)
    return len(rows)


class ReplicationToken(collections.namedtuple("ReplicationToken", (
    "events", "presence", "typing", "receipts", "account_data", "backfill",
))):
    __slots__ = []

    def __new__(cls, *args):
        if len(args) == 1:
            return cls(*(int(value) for value in args[0].split("/")))
        else:
            return super(ReplicationToken, cls).__new__(cls, *args)

    def __str__(self):
        return "/".join(str(value) for value in self)


STREAM_NAMES = (
    ("events",),
    ("presence",),
    (),
    (),
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

    def render_GET(self, request):
        self._async_render_GET(request)
        return NOT_DONE_YET

    @defer.inlineCallbacks
    def current_replication_token(self):
        stream_token = yield self.sources.get_current_token()
        backfill_token = yield self.store.get_current_backfill_token()

        defer.returnValue(ReplicationToken(
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

        current_token = yield self.current_replication_token()

        logger.info("Replicating up to %r", current_token)

        limit = parse_integer(request, "limit", 100)

        request.setHeader(b"Content-Type", b"application/x-synapse")
        total = 0
        total += yield self.account_data(request, current_token, limit)
        total += yield self.events(request, current_token, limit)
        total += yield self.presence(request, current_token, limit)
        total += self.streams(request, current_token)
        logger.info("Replicated %d rows", total)

        request.finish()

    def streams(self, request, current_token):
        request_token = parse_string(request, "streams")

        streams = []

        if request_token is not None:
            if request_token == "-1":
                for names, stream_id in zip(STREAM_NAMES, current_token):
                    streams.extend((name, stream_id) for name in names)
            else:
                items = zip(STREAM_NAMES, current_token, request_token)
                for names, last_id, current_id in items:
                    if last_id < current_id:
                        streams.extend((name, current_id) for name in names)

            if streams:
                write_header_and_rows(
                    request, "streams", streams, ("name", "stream_id"),
                    stream_id=current_token
                )

        return len(streams)

    @defer.inlineCallbacks
    def events(self, request, current_token, limit):
        request_events = parse_integer(request, "events")
        request_backfill = parse_integer(request, "backfill")

        total = 0
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
            total += write_header_and_rows(
                request, "events", events_rows,
                ("stream_id", "internal", "json")
            )
            total += write_header_and_rows(
                request, "backfill", backfill_rows,
                ("stream_id", "internal", "json")
            )
        defer.returnValue(total)

    @defer.inlineCallbacks
    def presence(self, request, current_token, limit):
        current_stream_id = current_token.presence

        request_presence = parse_integer(request, "presence")

        total = 0
        if request_presence is not None:
            presence_rows = yield self.presence_handler.get_all_presence_updates(
                request_presence, current_stream_id, limit
            )
            total += write_header_and_rows(
                request, "presence", presence_rows,
                ("stream_id", "user_id", "status")
            )
        defer.returnValue(total)

    @defer.inlineCallbacks
    def account_data(self, request, current_token, limit):
        current_stream_id = current_token.account_data

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
                request, "user_account_data", user_rows,
                ("stream_id", "user_id", "type", "content")
            )
            total += write_header_and_rows(
                request, "room_account_data", room_rows,
                ("stream_id", "user_id", "room_id", "type", "content")
            )

        if tag_account_data is not None:
            tag_rows = yield self.store.get_all_updated_tags(
                tag_account_data, current_stream_id, limit
            )
            total += write_header_and_rows(
                request, "tag_account_data", tag_rows,
                ("stream_id", "user_id", "room_id", "tag", "content")
            )

        defer.returnValue(total)
