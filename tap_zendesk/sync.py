import json
from zenpy.lib.api_objects import BaseObject
from zenpy.lib.proxy import ProxyList

import singer
import singer.metrics as metrics
from singer import metadata
from singer import Transformer

LOGGER = singer.get_logger()

# Sentinel that streams yield to indicate "all records up to this point have
# been emitted; the in-memory state's bookmark is safe to durably commit."
# sync_stream recognizes the sentinel and emits singer.write_state on a
# parent-count modulo so multi-day syncs make incremental progress instead
# of restarting from the previous successful bookmark on every failure.
CHECKPOINT_SENTINEL = object()

DEFAULT_CHECKPOINT_EVERY = 100

def process_record(record):
    """ Serializes Zenpy's internal classes into Python objects via ZendeskEncoder. """
    rec_str = json.dumps(record, cls=ZendeskEncoder)
    rec_dict = json.loads(rec_str)
    return rec_dict

def sync_stream(state, start_date, instance):
    stream = instance.stream

    # If we have a bookmark, use it; otherwise use start_date
    if (instance.replication_method == 'INCREMENTAL' and
            not state.get('bookmarks', {}).get(stream.tap_stream_id, {}).get(instance.replication_key)):
        singer.write_bookmark(state,
                              stream.tap_stream_id,
                              instance.replication_key,
                              start_date)

    checkpoint_every = max(
        1,
        int((instance.config or {}).get('checkpoint_every', DEFAULT_CHECKPOINT_EVERY) or DEFAULT_CHECKPOINT_EVERY),
    )
    checkpoints_seen = 0

    parent_stream = stream
    with metrics.record_counter(stream.tap_stream_id) as counter, Transformer() as transformer:
        for (stream, record) in instance.sync(state):
            if stream is CHECKPOINT_SENTINEL:
                checkpoints_seen += 1
                # Only INCREMENTAL streams have a meaningful bookmark to
                # persist mid-stream. FULL_TABLE replays from scratch on
                # the next run, so a partial-progress write is wasted.
                if (
                    instance.replication_method == "INCREMENTAL"
                    and checkpoints_seen % checkpoint_every == 0
                ):
                    singer.write_state(state)
                continue

            if stream.tap_stream_id == parent_stream.tap_stream_id:
                counter.increment()

            rec = process_record(record)
            rec = transformer.transform(rec, stream.schema.to_dict(), metadata.to_map(stream.metadata))

            singer.write_record(stream.tap_stream_id, rec)

        if instance.replication_method == "INCREMENTAL":
            singer.write_state(state)

        return counter.value

class ZendeskEncoder(json.JSONEncoder):
    def default(self, obj): # pylint: disable=arguments-differ,method-hidden
        if isinstance(obj, BaseObject):
            obj_dict = obj.to_dict()
            for k, v in list(obj_dict.items()):
                # NB: This might fail if the object inside is callable
                if callable(v):
                    obj_dict.pop(k)
            return obj_dict
        elif isinstance(obj, ProxyList):
            return obj.copy()
        return json.JSONEncoder.default(self, obj)
