"""Tests for the changes that make initial sync survivable and faster:

  - rate-limited token refresh (replaces one-shot _refresh_attempted)
  - CHECKPOINT_SENTINEL + periodic state writes in sync_stream
  - threadpool sub-stream pre-fetch in Tickets.sync (parent ordering preserved)
"""

import unittest
from unittest.mock import MagicMock, Mock, patch

from tap_zendesk import http
from tap_zendesk.sync import CHECKPOINT_SENTINEL, sync_stream


class TestTokenRefreshRateLimit(unittest.TestCase):
    """The previous one-shot _refresh_attempted prevented multi-day syncs
    from refreshing more than once. Replacement uses a monotonic cool-down
    so refresh succeeds repeatedly while preventing infinite loops."""

    def setUp(self):
        # Reset the module-level cool-down before each test
        http._next_refresh_allowed_at = 0.0

    def _config(self):
        return {
            "subdomain": "example",
            "refresh_token": "rt",
            "client_id": "cid",
            "client_secret": "cs",
        }

    @patch("tap_zendesk.http.requests.post")
    @patch("tap_zendesk.http.get_config_path", return_value=None)
    @patch("tap_zendesk.http.time.monotonic")
    def test_first_refresh_succeeds(self, mock_now, _mock_path, mock_post):
        mock_now.return_value = 1000.0
        mock_post.return_value = Mock(
            ok=True,
            raise_for_status=Mock(),
            json=Mock(return_value={"access_token": "new_token"}),
        )
        config = self._config()

        assert http.refresh_access_token(config) is True
        assert config["access_token"] == "new_token"
        assert http._next_refresh_allowed_at == 1000.0 + http.MIN_REFRESH_INTERVAL_SEC

    @patch("tap_zendesk.http.requests.post")
    @patch("tap_zendesk.http.get_config_path", return_value=None)
    @patch("tap_zendesk.http.time.monotonic")
    def test_rapid_second_refresh_suppressed(self, mock_now, _mock_path, mock_post):
        mock_post.return_value = Mock(
            ok=True,
            raise_for_status=Mock(),
            json=Mock(return_value={"access_token": "tok"}),
        )
        config = self._config()

        mock_now.return_value = 1000.0
        assert http.refresh_access_token(config) is True

        mock_now.return_value = 1005.0  # only 5s later, well inside cool-down
        assert http.refresh_access_token(config) is False
        # Second attempt did not call the network
        assert mock_post.call_count == 1

    @patch("tap_zendesk.http.requests.post")
    @patch("tap_zendesk.http.get_config_path", return_value=None)
    @patch("tap_zendesk.http.time.monotonic")
    def test_refresh_allowed_again_after_cooldown(self, mock_now, _mock_path, mock_post):
        mock_post.return_value = Mock(
            ok=True,
            raise_for_status=Mock(),
            json=Mock(return_value={"access_token": "tok"}),
        )
        config = self._config()

        mock_now.return_value = 1000.0
        assert http.refresh_access_token(config) is True

        mock_now.return_value = 1000.0 + http.MIN_REFRESH_INTERVAL_SEC + 1
        assert http.refresh_access_token(config) is True
        assert mock_post.call_count == 2


class TestCheckpointSentinel(unittest.TestCase):
    """sync_stream should call singer.write_state every Nth sentinel and
    skip the record-write/transform path for sentinels."""

    def _make_instance(self, records, replication_method="INCREMENTAL", checkpoint_every=2):
        instance = MagicMock()
        instance.replication_method = replication_method
        instance.replication_key = "updated_at"
        instance.config = {"checkpoint_every": checkpoint_every}

        # parent stream identity used by sync_stream
        parent = MagicMock()
        parent.tap_stream_id = "tickets"
        parent.schema.to_dict.return_value = {}
        parent.metadata = []
        instance.stream = parent

        instance.sync = Mock(return_value=iter(records))
        return instance, parent

    @patch("tap_zendesk.sync.singer")
    def test_state_written_on_checkpoint_modulo(self, mock_singer):
        records = [
            ("tickets_obj", {"id": 1}),
            (CHECKPOINT_SENTINEL, None),
            ("tickets_obj", {"id": 2}),
            (CHECKPOINT_SENTINEL, None),  # 2nd sentinel — should trigger write_state
            ("tickets_obj", {"id": 3}),
            (CHECKPOINT_SENTINEL, None),
            (CHECKPOINT_SENTINEL, None),  # 4th sentinel — should trigger again
        ]
        # parent identity match — tap_stream_id strings line up
        for tup in records:
            stream_obj, _ = tup
            if stream_obj is not CHECKPOINT_SENTINEL:
                # Replace string with a mock stream having the right id
                pass

        # Build a more realistic record list with proper stream objects
        parent = MagicMock()
        parent.tap_stream_id = "tickets"
        parent.schema.to_dict.return_value = {}
        parent.metadata = []
        records_real = []
        for stream_obj, payload in records:
            if stream_obj is CHECKPOINT_SENTINEL:
                records_real.append((CHECKPOINT_SENTINEL, payload))
            else:
                records_real.append((parent, payload))

        instance = MagicMock()
        instance.replication_method = "INCREMENTAL"
        instance.replication_key = "updated_at"
        instance.config = {"checkpoint_every": 2}
        instance.stream = parent
        instance.sync = Mock(return_value=iter(records_real))

        # Pretend bookmark already set so sync_stream doesn't try to write one
        state = {"bookmarks": {"tickets": {"updated_at": "2026-01-01"}}}
        sync_stream(state, "2025-01-01", instance)

        # write_state should have been called on the 2nd and 4th sentinels
        # plus the end-of-stream final write — total 3 calls.
        assert mock_singer.write_state.call_count == 3

    @patch("tap_zendesk.sync.singer")
    def test_sentinel_does_not_write_record(self, mock_singer):
        parent = MagicMock()
        parent.tap_stream_id = "tickets"
        parent.schema.to_dict.return_value = {}
        parent.metadata = []
        records = [
            (parent, {"id": 1}),
            (CHECKPOINT_SENTINEL, None),
            (parent, {"id": 2}),
        ]

        instance = MagicMock()
        instance.replication_method = "INCREMENTAL"
        instance.replication_key = "updated_at"
        instance.config = {}
        instance.stream = parent
        instance.sync = Mock(return_value=iter(records))

        state = {"bookmarks": {"tickets": {"updated_at": "2026-01-01"}}}
        sync_stream(state, "2025-01-01", instance)

        # Only 2 records were emitted; the sentinel is not a record
        assert mock_singer.write_record.call_count == 2


class TestTicketsConcurrentSubStreams(unittest.TestCase):
    """Tickets.sync emits parent + sub-streams in order even when fetches
    happen concurrently, and yields a CHECKPOINT_SENTINEL after each ticket."""

    @patch("tap_zendesk.streams.TicketComments")
    @patch("tap_zendesk.streams.TicketMetrics")
    @patch("tap_zendesk.streams.TicketAudits")
    def test_emits_parent_then_subs_then_sentinel(
        self, mock_audits_cls, mock_metrics_cls, mock_comments_cls
    ):
        from tap_zendesk.streams import Tickets

        # Two tickets, each with deterministic sub-stream payloads
        tickets_data = [
            {"id": 1, "generated_timestamp": 1700000000, "fields": []},
            {"id": 2, "generated_timestamp": 1700000100, "fields": []},
        ]

        # Sub-stream stubs: selected, return distinct records per ticket
        audits_inst = MagicMock()
        audits_inst.is_selected.return_value = True
        audits_inst.count = 0
        audits_inst.stream = MagicMock(tap_stream_id="ticket_audits")
        audits_inst.get_objects.side_effect = lambda tid: iter([{"a": tid}])

        metrics_inst = MagicMock()
        metrics_inst.is_selected.return_value = True
        metrics_inst.count = 0
        metrics_inst.stream = MagicMock(tap_stream_id="ticket_metrics")
        metrics_inst.item_key = "ticket_metric"
        metrics_inst.get_objects.side_effect = lambda tid: iter([{"m": tid}])

        comments_inst = MagicMock()
        comments_inst.is_selected.return_value = False  # keep this test focused
        comments_inst.count = 0

        mock_audits_cls.return_value = audits_inst
        mock_metrics_cls.return_value = metrics_inst
        mock_comments_cls.return_value = comments_inst

        # Build the Tickets instance and stub its export iterator
        tickets = Tickets(client=None, config={"subdomain": "x", "substream_workers": 2})
        tickets.stream = MagicMock(tap_stream_id="tickets")
        tickets.get_objects = Mock(return_value=iter(tickets_data))
        tickets.get_bookmark = Mock(return_value=Mock())
        tickets.update_bookmark = Mock()

        emitted = list(tickets.sync(state={}))

        # Strip out the sentinels for ordering checks
        non_sentinels = [
            (s.tap_stream_id if hasattr(s, "tap_stream_id") else s, payload)
            for s, payload in emitted
            if s is not CHECKPOINT_SENTINEL
        ]
        # Per-ticket order: parent → audit → metric (comments unselected)
        assert non_sentinels[0] == ("tickets", tickets_data[0])
        assert non_sentinels[1] == ("ticket_audits", {"a": 1})
        assert non_sentinels[2] == ("ticket_metrics", {"m": 1})
        assert non_sentinels[3] == ("tickets", tickets_data[1])
        assert non_sentinels[4] == ("ticket_audits", {"a": 2})
        assert non_sentinels[5] == ("ticket_metrics", {"m": 2})

        # Each ticket should be followed by exactly one sentinel
        sentinel_count = sum(1 for s, _ in emitted if s is CHECKPOINT_SENTINEL)
        assert sentinel_count == 2


if __name__ == "__main__":
    unittest.main()
