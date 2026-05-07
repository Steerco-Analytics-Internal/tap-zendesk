"""
Tickets.sync routes per-ticket sub-stream fetches through `_fetch_or_404`
which catches ZendeskNotFoundError and logs a warning instead of failing
the whole sync.

(ticket_metrics no longer fetched per-ticket — it's sideloaded onto the
 ticket via include=metric_sets — so a 404 path doesn't apply there.)
"""

import unittest
from unittest.mock import MagicMock, patch

from tap_zendesk import streams, http


@patch("tap_zendesk.streams.LOGGER.warning")
@patch("tap_zendesk.streams.Stream.update_bookmark")
@patch("tap_zendesk.metrics.capture")
@patch("tap_zendesk.streams.CursorBasedExportStream.get_objects")
@patch("tap_zendesk.streams.Stream.get_bookmark")
class TestSkip404Error(unittest.TestCase):
    @patch("tap_zendesk.streams.TicketComments")
    @patch("tap_zendesk.streams.TicketMetrics")
    @patch("tap_zendesk.streams.TicketAudits")
    def test_ticket_audits_skip_404_error(
        self,
        mock_audits_cls,
        mock_metrics_cls,
        mock_comments_cls,
        mock_get_bookmark,
        mock_get_object,
        mock_metrics,
        mock_update_bookmark,
        mock_logger,
    ):
        audits_inst = MagicMock(name="audits_inst")
        audits_inst.is_selected.return_value = True
        audits_inst.name = "ticket_audits"
        audits_inst.count = 0
        audits_inst.stream = MagicMock(tap_stream_id="ticket_audits")
        audits_inst.get_objects.side_effect = http.ZendeskNotFoundError
        mock_audits_cls.return_value = audits_inst

        metrics_inst = MagicMock(name="metrics_inst")
        metrics_inst.is_selected.return_value = False
        mock_metrics_cls.return_value = metrics_inst

        comments_inst = MagicMock(name="comments_inst")
        comments_inst.is_selected.return_value = False
        mock_comments_cls.return_value = comments_inst

        mock_get_object.return_value = [
            {"generated_timestamp": 12457845, "fields": {}, "id": "i1", "metric_set": None}
        ]

        tickets = streams.Tickets(client=None, config={"subdomain": "34", "access_token": "df"})
        tickets.stream = MagicMock(tap_stream_id="tickets")

        list(tickets.sync(state={}))

        mock_logger.assert_called_with(
            "Unable to retrieve %s for ticket (ID: %s), record not found",
            "ticket_audits",
            "i1",
        )

    @patch("tap_zendesk.streams.TicketComments")
    @patch("tap_zendesk.streams.TicketMetrics")
    @patch("tap_zendesk.streams.TicketAudits")
    def test_ticket_comments_skip_404_error(
        self,
        mock_audits_cls,
        mock_metrics_cls,
        mock_comments_cls,
        mock_get_bookmark,
        mock_get_object,
        mock_metrics,
        mock_update_bookmark,
        mock_logger,
    ):
        audits_inst = MagicMock(name="audits_inst")
        audits_inst.is_selected.return_value = False
        mock_audits_cls.return_value = audits_inst

        metrics_inst = MagicMock(name="metrics_inst")
        metrics_inst.is_selected.return_value = False
        mock_metrics_cls.return_value = metrics_inst

        comments_inst = MagicMock(name="comments_inst")
        comments_inst.is_selected.return_value = True
        comments_inst.name = "ticket_comments"
        comments_inst.replication_key = "created_at"
        comments_inst.starting_state = None
        comments_inst.count = 0
        comments_inst.stream = MagicMock(tap_stream_id="ticket_comments")
        comments_inst.fetch_records.side_effect = http.ZendeskNotFoundError
        mock_comments_cls.return_value = comments_inst

        mock_get_object.return_value = [
            {"generated_timestamp": 12457845, "fields": {}, "id": "i1", "metric_set": None}
        ]

        tickets = streams.Tickets(client=None, config={"subdomain": "34", "access_token": "df"})
        tickets.stream = MagicMock(tap_stream_id="tickets")

        list(tickets.sync(state={}))

        mock_logger.assert_called_with(
            "Unable to retrieve %s for ticket (ID: %s), record not found",
            "ticket_comments",
            "i1",
        )
