from unittest.mock import patch, MagicMock
from tap_zendesk.streams import Tickets


def _selected(stream_id):
    s = MagicMock()
    s.tap_stream_id = stream_id
    return s


@patch('tap_zendesk.streams.CursorBasedExportStream.get_objects')
def test_tickets_get_objects_sideloads_metric_sets(mock_super):
    mock_super.return_value = iter([])
    stream = Tickets(client=None, config={'subdomain': 'acme', 'access_token': 'tok', 'start_date': '2026-01-01T00:00:00Z'})

    list(stream.get_objects(1700000000))

    assert mock_super.call_args.kwargs.get('params') == {'include': 'metric_sets'}


@patch('tap_zendesk.streams.TicketComments')
@patch('tap_zendesk.streams.TicketAudits')
@patch('tap_zendesk.streams.TicketMetrics')
def test_tickets_sync_emits_sideloaded_metric_set_and_skips_metrics_endpoint(
    MockMetrics, MockAudits, MockComments,
):
    metrics_inst = MockMetrics.return_value
    metrics_inst.is_selected.return_value = True
    metrics_inst.stream = _selected('ticket_metrics')
    metrics_inst.count = 0

    audits_inst = MockAudits.return_value
    audits_inst.is_selected.return_value = False
    comments_inst = MockComments.return_value
    comments_inst.is_selected.return_value = False

    config = {'subdomain': 'acme', 'access_token': 'tok', 'start_date': '2026-01-01T00:00:00Z'}
    tickets = Tickets(client=None, config=config)
    tickets.stream = _selected('tickets')
    tickets.get_objects = MagicMock(return_value=iter([
        {'id': 1, 'generated_timestamp': 1700000000, 'fields': [], 'metric_set': {'id': 11, 'replies': 2}},
        {'id': 2, 'generated_timestamp': 1700000100, 'fields': [], 'metric_set': {'id': 22, 'replies': 5}},
    ]))
    tickets.get_bookmark = MagicMock(return_value=MagicMock())
    tickets.update_bookmark = MagicMock()

    emitted = list(tickets.sync({}))

    metrics_inst.get_objects.assert_not_called()

    metric_records = [r for s, r in emitted if s is metrics_inst.stream]
    assert metric_records == [
        {'id': 11, 'replies': 2},
        {'id': 22, 'replies': 5},
    ]


@patch('tap_zendesk.streams.TicketComments')
@patch('tap_zendesk.streams.TicketAudits')
@patch('tap_zendesk.streams.TicketMetrics')
def test_tickets_sync_does_not_emit_metrics_when_stream_unselected(
    MockMetrics, MockAudits, MockComments,
):
    metrics_inst = MockMetrics.return_value
    metrics_inst.is_selected.return_value = False
    metrics_inst.stream = _selected('ticket_metrics')
    audits_inst = MockAudits.return_value
    audits_inst.is_selected.return_value = False
    comments_inst = MockComments.return_value
    comments_inst.is_selected.return_value = False

    config = {'subdomain': 'acme', 'access_token': 'tok', 'start_date': '2026-01-01T00:00:00Z'}
    tickets = Tickets(client=None, config=config)
    tickets.stream = _selected('tickets')
    tickets.get_objects = MagicMock(return_value=iter([
        {'id': 1, 'generated_timestamp': 1700000000, 'fields': [], 'metric_set': {'id': 11}},
    ]))
    tickets.get_bookmark = MagicMock(return_value=MagicMock())
    tickets.update_bookmark = MagicMock()

    emitted = list(tickets.sync({}))

    record_streams = [s for s, _ in emitted]
    assert metrics_inst.stream not in record_streams
