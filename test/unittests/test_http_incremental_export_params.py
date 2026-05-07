from unittest.mock import patch, MagicMock
from tap_zendesk import http
from tap_zendesk.streams import CursorBasedExportStream


class _Probe(CursorBasedExportStream):
    name = 'probe'
    item_key = 'tickets'
    endpoint = 'https://{}.zendesk.com/api/v2/incremental/tickets/cursor.json'


@patch('tap_zendesk.http.call_api')
def test_get_incremental_export_merges_extra_params(mock_call_api):
    fake = MagicMock()
    fake.json.return_value = {'tickets': [], 'end_of_stream': True}
    mock_call_api.return_value = fake

    list(http.get_incremental_export(
        url='https://acme.zendesk.com/api/v2/incremental/tickets/cursor.json',
        access_token='tok',
        request_timeout=30,
        start_time=1700000000,
        params={'include': 'metric_sets'},
    ))

    sent = mock_call_api.call_args.kwargs['params']
    assert sent['start_time'] == 1700000000
    assert sent['include'] == 'metric_sets'


@patch('tap_zendesk.http.call_api')
def test_get_incremental_export_works_without_extra_params(mock_call_api):
    fake = MagicMock()
    fake.json.return_value = {'tickets': [], 'end_of_stream': True}
    mock_call_api.return_value = fake

    list(http.get_incremental_export(
        url='https://acme.zendesk.com/api/v2/incremental/tickets/cursor.json',
        access_token='tok',
        request_timeout=30,
        start_time=1700000000,
    ))

    sent = mock_call_api.call_args.kwargs['params']
    assert sent == {'start_time': 1700000000}


@patch('tap_zendesk.streams.http.get_incremental_export')
def test_cursor_export_stream_forwards_params(mock_get):
    mock_get.return_value = iter([{'tickets': []}])
    stream = _Probe(client=None, config={'subdomain': 'acme', 'access_token': 'tok'})

    list(stream.get_objects(1700000000, params={'include': 'metric_sets'}))

    assert mock_get.call_args.kwargs.get('params') == {'include': 'metric_sets'}
