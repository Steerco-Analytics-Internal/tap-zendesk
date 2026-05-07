"""
Some Singer runners pass start_date with fractional seconds
(e.g. "2025-01-01T06:00:00.000Z"). The CursorBasedExportStream and
Tickets `check_access` paths used a strict strptime format that didn't
tolerate the .000Z suffix and crashed discover with:

    ValueError: time data '2025-01-01T06:00:00.000Z' does not match
                format '%Y-%m-%dT%H:%M:%SZ'

singer.utils.strptime_with_tz handles both forms.
"""

from unittest.mock import MagicMock, patch

from tap_zendesk.streams import CursorBasedExportStream, Tickets


class _ProbeExport(CursorBasedExportStream):
    name = "probe_export"
    item_key = "tickets"
    endpoint = "https://{}.zendesk.com/api/v2/incremental/probe/cursor.json"


@patch("tap_zendesk.streams.http.call_api")
def test_cursor_export_check_access_tolerates_milliseconds(mock_call_api):
    mock_call_api.return_value = MagicMock()
    config = {
        "subdomain": "acme",
        "access_token": "tok",
        "start_date": "2025-01-01T06:00:00.000Z",
    }
    stream = _ProbeExport(client=None, config=config)
    stream.check_access()  # must not raise ValueError

    sent_params = mock_call_api.call_args.kwargs["params"]
    assert isinstance(sent_params["start_time"], int)


@patch("tap_zendesk.streams.http.call_api")
def test_tickets_check_access_tolerates_milliseconds(mock_call_api):
    mock_call_api.return_value = MagicMock()
    config = {
        "subdomain": "acme",
        "access_token": "tok",
        "start_date": "2025-01-01T06:00:00.000Z",
    }
    stream = Tickets(client=None, config=config)
    stream.check_access()  # must not raise ValueError

    sent_params = mock_call_api.call_args.kwargs["params"]
    assert isinstance(sent_params["start_time"], int)


@patch("tap_zendesk.streams.http.call_api")
def test_check_access_still_works_for_canonical_format(mock_call_api):
    """Regression: '2025-01-01T06:00:00Z' (no fractional seconds) keeps working."""
    mock_call_api.return_value = MagicMock()
    config = {
        "subdomain": "acme",
        "access_token": "tok",
        "start_date": "2025-01-01T06:00:00Z",
    }
    Tickets(client=None, config=config).check_access()
    _ProbeExport(client=None, config=config).check_access()
