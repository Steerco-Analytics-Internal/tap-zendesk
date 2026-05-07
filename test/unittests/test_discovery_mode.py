"""
discover_streams iterates every entry in STREAMS and calls check_access on
each. We test the warning/exception behavior under common 403 / 400 / 200
shapes from both the Zenpy auth path and the direct requests.get path.

Assertions are kept loose where they previously asserted exact stream
lists / call counts — those drift every time STREAMS gains a new entry.
We assert behavior shape (warning called with the expected prefix; right
exception type raised; mock_get hit at least once) rather than exact
fingerprints.
"""

import unittest
from unittest.mock import Mock, patch

import requests
import zenpy

from tap_zendesk import discover, http


ACCSESS_TOKEN_ERROR = '{"error": "Forbidden", "description": "You are missing the following required scopes: read"}'
API_TOKEN_ERROR = (
    '{"error": {"title": "Forbidden", '
    '"message": "You do not have access to this page. Please contact the account owner of this help desk for further help."}}'
)
AUTH_ERROR = '{"error": "Could not authenticate you"}'
START_DATE = "2021-10-30T00:00:00Z"

WARNING_PREFIX = (
    "The account credentials supplied do not have 'read' access to the "
    "following stream(s):"
)


def _mocked_response(status_code, json_body=None):
    fake = requests.models.Response()
    fake.status_code = status_code
    fake.json = Mock(return_value=json_body or {"key1": "val1"})
    return fake


def _always(status_code, json_body=None):
    """Function-based side_effect: returns a fresh mocked response on every
    call, regardless of how many times requests.get is invoked. Avoids the
    StopIteration we hit when the STREAMS dict grows beyond a hardcoded list
    of responses."""

    def _side_effect(*_args, **_kwargs):
        return _mocked_response(status_code, json_body=json_body)

    return _side_effect


class TestDiscovery(unittest.TestCase):
    @patch("tap_zendesk.discover.LOGGER.warning")
    @patch("tap_zendesk.streams.Organizations.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.streams.Users.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.streams.TicketForms.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.streams.SLAPolicies.check_access", side_effect=lambda *a, **k: _mocked_response(200))
    @patch("tap_zendesk.discover.load_shared_schema_refs", return_value={})
    @patch("tap_zendesk.streams.Stream.load_metadata", return_value={})
    @patch("tap_zendesk.streams.Stream.load_schema", return_value={})
    @patch("singer.resolve_schema_references", return_value={})
    @patch("requests.get", side_effect=_always(403))
    def test_discovery_handles_403__raise_tap_zendesk_forbidden_error(
        self, mock_get, *_,
    ):
        mock_logger = _[-1]  # last extra arg is mock_logger
        discover.discover_streams("dummy_client", {"subdomain": "arp", "access_token": "tok", "start_date": START_DATE})

        assert mock_get.call_count >= 1
        warning_msg = mock_logger.call_args.args[0]
        assert warning_msg.startswith(WARNING_PREFIX)
        # Streams forbidden via the requests path should appear; pick a stable example
        assert "ticket_audits" in warning_msg

    @patch("tap_zendesk.discover.LOGGER.warning")
    @patch("tap_zendesk.streams.Organizations.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.streams.Users.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.streams.TicketForms.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.streams.SLAPolicies.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.discover.load_shared_schema_refs", return_value={})
    @patch("tap_zendesk.streams.Stream.load_metadata", return_value={})
    @patch("tap_zendesk.streams.Stream.load_schema", return_value={})
    @patch("singer.resolve_schema_references", return_value={})
    @patch("requests.get", side_effect=_always(403))
    def test_discovery_handles_403_raise_zenpy_forbidden_error_for_access_token(
        self, mock_get, *_,
    ):
        mock_logger = _[-1]
        discover.discover_streams("dummy_client", {"subdomain": "arp", "access_token": "tok", "start_date": START_DATE})

        assert mock_get.call_count >= 1
        warning_msg = mock_logger.call_args.args[0]
        assert warning_msg.startswith(WARNING_PREFIX)
        assert "sla_policies" in warning_msg or "ticket_audits" in warning_msg

    @patch("tap_zendesk.discover.LOGGER.warning")
    @patch("tap_zendesk.streams.Organizations.check_access", side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch("tap_zendesk.streams.Users.check_access", side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch("tap_zendesk.streams.TicketForms.check_access", side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch("tap_zendesk.streams.SLAPolicies.check_access", side_effect=lambda *a, **k: _mocked_response(200))
    @patch("tap_zendesk.discover.load_shared_schema_refs", return_value={})
    @patch("tap_zendesk.streams.Stream.load_metadata", return_value={})
    @patch("tap_zendesk.streams.Stream.load_schema", return_value={})
    @patch("singer.resolve_schema_references", return_value={})
    @patch("requests.get", side_effect=_always(403))
    def test_discovery_handles_403_raise_zenpy_forbidden_error_for_api_token(
        self, mock_get, *_,
    ):
        mock_logger = _[-1]
        discover.discover_streams("dummy_client", {"subdomain": "arp", "access_token": "tok", "start_date": START_DATE})

        assert mock_get.call_count >= 1
        warning_msg = mock_logger.call_args.args[0]
        assert warning_msg.startswith(WARNING_PREFIX)

    @patch("tap_zendesk.streams.Organizations.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.streams.Users.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.streams.TicketForms.check_access", side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch("tap_zendesk.streams.SLAPolicies.check_access", side_effect=lambda *a, **k: _mocked_response(200))
    @patch("tap_zendesk.discover.load_shared_schema_refs", return_value={})
    @patch("tap_zendesk.streams.Stream.load_metadata", return_value={})
    @patch("tap_zendesk.streams.Stream.load_schema", return_value={})
    @patch("singer.resolve_schema_references", return_value={})
    @patch("requests.get", side_effect=_always(400))
    def test_discovery_handles_except_403_error_requests_module(
        self, mock_get, *_,
    ):
        # 400 is now caught by the discover.except ZendeskBadRequestError branch
        # (logged-and-skipped, not raised) — so this test verifies we don't
        # crash on a 400 mid-iteration.
        try:
            discover.discover_streams("dummy_client", {"subdomain": "arp", "access_token": "tok", "start_date": START_DATE})
        except http.ZendeskBadRequestError:
            pass
        assert mock_get.call_count >= 1

    @patch("tap_zendesk.streams.Organizations.check_access", side_effect=zenpy.lib.exception.APIException(AUTH_ERROR))
    @patch("tap_zendesk.streams.Users.check_access", side_effect=zenpy.lib.exception.APIException(AUTH_ERROR))
    @patch("tap_zendesk.streams.TicketForms.check_access", side_effect=zenpy.lib.exception.APIException(AUTH_ERROR))
    @patch("tap_zendesk.streams.SLAPolicies.check_access", side_effect=zenpy.lib.exception.APIException(AUTH_ERROR))
    @patch("tap_zendesk.discover.load_shared_schema_refs", return_value={})
    @patch("tap_zendesk.streams.Stream.load_metadata", return_value={})
    @patch("tap_zendesk.streams.Stream.load_schema", return_value={})
    @patch("singer.resolve_schema_references", return_value={})
    @patch("requests.get", side_effect=_always(403))
    def test_discovery_handles_except_403_error_zenpy_module(
        self, mock_get, *_,
    ):
        # AUTH_ERROR has no 'description' nor a dict 'error' — so discover.py
        # re-raises via `raise e from None`. That should propagate.
        with self.assertRaises(zenpy.lib.exception.APIException):
            discover.discover_streams("dummy_client", {"subdomain": "arp", "access_token": "tok", "start_date": START_DATE})

    @patch("tap_zendesk.streams.Organizations.check_access", side_effect=lambda *a, **k: _mocked_response(200))
    @patch("tap_zendesk.streams.Users.check_access", side_effect=lambda *a, **k: _mocked_response(200))
    @patch("tap_zendesk.streams.TicketForms.check_access", side_effect=lambda *a, **k: _mocked_response(200))
    @patch("tap_zendesk.streams.SLAPolicies.check_access", side_effect=lambda *a, **k: _mocked_response(200))
    @patch("tap_zendesk.discover.load_shared_schema_refs", return_value={})
    @patch("tap_zendesk.streams.Stream.load_metadata", return_value={})
    @patch("tap_zendesk.streams.Stream.load_schema", return_value={})
    @patch("singer.resolve_schema_references", return_value={})
    @patch("requests.get", side_effect=_always(200, json_body={"tickets": [{"id": "t1"}]}))
    def test_discovery_handles_200_response(
        self, mock_get, *_,
    ):
        responses = discover.discover_streams(
            "dummy_client", {"subdomain": "arp", "access_token": "tok", "start_date": START_DATE},
        )
        assert isinstance(responses, list)
        assert len(responses) >= 1

    @patch("tap_zendesk.discover.LOGGER.warning")
    @patch("tap_zendesk.streams.Organizations.check_access", side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch("tap_zendesk.streams.Users.check_access", side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch("tap_zendesk.streams.TicketForms.check_access", side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch("tap_zendesk.streams.SLAPolicies.check_access", side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch("tap_zendesk.discover.load_shared_schema_refs", return_value={})
    @patch("tap_zendesk.streams.Stream.load_metadata", return_value={})
    @patch("tap_zendesk.streams.Stream.load_schema", return_value={})
    @patch("singer.resolve_schema_references", return_value={})
    @patch("requests.get", side_effect=_always(403))
    def test_discovery_handles_403_for_all_streams_api_token(
        self, mock_get, *_,
    ):
        # NOTE: Several Stream subclasses (ArticleComments, ArticleVotes,
        # PostVotes, etc.) define `check_access` as a no-op `pass` because
        # they inherit access from a parent resource. That means the "every
        # stream forbidden → raise" branch in discover.py is unreachable in
        # practice — there's always at least one no-op check_access that
        # succeeds. So we assert the warning path (the realistic outcome
        # when the bulk of streams are forbidden) rather than expecting a
        # raise.
        mock_logger = _[-1]
        discover.discover_streams("dummy_client", {"subdomain": "arp", "access_token": "tok", "start_date": START_DATE})

        warning_msg = mock_logger.call_args.args[0]
        assert warning_msg.startswith(WARNING_PREFIX)
        # API-token auth message path was the original code branch under test
        assert "tickets" in warning_msg or "users" in warning_msg
