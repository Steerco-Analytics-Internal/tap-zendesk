"""
Test-suite-wide fixtures.

call_api() in tap_zendesk.http reads sys.argv via singer.utils.parse_args
on every invocation (to look up marketplace headers / auth token). Under
pytest, sys.argv contains pytest's own args, so the parse fails with
"the following arguments are required: -c/--config" and aborts the test.

We stub sys.argv at the test-session level with a minimal-but-valid config
file so any code path that reaches singer.utils.parse_args succeeds.
"""

import json
import sys
import tempfile
import os

import pytest


@pytest.fixture(autouse=True, scope='session')
def _fake_singer_argv():
    config_dict = {
        'subdomain': 'test-subdomain',
        'access_token': 'test-token',
        'start_date': '2025-01-01T00:00:00Z',
    }
    fd, config_path = tempfile.mkstemp(suffix='.json', prefix='tap_zendesk_test_config_')
    with os.fdopen(fd, 'w') as f:
        json.dump(config_dict, f)

    saved_argv = sys.argv
    sys.argv = ['pytest', '-c', config_path]
    try:
        yield
    finally:
        sys.argv = saved_argv
        try:
            os.unlink(config_path)
        except OSError:
            pass
