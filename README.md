# tap-zendesk
Tap for Zendesk

## Installation

1. Create and activate a virtualenv
1. `pip install -e '.[dev]'`

---

## Authentication

### Using OAuth

OAuth is the default authentication method for `tap-zendesk`. To use OAuth, you will need to fetch an `access_token` from a configured Zendesk integration. See https://support.zendesk.com/hc/en-us/articles/203663836 for more details on how to integrate your application with Zendesk.

**config.json**
```json
{
  "access_token": "AVERYLONGOAUTHTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z",
  "request_timeout": 300
}
```
- `request_timeout` (integer, `300`): It is the time for which request should wait to get response. It is an optional parameter and default request_timeout is 300 seconds.
### Using API Tokens

For a simplified, but less granular setup, you can use the API Token authentication which can be generated from the Zendesk Admin page. See https://support.zendesk.com/hc/en-us/articles/226022787-Generating-a-new-API-token- for more details about generating an API Token. You'll then be able to use the admins's `email` and the generated `api_token` to authenticate.

**config.json**
```json
{
  "email": "user@domain.com",
  "api_token": "THISISAVERYLONGTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z",
  "request_timeout": 300
}
```
- `request_timeout` (integer, `300`):It is the time for which request should wait to get response. It is an optional parameter and default request_timeout is 300 seconds.

## Performance & resilience tuning

Two optional config keys control how the tap behaves during long-running syncs (e.g. an initial backfill of a large customer's tickets):

- `substream_workers` (integer, `6`): number of concurrent worker threads that pre-fetch ticket sub-streams (audits, metrics, comments) ahead of the main emission loop. Records are still emitted in ticket order, but the network calls for upcoming tickets pipeline in parallel. Stay within your Zendesk plan's per-minute API quota — at Enterprise (700 req/min) `6`–`8` is comfortable. Set to `1` to disable concurrency entirely.
- `checkpoint_every` (integer, `100`): how often (in completed parent tickets) the tap emits a Singer `STATE` message. Smaller values give finer-grained resumability at the cost of more state churn. The default is fine for most syncs; lower it (e.g. `25`) if jobs are getting interrupted often.

Copyright &copy; 2018 Stitch
