import os
import json
import datetime
import time
import pytz
import zenpy
import copy
import singer
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from singer import metadata
from singer import utils
from singer.metrics import Point
from tap_zendesk import metrics as zendesk_metrics
from tap_zendesk import http
from tap_zendesk.sync import CHECKPOINT_SENTINEL
from dateutil.parser import isoparse


DEFAULT_SUBSTREAM_WORKERS = 12


LOGGER = singer.get_logger()
KEY_PROPERTIES = ['id']

REQUEST_TIMEOUT = 300
START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}
CUSTOM_TYPES = {
    'text': 'string',
    'textarea': 'string',
    'date': 'string',
    'regexp': 'string',
    'dropdown': 'string',
    'integer': 'integer',
    'decimal': 'number',
    'checkbox': 'boolean',
    'lookup': 'string',
}

DEFAULT_SEARCH_WINDOW_SIZE = (60 * 60 * 24) * 30 # defined in seconds, default to a month (30 days)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def process_custom_field(field):
    """ Take a custom field description and return a schema for it. """
    zendesk_type = field.type
    json_type = CUSTOM_TYPES.get(zendesk_type)
    if json_type is None:
        raise Exception("Discovered unsupported type for custom field {} (key: {}): {}"
                        .format(field.title,
                                field.key,
                                zendesk_type))
    field_schema = {'type': [
        json_type,
        'null'
    ]}

    if zendesk_type == 'date':
        field_schema['format'] = 'datetime'
    if zendesk_type == 'dropdown':
        field_schema['enum'] = [o.value for o in field.custom_field_options]

    return field_schema

class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = KEY_PROPERTIES
    stream = None
    endpoint = None
    request_timeout = None

    def __init__(self, client=None, config=None):
        self.client = client
        self.config = config
        # Set and pass request timeout to config param `request_timeout` value.
        config_request_timeout = self.config.get('request_timeout')
        if config_request_timeout and float(config_request_timeout):
            self.request_timeout = float(config_request_timeout)
        else:
            self.request_timeout = REQUEST_TIMEOUT # If value is 0,"0","" or not passed then it set default to 300 seconds.

    def get_bookmark(self, state):
        return utils.strptime_with_tz(singer.get_bookmark(state, self.name, self.replication_key))

    def update_bookmark(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key, value)


    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return self._add_custom_fields(schema)

    def _add_custom_fields(self, schema): # pylint: disable=no-self-use
        return schema

    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        url = self.endpoint.format(self.config['subdomain'])
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])

        http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)

class CursorBasedStream(Stream):
    item_key = None
    endpoint = None

    def get_objects(self, **kwargs):
        '''
        Cursor based object retrieval
        '''
        url = self.endpoint.format(self.config['subdomain'])
        # Pass `request_timeout` parameter
        for page in http.get_cursor_based(url, self.config['access_token'], self.request_timeout, **kwargs):
            yield from page[self.item_key]

class CursorBasedExportStream(Stream):
    endpoint = None
    item_key = None

    def check_access(self):
        url = self.endpoint.format(self.config['subdomain'])
        # utils.strptime_with_tz tolerates both "...Z" and "....000Z" — strict
        # strptime crashed discover when callers passed millisecond precision.
        start_time = int(utils.strptime_with_tz(self.config['start_date']).timestamp())
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])
        http.call_api(url, self.request_timeout, params={'start_time': start_time, 'per_page': 1}, headers=HEADERS)

    def get_objects(self, start_time, params=None):
        '''
        Retrieve objects from the incremental exports endpoint using cursor based pagination.
        Optional `params` are forwarded to the HTTP layer (e.g. include= sideloads).
        '''
        url = self.endpoint.format(self.config['subdomain'])
        for page in http.get_incremental_export(
            url, self.config['access_token'], self.request_timeout, start_time, params=params,
        ):
            if "error" in page and self.item_key not in page:
                raise Exception("Error: "+page.get("error",{}).get("message","Error found in the account."))
            yield from page[self.item_key]


def raise_or_log_zenpy_apiexception(schema, stream, e):
    # There are multiple tiers of Zendesk accounts. Some of them have
    # access to `custom_fields` and some do not. This is the specific
    # error that appears to be return from the API call in the event that
    # it doesn't have access.
    if not isinstance(e, zenpy.lib.exception.APIException):
        raise ValueError("Called with a bad exception type") from e

    #If read permission is not available in OAuth access_token, then it returns the below error.
    if json.loads(e.args[0]).get('description') == "You are missing the following required scopes: read":
        LOGGER.warning("The account credentials supplied do not have access to `%s` custom fields.",
                       stream)
        return schema
    error = json.loads(e.args[0]).get('error')
    # check if the error is of type dictionary and the message retrieved from the dictionary
    # is the expected message. If so, only then print the logger message and return the schema
    if isinstance(error, dict) and error.get('message', None) == "You do not have access to this page. Please contact the account owner of this help desk for further help.":
        LOGGER.warning("The account credentials supplied do not have access to `%s` custom fields.",
                       stream)
        return schema
    else:
        raise e


class Organizations(Stream):
    name = "organizations"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/organizations'
    item_key = 'organizations'

    def _add_custom_fields(self, schema):
        endpoint = self.client.organizations.endpoint
        # NB: Zenpy doesn't have a public endpoint for this at time of writing
        #     Calling into underlying query method to grab all fields
        try:
            field_gen = self.client.organizations._query_zendesk(endpoint.organization_fields, # pylint: disable=protected-access
                                                                 'organization_field')
        except zenpy.lib.exception.APIException as e:
            return raise_or_log_zenpy_apiexception(schema, self.name, e)
        schema['properties']['organization_fields']['properties'] = {}
        for field in field_gen:
            schema['properties']['organization_fields']['properties'][field.key] = process_custom_field(field)

        return schema

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        organizations = self.client.organizations.incremental(start_time=bookmark)
        for organization in organizations:
            self.update_bookmark(state, organization.updated_at)
            yield (self.stream, organization)

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # Convert datetime object to standard format with timezone. Used utcnow to reduce API call burden at discovery time.
        # Because API will return records from now which will be very less
        start_time = datetime.datetime.utcnow().strftime(START_DATE_FORMAT)
        self.client.organizations.incremental(start_time=start_time)

class Users(CursorBasedExportStream):
    name = "users"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    item_key = "users"
    endpoint = "https://{}.zendesk.com/api/v2/incremental/users/cursor.json"

    def _add_custom_fields(self, schema):
        try:
            field_gen = self.client.user_fields()
        except zenpy.lib.exception.APIException as e:
            return raise_or_log_zenpy_apiexception(schema, self.name, e)
        schema['properties']['user_fields']['properties'] = {}
        for field in field_gen:
            schema['properties']['user_fields']['properties'][field.key] = process_custom_field(field)

        return schema

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())
        users = self.get_objects(epoch_bookmark)

        for user in users:
            self.update_bookmark(state, user["updated_at"])
            yield (self.stream, user)

        #singer.write_state(state)


    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # Convert datetime object to standard format with timezone. Used utcnow to reduce API call burden at discovery time.
        # Because API will return records from now which will be very less
        start_time = datetime.datetime.utcnow().strftime(START_DATE_FORMAT)
        self.client.search("", updated_after=start_time, updated_before='2000-01-02T00:00:00Z', type="user")


class Tickets(CursorBasedExportStream):
    name = "tickets"
    replication_method = "INCREMENTAL"
    replication_key = "generated_timestamp"
    item_key = "tickets"
    endpoint = "https://{}.zendesk.com/api/v2/incremental/tickets/cursor.json"

    def get_objects(self, start_time, params=None):
        merged = {'include': 'metric_sets'}
        if params:
            merged.update(params)
        return super().get_objects(start_time, params=merged)

    def sync(self, state): #pylint: disable=too-many-statements

        bookmark = self.get_bookmark(state)
        tickets = self.get_objects(bookmark)

        audits_stream = TicketAudits(self.client, self.config)
        metrics_stream = TicketMetrics(self.client, self.config)
        comments_stream = TicketComments(self.client, self.config)

        def emit_sub_stream_metrics(sub_stream):
            if sub_stream.is_selected():
                singer.metrics.log(LOGGER, Point(metric_type='counter',
                                                 metric=singer.metrics.Metric.record_count,
                                                 value=sub_stream.count,
                                                 tags={'endpoint':sub_stream.stream.tap_stream_id}))
                sub_stream.count = 0

        if audits_stream.is_selected():
            LOGGER.info("Syncing ticket_audits per ticket...")

        worker_count = max(1, int(self.config.get('substream_workers') or DEFAULT_SUBSTREAM_WORKERS))
        any_sub_selected = (
            audits_stream.is_selected()
            or comments_stream.is_selected()
        )

        def _fetch_or_404(stream_obj, fetch_fn, ticket_id):
            try:
                return fetch_fn()
            except http.ZendeskNotFoundError:
                LOGGER.warning(
                    "Unable to retrieve %s for ticket (ID: %s), record not found",
                    stream_obj.name, ticket_id,
                )
                return []

        def fetch_substreams(ticket_id):
            """Pure-HTTP sub-stream fetch. Audits + comments only; metrics
            now come from the metric_sets sideload on the parent ticket."""
            audits = comments = None
            if audits_stream.is_selected():
                audits = _fetch_or_404(
                    audits_stream,
                    lambda: list(audits_stream.get_objects(ticket_id)),
                    ticket_id,
                )
            if comments_stream.is_selected():
                comments = _fetch_or_404(
                    comments_stream,
                    lambda: comments_stream.fetch_records(ticket_id),
                    ticket_id,
                )
            return audits, comments

        def emit_ticket(ticket, audits, comments):
            zendesk_metrics.capture('ticket')
            generated_timestamp_dt = datetime.datetime.utcfromtimestamp(
                ticket.get('generated_timestamp')
            ).replace(tzinfo=pytz.UTC)
            ticket.pop('fields', None)
            metric_set = ticket.pop('metric_set', None)
            yield (self.stream, ticket)

            if metrics_stream.is_selected() and metric_set is not None:
                zendesk_metrics.capture('ticket_metric')
                metrics_stream.count += 1
                yield (metrics_stream.stream, metric_set)

            for audit in audits or []:
                zendesk_metrics.capture('ticket_audit')
                audits_stream.count += 1
                yield (audits_stream.stream, audit)
            if comments is not None:
                # process_records mutates state and must run main-thread only
                yield from comments_stream.process_records(ticket['id'], comments, state)

            # Bookmark advances after all sub-streams for this ticket have
            # been emitted; the sentinel that follows is a safe commit point.
            self.update_bookmark(state, utils.strftime(generated_timestamp_dt))
            yield (CHECKPOINT_SENTINEL, None)

        if not any_sub_selected:
            for ticket in tickets:
                yield from emit_ticket(ticket, None, None)
            emit_sub_stream_metrics(audits_stream)
            emit_sub_stream_metrics(comments_stream)
            return

        # Prime starting_state once up-front so the comment-bookmark
        # fallback doesn't depend on worker completion order.
        if comments_stream.is_selected() and not comments_stream.starting_state:
            primed_state = singer.bookmarks.ensure_bookmark_path(
                state,
                ['bookmarks', comments_stream.name, comments_stream.replication_key],
            )
            comments_stream.starting_state = copy.deepcopy(primed_state)
            comments_stream.starting_bookmark = singer.get_bookmark(
                comments_stream.starting_state,
                comments_stream.name,
                comments_stream.replication_key,
            ) or {}

        executor = ThreadPoolExecutor(max_workers=worker_count)
        try:
            in_flight = deque()
            tickets_iter = iter(tickets)

            def submit_next():
                try:
                    ticket = next(tickets_iter)
                except StopIteration:
                    return False
                future = executor.submit(fetch_substreams, ticket['id'])
                in_flight.append((ticket, future))
                return True

            for _ in range(worker_count):
                if not submit_next():
                    break

            while in_flight:
                ticket, future = in_flight.popleft()
                audits, comments = future.result()
                yield from emit_ticket(ticket, audits, comments)
                submit_next()
        finally:
            # On exception, drop already-queued fetches instead of burning
            # rate-limit quota completing them.
            executor.shutdown(wait=True, cancel_futures=True)

        emit_sub_stream_metrics(audits_stream)
        emit_sub_stream_metrics(comments_stream)

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        url = self.endpoint.format(self.config['subdomain'])
        start_time = int(utils.strptime_with_tz(self.config['start_date']).timestamp())
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])

        http.call_api(url, self.request_timeout, params={'start_time': start_time, 'per_page': 1}, headers=HEADERS)


class TicketAudits(Stream):
    name = "ticket_audits"
    replication_method = "INCREMENTAL"
    count = 0
    endpoint='https://{}.zendesk.com/api/v2/tickets/{}/audits.json'
    item_key='audits'

    def get_objects(self, ticket_id):
        url = self.endpoint.format(self.config['subdomain'], ticket_id)
        # Pass `request_timeout` parameter
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, ticket_id):
        ticket_audits = self.get_objects(ticket_id)
        for ticket_audit in ticket_audits:
            zendesk_metrics.capture('ticket_audit')
            self.count += 1
            yield (self.stream, ticket_audit)

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''

        url = self.endpoint.format(self.config['subdomain'], '1')
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])
        try:
            http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)
        except http.ZendeskNotFoundError:
            #Skip 404 ZendeskNotFoundError error as goal is just to check whether TicketComments have read permission or not
            pass

class TicketMetrics(CursorBasedStream):
    name = "ticket_metrics"
    replication_method = "INCREMENTAL"
    count = 0
    endpoint = 'https://{}.zendesk.com/api/v2/tickets/{}/metrics'
    item_key = 'ticket_metric'

    def get_objects(self, ticket_id):
        url = self.endpoint.format(self.config['subdomain'], ticket_id)
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)
        for page in pages:
            yield page[self.item_key]

    def sync(self, ticket_id):
        for metric in self.get_objects(ticket_id):
            zendesk_metrics.capture('ticket_metric')
            self.count += 1
            yield (self.stream, metric)

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        url = self.endpoint.format(self.config['subdomain'], '1')
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])
        try:
            http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)
        except http.ZendeskNotFoundError:
            #Skip 404 ZendeskNotFoundError error as goal is just to check whether TicketComments have read permission or not
            pass

class TicketComments(Stream):
    name = "ticket_comments"
    replication_key = "created_at"
    replication_method = "INCREMENTAL"
    count = 0
    starting_state = None
    starting_bookmark = None
    endpoint = "https://{}.zendesk.com/api/v2/tickets/{}/comments.json"
    item_key='comments'

    def get_objects(self, ticket_id):
        url = self.endpoint.format(self.config['subdomain'], ticket_id)
        # Pass `request_timeout` parameter
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)

        for page in pages:
            items = page.get(self.item_key)
            if items:
                yield from items

    def fetch_records(self, ticket_id):
        """HTTP-only; threadsafe. Pair with process_records on the main thread."""
        records = []
        for ticket_comment in self.get_objects(ticket_id):
            ticket_comment['ticket_id'] = ticket_id
            records.append(ticket_comment)
        return records

    def process_records(self, ticket_id, records, state):
        """Mutates state — main-thread only."""
        for ticket_comment in records:
            if not self.starting_state:
                state = singer.bookmarks.ensure_bookmark_path(state, ['bookmarks', self.name, self.replication_key])
                # If bookmark is not available for ticket_comments, then check for bookmark for tickets
                tc_bookmark = state['bookmarks']['ticket_comments'].get(self.replication_key)
                if not tc_bookmark and len(tc_bookmark) == 0:
                    state['bookmarks']['ticket_comments'][self.replication_key] = { str(ticket_id): state['bookmarks']['tickets']['generated_timestamp']}
                self.starting_state = copy.deepcopy(state)
                self.starting_bookmark = singer.get_bookmark(self.starting_state, self.name, self.replication_key)

            created_at = ticket_comment.get('created_at')
            current_bookmark = singer.get_bookmark(state, self.name, self.replication_key)
            if not current_bookmark.get(ticket_id):
                state['bookmarks'][self.name][self.replication_key][ticket_id] = created_at
            else:
                current_bookmark = utils.strptime_with_tz(current_bookmark.get(ticket_id))
                if created_at and utils.strptime_with_tz(created_at) > current_bookmark:
                    state['bookmarks'][self.name][self.replication_key][ticket_id] = created_at

            if self.starting_bookmark.get(str(ticket_id)):
                ticket_bookmark = utils.strptime_with_tz(self.starting_bookmark.get(str(ticket_id)))
            elif self.starting_state['bookmarks'].get("tickets").get(Tickets.replication_key):
                ticket_bookmark = utils.strptime_with_tz(self.starting_state["bookmarks"].get("tickets").get(Tickets.replication_key))
            else:
                ticket_bookmark = utils.strptime_with_tz(self.config.get("start_date"))
            if utils.strptime_with_tz(created_at) > ticket_bookmark:
                yield (self.stream, ticket_comment)
                zendesk_metrics.capture('ticket_comment')
                self.count += 1

    def sync(self, ticket_id, state):
        records = self.fetch_records(ticket_id)
        yield from self.process_records(ticket_id, records, state)

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        url = self.endpoint.format(self.config['subdomain'], '1')
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])
        try:
            http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)
        except http.ZendeskNotFoundError:
            #Skip 404 ZendeskNotFoundError error as goal is to just check to whether TicketComments have read permission or not
            pass

class SatisfactionRatings(CursorBasedStream):
    name = "satisfaction_ratings"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/satisfaction_ratings'
    item_key = 'satisfaction_ratings'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())
        params = {'start_time': epoch_bookmark}
        ratings = self.get_objects(params=params)
        for rating in ratings:
            if utils.strptime_with_tz(rating['updated_at']) >= bookmark:
                self.update_bookmark(state, rating['updated_at'])
                yield (self.stream, rating)


class Groups(CursorBasedStream):
    name = "groups"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/groups'
    item_key = 'groups'

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        groups = self.get_objects()
        for group in groups:
            if utils.strptime_with_tz(group['updated_at']) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, group['updated_at'])
                yield (self.stream, group)

class Macros(CursorBasedStream):
    name = "macros"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/macros'
    item_key = 'macros'

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        macros = self.get_objects()
        for macro in macros:
            if utils.strptime_with_tz(macro['updated_at']) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, macro['updated_at'])
                yield (self.stream, macro)

class Tags(CursorBasedStream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = ["name"]
    endpoint = 'https://{}.zendesk.com/api/v2/tags'
    item_key = 'tags'

    def sync(self, state): # pylint: disable=unused-argument
        tags = self.get_objects()

        for tag in tags:
            yield (self.stream, tag)

class TicketFields(CursorBasedStream):
    name = "ticket_fields"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/ticket_fields'
    item_key = 'ticket_fields'

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        fields = self.get_objects()
        for field in fields:
            if utils.strptime_with_tz(field['updated_at']) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, field['updated_at'])
                yield (self.stream, field)

class TicketForms(Stream):
    name = "ticket_forms"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        forms = self.client.ticket_forms()
        for form in forms:
            if utils.strptime_with_tz(form.updated_at) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, form.updated_at)
                yield (self.stream, form)

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        self.client.ticket_forms()

class GroupMemberships(CursorBasedStream):
    name = "group_memberships"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/group_memberships'
    item_key = 'group_memberships'


    def sync(self, state):
        bookmark = self.get_bookmark(state)
        memberships = self.get_objects()

        for membership in memberships:
            # some group memberships come back without an updated_at
            if membership['updated_at']:
                if utils.strptime_with_tz(membership['updated_at']) >= bookmark:
                    # NB: We don't trust that the records come back ordered by
                    # updated_at (we've observed out-of-order records),
                    # so we can't save state until we've seen all records
                    self.update_bookmark(state, membership['updated_at'])
                    yield (self.stream, membership)
            else:
                if membership['id']:
                    LOGGER.info('group_membership record with id: ' + str(membership['id']) +
                                ' does not have an updated_at field so it will be syncd...')
                    yield (self.stream, membership)
                else:
                    LOGGER.info('Received group_membership record with no id or updated_at, skipping...')

class SLAPolicies(Stream):
    name = "sla_policies"
    replication_method = "FULL_TABLE"

    def sync(self, state): # pylint: disable=unused-argument
        for policy in self.client.sla_policies():
            yield (self.stream, policy)

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        self.client.sla_policies()

from tap_zendesk.streams_help_center import (
    Articles, ArticleAttachments, ArticleComments, ArticleVotes, ArticleCommentVotes,
    Categories, Sections,
)
from tap_zendesk.streams_community import (
    Topics, Posts, PostComments, PostVotes, PostCommentVotes,
)
from tap_zendesk.streams_support import (
    AuditLogs, Automations, Brands, CustomRoles, DeletedTickets,
    OrganizationFields, OrganizationMemberships, Schedules,
    TicketActivities, TicketMetricEvents, TicketSkips, Triggers,
    UserFields, UserIdentities,
)

STREAMS = {
    "tickets": Tickets,
    "groups": Groups,
    "users": Users,
    "organizations": Organizations,
    "ticket_audits": TicketAudits,
    "ticket_comments": TicketComments,
    "ticket_fields": TicketFields,
    "ticket_forms": TicketForms,
    "group_memberships": GroupMemberships,
    "macros": Macros,
    "satisfaction_ratings": SatisfactionRatings,
    "tags": Tags,
    "ticket_metrics": TicketMetrics,
    "sla_policies": SLAPolicies,
    "articles": Articles,
    "article_attachments": ArticleAttachments,
    "article_comments": ArticleComments,
    "article_votes": ArticleVotes,
    "article_comment_votes": ArticleCommentVotes,
    "categories": Categories,
    "sections": Sections,
    "topics": Topics,
    "posts": Posts,
    "post_comments": PostComments,
    "post_votes": PostVotes,
    "post_comment_votes": PostCommentVotes,
    "audit_logs": AuditLogs,
    "automations": Automations,
    "brands": Brands,
    "custom_roles": CustomRoles,
    "deleted_tickets": DeletedTickets,
    "organization_fields": OrganizationFields,
    "organization_memberships": OrganizationMemberships,
    "schedules": Schedules,
    "ticket_activities": TicketActivities,
    "ticket_metric_events": TicketMetricEvents,
    "ticket_skips": TicketSkips,
    "triggers": Triggers,
    "user_fields": UserFields,
    "user_identities": UserIdentities,
}
