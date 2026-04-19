import datetime
import singer
from singer import utils
from tap_zendesk.streams import Stream, CursorBasedStream, CursorBasedExportStream, START_DATE_FORMAT, HEADERS
from tap_zendesk import http

LOGGER = singer.get_logger()


class AuditLogs(CursorBasedStream):
    name = "audit_logs"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"
    endpoint = 'https://{}.zendesk.com/api/v2/audit_logs'
    item_key = 'audit_logs'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        params = {
            'filter[created_at][]': utils.strftime(bookmark),
            'sort': 'created_at',
        }
        logs = self.get_objects(params=params)
        for log in logs:
            if utils.strptime_with_tz(log['created_at']) >= bookmark:
                self.update_bookmark(state, log['created_at'])
                yield (self.stream, log)


class Automations(CursorBasedStream):
    name = "automations"
    replication_method = "FULL_TABLE"
    endpoint = 'https://{}.zendesk.com/api/v2/automations'
    item_key = 'automations'

    def sync(self, state):
        automations = self.get_objects()
        for automation in automations:
            yield (self.stream, automation)


class Brands(CursorBasedStream):
    name = "brands"
    replication_method = "FULL_TABLE"
    endpoint = 'https://{}.zendesk.com/api/v2/brands'
    item_key = 'brands'

    def sync(self, state):
        brands = self.get_objects()
        for brand in brands:
            yield (self.stream, brand)


class CustomRoles(CursorBasedStream):
    name = "custom_roles"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/custom_roles'
    item_key = 'custom_roles'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        roles = self.get_objects()
        for role in roles:
            if utils.strptime_with_tz(role['updated_at']) >= bookmark:
                self.update_bookmark(state, role['updated_at'])
                yield (self.stream, role)


class DeletedTickets(CursorBasedStream):
    name = "deleted_tickets"
    replication_method = "FULL_TABLE"
    endpoint = 'https://{}.zendesk.com/api/v2/deleted_tickets'
    item_key = 'deleted_tickets'

    def sync(self, state):
        deleted = self.get_objects()
        for ticket in deleted:
            yield (self.stream, ticket)

    def check_access(self):
        url = self.endpoint.format(self.config['subdomain'])
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config['access_token'])
        try:
            http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)
        except http.ZendeskForbidden:
            LOGGER.warning("No access to deleted_tickets — user may lack view_deleted_tickets permission")
            raise
        except http.ZendeskNotFound:
            pass


class OrganizationFields(CursorBasedStream):
    name = "organization_fields"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/organization_fields'
    item_key = 'organization_fields'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        fields = self.get_objects()
        for field in fields:
            if utils.strptime_with_tz(field['updated_at']) >= bookmark:
                self.update_bookmark(state, field['updated_at'])
                yield (self.stream, field)


class OrganizationMemberships(CursorBasedStream):
    name = "organization_memberships"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/organization_memberships'
    item_key = 'organization_memberships'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        memberships = self.get_objects()
        for membership in memberships:
            if utils.strptime_with_tz(membership['updated_at']) >= bookmark:
                self.update_bookmark(state, membership['updated_at'])
                yield (self.stream, membership)


class Schedules(CursorBasedStream):
    name = "schedules"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/business_hours/schedules.json'
    item_key = 'schedules'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        schedules = self.get_objects()
        for schedule in schedules:
            if utils.strptime_with_tz(schedule['updated_at']) >= bookmark:
                self.update_bookmark(state, schedule['updated_at'])
                yield (self.stream, schedule)


class TicketActivities(CursorBasedStream):
    name = "ticket_activities"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/activities'
    item_key = 'activities'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        params = {'sort_by': 'created_at', 'sort_order': 'asc'}
        activities = self.get_objects(params=params)
        for activity in activities:
            if utils.strptime_with_tz(activity['updated_at']) >= bookmark:
                self.update_bookmark(state, activity['updated_at'])
                yield (self.stream, activity)


class TicketMetricEvents(CursorBasedExportStream):
    name = "ticket_metric_events"
    replication_method = "INCREMENTAL"
    replication_key = "time"
    item_key = "ticket_metric_events"
    endpoint = "https://{}.zendesk.com/api/v2/incremental/ticket_metric_events"

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())
        events = self.get_objects(epoch_bookmark)
        for event in events:
            if event.get('time'):
                self.update_bookmark(state, event['time'])
            yield (self.stream, event)


class TicketSkips(CursorBasedStream):
    name = "ticket_skips"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/skips.json'
    item_key = 'skips'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        params = {'sort_order': 'desc'}
        skips = self.get_objects(params=params)
        for skip in skips:
            if utils.strptime_with_tz(skip['updated_at']) >= bookmark:
                self.update_bookmark(state, skip['updated_at'])
                yield (self.stream, skip)


class Triggers(CursorBasedStream):
    name = "triggers"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/triggers'
    item_key = 'triggers'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        triggers = self.get_objects()
        for trigger in triggers:
            if utils.strptime_with_tz(trigger['updated_at']) >= bookmark:
                self.update_bookmark(state, trigger['updated_at'])
                yield (self.stream, trigger)


class UserFields(CursorBasedStream):
    name = "user_fields"
    replication_method = "FULL_TABLE"
    endpoint = 'https://{}.zendesk.com/api/v2/user_fields'
    item_key = 'user_fields'

    def sync(self, state):
        fields = self.get_objects()
        for field in fields:
            yield (self.stream, field)


class UserIdentities(CursorBasedExportStream):
    name = "user_identities"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    item_key = "identities"
    endpoint = "https://{}.zendesk.com/api/v2/incremental/users/cursor.json"

    def get_objects(self, start_time):
        url = self.endpoint.format(self.config['subdomain'])
        for page in http.get_incremental_export(url, self.config['access_token'], self.request_timeout, start_time):
            if "error" in page and self.item_key not in page:
                raise Exception("Error: " + page.get("error", {}).get("message", "Error found in the account."))
            yield from page.get(self.item_key, [])

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())
        identities = self.get_objects(epoch_bookmark)
        for identity in identities:
            self.update_bookmark(state, identity['updated_at'])
            yield (self.stream, identity)

    def check_access(self):
        start_time = datetime.datetime.utcnow().strftime(START_DATE_FORMAT)
        url = self.endpoint.format(self.config['subdomain'])
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.config['access_token']),
        }
        try:
            http.call_api(url, self.request_timeout, params={'start_time': start_time, 'per_page': 1, 'include': 'identities'}, headers=headers)
        except http.ZendeskForbidden:
            raise
        except http.ZendeskNotFound:
            pass
