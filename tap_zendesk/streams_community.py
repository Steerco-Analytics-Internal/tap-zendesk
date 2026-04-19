import singer
from singer import utils
from tap_zendesk.streams import Stream, CursorBasedStream
from tap_zendesk import http
from tap_zendesk import metrics as zendesk_metrics

LOGGER = singer.get_logger()


class Topics(CursorBasedStream):
    name = "topics"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/community/topics'
    item_key = 'topics'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        topics = self.get_objects()
        for topic in topics:
            if utils.strptime_with_tz(topic['updated_at']) >= bookmark:
                self.update_bookmark(state, topic['updated_at'])
                yield (self.stream, topic)


class Posts(Stream):
    name = "posts"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/community/posts'
    item_key = 'posts'

    def get_objects(self, start_time=None):
        url = self.endpoint.format(self.config['subdomain'])
        params = {}
        if start_time:
            params['start_time'] = start_time
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout, params=params)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())

        comments_stream = PostComments(self.client, self.config)
        votes_stream = PostVotes(self.client, self.config)

        for post in self.get_objects(epoch_bookmark):
            self.update_bookmark(state, post['updated_at'])
            yield (self.stream, post)

            post_id = post['id']

            if comments_stream.is_selected():
                try:
                    for comment in comments_stream.sync(post_id, state):
                        yield comment
                except http.ZendeskNotFound:
                    LOGGER.warning("Unable to retrieve comments for post (ID: %s), not found", post_id)

            if votes_stream.is_selected():
                try:
                    for vote in votes_stream.sync(post_id):
                        yield vote
                except http.ZendeskNotFound:
                    LOGGER.warning("Unable to retrieve votes for post (ID: %s), not found", post_id)

    def check_access(self):
        url = self.endpoint.format(self.config['subdomain'])
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.config['access_token']),
        }
        try:
            http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=headers)
        except http.ZendeskForbidden:
            raise
        except http.ZendeskNotFound:
            pass


class PostComments(Stream):
    name = "post_comments"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    count = 0
    endpoint = 'https://{}.zendesk.com/api/v2/community/posts/{}/comments'
    item_key = 'comments'

    def get_objects(self, post_id):
        url = self.endpoint.format(self.config['subdomain'], post_id)
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, post_id, state):
        comment_votes_stream = PostCommentVotes(self.client, self.config)

        for comment in self.get_objects(post_id):
            comment['post_id'] = post_id
            zendesk_metrics.capture('post_comment')
            self.count += 1
            yield (self.stream, comment)

            if comment_votes_stream.is_selected():
                try:
                    for vote in comment_votes_stream.sync(post_id, comment['id']):
                        yield vote
                except http.ZendeskNotFound:
                    LOGGER.warning("Unable to retrieve votes for post comment (post: %s, comment: %s), not found",
                                   post_id, comment['id'])

    def check_access(self):
        pass


class PostVotes(Stream):
    name = "post_votes"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    count = 0
    endpoint = 'https://{}.zendesk.com/api/v2/community/posts/{}/votes'
    item_key = 'votes'

    def get_objects(self, post_id):
        url = self.endpoint.format(self.config['subdomain'], post_id)
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, post_id):
        for vote in self.get_objects(post_id):
            zendesk_metrics.capture('post_vote')
            self.count += 1
            yield (self.stream, vote)

    def check_access(self):
        pass


class PostCommentVotes(Stream):
    name = "post_comment_votes"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    count = 0
    endpoint = 'https://{}.zendesk.com/api/v2/community/posts/{}/comments/{}/votes'
    item_key = 'votes'

    def get_objects(self, post_id, comment_id):
        url = self.endpoint.format(self.config['subdomain'], post_id, comment_id)
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, post_id, comment_id):
        for vote in self.get_objects(post_id, comment_id):
            zendesk_metrics.capture('post_comment_vote')
            self.count += 1
            yield (self.stream, vote)

    def check_access(self):
        pass
