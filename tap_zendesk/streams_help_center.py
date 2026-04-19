import singer
from singer import utils
from tap_zendesk.streams import Stream, CursorBasedStream
from tap_zendesk import http
from tap_zendesk import metrics as zendesk_metrics

LOGGER = singer.get_logger()


class Articles(Stream):
    name = "articles"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/help_center/incremental/articles'
    item_key = 'articles'

    def get_objects(self, start_time):
        url = self.endpoint.format(self.config['subdomain'])
        params = {'start_time': start_time}
        if not isinstance(start_time, int):
            params = {'start_time': int(start_time.timestamp())}
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout, params=params)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())

        attachments_stream = ArticleAttachments(self.client, self.config)
        comments_stream = ArticleComments(self.client, self.config)
        votes_stream = ArticleVotes(self.client, self.config)

        for article in self.get_objects(epoch_bookmark):
            self.update_bookmark(state, article['updated_at'])
            yield (self.stream, article)

            article_id = article['id']

            if attachments_stream.is_selected():
                try:
                    for attachment in attachments_stream.sync(article_id):
                        yield attachment
                except http.ZendeskNotFound:
                    LOGGER.warning("Unable to retrieve attachments for article (ID: %s), not found", article_id)

            if comments_stream.is_selected():
                try:
                    for comment in comments_stream.sync(article_id, state):
                        yield comment
                except http.ZendeskNotFound:
                    LOGGER.warning("Unable to retrieve comments for article (ID: %s), not found", article_id)

            if votes_stream.is_selected():
                try:
                    for vote in votes_stream.sync(article_id):
                        yield vote
                except http.ZendeskNotFound:
                    LOGGER.warning("Unable to retrieve votes for article (ID: %s), not found", article_id)

    def check_access(self):
        url = self.endpoint.format(self.config['subdomain'])
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.config['access_token']),
        }
        try:
            http.call_api(url, self.request_timeout, params={'start_time': 0}, headers=headers)
        except http.ZendeskForbidden:
            raise
        except http.ZendeskNotFound:
            pass


class ArticleAttachments(Stream):
    name = "article_attachments"
    replication_method = "INCREMENTAL"
    count = 0
    endpoint = 'https://{}.zendesk.com/api/v2/help_center/articles/{}/attachments'
    item_key = 'article_attachments'

    def get_objects(self, article_id):
        url = self.endpoint.format(self.config['subdomain'], article_id)
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, article_id):
        for attachment in self.get_objects(article_id):
            attachment['article_id'] = article_id
            zendesk_metrics.capture('article_attachment')
            self.count += 1
            yield (self.stream, attachment)

    def check_access(self):
        pass


class ArticleComments(Stream):
    name = "article_comments"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    count = 0
    endpoint = 'https://{}.zendesk.com/api/v2/help_center/articles/{}/comments'
    item_key = 'comments'

    def get_objects(self, article_id):
        url = self.endpoint.format(self.config['subdomain'], article_id)
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, article_id, state):
        comment_votes_stream = ArticleCommentVotes(self.client, self.config)

        for comment in self.get_objects(article_id):
            comment['article_id'] = article_id
            zendesk_metrics.capture('article_comment')
            self.count += 1
            yield (self.stream, comment)

            if comment_votes_stream.is_selected():
                try:
                    for vote in comment_votes_stream.sync(article_id, comment['id']):
                        yield vote
                except http.ZendeskNotFound:
                    LOGGER.warning("Unable to retrieve votes for article comment (article: %s, comment: %s), not found",
                                   article_id, comment['id'])

    def check_access(self):
        pass


class ArticleVotes(Stream):
    name = "article_votes"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    count = 0
    endpoint = 'https://{}.zendesk.com/api/v2/help_center/articles/{}/votes'
    item_key = 'votes'

    def get_objects(self, article_id):
        url = self.endpoint.format(self.config['subdomain'], article_id)
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, article_id):
        for vote in self.get_objects(article_id):
            zendesk_metrics.capture('article_vote')
            self.count += 1
            yield (self.stream, vote)

    def check_access(self):
        pass


class ArticleCommentVotes(Stream):
    name = "article_comment_votes"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    count = 0
    endpoint = 'https://{}.zendesk.com/api/v2/help_center/articles/{}/comments/{}/votes'
    item_key = 'votes'

    def get_objects(self, article_id, comment_id):
        url = self.endpoint.format(self.config['subdomain'], article_id, comment_id)
        pages = http.get_offset_based(url, self.config['access_token'], self.request_timeout)
        for page in pages:
            yield from page.get(self.item_key, [])

    def sync(self, article_id, comment_id):
        for vote in self.get_objects(article_id, comment_id):
            zendesk_metrics.capture('article_comment_vote')
            self.count += 1
            yield (self.stream, vote)

    def check_access(self):
        pass


class Categories(CursorBasedStream):
    name = "categories"
    replication_method = "FULL_TABLE"
    endpoint = 'https://{}.zendesk.com/api/v2/help_center/categories'
    item_key = 'categories'

    def sync(self, state):
        categories = self.get_objects()
        for category in categories:
            yield (self.stream, category)


class Sections(CursorBasedStream):
    name = "sections"
    replication_method = "FULL_TABLE"
    endpoint = 'https://{}.zendesk.com/api/v2/help_center/sections'
    item_key = 'sections'

    def sync(self, state):
        sections = self.get_objects()
        for section in sections:
            yield (self.stream, section)
