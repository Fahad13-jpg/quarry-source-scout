import asyncio
import json
import logging
import os
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode
from .models import SourceEvent, timestamp, utcnow
from .extract import extract


class Collector:
    healthy = False

    def __init__(self, store, http, config, mode):
        self.store, self.http, self.config, self.mode = store, http, config, mode
        self.counters = {'started_at': utcnow().isoformat(), 'connections': 0, 'records': 0, 'replies': 0, 'occurrences': 0}

    def source_healthy(self, source):
        return self.healthy

    def save_stats(self):
        self.store.stats(self.key, self.counters)

    def emit(self, event, checkpoint=None):
        event.is_backfill |= self.mode == 'backfill'
        result = self.store.ingest(event, checkpoint)
        self.counters['occurrences'] += len(result)
        return result


class NodeSeek(Collector):
    key = 'nodeseek'
    sources = ['nodeseek:topics']

    @staticmethod
    def parse(body):
        root = ET.fromstring(body)
        if root.tag != 'rss':
            raise ValueError('Expected RSS')
        result = []
        for item in root.findall('./channel/item'):
            try:
                published = parsedate_to_datetime(item.findtext('pubDate', ''))
                if not published.tzinfo:
                    published = None
            except (ValueError, TypeError):
                published = None
            link = item.findtext('link', '')
            result.append(SourceEvent('nodeseek', 'topics', item.findtext('guid') or link,
                link, [item.findtext('title', ''), item.findtext('description', ''),
                       item.findtext('{http://purl.org/rss/1.0/modules/content/}encoded', '')], published, 'official_rss',
                source_author=item.findtext('author') or item.findtext('{http://purl.org/dc/elements/1.1/}creator')))
        return result

    async def poll(self):
        state = self.store.checkpoint(self.key, {})
        headers = {}
        if state.get('etag'):
            headers['If-None-Match'] = state['etag']
        status, response_headers, body = await self.http.request('https://rss.nodeseek.com', headers=headers)
        if status == 304:
            self.healthy = True
            self.counters['not_modified'] = self.counters.get('not_modified', 0) + 1
            self.save_stats()
            return
        if status != 200:
            raise RuntimeError(f'RSS HTTP {status}')
        events = self.parse(body)
        self.counters['connections'] += 1
        self.counters['records'] += len(events)
        ids = [e.source_item_id for e in events]
        if state.get('anchor') and state['anchor'] not in ids:
            self.store.diagnostic(self.key, 'possible_feed_gap', 'Previous anchor absent from finite topic feed')
        for event in reversed(events):
            event.is_backfill = not state
            self.emit(event)
        self.store.save_checkpoint(self.key, {'anchor': ids[0] if ids else state.get('anchor'), 'etag': response_headers.get('ETag')})
        self.healthy = True
        self.counters['last_poll_at'] = utcnow().isoformat()
        self.counters['latest_item'] = ids[0] if ids else None
        self.save_stats()
        logging.info('nodeseek RSS received %s topic excerpts; replies not represented', len(events))

    async def run(self):
        self.store.diagnostic(self.key, 'coverage', 'RSS topic excerpts only; replies unverified and not collected')
        while True:
            await self.poll()
            await asyncio.sleep(max(15, self.config.get('poll_seconds', 30)))


class Reddit(Collector):
    key = 'reddit'

    def __init__(self, *args):
        super().__init__(*args)
        self.subs = self.config.get('subreddits', ['ClaudeCode', 'claude', 'referralcodes', 'AskVibecoders', 'claudeskills', 'ClaudeCowork'])
        self.sources = [f'reddit:{sub}' for sub in self.subs]
        self.last_request = 0
        self.rate_lock = asyncio.Lock()
        self.blocked_until = 0
        self.stream_health = {}

    @staticmethod
    def configuration_errors():
        missing = []
        if os.getenv('REDDIT_ACCESS_APPROVED', '').lower() != 'true':
            missing.append('REDDIT_ACCESS_APPROVED=true (only with actual approval)')
        if not os.getenv('REDDIT_ACCESS_TOKEN'):
            missing.append('REDDIT_ACCESS_TOKEN')
        if not os.getenv('REDDIT_USER_AGENT'):
            missing.append('REDDIT_USER_AGENT')
        return missing

    def source_healthy(self, source):
        sub = source.split(':',1)[1]
        return all(self.stream_health.get(f'reddit:{sub}:{kind}', False) for kind in ('new','comments'))

    async def get(self, path, params):
        if self.configuration_errors():
            raise PermissionError('Approved Reddit OAuth access required')
        interval = 60 / max(1, min(90, self.config.get('requests_per_minute', 30)))
        loop = asyncio.get_running_loop()
        async with self.rate_lock:
            await asyncio.sleep(max(0, max(self.last_request + interval, self.blocked_until) - loop.time()))
            self.last_request = loop.time()
        status, headers, body = await self.http.request('https://oauth.reddit.com' + path + '?' + urlencode(params), headers={
            'Authorization': 'Bearer ' + os.environ['REDDIT_ACCESS_TOKEN'],
            'User-Agent': os.getenv('REDDIT_USER_AGENT', 'QuarrySourceScout/0.1 (research)')})
        if status == 429:
            self.blocked_until = loop.time() + max(1, float(headers.get('Retry-After', 60)))
            raise RuntimeError('Reddit rate limited')
        if status != 200:
            raise RuntimeError(f'Reddit HTTP {status}')
        remaining = headers.get('x-ratelimit-remaining', headers.get('X-Ratelimit-Remaining'))
        if remaining is not None and float(remaining) < 1:
            self.blocked_until = loop.time() + max(1, float(headers.get('x-ratelimit-reset', 60)))
        return json.loads(body)

    def event(self, data, backfill=False):
        return SourceEvent('reddit', data.get('subreddit', 'unknown'), data['name'],
            'https://www.reddit.com' + data.get('permalink', ''),
            [data.get('title', ''), data.get('selftext', ''), data.get('body', ''), data.get('url', '')],
            timestamp(data.get('created_utc')), 'reddit_oauth', edited_at=timestamp(data.get('edited')),
            is_backfill=backfill, source_author=data.get('author'))

    async def listing(self, sub, kind):
        key = f'reddit:{sub}:{kind}'
        anchor = self.store.checkpoint(key)
        after, items, reached = None, [], False
        for _ in range(self.config.get('max_pages', 5)):
            params = {'limit': 100, 'raw_json': 1}
            if after:
                params['after'] = after
            data = (await self.get(f'/r/{sub}/{kind}', params))['data']
            children = data['children']
            items.extend(c['data'] for c in children)
            reached = any(c['data']['name'] == anchor for c in children)
            after = data.get('after')
            if reached or not after or anchor is None:
                break
        if anchor and not reached:
            self.store.diagnostic(key, 'listing_gap', 'Saved anchor not reached within listing budget')
        for item in reversed(items):
            self.emit(self.event(item, anchor is None))
        if items:
            self.store.save_checkpoint(key, items[0]['name'])

    async def thread(self, thread_id):
        key = 'reddit:thread:' + thread_id
        baseline = self.store.checkpoint(key) is None
        result = await self.get('/comments/' + thread_id, {'sort': 'new', 'limit': 500, 'raw_json': 1})
        def walk(children):
            for child in children:
                data = child['data']
                if child['kind'] == 'more':
                    self.store.diagnostic(key, 'comment_gap', 'Reddit more children omitted; thread coverage incomplete')
                    continue
                if data.get('name'):
                    self.emit(self.event(data, baseline))
                replies = data.get('replies')
                if isinstance(replies, dict):
                    walk(replies['data']['children'])
        for listing in result:
            walk(listing['data']['children'])
        self.store.save_checkpoint(key, True)

    async def run(self):
        missing = self.configuration_errors()
        if missing:
            logging.error('Reddit unavailable: provide %s', ', '.join(missing))
            raise PermissionError('Reddit configuration missing')
        async def poll(key, callback, *args):
            while True:
                try:
                    await callback(*args)
                    self.stream_health[key] = True
                    self.healthy = any(self.stream_health.values())
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.stream_health[key] = False
                    detail = str(error) if isinstance(error, RuntimeError) else type(error).__name__
                    self.store.diagnostic(key, 'collector_error', detail)
                    logging.warning('%s poll error: %s', key, detail)
                await asyncio.sleep(max(10, self.config.get('poll_seconds', 30)))
        tasks = [asyncio.create_task(poll(f'reddit:{sub}:{kind}', self.listing, sub, kind))
                 for sub in self.subs for kind in ('comments', 'new')]
        tasks += [asyncio.create_task(poll(f'reddit:thread:{thread}', self.thread, thread))
                  for thread in self.config.get('older_threads', [])]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


class Bluesky(Collector):
    key = 'bluesky'
    sources = ['bluesky:global']
    last_checkpoint_at = None

    def frame(self, message):
        payload = message.get('payload', message)
        if str(payload.get('$type','')).endswith('#info'):
            notice = payload.get('name', payload.get('info', 'unknown'))
            self.store.diagnostic(self.key, 'cursor_notice', str(notice))
            raise RuntimeError('Jetstream cursor advisory requires review; refusing a silent gap')
        if payload.get('collection') != 'app.bsky.feed.post':
            return
        cursor = payload.get('seq')
        if cursor is None:
            raise ValueError('Missing resumable Jetstream sequence')
        record = payload.get('record')
        self.counters['records'] += 1
        self.counters.setdefault('first_cursor', cursor)
        if record and record.get('reply'):
            self.counters['replies'] += 1
        self.counters['last_cursor'] = cursor
        self.counters['last_record_at'] = utcnow().isoformat()
        if record is None or not extract(record):
            now = utcnow()
            if self.last_checkpoint_at is None or (now - self.last_checkpoint_at).total_seconds() >= 1:
                self.store.save_checkpoint(self.key, cursor)
                self.last_checkpoint_at = now
                self.save_stats()
            return
        did, rkey = payload['did'], payload['rkey']
        event = SourceEvent('bluesky', 'global', f'at://{did}/app.bsky.feed.post/{rkey}',
            f'https://bsky.app/profile/{did}/post/{rkey}', record, timestamp(record.get('createdAt')),
            'jetstream_v2', revision=payload.get('cid', ''),
            edited_at=timestamp(payload.get('time')) if payload.get('operation') == 'update' else None,
            source_author=did)
        self.emit(event, (self.key, cursor))

    async def run(self):
        from websockets.asyncio.client import connect
        params = [('collections', 'app.bsky.feed.post'), ('kinds', 'commit')]
        cursor = self.store.checkpoint(self.key)
        self.counters['resume_cursor'] = cursor
        if cursor is not None:
            params.append(('cursor', str(cursor)))
        endpoint = 'wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents'
        async with connect(endpoint + '?' + urlencode(params), subprotocols=['xrpc.v1.json'],
                           max_queue=32, max_size=4_000_000, ping_interval=20, ping_timeout=20, open_timeout=20) as ws:
            self.healthy = True
            self.counters['connections'] += 1
            self.save_stats()
            logging.info('bluesky connected to official Jetstream; resume_cursor=%s', cursor)
            try:
                async for raw in ws:
                    message = json.loads(raw)
                    if message.get('$type') in ('error', 'network.bsky.jetstream.error'):
                        self.store.diagnostic(self.key, 'stream_error', str(message.get('error', 'unknown')))
                        raise RuntimeError('Jetstream server error; cursor retained')
                    self.frame(message)
                    if self.counters['records'] == 1 or self.counters['records'] % 1000 == 0:
                        logging.info('bluesky receiving: records=%s replies=%s occurrences=%s',
                                     self.counters['records'], self.counters['replies'], self.counters['occurrences'])
            finally:
                self.healthy = False
                self.save_stats()
