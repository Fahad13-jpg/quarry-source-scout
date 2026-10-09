import asyncio
import json
import subprocess
import sys
import os
import sqlite3
from unittest.mock import patch
import shutil
import uuid
import unittest
from datetime import timedelta
from pathlib import Path
from quarry.collectors import Bluesky, NodeSeek, Reddit
from quarry.extract import extract
from quarry.metrics import report
from quarry.models import SourceEvent, ValidationResult, utcnow, iso
from quarry.pipeline import Pipeline, StubValidator
from quarry.store import Store

URL = 'https://claude.ai/referral/Ab_C-d'


class TestDirectory:
    """Use ordinary workspace directories, compatible with Windows sandbox ACLs."""
    def __init__(self):
        self.path = Path.cwd() / 'data' / ('test-' + uuid.uuid4().hex)
        self.path.mkdir(parents=True)
        self.name = str(self.path)
    def cleanup(self):
        assert self.path.resolve().is_relative_to((Path.cwd() / 'data').resolve())
        shutil.rmtree(self.path)


class ExtractionTests(unittest.TestCase):
    def test_simple(self): self.assertEqual(extract(URL)[0].referral_code, 'Ab_C-d')
    def test_query(self):
        ref = extract(URL + '?utm_source=a&x=2')[0]
        self.assertEqual(ref.referral_url, URL + '?utm_source=a&x=2')
        self.assertEqual(ref.canonical_url, URL)
    def test_json_escaped(self): self.assertEqual(extract(URL.replace('/', '\\/'))[0].referral_code, 'Ab_C-d')
    def test_markdown(self): self.assertEqual(extract('[go](' + URL + ').')[0].referral_url, URL)
    def test_html(self): self.assertEqual(extract('<a href="' + URL + '?a=1&amp;b=2">x</a>')[0].referral_url, URL + '?a=1&b=2')
    def test_underscore(self): self.assertEqual(extract(URL.replace('_', '\\_'))[0].referral_code, 'Ab_C-d')
    def test_hyphen(self): self.assertEqual(extract('https://claude.ai/referral/a-b')[0].referral_code, 'a-b')
    def test_case_sensitive(self): self.assertNotEqual(extract(URL)[0].referral_code, extract(URL.lower())[0].referral_code)
    def test_variants(self): self.assertEqual(len(extract([URL, URL + '?a=1'])), 2)
    def test_lookalikes(self):
        for host in ('claude.ai.evil.test', 'evilclaude.ai', 'claude.ai@evil.test', 'claude.ai:444'):
            self.assertEqual(extract('https://' + host + '/referral/ABC'), [])
    def test_extra_path(self): self.assertEqual(extract(URL + '/extra'), [])
    def test_facets_and_embeds(self):
        self.assertEqual(len(extract({'facets':[{'features':[{'uri':URL}]}], 'embed':{'external':{'uri':URL + '?x=1'}}})), 2)
    def test_raw_spelling(self): self.assertIn('\\_', extract(URL.replace('_', '\\_'))[0].raw_url)
    def test_query_punctuation_preserved(self): self.assertEqual(extract(URL+'?value=wow!')[0].referral_url, URL+'?value=wow!')


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = TestDirectory()
        self.path = Path(self.temp.name)/'scout.sqlite'
        self.store = Store(self.path)
    def tearDown(self):
        self.store.close()
        self.temp.cleanup()
    def event(self, platform='reddit', item='1', content=URL, **kwargs):
        return SourceEvent(platform, 'test', item, 'https://source.test/' + item, content, kwargs.pop('published_at', utcnow()), 'fixture', **kwargs)
    def test_cross_source_duplicate(self):
        self.store.ingest(self.event())
        self.store.ingest(self.event('nodeseek'))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM referral_links').fetchone()[0], 1)
        self.assertEqual([r[0] for r in self.store.db.execute('SELECT duplicate FROM occurrences ORDER BY id')], [0,1])
    def test_forum_mirror_global_identity_without_mirror_collector(self):
        self.store.ingest(self.event('nodeseek'))
        self.store.ingest(self.event('telegram', content=URL+'?mirror=1'))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM referral_links').fetchone()[0],1)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM occurrences').fetchone()[0],2)
    def test_restart_replay(self):
        event = self.event()
        self.store.ingest(event, ('cursor', 17))
        self.store.claim()
        self.store.close()
        self.store = Store(self.path)
        self.store.recover()
        self.assertEqual(self.store.ingest(event), [])
        self.assertEqual(self.store.checkpoint('cursor'), 17)
        self.assertIsNotNone(self.store.claim())
    def test_edit_introduction(self):
        old = utcnow() - timedelta(days=7)
        self.store.ingest(self.event(content='no link', published_at=old))
        self.store.ingest(self.event(published_at=old, edited_at=utcnow()))
        self.assertEqual(self.store.db.execute('SELECT is_backfill FROM occurrences').fetchone()[0], 0)
        self.store.ingest(self.event(published_at=old, edited_at=utcnow(), content=URL + ' some edit'))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM occurrences').fetchone()[0], 1)
    def test_backfill_excluded(self):
        self.store.ingest(self.event(is_backfill=True))
        rows = report(self.store, utcnow().date().isoformat())
        self.assertEqual(rows[0]['globally_new_links'], 0)
    def test_variants_one_job(self):
        self.store.ingest(self.event(content=[URL, URL+'?q=1']))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM occurrences').fetchone()[0], 2)
    def test_case_two_codes(self):
        self.store.ingest(self.event(content=[URL, URL.lower()]))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM referral_links').fetchone()[0], 2)
    def test_future_and_missing_times(self):
        self.store.ingest(self.event(published_at=None))
        self.assertEqual(self.store.db.execute('SELECT is_backfill FROM occurrences').fetchone()[0], 1)
    def test_stages_and_cached_duplicates(self):
        self.store.ingest(self.event())
        c = self.store.claim()
        self.store.finish(c, ValidationResult('valid', 'fixture'), utcnow())
        self.store.ingest(self.event('nodeseek'))
        rows = {r['source']:r for r in report(self.store,utcnow().date().isoformat())}
        self.assertEqual(rows['nodeseek:test']['valid_links'], 1)
        self.assertEqual(rows['nodeseek:test']['extracted_to_validated_samples'], 0)
    def test_bluesky_reply_facet_checkpoint(self):
        collector = Bluesky(self.store, None, {}, 'live')
        collector.frame({'payload': {'seq':42, 'did':'did:plc:test', 'rkey':'abc', 'collection':'app.bsky.feed.post',
            'operation':'create', 'record':{'createdAt':iso(utcnow()), 'text':'reply', 'reply':{}, 'facets':[{'uri':URL}]}}})
        self.assertEqual(self.store.checkpoint('bluesky'), 42)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM occurrences').fetchone()[0],1)
    def test_atomic_checkpoint_rollback(self):
        self.store.db.execute("CREATE TRIGGER fail_job BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(Exception): self.store.ingest(self.event(), ('x', 1))
        self.assertIsNone(self.store.checkpoint('x'))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM occurrences').fetchone()[0],0)
    def test_cli_json_report_and_export(self):
        self.store.ingest(self.event())
        command=[sys.executable,'-m','quarry','--database',str(self.path)]
        result=subprocess.run(command+['report','--date',utcnow().date().isoformat(),'--format','json'],capture_output=True,text=True,check=True)
        self.assertEqual(json.loads(result.stdout)[0]['globally_new_links'],1)
        exported=subprocess.run(command+['export-occurrences'],capture_output=True,text=True,check=True)
        self.assertEqual(json.loads(exported.stdout)['referral_code'],'Ab_C-d')
    def test_backfill_valid_never_queued_for_forwarding(self):
        self.store.ingest(self.event(is_backfill=True))
        candidate=self.store.claim()
        self.store.finish(candidate,ValidationResult('valid'),utcnow())
        self.assertIsNone(self.store.claim('outbox'))
    def test_outbox_recovers_after_crash(self):
        self.store.ingest(self.event())
        candidate=self.store.claim()
        self.store.finish(candidate,ValidationResult('valid'),utcnow())
        self.assertEqual(self.store.claim('outbox').referral_code,candidate.referral_code)
        self.store.close()
        self.store=Store(self.path)
        self.store.recover()
        self.assertEqual(self.store.claim('outbox').referral_code,candidate.referral_code)

    def test_unknown_rate_and_discovery_metadata(self):
        event=self.event(source_author='did:plc:public-author')
        self.store.ingest(event)
        row=self.store.db.execute('SELECT * FROM occurrences').fetchone()
        self.assertEqual(row['source_author'],'did:plc:public-author')
        self.assertEqual(row['discovered_at'],row['detected_at'])
        self.assertEqual(row['validator_reason'],'validator_not_connected')
        self.assertIsNone(report(self.store,utcnow().date().isoformat())[0]['valid_rate'])
        candidate=self.store.claim()
        self.store.finish(candidate,ValidationResult('expired','later client check'),utcnow())
        later=self.store.db.execute('SELECT * FROM occurrences').fetchone()
        self.assertEqual(later['discovered_at'],row['discovered_at'])
        self.assertEqual(later['status_at_discovery'],'unknown')
        self.assertEqual(later['validator_status'],'expired')
        self.assertEqual(later['referral_url'],row['referral_url'])

    def test_new_log_once_for_global_code(self):
        with self.assertLogs(level='INFO') as logs:
            self.store.ingest(self.event())
            self.store.ingest(self.event('nodeseek'))
            self.store.ingest(self.event())
        self.assertEqual(sum('[NEW]' in msg for msg in logs.output),1)

    def test_stream_replay_and_author(self):
        collector=Bluesky(self.store,None,{},'live')
        frame={'payload':{'seq':100,'did':'did:plc:author','rkey':'abc','operation':'create',
            'collection':'app.bsky.feed.post','record':{'text':URL,'createdAt':iso(utcnow()),'reply':{'parent':{}}}}}
        collector.frame(frame)
        collector.frame(frame)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM occurrences').fetchone()[0],1)
        self.assertEqual(self.store.db.execute('SELECT source_author FROM occurrences').fetchone()[0],'did:plc:author')
        self.assertEqual(self.store.checkpoint('bluesky'),100)

    def test_cursor_advisory_preserves_checkpoint(self):
        self.store.save_checkpoint('bluesky',100)
        collector=Bluesky(self.store,None,{},'live')
        with self.assertRaises(RuntimeError):
            collector.frame({'payload':{'$type':'network.bsky.jetstream.subscribeEvents#info','name':'FutureCursor'}})
        self.assertEqual(self.store.checkpoint('bluesky'),100)

    def test_version_one_database_upgrade_preserves_occurrence(self):
        path=Path(self.temp.name)/'legacy.sqlite'
        db=sqlite3.connect(path)
        db.executescript((Path('quarry/migrations/001_initial.sql')).read_text())
        now=iso(utcnow())
        db.execute('''INSERT INTO occurrences(event_key,referral_url,raw_url,canonical_url,referral_code,source_key,
            source_platform,source_name,source_item_id,source_url,detected_at,extracted_at,duplicate,is_backfill)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',('old',URL,URL,URL,'Ab_C-d','reddit:test','reddit','test','1','source',now,now,0,0))
        db.commit(); db.close()
        upgraded=Store(path)
        try:
            self.assertEqual(upgraded.db.execute('SELECT discovered_at FROM occurrences').fetchone()[0],now)
            self.assertEqual(upgraded.db.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0],3)
        finally:
            upgraded.close()


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TestDirectory()
        self.store = Store(Path(self.temp.name)/'db')
    async def asyncTearDown(self):
        self.store.close()
        self.temp.cleanup()
    async def test_slow_validator_does_not_block_ingestion(self):
        gate = asyncio.Event()
        started = asyncio.Event()
        class Slow:
            async def validate(self, candidate):
                started.set()
                await gate.wait()
                return ValidationResult('valid')
        pipeline = Pipeline(self.store, Slow(), workers=1, queue_size=1)
        task = asyncio.create_task(pipeline.run([], 1))
        self.store.ingest(SourceEvent('test','fast','0','fixture',URL+'0',utcnow(),'fixture'))
        await asyncio.wait_for(started.wait(), .5)
        for i in range(1,30):
            self.store.ingest(SourceEvent('test','fast',str(i),'fixture',URL+str(i),utcnow(),'fixture'))
        await task
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0],30)
        self.store.recover()
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM jobs WHERE state='pending'").fetchone()[0],30)
    async def test_failed_collector_isolated(self):
        class Bad:
            key='bad'; sources=['bad:test']; healthy=False
            async def run(self): raise RuntimeError('fixture')
        store=self.store
        class Good:
            key='good'; sources=['good:test']; healthy=True
            async def run(self):
                store.ingest(SourceEvent('good','test','1','fixture',URL,utcnow(),'fixture'))
                await asyncio.Future()
        await Pipeline(store).run([Bad(),Good()], .25)
        self.assertEqual(store.db.execute('SELECT validator_reason FROM occurrences').fetchone()[0], 'validator_not_connected')
    async def test_reddit_posts_and_comments_and_old_thread(self):
        collector = Reddit(self.store, None, {}, 'live')
        calls=[]
        async def fake(path, params):
            calls.append(path)
            child={'kind':'t1','data':{'name':path,'subreddit':'claude','created_utc':utcnow().timestamp(),'body':URL}}
            listing={'data':{'children':[child], 'after':None}}
            return [listing] if path.startswith('/comments/') else listing
        collector.get=fake
        await collector.listing('claude','new')
        await collector.listing('claude','comments')
        await collector.thread('older')
        self.assertEqual(calls,['/r/claude/new','/r/claude/comments','/comments/older'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM occurrences').fetchone()[0],3)
    async def test_rss_baseline_and_live(self):
        class FakeHTTP:
            counter=0
            async def request(self,*args,**kwargs):
                self.counter+=1
                from email.utils import format_datetime
                body=f'<rss><channel><item><guid>{self.counter}</guid><link>https://www.nodeseek.com/post-{self.counter}-1</link><pubDate>{format_datetime(utcnow())}</pubDate><description>{URL}{self.counter}</description></item></channel></rss>'
                return 200,{},body.encode()
        collector=NodeSeek(self.store,FakeHTTP(),{},'live')
        await collector.poll()
        await asyncio.sleep(1.05) # RSS timestamps have second precision.
        await collector.poll()
        self.assertEqual([r[0] for r in self.store.db.execute('SELECT is_backfill FROM occurrences ORDER BY id')],[1,0])

    async def test_disconnected_validator_leaves_durable_pending_job(self):
        self.store.ingest(SourceEvent('test','live','1','source',URL,utcnow(),'fixture'))
        await Pipeline(self.store).run([], .05)
        row=self.store.db.execute('SELECT * FROM occurrences').fetchone()
        self.assertEqual(row['validator_reason'],'validator_not_connected')
        self.assertIsNone(row['validated_at'])
        self.assertIsNone(row['validation_started_at'])
        self.assertEqual(self.store.db.execute('SELECT state FROM jobs').fetchone()[0],'pending')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM validation_results').fetchone()[0],0)

    async def test_reddit_missing_configuration_never_requests(self):
        class NoHTTP:
            async def request(self,*args,**kwargs):
                raise AssertionError('No network should be attempted')
        with patch.dict(os.environ,{},clear=True):
            collector=Reddit(self.store,NoHTTP(),{},'live')
            self.assertEqual(len(collector.configuration_errors()),3)
            with self.assertRaises(PermissionError):
                await collector.get('/r/claude/comments',{})

    async def test_reddit_comments_independent_of_slow_submission(self):
        collector=Reddit(self.store,None,{'subreddits':['claude']},'live')
        collector.configuration_errors=lambda:[]
        comment_seen=asyncio.Event()
        async def listing(sub,kind):
            if kind=='new':
                await asyncio.Future()
            comment_seen.set()
        collector.listing=listing
        task=asyncio.create_task(collector.run())
        try:
            await asyncio.wait_for(comment_seen.wait(),1)
        finally:
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)

    async def test_automatic_reconnect(self):
        reconnected=asyncio.Event()
        class Connection:
            key='bluesky'; sources=['bluesky:global']; healthy=False; attempts=0
            async def run(self):
                self.attempts+=1
                if self.attempts==1:
                    raise ConnectionError('simulated connection loss')
                reconnected.set()
                await asyncio.Future()
        collector=Connection()
        task=asyncio.create_task(Pipeline(self.store).supervise(collector))
        try:
            await asyncio.wait_for(reconnected.wait(),4)
            self.assertEqual(collector.attempts,2)
        finally:
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)


if __name__ == '__main__': unittest.main()
