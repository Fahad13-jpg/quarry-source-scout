import hashlib
import json
import logging
import sqlite3
from pathlib import Path
from .extract import extract
from .models import Candidate, iso, timestamp, utcnow


class Store:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript((Path(__file__).parent / 'migrations/001_initial.sql').read_text())
        for migration in sorted((Path(__file__).parent / 'migrations').glob('*.sql')):
            version = int(migration.name.split('_')[0])
            if not self.db.execute('SELECT 1 FROM schema_migrations WHERE version=?', (version,)).fetchone():
                self.db.executescript('BEGIN;\n' + migration.read_text() + '\nCOMMIT;')
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO metadata VALUES('experiment_start',?)", (iso(utcnow()),))
        self.start = timestamp(self.db.execute("SELECT value FROM metadata WHERE key='experiment_start'").fetchone()[0])

    def recover(self):
        with self.db:
            for table in ('jobs', 'outbox'):
                self.db.execute(f"UPDATE {table} SET state='pending' WHERE state='processing'")

    def checkpoint(self, key, default=None):
        row = self.db.execute('SELECT value FROM checkpoints WHERE collector_key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def _checkpoint(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO checkpoints VALUES(?,?)', (key, json.dumps(value)))

    def save_checkpoint(self, key, value):
        with self.db:
            self._checkpoint(key, value)

    def diagnostic(self, key, kind, detail):
        with self.db:
            self.db.execute('INSERT INTO diagnostics(collector_key,recorded_at,kind,detail) VALUES(?,?,?,?)',
                            (key, iso(utcnow()), kind, detail))

    def stats(self, key, value):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO collector_stats VALUES(?,?,?)', (key, iso(utcnow()), json.dumps(value)))

    def ingest(self, event, checkpoint=None):
        refs = extract(event.content)
        revision = event.revision or hashlib.sha256(json.dumps(event.content, sort_keys=True, ensure_ascii=True).encode()).hexdigest()
        event_key = json.dumps([event.source_key, event.source_item_id, revision])
        extracted = iso(utcnow())
        event_time = event.edited_at or event.published_at
        backfill = event.is_backfill or event_time is None or event_time < self.start
        inserted = []
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO sources VALUES(?,?,?)',
                            (event.source_key, event.source_platform, event.source_name))
            for ref in refs:
                if self.db.execute('SELECT 1 FROM occurrences WHERE event_key=? AND referral_url=?', (event_key, ref.referral_url)).fetchone():
                    continue
                existing = self.db.execute('SELECT 1 FROM referral_links WHERE referral_code=?', (ref.referral_code,)).fetchone()
                # An edit repeating the same URL is not a new occurrence; new variants remain auditable.
                if self.db.execute('SELECT 1 FROM occurrences WHERE source_key=? AND source_item_id=? AND referral_url=?',
                                   (event.source_key, event.source_item_id, ref.referral_url)).fetchone():
                    continue
                row = self.db.execute('''INSERT INTO occurrences(event_key,referral_url,raw_url,canonical_url,referral_code,
                    source_key,source_platform,source_name,source_item_id,source_url,published_at,edited_at,detected_at,
                    extracted_at,collector_used,collector_version,duplicate,is_backfill,source_author,discovered_at,validator_reason)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (event_key, ref.referral_url, ref.raw_url, ref.canonical_url, ref.referral_code, event.source_key,
                     event.source_platform, event.source_name, event.source_item_id, event.source_url, iso(event.published_at),
                     iso(event.edited_at), iso(event.detected_at), extracted, event.collector_used, event.collector_version,
                     int(bool(existing)), int(backfill), event.source_author, iso(event.detected_at), 'validator_not_connected'))
                oid = row.lastrowid
                inserted.append(oid)
                if not existing:
                    self.db.execute('INSERT INTO referral_links VALUES(?,?,?,?,?)',
                                    (ref.referral_code, ref.canonical_url, oid, iso(event.detected_at), event.source_key))
                    self.db.execute('INSERT INTO jobs(referral_code,occurrence_id,due_at) VALUES(?,?,?)',
                                    (ref.referral_code, oid, extracted))
                else:
                    result = self.db.execute('SELECT status,reason FROM validation_results WHERE referral_code=? ORDER BY id DESC LIMIT 1', (ref.referral_code,)).fetchone()
                    if result:
                        self.db.execute('UPDATE occurrences SET validator_status=?,validator_reason=? WHERE id=?', (*result, oid))
            if checkpoint:
                self._checkpoint(*checkpoint)
        for oid in inserted:
            row = self.db.execute('SELECT * FROM occurrences WHERE id=?', (oid,)).fetchone()
            if row['duplicate']:
                logging.debug('[DUPLICATE] %s %s', event.source_key, row['referral_code'])
            else:
                latency = (event.detected_at-event_time).total_seconds() if event_time else None
                latency = f'{latency:.3f}s' if latency is not None and latency >= 0 else 'unavailable'
                logging.info('[NEW] %s%s\nCode: %s\nURL: %s\nPublished: %s\nDetected: %s\nLatency: %s\nSource: %s',
                    event.source_key, ' [BACKFILL]' if backfill else '', row['referral_code'], row['referral_url'],
                    iso(event.published_at), iso(event.detected_at), latency, event.source_url)
        return inserted

    def claim(self, table='jobs'):
        assert table in ('jobs', 'outbox')
        with self.db:
            row = self.db.execute(f"SELECT occurrence_id FROM {table} WHERE state='pending' AND due_at<=? ORDER BY due_at LIMIT 1", (iso(utcnow()),)).fetchone()
            if not row:
                return None
            self.db.execute(f"UPDATE {table} SET state='processing',attempts=attempts+1 WHERE occurrence_id=?", (row[0],))
        item = dict(self.db.execute('SELECT * FROM occurrences WHERE id=?', (row[0],)).fetchone())
        return Candidate(**{key: item['id'] if key == 'occurrence_id' else item[key] for key in Candidate.__dataclass_fields__})

    def finish(self, candidate, result, started):
        checked = iso(utcnow())
        with self.db:
            self.db.execute('INSERT INTO validation_results(referral_code,occurrence_id,started_at,checked_at,status,reason) VALUES(?,?,?,?,?,?)',
                            (candidate.referral_code, candidate.occurrence_id, iso(started), checked, result.status, result.reason))
            self.db.execute('UPDATE occurrences SET validator_status=?,validator_reason=? WHERE referral_code=?', (result.status, result.reason, candidate.referral_code))
            self.db.execute('UPDATE occurrences SET validation_started_at=?,validated_at=? WHERE id=?', (iso(started), checked, candidate.occurrence_id))
            self.db.execute("UPDATE jobs SET state='done' WHERE referral_code=?", (candidate.referral_code,))
            if result.status == 'valid' and not candidate.is_backfill:
                self.db.execute('INSERT OR IGNORE INTO outbox(referral_code,occurrence_id,due_at) VALUES(?,?,?)',
                                (candidate.referral_code, candidate.occurrence_id, checked))

    def close(self):
        self.db.close()
