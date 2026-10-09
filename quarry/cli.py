import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import tomllib
from pathlib import Path
from .collectors import Reddit, NodeSeek, Bluesky
from .http import HTTP
from .metrics import report
from .models import iso, utcnow
from .pipeline import ClientAdapter, Pipeline, StubValidator
from .store import Store


class Lock:
    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.file = open(self.path, 'a+b')
        self.file.seek(0)
        self.file.write(b'0')
        self.file.flush()
        self.file.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise RuntimeError('Another Scout owns this database') from None
        return self

    def __exit__(self, *args):
        self.file.close()


def load_env():
    if Path('.env').exists():
        for line in Path('.env').read_text().splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                key, value = line.split('=', 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


async def run(store, config, args):
    http, client_http = HTTP(4), HTTP(2)
    try:
        validation_enabled = os.getenv('VALIDATION_ENABLED', 'false').lower() == 'true'
        endpoint = os.getenv('VALIDATOR_ENDPOINT') if validation_enabled else None
        validator = ClientAdapter(client_http, endpoint, os.getenv('VALIDATOR_TOKEN', '')) if endpoint else StubValidator()
        forward_endpoint = os.getenv('EXISTING_PIPELINE_ENDPOINT') if endpoint else None
        forwarder = ClientAdapter(client_http, forward_endpoint, os.getenv('EXISTING_PIPELINE_TOKEN', '')) if forward_endpoint else None
        collectors = []
        for name, cls in [('bluesky', Bluesky), ('reddit', Reddit), ('nodeseek', NodeSeek)]:
            if config.get(name, {}).get('enabled', False):
                collector = cls(store, http, config[name], args.mode)
                collectors.append(collector)
                with store.db:
                    for source in collector.sources:
                        platform, source_name = source.split(':',1)
                        store.db.execute('INSERT OR IGNORE INTO sources VALUES(?,?,?)', (source,platform,source_name))
        logging.info('Validation %s; candidates are persisted before any validation', 'connected' if endpoint else 'not connected (unknown only)')
        if not collectors:
            raise ValueError('Enable at least one collector in scout.toml')
        await Pipeline(store, validator, forwarder, config.get('workers', 2), config.get('queue_size', 64)).run(collectors, args.duration)
    finally:
        http.close()
        client_http.close()


def main():
    load_env()
    parser = argparse.ArgumentParser(description='Quarry source measurement scout')
    parser.add_argument('--config', default='scout.toml')
    parser.add_argument('--database', default=None)
    sub = parser.add_subparsers(dest='command', required=True)
    start = sub.add_parser('run')
    start.add_argument('--mode', choices=['live', 'backfill'], default='live')
    start.add_argument('--duration', type=float)
    daily = sub.add_parser('report')
    daily.add_argument('--date', default=utcnow().date().isoformat())
    daily.add_argument('--timezone', default='UTC')
    daily.add_argument('--format', choices=['table', 'json', 'csv'], default='table')
    sub.add_parser('export-occurrences')
    sub.add_parser('init-db')
    sub.add_parser('retry-unknown')
    cost = sub.add_parser('add-cost')
    cost.add_argument('source')
    cost.add_argument('amount', type=float)
    cost.add_argument('--note', required=True)
    sub.add_parser('diagnostics')
    sub.add_parser('status')
    sub.add_parser('check-config')
    args = parser.parse_args()
    config = tomllib.loads(Path(args.config).read_text()) if Path(args.config).exists() else {}
    database = args.database or os.getenv('DATABASE_URL') or config.get('database', 'data/scout.sqlite')
    if database.startswith('sqlite:///'):
        database = database[10:]
    elif '://' in database:
        parser.error('Scout supports SQLite; no existing PostgreSQL was found')
    store = Store(database)
    logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))
    try:
        if args.command == 'run':
            with Lock(database + '.lock'):
                try:
                    asyncio.run(run(store, config, args))
                except KeyboardInterrupt:
                    logging.info('Scout stopped; committed candidates and checkpoints retained')
        elif args.command == 'report':
            rows = report(store, args.date, args.timezone)
            if args.format == 'json':
                print(json.dumps(rows, indent=2))
            elif args.format == 'csv':
                if rows:
                    writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
            else:
                fields = ['source', 'links_found', 'unique_links', 'globally_new_links', 'unknown', 'duplicate_count', 'median_detection_latency', 'p95_detection_latency', 'healthy_hours', 'errors', 'valid_rate']
                print(' | '.join(fields))
                for row in rows:
                    print(' | '.join(str(round(row[f], 3)) if isinstance(row[f], float) else str(row[f]) for f in fields))
        elif args.command in ('export-occurrences', 'diagnostics', 'status'):
            table = {'export-occurrences':'occurrences','diagnostics':'diagnostics','status':'collector_stats'}[args.command]
            for row in store.db.execute(f'SELECT * FROM {table}'):
                print(json.dumps(dict(row)))
        elif args.command == 'check-config':
            import importlib.util
            print(json.dumps({'bluesky_dependency_ready':importlib.util.find_spec('websockets') is not None,
                              'reddit_enabled':config.get('reddit',{}).get('enabled',False),
                              'reddit_missing':Reddit.configuration_errors(),
                              'validation_enabled':os.getenv('VALIDATION_ENABLED','false').lower()=='true'},indent=2))
        elif args.command == 'retry-unknown':
            with Lock(database + '.lock'), store.db:
                store.db.execute("UPDATE jobs SET state='pending',due_at=? WHERE referral_code IN (SELECT referral_code FROM occurrences WHERE validator_status='unknown')", (iso(utcnow()),))
        elif args.command == 'add-cost':
            with store.db:
                store.db.execute('INSERT INTO costs(source_key,incurred_at,amount_usd,note) VALUES(?,?,?,?)', (args.source, iso(utcnow()), args.amount, args.note))
    finally:
        store.close()


if __name__ == '__main__':
    main()
