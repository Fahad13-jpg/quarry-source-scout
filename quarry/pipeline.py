import asyncio
import json
import logging
from dataclasses import asdict
from datetime import timedelta
from typing import Protocol
from urllib.parse import urlsplit
from .models import Candidate, ValidationResult, iso, utcnow


class Validator(Protocol):
    async def validate(self, candidate: Candidate) -> ValidationResult: ...


class StubValidator:
    async def validate(self, candidate):
        return ValidationResult('unknown', 'validator_not_connected')


class ClientAdapter:
    def __init__(self, http, endpoint, token=''):
        if urlsplit(endpoint).scheme != 'https':
            raise ValueError('Client adapter endpoint must use HTTPS')
        self.http, self.endpoint, self.token = http, endpoint, token

    async def send(self, candidate):
        headers = {'Content-Type': 'application/json', 'Idempotency-Key': candidate.referral_code}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        return await self.http.request(self.endpoint, headers=headers, data=json.dumps(asdict(candidate)).encode())

    async def validate(self, candidate):
        status, _, body = await self.send(candidate)
        if status != 200:
            return ValidationResult('unknown', f'validator_http_{status}')
        result = json.loads(body)
        return ValidationResult(result['status'], result.get('reason', ''))


class Pipeline:
    def __init__(self, store, validator=None, forwarder=None, workers=2, queue_size=64):
        self.store, self.validator, self.forwarder = store, validator or StubValidator(), forwarder
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.workers = workers

    async def pump(self):
        while True:
            if isinstance(self.validator, StubValidator):
                # No validation happened: keep timestamps null and jobs durable for later.
                await asyncio.sleep(.5)
                continue
            if not self.queue.full():
                candidate = self.store.claim()
                if candidate:
                    await self.queue.put(candidate)
                    continue
            await asyncio.sleep(.1)

    async def worker(self):
        while True:
            candidate = await self.queue.get()
            started = utcnow()
            try:
                with self.store.db:
                    self.store.db.execute('UPDATE occurrences SET validation_started_at=? WHERE id=?',
                                          (iso(started), candidate.occurrence_id))
                try:
                    result = await asyncio.wait_for(self.validator.validate(candidate), 30)
                except Exception as error:
                    result = ValidationResult('unknown', f'validator_error:{type(error).__name__}')
                self.store.finish(candidate, result, started)
            finally:
                self.queue.task_done()

    async def forward(self):
        while True:
            candidate = self.store.claim('outbox') if self.forwarder else None
            if not candidate:
                await asyncio.sleep(.5)
                continue
            try:
                status, _, _ = await self.forwarder.send(candidate)
                if not 200 <= status < 300:
                    raise RuntimeError('forward_failed')
                with self.store.db:
                    self.store.db.execute("UPDATE outbox SET state='done' WHERE referral_code=?", (candidate.referral_code,))
                    self.store.db.execute('UPDATE occurrences SET forwarded_at=? WHERE id=?', (iso(utcnow()), candidate.occurrence_id))
            except Exception as error:
                with self.store.db:
                    self.store.db.execute("UPDATE outbox SET state='pending',due_at=?,last_error=? WHERE referral_code=?",
                        (iso(utcnow() + timedelta(seconds=60)), type(error).__name__, candidate.referral_code))

    async def supervise(self, collector):
        attempts = 0
        while True:
            try:
                logging.info('%s starting%s', collector.key, ' (reconnect)' if attempts else '')
                await collector.run()
                raise RuntimeError('collector_returned')
            except asyncio.CancelledError:
                raise
            except Exception as error:
                collector.healthy = False
                detail = type(error).__name__
                if isinstance(error, PermissionError):
                    detail += ': approved access/configuration missing; see .env.example'
                elif hasattr(error, 'response'):
                    detail += f': HTTP {getattr(error.response,"status_code","unknown")}'
                self.store.diagnostic(collector.key, 'collector_error', detail)
                attempts += 1
                delay = min(60, 2 ** min(attempts, 6))
                logging.warning('%s access/connection error (%s); reconnect in %ss', collector.key, detail, delay)
                await asyncio.sleep(delay)

    async def health(self, collectors):
        previous = utcnow()
        while True:
            await asyncio.sleep(5)
            now = utcnow()
            with self.store.db:
                for collector in collectors:
                    for source in collector.sources:
                        self.store.db.execute('INSERT INTO health_intervals VALUES(?,?,?,?)',
                            (source, iso(previous), iso(now), int(collector.source_healthy(source) if hasattr(collector, 'source_healthy') else collector.healthy)))
            previous = now

    async def run(self, collectors, duration=None):
        self.store.recover()
        tasks = [asyncio.create_task(self.pump()), asyncio.create_task(self.forward()), asyncio.create_task(self.health(collectors))]
        tasks += [asyncio.create_task(self.worker()) for _ in range(self.workers)]
        tasks += [asyncio.create_task(self.supervise(c)) for c in collectors]
        try:
            if duration is not None:
                tasks.append(asyncio.create_task(asyncio.sleep(duration)))
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
