# Quarry Search Source Scout

A small Python monitoring tool being developed for a client to measure public Claude
referral-link discovery, source yield, duplicates and detection latency. It does not
promise a daily yield or infer referral validity.

## Scope and access

- **Bluesky:** official public Jetstream v2, including post/reply text, facets and embeds.
- **Reddit:** approved OAuth access only; independent submission and comment polling in
  r/ClaudeCode, r/claude, r/referralcodes, r/AskVibecoders, r/claudeskills and r/ClaudeCowork.
  Selected older threads can also be rechecked. Reddit is disabled by default pending access.
- **NodeSeek:** official RSS topic excerpts. Full topic bodies and replies are not collected.

No automatic posting, voting, comments, direct messages, private-community collection or
referral redemption. No Telegram collector is included. An optional adapter can feed an
existing client notification pipeline when real validation is explicitly enabled later.

## Setup

Requires Python 3.11+. From this repository directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -e .
Copy-Item .env.example .env
.\.venv\Scripts\python -m quarry check-config
.\.venv\Scripts\python -m quarry run --mode live
```

On Linux/macOS use `.venv/bin/python` and `cp .env.example .env`. Stop with Ctrl+C.
`--duration 60` makes a bounded run. Configuration lives in `scout.toml` and environment
variables; credentials must stay in the ignored `.env`, never in source control.

For Reddit, supply `REDDIT_ACCESS_APPROVED=true` only after obtaining actual approval,
`REDDIT_ACCESS_TOKEN` and an identifying `REDDIT_USER_AGENT`. Then enable `[reddit]` in
`scout.toml`. OAuth tokens must be renewed externally; no access-control workaround exists.

## Persistence and validation

Candidates are immediately stored as `unknown / validator_not_connected` in SQLite.
Leave `VALIDATION_ENABLED=false` for collection without a validator. Jobs remain durable
and pending; validation timestamps stay null until a real validation attempt occurs.

The case-sensitive referral code is the global identity. Original URL/query spelling,
source item/URL, public author identifier, publication/edit time and immutable discovery
time remain attached to occurrences. Full post bodies and user profiles are not stored.
Repeated reads and reconnect replay are idempotent. SQLite migrations upgrade existing
databases; pending jobs recover after restart. Only one Scout process may own a database.

When the client validator becomes available, configure `VALIDATOR_ENDPOINT`, optionally
`VALIDATOR_TOKEN`, and `VALIDATION_ENABLED=true`. The HTTPS JSON adapter accepts statuses
`valid`, `redeemed`, `expired`, `invalid`, `unknown`. It never opens referral landing pages
to infer validity. Later results are appended to validation history without replacing
original discovery metadata. See `quarry/pipeline.py` for the adapter contract.

## Reports and diagnostics

```powershell
python -m quarry report --format json
python -m quarry report --date 2026-10-10 --timezone UTC --format csv
python -m quarry status
python -m quarry diagnostics
python -m quarry export-occurrences
```

Run these with the virtual environment's Python or after activating it. `--database`
and `--config` precede the subcommand. Reports exclude startup backfill and group by
detection day. `valid_rate` is null while all results are unknown. Once results exist,
it is valid/all unique codes, including remaining unknowns in the denominator.

Publication/edit-to-detection, detection-to-extraction, extraction-to-validation and
validation-to-forwarding latencies are separate, with median/p95/sample counts. A later
source edit time is used as a proxy for new-link introduction time. Missing/future times
are not fabricated. Health observations do not guarantee complete source coverage.
Cost is manually recorded; null means unrecorded. New global codes log once after commit.

Initial polling snapshots and explicit `--mode backfill` discoveries never inflate fresh
yield. Codes first seen in backfill remain globally known. Polling gaps, omitted Reddit
`more` comments and cursor errors are diagnostic events. Jetstream resumes only within
the upstream lookback window; expired cursors do not silently reset to live.

## Tests

```powershell
python -m unittest discover -s tests -v
```

See [verification](VERIFICATION.md) and [architecture](ARCHITECTURE.md). This repository
contains source, fixtures and documentation only: no credentials, live exports or databases.
