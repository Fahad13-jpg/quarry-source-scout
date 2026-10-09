# Verification

The complete 41-test suite passed before this source publication, using CPython 3.12 on
Windows. Tests cover extraction, case-sensitive deduplication, cross-source/mirror identity,
restart recovery, migrations, deferred validation, slow-validator isolation, independent
Reddit comment polling, missing credentials, reconnects and report/export commands.

Earlier bounded live checks on 2026-10-06 received 1,083 and 4,229 Bluesky records, including
418 and 1,673 replies respectively. The second run resumed at the exact saved inclusive
cursor. No referral candidate appeared in either window. These are historical connectivity
checks, not evidence of current uptime or daily referral yield. Raw logs/data are not published.

NodeSeek RSS returned HTTP 200 and 20 items. Three samples over roughly 61 seconds showed
one new topic in the final sample. Descriptions were empty or short excerpts (0–158 characters),
without full-content or reply entries. Ordinary URLs appeared, but no Claude referral was
observed, so survival of actual referral URLs in this feed remains unverified. The forum
robots endpoint returned 403; no attempt was made to bypass it or retrieve private endpoints.

Reddit live access has not been verified: approved OAuth credentials are still required.
The client validator is unavailable. All actual candidates must remain unknown until it
is connected. No referral was redeemed and no external notification was sent in testing.

The public stream dependency is `websockets`, declared in pyproject.toml. The local test
environment used 15.0.1. Windows pip temporary-directory permissions required installing
the public pure-Python wheel into the local virtual environment after SHA-256 verification
against PyPI metadata. Normal environments should use the documented pip setup.

SQLite synchronous commits occasionally produced asyncio slow-callback warnings on the
test machine. Sustained throughput is not benchmarked. Long outages may exceed source
retention. NodeSeek reply coverage, Reddit approval and real validation remain limitations.
