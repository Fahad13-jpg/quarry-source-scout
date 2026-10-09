CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS sources(source_key TEXT PRIMARY KEY, platform TEXT, name TEXT);
CREATE TABLE IF NOT EXISTS checkpoints(collector_key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source_items(
 source_key TEXT, item_id TEXT, revision TEXT, first_seen_at TEXT, last_seen_at TEXT,
 PRIMARY KEY(source_key,item_id));
CREATE TABLE IF NOT EXISTS referral_links(
 referral_code TEXT PRIMARY KEY COLLATE BINARY, canonical_url TEXT NOT NULL,
 first_occurrence_id INTEGER, first_seen_at TEXT NOT NULL, first_source TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS occurrences(
 id INTEGER PRIMARY KEY, event_key TEXT NOT NULL, referral_url TEXT NOT NULL,
 raw_url TEXT NOT NULL, canonical_url TEXT NOT NULL, referral_code TEXT NOT NULL COLLATE BINARY,
 source_key TEXT NOT NULL, source_platform TEXT NOT NULL, source_name TEXT NOT NULL,
 source_item_id TEXT NOT NULL, source_url TEXT NOT NULL, published_at TEXT, edited_at TEXT,
 detected_at TEXT NOT NULL, extracted_at TEXT NOT NULL, validation_started_at TEXT,
 validated_at TEXT, forwarded_at TEXT, collector_used TEXT, collector_version TEXT,
 duplicate INTEGER NOT NULL, is_backfill INTEGER NOT NULL,
 validator_status TEXT NOT NULL DEFAULT 'unknown', validator_reason TEXT NOT NULL DEFAULT 'pending',
 UNIQUE(event_key,referral_url));
CREATE INDEX IF NOT EXISTS occurrences_source_time ON occurrences(source_key,detected_at);
CREATE INDEX IF NOT EXISTS occurrences_code ON occurrences(referral_code);
CREATE TABLE IF NOT EXISTS jobs(
 referral_code TEXT PRIMARY KEY COLLATE BINARY, occurrence_id INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
 due_at TEXT NOT NULL, last_error TEXT);
CREATE TABLE IF NOT EXISTS validation_results(
 id INTEGER PRIMARY KEY, referral_code TEXT NOT NULL, occurrence_id INTEGER NOT NULL,
 started_at TEXT NOT NULL, checked_at TEXT NOT NULL, status TEXT NOT NULL, reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outbox(
 referral_code TEXT PRIMARY KEY COLLATE BINARY, occurrence_id INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
 due_at TEXT NOT NULL, last_error TEXT);
CREATE TABLE IF NOT EXISTS collector_runs(
 id INTEGER PRIMARY KEY, collector_key TEXT, started_at TEXT NOT NULL, ended_at TEXT,
 status TEXT, reason TEXT);
CREATE TABLE IF NOT EXISTS health_intervals(
 source_key TEXT, started_at TEXT NOT NULL, ended_at TEXT NOT NULL, healthy INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS costs(
 id INTEGER PRIMARY KEY, source_key TEXT NOT NULL, incurred_at TEXT NOT NULL,
 amount_usd REAL NOT NULL CHECK(amount_usd>=0), note TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS diagnostics(
 id INTEGER PRIMARY KEY, collector_key TEXT, recorded_at TEXT, kind TEXT, detail TEXT);
INSERT OR IGNORE INTO schema_migrations VALUES(1);
