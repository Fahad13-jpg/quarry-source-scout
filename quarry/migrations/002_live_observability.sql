ALTER TABLE occurrences ADD COLUMN source_author TEXT;
ALTER TABLE occurrences ADD COLUMN discovered_at TEXT;
ALTER TABLE occurrences ADD COLUMN status_at_discovery TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE occurrences ADD COLUMN reason_at_discovery TEXT NOT NULL DEFAULT 'validator_not_connected';
UPDATE occurrences SET discovered_at=detected_at;
CREATE TABLE collector_stats(collector_key TEXT PRIMARY KEY, updated_at TEXT NOT NULL, value TEXT NOT NULL);
CREATE INDEX occurrences_item_url ON occurrences(source_key,source_item_id,referral_url);
INSERT INTO schema_migrations VALUES(2);
