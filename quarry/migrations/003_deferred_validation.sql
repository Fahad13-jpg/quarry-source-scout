-- Earlier Scout versions timestamped stub responses as validation. Retain their audit
-- rows, but don't present an unavailable validator as an actual validation measurement.
UPDATE occurrences SET validation_started_at=NULL, validated_at=NULL
WHERE validator_status='unknown' AND validator_reason='validator_not_connected';
UPDATE occurrences SET validator_reason='validator_not_connected'
WHERE validator_status='unknown' AND validator_reason='pending';
UPDATE jobs SET state='pending'
WHERE state='done' AND referral_code IN (
 SELECT referral_code FROM occurrences
 WHERE validator_status='unknown' AND validator_reason='validator_not_connected'
);
INSERT INTO schema_migrations VALUES(3);
