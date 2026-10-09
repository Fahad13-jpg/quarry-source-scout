from datetime import datetime, timedelta, timezone
from statistics import median
from zoneinfo import ZoneInfo
from .models import timestamp, iso, utcnow


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * p
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def report(store, day, zone='UTC'):
    tz = timezone.utc if zone == 'UTC' else ZoneInfo(zone)
    start = datetime.fromisoformat(day).replace(tzinfo=tz)
    end = start + timedelta(days=1)
    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    rows = []
    sources = {r[0] for r in store.db.execute('SELECT source_key FROM sources UNION SELECT source_key FROM health_intervals')}
    for source in sorted(sources):
        occurrences = [dict(r) for r in store.db.execute('SELECT * FROM occurrences WHERE source_key=? AND detected_at>=? AND detected_at<? AND is_backfill=0', (source, iso(start), iso(end)))]
        codes = {r['referral_code'] for r in occurrences}
        counts = {status: len({r['referral_code'] for r in occurrences if r['validator_status'] == status}) for status in ('valid', 'redeemed', 'expired', 'invalid', 'unknown')}
        fresh = sum(not r['duplicate'] for r in occurrences)
        stages = {key: [] for key in ('published_to_detected', 'detected_to_extracted', 'extracted_to_validated', 'validated_to_forwarded')}
        for row in occurrences:
            # A source-supplied edit time is the earliest known introduction time for an edit.
            pairs = [(row.get('edited_at') or row['published_at'], row['detected_at']),
                     (row['detected_at'], row['extracted_at']), (row['extracted_at'], row['validated_at']),
                     (row['validated_at'], row['forwarded_at'])]
            for key, (left, right) in zip(stages, pairs):
                if left and right:
                    seconds = (timestamp(right) - timestamp(left)).total_seconds()
                    if seconds >= 0:
                        stages[key].append(seconds)
        healthy = 0
        for interval in store.db.execute('SELECT * FROM health_intervals WHERE source_key=? AND healthy=1', (source,)):
            healthy += max(0, (min(end, timestamp(interval['ended_at'])) - max(start, timestamp(interval['started_at']))).total_seconds())
        cost = store.db.execute('SELECT SUM(amount_usd) FROM costs WHERE source_key=? AND incurred_at>=? AND incurred_at<?', (source, iso(start), iso(end))).fetchone()[0]
        platform = source.split(':',1)[0]
        errors = store.db.execute("SELECT COUNT(*) FROM diagnostics WHERE kind='collector_error' AND recorded_at>=? AND recorded_at<? AND (collector_key=? OR collector_key=? OR collector_key LIKE ?)",
                                 (iso(start),iso(end),platform,source,source+':%')).fetchone()[0]
        row = dict(source=source, links_found=len(occurrences), unique_links=len(codes), globally_new_links=fresh,
                   globally_new_valid_links=sum(not r['duplicate'] and r['validator_status']=='valid' for r in occurrences),
                   valid_links=counts['valid'], **{k:v for k,v in counts.items() if k != 'valid'},
                   duplicate_count=sum(r['duplicate'] for r in occurrences),
                   valid_rate=counts['valid']/len(codes) if codes and counts['unknown'] < len(codes) else None,
                   errors=errors,
                   duplicates_pct=100*sum(r['duplicate'] for r in occurrences)/len(occurrences) if occurrences else None,
                   timestamp_coverage=len(stages['published_to_detected'])/len(occurrences) if occurrences else None,
                   healthy_hours=healthy/3600,
                   wall_time_hours=max(0,(min(end,utcnow())-max(start,store.start)).total_seconds())/3600,
                   cost=cost)
        for key, values in stages.items():
            row[key + '_median_seconds'] = median(values) if values else None
            row[key + '_p95_seconds'] = percentile(values, .95)
            row[key + '_samples'] = len(values)
        row['median_detection_latency'] = row['published_to_detected_median_seconds']
        row['p95_detection_latency'] = row['published_to_detected_p95_seconds']
        rows.append(row)
    return rows
