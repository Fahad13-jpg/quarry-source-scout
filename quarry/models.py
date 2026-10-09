from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from . import __version__

STATUSES = frozenset({"valid", "redeemed", "expired", "invalid", "unknown"})


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("Naive timestamps are not accepted")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def timestamp(value: Any) -> datetime | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = (datetime.fromtimestamp(value, timezone.utc) if isinstance(value, (int, float))
                  else datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        return result.astimezone(timezone.utc) if result.tzinfo else None
    except (ValueError, OverflowError, OSError):
        return None


@dataclass(frozen=True)
class Referral:
    referral_url: str
    canonical_url: str
    referral_code: str
    raw_url: str


@dataclass
class SourceEvent:
    source_platform: str
    source_name: str
    source_item_id: str
    source_url: str
    content: Any
    published_at: datetime | None
    collector_used: str
    detected_at: datetime = field(default_factory=utcnow)
    revision: str = ""
    edited_at: datetime | None = None
    is_backfill: bool = False
    deleted: bool = False
    collector_version: str = __version__
    source_author: str | None = None

    @property
    def source_key(self) -> str:
        return f"{self.source_platform}:{self.source_name}"


@dataclass(frozen=True)
class ValidationResult:
    status: str
    reason: str = ""

    def __post_init__(self):
        if self.status not in STATUSES:
            raise ValueError("Unrecognized validator status")
        if not isinstance(self.reason, str):
            raise ValueError("Validator reason must be text")


@dataclass(frozen=True)
class Candidate:
    occurrence_id: int
    referral_code: str
    referral_url: str
    canonical_url: str
    source_platform: str
    source_name: str
    source_url: str
    is_backfill: bool
