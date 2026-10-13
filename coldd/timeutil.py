"""Day and week boundaries in the configured timezone.

All day keys are ISO dates ("2026-10-13") in the configured timezone. Timestamps
stored in data.json are integer Unix seconds (UTC).
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def now_ts() -> int:
    return int(time.time())


def to_local(ts: float, tz: ZoneInfo) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz)


def day_key(ts: float, tz: ZoneInfo) -> str:
    return to_local(ts, tz).date().isoformat()


def local_hour(ts: float, tz: ZoneInfo) -> int:
    return to_local(ts, tz).hour


def parse_day(key: str) -> date:
    return date.fromisoformat(key)


def day_start_ts(d: date, tz: ZoneInfo) -> int:
    """Unix timestamp of local midnight at the start of ``d`` (DST-safe)."""
    return int(datetime(d.year, d.month, d.day, tzinfo=tz).timestamp())


def week_start(d: date, week_start_day: int) -> date:
    """First day of the week containing ``d``. ``week_start_day``: 0=Monday..6=Sunday."""
    return d - timedelta(days=(d.weekday() - week_start_day) % 7)


def week_days(start: date) -> list[str]:
    return [(start + timedelta(days=i)).isoformat() for i in range(7)]


def week_bounds_ts(start: date, tz: ZoneInfo) -> tuple[int, int]:
    return day_start_ts(start, tz), day_start_ts(start + timedelta(days=7), tz)


def days_back(end: date, n: int) -> list[str]:
    """The ``n`` day keys ending with (and including) ``end``, oldest first."""
    return [(end - timedelta(days=n - 1 - i)).isoformat() for i in range(n)]


def fmt_ts(ts: float | None, tz: ZoneInfo) -> str:
    if not ts:
        return "never"
    return to_local(ts, tz).strftime("%Y-%m-%d %H:%M %Z")
