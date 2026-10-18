"""Exact rebuild of message and reaction counts from Discord's own history.

Live counting can drift from what Discord shows: the bot may have been offline, a message may be deleted long
after it was counted, and reactions may predate the bot. A rebuild fixes that for whole, completed days. It
replaces the message-derived counters of those days with counts computed from the channel history, so the result
matches what Discord's search shows.

* Idempotent: running it again over the same days gives the same numbers.
* Applied per batch of days in one synchronous step, so live events never interleave with it.
* Voice time cannot be recovered from history and is left untouched.
* Reactions are attributed to the day of the *message* they were left on (Discord does not expose when a
  reaction was added).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, NamedTuple

from .collector import Collector, inc
from .config import Config
from .timeutil import day_key

# Per-member, per-day fields that come from messages and reactions (voice "v"/"vs" is not touched).
# "sm/sd/sb/sr/sv" are the scored-channel mirrors, "sh" the hours active and "pp" the conversation partners.
USER_FIELDS = ("m", "d", "b", "r", "i", "c", "rx", "rg", "sm", "sd", "sb", "sr", "sv", "sh", "pp")
# Per-day server fields that come from messages.
SERVER_FIELDS = ("m", "bm", "d", "b", "im", "h", "ch")


class Msg(NamedTuple):
    ts: int
    uid: int
    cid: int  # channel id used for counting (threads count toward their parent)
    fp: str | None
    reply: bool
    name: str | None
    is_bot: bool
    reactors: tuple[int, ...] = ()  # distinct human reactors, excluding the author
    scored: bool = True  # posted in a channel that counts toward scoring
    partners: tuple[int, ...] = ()  # distinct humans they replied to or mentioned, excluding themselves


def last_complete_days(today: date, n: int) -> list[str]:
    """The last ``n`` complete local days, oldest first, ending yesterday."""
    return [(today - timedelta(days=i)).isoformat() for i in range(n, 0, -1)]


def verified_days(data: dict[str, Any], days: list[str]) -> list[str]:
    stamps = data["meta"].get("recounted", {})
    return [d for d in days if d in stamps]


def verification(data: dict[str, Any], days: list[str], today: date) -> dict[str, Any]:
    """How much of ``days`` has been rebuilt from Discord. Only completed days (before today) can be verified."""
    complete = [d for d in days if d < today.isoformat()]
    stamps = data["meta"].get("recounted", {})
    done = [d for d in complete if d in stamps]
    return {
        "complete_days": len(complete),
        "total_days": len(days),
        "verified_days": len(done),
        "all_verified": len(done) == len(days) and len(complete) == len(days),
        "oldest_verified_at": min((stamps[d] for d in done), default=None),
    }


REACTION_FIELDS = ("rx", "rg")


def _clear(data: dict[str, Any], days: list[str], reactions: bool = True) -> None:
    fields = USER_FIELDS if reactions else tuple(f for f in USER_FIELDS if f not in REACTION_FIELDS)
    for day in days:
        rec = data["days"].get(day)
        if rec is not None:
            users = rec["u"]
            for uid in list(users):
                u = users[uid]
                for f in fields:
                    u.pop(f, None)
                if not u:
                    del users[uid]
            if not users:
                del data["days"][day]
        srv = data["server"].get(day)
        if srv is not None:
            for f in SERVER_FIELDS:
                srv.pop(f, None)


def rebuild_days(data: dict[str, Any], cfg: Config, msgs: list[Msg], days: list[str], now: int,
                 reactions: bool = True) -> dict[str, int]:
    """Replace the message and reaction counters of ``days`` with counts computed from ``msgs``.

    Synchronous by design: it must never be split across an ``await``. ``msgs`` must contain every qualifying
    message posted on those days (completeness is the caller's job); anything outside ``days`` is ignored.
    """
    day_set = set(days)
    members = data["members"]
    excluded_before = {uid for uid, rec in members.items() if rec.get("e")}
    last_event = data["meta"].get("last_event")

    _clear(data, days, reactions)

    col = Collector(data, cfg)  # fresh duplicate/burst tracker; shares the dataset
    counted = 0
    for m in sorted(msgs, key=lambda x: (x.ts, x.uid, x.cid)):
        day = day_key(m.ts, cfg.tz)
        if day not in day_set:
            continue
        col.record_message(m.uid, m.cid, m.ts, name=m.name, is_bot=m.is_bot, fingerprint=m.fp,
                           reply_to_other=m.reply, excluded_role=False, scored=m.scored, partners=m.partners)
        counted += 1
        if reactions and cfg.reactions_enabled and m.reactors and not (m.is_bot and not cfg.count_bots):
            inc(col.user_day(day, m.uid), "rx", len(m.reactors))
            for reactor in m.reactors:
                inc(col.user_day(day, reactor), "rg")

    # History does not tell us a member's current roles, so keep the exclusion flags exactly as they were.
    for uid, rec in members.items():
        if uid in excluded_before:
            rec["e"] = 1
        else:
            rec.pop("e", None)
    if last_event is not None:
        data["meta"]["last_event"] = last_event

    stamps = data["meta"].setdefault("recounted", {})
    for day in days:
        stamps[day] = now
    return {"days": len(days), "messages": counted}
