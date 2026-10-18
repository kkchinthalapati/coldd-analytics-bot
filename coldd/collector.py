"""Incremental aggregation of Discord events into compact daily counters.

Nothing in this module stores message text. Content is reduced to a short hash
held in memory for a few minutes to detect duplicate spam, then discarded.

data.json layout (see README "Data format"):

  days[day]["u"][user_id] = {m, d, b, r, rx, v, vs, i, c:{channel_id: n}}   (member detail, ~90 days)
  server[day] = {m, bm, d, b, j, lv, bn, h:[24], ch:{channel_id: n}, cov, v, im, del, a?, chu?}  (~180 days)

Keys are omitted while zero to keep the file small.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sys
from collections import OrderedDict, deque
from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

from .config import Config
from .timeutil import day_key, local_hour, now_ts

log = logging.getLogger("coldd.collector")

MAX_GAPS = 500
F_DUP, F_BURST, F_REPLY, F_IMPORTED = 1, 2, 4, 8
_WS = re.compile(r"\s+")


def content_fingerprint(text: str) -> str | None:
    """Short, non-reversible fingerprint of normalised message text (never persisted)."""
    norm = _WS.sub(" ", text.strip().lower())
    if not norm:
        return None
    return hashlib.blake2b(norm.encode("utf-8"), digest_size=8).hexdigest()


def inc(rec: dict[str, Any], key: str, n: int | float = 1) -> None:
    value = rec.get(key, 0) + n
    if value <= 0:
        rec.pop(key, None)
    else:
        rec[key] = value


class SpamTracker:
    """Bounded per-user memory of recent message times and fingerprints."""

    PER_USER = 32

    def __init__(self, dup_window: int, burst_window: int, burst_threshold: int):
        self.dup_window = dup_window
        self.burst_window = burst_window
        self.burst_threshold = burst_threshold
        self._recent: dict[int, deque[tuple[int, str | None]]] = {}

    def check(self, uid: int, ts: int, fingerprint: str | None) -> tuple[bool, bool]:
        """Return (is_duplicate, is_burst) and remember this message."""
        q = self._recent.get(uid)
        if q is None:
            q = self._recent[uid] = deque(maxlen=self.PER_USER)
        horizon = ts - max(self.dup_window, self.burst_window)
        while q and q[0][0] < horizon:
            q.popleft()
        dup = fingerprint is not None and any(
            fp == fingerprint and ts - t <= self.dup_window for t, fp in q
        )
        burst = sum(1 for t, _ in q if ts - t <= self.burst_window) >= self.burst_threshold
        q.append((ts, fingerprint))
        return dup, burst

    def sweep(self, ts: int) -> None:
        horizon = ts - max(self.dup_window, self.burst_window)
        for uid in [u for u, q in self._recent.items() if not q or q[-1][0] < horizon]:
            del self._recent[uid]

    def __len__(self) -> int:
        return len(self._recent)


class Collector:
    def __init__(self, data: dict[str, Any], cfg: Config):
        self.data = data
        self.cfg = cfg
        self.tz = cfg.tz
        self.dirty = False
        self.spam = SpamTracker(cfg.duplicate_window_seconds, cfg.burst_window_seconds, cfg.burst_threshold)
        # In-memory only, bounded by count and age, never persisted:
        # message_id -> [author_id, day, channel_id, hour, flags, reactor_ids|None, ts]
        self.recent: OrderedDict[int, list[Any]] = OrderedDict()
        self._voice_present: set[int] = set()
        self.events_seen = 0

    # helpers ----------------------------------------------------------------

    def server_day(self, day: str) -> dict[str, Any]:
        rec = self.data["server"].get(day)
        if rec is None:
            rec = self.data["server"][day] = {}
        return rec

    def user_day(self, day: str, uid: int) -> dict[str, Any]:
        d = self.data["days"].get(day)
        if d is None:
            d = self.data["days"][day] = {"u": {}}
        key = sys.intern(str(uid))  # one shared string per member across all days
        rec = d["u"].get(key)
        if rec is None:
            rec = d["u"][key] = {}
        return rec

    def touch_member(self, uid: int, ts: int, name: str | None = None, *, active: bool = False,
                     is_bot: bool | None = None, excluded: bool | None = None) -> dict[str, Any]:
        key = sys.intern(str(uid))
        rec = self.data["members"].get(key)
        if rec is None:
            rec = self.data["members"][key] = {"f": ts}
        if name and rec.get("n") != name:
            rec["n"] = name[:64]
        if active:
            rec.setdefault("f", ts)
            if ts > rec.get("l", 0):
                rec["l"] = ts
        if is_bot is not None:
            if is_bot:
                rec["b"] = 1
            else:
                rec.pop("b", None)
        if excluded is not None:
            if excluded:
                rec["e"] = 1
            else:
                rec.pop("e", None)
        return rec

    def channel_allowed(self, *channel_ids: int | None) -> bool:
        ids = {c for c in channel_ids if c}
        if ids & self.cfg.exclude_channel_ids:
            return False
        if self.cfg.include_channel_ids and not (ids & self.cfg.include_channel_ids):
            return False
        return True

    def note_channel(self, cid: int, name: str | None) -> None:
        if not name:
            return
        rec = self.data["channels"].setdefault(str(cid), {})
        if rec.get("n") != name:
            rec["n"] = name[:100]
            self.dirty = True

    def _remember(self, mid: int, entry: list[Any], ts: int) -> None:
        if self.cfg.recent_message_cache <= 0:
            return
        self.recent[mid] = entry
        while len(self.recent) > self.cfg.recent_message_cache:
            self.recent.popitem(last=False)

    def expire_recent(self, ts: int) -> None:
        horizon = ts - self.cfg.recent_message_hours * 3600
        while self.recent:
            mid, entry = next(iter(self.recent.items()))
            if entry[6] >= horizon:
                break
            self.recent.popitem(last=False)

    # messages ---------------------------------------------------------------

    def record_message(
        self,
        uid: int,
        channel_id: int,
        ts: int,
        *,
        name: str | None = None,
        is_bot: bool = False,
        fingerprint: str | None = None,
        reply_to_other: bool = False,
        excluded_role: bool = False,
        message_id: int | None = None,
        imported: bool = False,
        flags_override: tuple[bool, bool] | None = None,
    ) -> int:
        """Count one qualifying message. Returns the flag bits applied."""
        day = day_key(ts, self.tz)
        hour = local_hour(ts, self.tz)
        srv = self.server_day(day)
        self.events_seen += 1
        self.dirty = True
        if not imported:
            self.data["meta"]["last_event"] = ts

        if is_bot and not self.cfg.count_bots:
            inc(srv, "bm")
            return 0

        if flags_override is not None:
            dup, burst = flags_override
        else:
            dup, burst = self.spam.check(uid, ts, fingerprint)
        flags = (F_DUP if dup else 0) | (F_BURST if burst else 0) | (F_REPLY if reply_to_other else 0)
        if imported:
            flags |= F_IMPORTED

        rec = self.user_day(day, uid)
        inc(rec, "m")
        if dup:
            inc(rec, "d")
            inc(srv, "d")
        if burst:
            inc(rec, "b")
            inc(srv, "b")
        if reply_to_other:
            inc(rec, "r")
        if imported:
            inc(rec, "i")
            inc(srv, "im")
        chans = rec.setdefault("c", {})
        cid = str(channel_id)
        chans[cid] = chans.get(cid, 0) + 1

        inc(srv, "m")
        hours = srv.get("h")
        if hours is None:
            hours = srv["h"] = [0] * 24
        hours[hour] += 1
        sch = srv.setdefault("ch", {})
        sch[cid] = sch.get(cid, 0) + 1

        self.touch_member(uid, ts, name, active=True, is_bot=is_bot or None, excluded=excluded_role)
        if message_id is not None and not imported:
            self._remember(message_id, [uid, day, channel_id, hour, flags, None, ts], ts)
        return flags

    def record_delete(self, message_id: int) -> bool:
        """Undo the counters of a recently counted message. Returns True if it was known."""
        entry = self.recent.pop(message_id, None)
        if entry is None:
            return False
        uid, day, channel_id, hour, flags, reactors, _ts = entry
        self.dirty = True
        srv = self.data["server"].get(day)
        urec = self.data["days"].get(day, {}).get("u", {}).get(str(uid))
        cid = str(channel_id)
        if urec is not None:
            inc(urec, "m", -1)
            if flags & F_DUP:
                inc(urec, "d", -1)
            if flags & F_BURST:
                inc(urec, "b", -1)
            if flags & F_REPLY:
                inc(urec, "r", -1)
            chans = urec.get("c", {})
            if chans.get(cid, 0) > 1:
                chans[cid] -= 1
            else:
                chans.pop(cid, None)
            if not chans:
                urec.pop("c", None)
        if srv is not None:
            inc(srv, "m", -1)
            if flags & F_DUP:
                inc(srv, "d", -1)
            if flags & F_BURST:
                inc(srv, "b", -1)
            if "h" in srv and srv["h"][hour] > 0:
                srv["h"][hour] -= 1
            sch = srv.get("ch", {})
            if sch.get(cid, 0) > 1:
                sch[cid] -= 1
            else:
                sch.pop(cid, None)
            inc(srv, "del")
        # Reactions the deleted message received are withdrawn from its author.
        if reactors and urec is not None:
            inc(urec, "rx", -len(reactors))
        return True

    def record_reaction(self, message_id: int, reactor_id: int, ts: int, added: bool) -> bool:
        """Credit (or withdraw) a reaction from another member on a recent message."""
        if not self.cfg.reactions_enabled:
            return False
        entry = self.recent.get(message_id)
        author = entry[0] if entry is not None else None
        if author == reactor_id:
            return False  # self-reactions count for nothing
        reactors: set[int] | None = entry[5] if entry is not None else None

        # Reactions GIVEN: credited to the reactor on the day of the reaction. One per member per cached message;
        # for uncached messages every add counts, and the daily cap in scoring/ranking limits spam.
        day = day_key(ts, self.cfg.tz)
        if added:
            if reactors is None or reactor_id not in reactors:
                inc(self.user_day(day, reactor_id), "rg")
                self.dirty = True
        else:
            rec = self.data["days"].get(day, {}).get("u", {}).get(str(reactor_id))
            if rec is not None:
                inc(rec, "rg", -1)
                self.dirty = True

        if entry is None:
            return added
        # Reactions RECEIVED: credited to the author for messages in the recent cache.
        if added:
            if reactors is None:
                reactors = entry[5] = set()
            if reactor_id in reactors:
                return False  # one credit per reacting member per message
            reactors.add(reactor_id)
            inc(self.user_day(entry[1], entry[0]), "rx")
        else:
            if not reactors or reactor_id not in reactors:
                return False
            reactors.discard(reactor_id)
            urec = self.data["days"].get(entry[1], {}).get("u", {}).get(str(author))
            if urec is not None:
                inc(urec, "rx", -1)
        self.dirty = True
        return True

    # membership -------------------------------------------------------------

    def record_join(self, uid: int, ts: int, name: str | None = None, is_bot: bool = False) -> None:
        srv = self.server_day(day_key(ts, self.tz))
        inc(srv, "jb" if is_bot else "j")
        rec = self.touch_member(uid, ts, name, is_bot=is_bot)
        rec["j"] = ts
        rec.pop("x", None)
        self.dirty = True

    def record_leave(self, uid: int, ts: int, is_bot: bool = False) -> None:
        srv = self.server_day(day_key(ts, self.tz))
        inc(srv, "lb" if is_bot else "lv")
        rec = self.data["members"].get(str(uid))
        if rec is not None:
            rec["x"] = ts
        self.dirty = True

    def record_ban(self, ts: int) -> None:
        inc(self.server_day(day_key(ts, self.tz)), "bn")
        self.dirty = True

    # voice ------------------------------------------------------------------

    def credit_voice(self, present: dict[int, str | None], seconds: int, ts: int) -> None:
        """Credit ``seconds`` of observed voice participation to each member in ``present``.

        Called on a fixed timer with the members currently in qualifying voice
        channels. A new session is counted when a member appears who was absent
        at the previous sample.
        """
        if not self.cfg.voice_enabled:
            return
        day = day_key(ts, self.tz)
        if present:
            srv = self.server_day(day)
            for uid, name in present.items():
                rec = self.user_day(day, uid)
                inc(rec, "v", seconds)
                if uid not in self._voice_present:
                    inc(rec, "vs")
                self.touch_member(uid, ts, name, active=True)
            inc(srv, "v", seconds * len(present))
            self.dirty = True
        self._voice_present = set(present)

    # coverage ---------------------------------------------------------------

    def heartbeat(self, ts: int, connected: bool, interval: int) -> None:
        """Account observed (connected) time per day and record gaps in collection."""
        meta = self.data["meta"]
        if not connected:
            return
        if meta.get("collection_start") is None:
            meta["collection_start"] = ts
        last = meta.get("last_heartbeat")
        if last:
            elapsed = ts - last
            if elapsed > max(3 * interval, 180):
                meta["gaps"].append([last, ts])
                del meta["gaps"][:-MAX_GAPS]
                log.warning("Collection gap detected: %d minutes without observation.", elapsed // 60)
            elif elapsed > 0:
                inc(self.server_day(day_key(ts, self.tz)), "cov", elapsed)
        meta["last_heartbeat"] = ts
        self.dirty = True

    def mark_reconnected(self, offline_since: int | None, ts: int, min_gap: int = 60) -> None:
        """Record the offline period (if longer than ``min_gap``) and restart coverage accounting."""
        meta = self.data["meta"]
        if meta.get("collection_start") is None:
            meta["collection_start"] = ts
        if offline_since and ts - offline_since > min_gap:
            meta["gaps"].append([offline_since, ts])
            del meta["gaps"][:-MAX_GAPS]
            log.warning("Collection gap recorded: %d minutes offline.", (ts - offline_since) // 60)
        meta["last_heartbeat"] = ts
        self.dirty = True

    def note_disconnected_since(self, ts: int) -> None:
        """Called on disconnect: stop crediting coverage until the next heartbeat after reconnect."""
        meta = self.data["meta"]
        if meta.get("last_heartbeat") and ts > meta["last_heartbeat"]:
            elapsed = ts - meta["last_heartbeat"]
            if elapsed <= 180:
                inc(self.server_day(day_key(ts, self.tz)), "cov", elapsed)
            meta["last_heartbeat"] = ts
        self.dirty = True

    # retention --------------------------------------------------------------

    def prune(self, today: date, force: bool = False) -> dict[str, int]:
        """Drop data older than the retention windows. Runs at most once per day."""
        meta = self.data["meta"]
        today_key = today.isoformat()
        if not force and meta.get("last_prune_day") == today_key:
            return {}
        removed = {"member_days": 0, "server_days": 0, "awards": 0, "members": 0, "channels": 0}

        member_cutoff = (today - timedelta(days=self.cfg.member_retention_days)).isoformat()
        for day in sorted(k for k in self.data["days"] if k < member_cutoff):
            users = self.data["days"].pop(day)["u"]
            srv = self.server_day(day)
            # Keep a compact snapshot so long-range server reports still have unique counts.
            srv["a"] = len(users)
            chu: dict[str, int] = {}
            for urec in users.values():
                for cid in urec.get("c", {}):
                    chu[cid] = chu.get(cid, 0) + 1
            if chu:
                srv["chu"] = chu
            removed["member_days"] += 1

        server_cutoff = (today - timedelta(days=self.cfg.server_retention_days)).isoformat()
        for day in [k for k in self.data["server"] if k < server_cutoff]:
            del self.data["server"][day]
            removed["server_days"] += 1

        award_cutoff = (today - timedelta(days=self.cfg.award_retention_days)).isoformat()
        for section in ("awards", "nominations"):
            for week in [k for k in self.data[section] if k < award_cutoff]:
                del self.data[section][week]
                if section == "awards":
                    removed["awards"] += 1

        winners = {str(a.get("winner")) for a in self.data["awards"].values() if a.get("winner")}
        member_cutoff_ts = now_ts() - self.cfg.inactive_member_retention_days * 86400
        for uid in [u for u, rec in self.data["members"].items()
                    if u not in winners and max(rec.get("l", 0), rec.get("j", 0), rec.get("x", 0), rec.get("f", 0)) < member_cutoff_ts]:
            del self.data["members"][uid]
            removed["members"] += 1

        live_channels: set[str] = set()
        for srv in self.data["server"].values():
            live_channels.update(srv.get("ch", {}))
        for cid in [c for c in self.data["channels"] if c not in live_channels]:
            del self.data["channels"][cid]
            removed["channels"] += 1

        gap_cutoff = now_ts() - self.cfg.server_retention_days * 86400
        meta["gaps"] = [g for g in meta.get("gaps", []) if g[1] >= gap_cutoff]
        meta["last_prune_day"] = today_key
        self.dirty = True
        if any(removed.values()):
            log.info("Retention pruning removed %s", removed)
        return removed

    def forget_member(self, uid: int) -> int:
        """Delete a member's stored statistics (privacy request). Returns records removed."""
        key = str(uid)
        n = 0
        if self.data["members"].pop(key, None) is not None:
            n += 1
        for d in self.data["days"].values():
            if d["u"].pop(key, None) is not None:
                n += 1
        for noms in self.data["nominations"].values():
            if noms.pop(key, None) is not None:
                n += 1
        for award in self.data["awards"].values():
            award["shortlist"] = [row for row in award.get("shortlist", []) if str(row[0]) != key]
            if str(award.get("winner")) == key:
                award["winner"] = "forgotten"
                n += 1
        for mid in [m for m, e in self.recent.items() if e[0] == uid]:
            del self.recent[mid]
        self.dirty = True
        return n

    def unreadable_channels(self, ids: Iterable[int]) -> None:
        self.data["meta"]["unreadable_channels"] = sorted(str(i) for i in ids)
        self.dirty = True
