"""Member of the Week scoring: deterministic, explainable, manual approval only.

Per eligible candidate, over the selected week (configured timezone):

  effective_messages = sum over days of min(messages - duplicates - burst_messages, DAILY_MESSAGE_CAP)
  active_days        = days with >= MIN_DAILY_MESSAGES_FOR_ACTIVE effective messages
  helpful_points     = sum of min(replies_to_others, DAILY_REPLY_CAP)
                       + 0.5 * sum of min(reactions_from_others, DAILY_REACTION_CAP)
                       + NOMINATION_WEIGHT * staff_nominations
  voice_minutes      = observed voice time with >= VOICE_MIN_MEMBERS humans present

Components (each 0..1, normalised against the best eligible candidate):

  participation = sqrt(effective_messages) / sqrt(max effective_messages)   (sqrt dampens raw volume)
  consistency   = active_days / days_elapsed_in_week
  helpfulness   = helpful_points / max helpful_points
  extra         = sqrt(voice_minutes) / sqrt(max voice_minutes)

score = 100 * sum(weight_i * component_i) / sum(weight_i over AVAILABLE components)

A component is "unavailable" for the whole week when no eligible candidate has
any data for it (for example voice disabled). Its weight is redistributed
proportionally across the remaining components, so nobody is penalised for a
metric the bot could not observe.
"""

from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

from .config import Config
from .timeutil import day_start_ts, week_bounds_ts, week_days, week_start


@dataclass
class Candidate:
    uid: str
    name: str
    messages_raw: int = 0
    effective_messages: int = 0
    duplicates: int = 0
    bursts: int = 0
    active_days: int = 0
    replies: int = 0
    reactions: int = 0
    nominations: int = 0
    voice_minutes: float = 0.0
    imported: int = 0
    channels: int = 0
    components: dict[str, float] = field(default_factory=dict)
    score: float = 0.0
    rank: int = 0
    tie: bool = False
    flags: list[str] = field(default_factory=list)
    ai_advisory: dict[str, Any] | None = None

    @property
    def helpful_points(self) -> float:
        return self.replies + 0.5 * self.reactions

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["score"] = round(self.score, 2)
        out["components"] = {k: round(v, 3) for k, v in self.components.items()}
        return out


def week_key(d: date) -> str:
    return d.isoformat()


def resolve_week(cfg: Config, today: date, offset: int = 0) -> date:
    """offset 0 = current week, -1 = previous week, etc."""
    return week_start(today, cfg.week_start_day) + timedelta(weeks=offset)


def week_coverage(data: dict[str, Any], cfg: Config, start: date, now: int) -> dict[str, Any]:
    tz = cfg.tz
    begin, end = week_bounds_ts(start, tz)
    elapsed = max(0, min(now, end) - begin)
    observed = sum(data["server"].get(d, {}).get("cov", 0) for d in week_days(start))
    gaps = [g for g in data["meta"].get("gaps", []) if g[1] > begin and g[0] < end]
    collection_start = data["meta"].get("collection_start")
    ratio = min(1.0, observed / elapsed) if elapsed else 0.0
    return {
        "observed_hours": round(observed / 3600, 1),
        "elapsed_hours": round(elapsed / 3600, 1),
        "ratio": round(ratio, 3),
        "gaps": len(gaps),
        "complete_week": now >= end,
        "started_mid_week": bool(collection_start and collection_start > begin),
    }


def _recent_winners(data: dict[str, Any], start: date, cooldown_weeks: int) -> set[str]:
    if cooldown_weeks <= 0:
        return set()
    earliest = (start - timedelta(weeks=cooldown_weeks)).isoformat()
    return {
        str(a["winner"]) for wk, a in data["awards"].items()
        if a.get("winner") and earliest <= wk < start.isoformat()
    }


def score_week(data: dict[str, Any], cfg: Config, start: date, now: int | None = None) -> dict[str, Any]:
    t0 = time.perf_counter()
    now = int(time.time()) if now is None else now
    tz = cfg.tz
    wk = week_key(start)
    days = week_days(start)
    begin, end = week_bounds_ts(start, tz)
    elapsed_days = sum(1 for i in range(7) if day_start_ts(start + timedelta(days=i), tz) <= now)
    elapsed_days = max(1, elapsed_days)

    cands: dict[str, Candidate] = {}
    members = data["members"]
    for day in days:
        users = data["days"].get(day, {}).get("u", {})
        for uid, rec in users.items():
            c = cands.get(uid)
            if c is None:
                c = cands[uid] = Candidate(uid=uid, name=members.get(uid, {}).get("n", uid))
            m, d, b = rec.get("m", 0), rec.get("d", 0), rec.get("b", 0)
            eff = max(0, m - d - b)
            c.messages_raw += m
            c.duplicates += d
            c.bursts += b
            c.effective_messages += min(eff, cfg.daily_message_cap)
            if eff >= cfg.min_daily_messages_for_active:
                c.active_days += 1
            c.replies += min(rec.get("r", 0), cfg.daily_reply_cap)
            c.reactions += min(rec.get("rx", 0), cfg.daily_reaction_cap)
            c.voice_minutes += rec.get("v", 0) / 60
            c.imported += rec.get("i", 0)
            c.channels = max(c.channels, len(rec.get("c", {})))

    noms = data["nominations"].get(wk, {})
    for uid, n in noms.items():
        c = cands.get(uid)
        if c is None:
            c = cands[uid] = Candidate(uid=uid, name=members.get(uid, {}).get("n", uid))
        c.nominations = int(n.get("c", 0))
        if n.get("ai"):
            c.ai_advisory = n["ai"]

    winners = _recent_winners(data, start, cfg.winner_cooldown_weeks)
    ineligible: dict[str, int] = {}
    eligible: list[Candidate] = []
    for c in cands.values():
        meta = members.get(c.uid, {})
        reason = None
        if meta.get("b"):
            reason = "bot account"
        elif meta.get("e"):
            reason = "excluded role"
        elif meta.get("x") and meta.get("x", 0) >= meta.get("j", 0):
            reason = "left the server"
        elif c.uid in winners:
            reason = f"won within last {cfg.winner_cooldown_weeks} weeks"
        elif c.effective_messages < cfg.min_weekly_messages:
            reason = f"fewer than {cfg.min_weekly_messages} effective messages"
        elif c.active_days < min(cfg.min_active_days, elapsed_days):
            reason = f"active on fewer than {cfg.min_active_days} days"
        if reason:
            ineligible[reason] = ineligible.get(reason, 0) + 1
        else:
            eligible.append(c)

    def maxof(attr: str) -> float:
        return max((getattr(c, attr) for c in eligible), default=0)

    max_msgs = maxof("effective_messages")
    max_help = max((c.helpful_points + cfg.nomination_weight * c.nominations for c in eligible), default=0)
    max_voice = maxof("voice_minutes")

    available = {"participation": True, "consistency": True,
                 "helpfulness": max_help > 0, "extra": cfg.voice_enabled and max_voice > 0}
    raw_weights = cfg.weights.as_dict()
    total_w = sum(w for k, w in raw_weights.items() if available[k])
    weights = {k: (w / total_w if available[k] and total_w else 0.0) for k, w in raw_weights.items()}

    for c in eligible:
        comp = {
            "participation": math.sqrt(c.effective_messages) / math.sqrt(max_msgs) if max_msgs else 0.0,
            "consistency": min(1.0, c.active_days / elapsed_days),
        }
        if available["helpfulness"]:
            comp["helpfulness"] = (c.helpful_points + cfg.nomination_weight * c.nominations) / max_help
        if available["extra"]:
            comp["extra"] = math.sqrt(c.voice_minutes) / math.sqrt(max_voice)
        c.components = comp
        c.score = 100 * sum(weights[k] * v for k, v in comp.items())
        if c.messages_raw and (c.duplicates + c.bursts) / c.messages_raw >= cfg.spam_flag_ratio:
            c.flags.append(
                f"review: {100 * (c.duplicates + c.bursts) / c.messages_raw:.0f}% of messages were duplicates/bursts (excluded from score)"
            )
        if c.imported:
            c.flags.append(f"{c.imported} messages came from retrospective import")
        joined = members.get(c.uid, {}).get("j")
        if joined and begin <= joined < end:
            c.flags.append("joined this week")

    eligible.sort(key=lambda c: (-round(c.score, 2), -c.active_days, -c.effective_messages, c.uid))
    for i, c in enumerate(eligible):
        c.rank = i + 1
        prev_tie = i > 0 and round(eligible[i - 1].score, 2) == round(c.score, 2)
        next_tie = i + 1 < len(eligible) and round(eligible[i + 1].score, 2) == round(c.score, 2)
        c.tie = prev_tie or next_tie

    coverage = week_coverage(data, cfg, start, now)
    warnings = coverage_warnings(coverage, data)
    if not available["helpfulness"]:
        warnings.append("No helpfulness evidence (replies, reactions, nominations) this week; its weight was redistributed.")
    if cfg.voice_enabled and not available["extra"]:
        warnings.append("No voice participation observed this week; the extra-participation weight was redistributed.")
    shortlist = eligible[: cfg.shortlist_size]
    if len(shortlist) >= 2 and shortlist[0].tie:
        warnings.append("The top position is tied; staff must break the tie manually.")

    return {
        "week": wk,
        "days": days,
        "elapsed_days": elapsed_days,
        "coverage": coverage,
        "warnings": warnings,
        "weights": {k: round(v, 3) for k, v in weights.items()},
        "available_components": [k for k, v in available.items() if v],
        "eligible_count": len(eligible),
        "considered_count": len(cands),
        "ineligible": ineligible,
        "shortlist": [c.to_dict() for c in shortlist],
        "all_eligible": eligible,
        "award": data["awards"].get(wk),
        "duration_ms": round(1000 * (time.perf_counter() - t0), 1),
    }


def coverage_warnings(coverage: dict[str, Any], data: dict[str, Any]) -> list[str]:
    out = []
    if not coverage["complete_week"]:
        out.append("This week is still in progress; rankings will change.")
    if coverage["ratio"] < 0.9:
        out.append(
            f"Collection coverage is {coverage['ratio']:.0%} of the elapsed period "
            f"({coverage['observed_hours']}h of {coverage['elapsed_hours']}h). Activity while the bot was offline is missing."
        )
    if coverage["started_mid_week"]:
        out.append("Collection started part-way through this week.")
    if coverage["gaps"]:
        out.append(f"{coverage['gaps']} collection gap(s) overlap this week.")
    unreadable = data["meta"].get("unreadable_channels") or []
    if unreadable:
        out.append(f"{len(unreadable)} channel(s) are not readable by the bot and are not counted.")
    return out


def approve_winner(data: dict[str, Any], cfg: Config, result: dict[str, Any], uid: int, staff_id: int,
                   note: str = "", replace: bool = False) -> tuple[bool, str]:
    """Record a staff decision. Never pays out or announces anything."""
    wk = result["week"]
    existing = data["awards"].get(wk)
    if existing and existing.get("winner") and not replace:
        return False, f"Week {wk} already has an approved winner (<@{existing['winner']}>). Use replace:True to change it."
    on_shortlist = any(row["uid"] == str(uid) for row in result["shortlist"])
    data["awards"][wk] = {
        "status": "approved",
        "winner": str(uid),
        "approved_by": str(staff_id),
        "approved_at": int(time.time()),
        "amount": cfg.award_amount,
        "currency": cfg.award_currency,
        "paid": False,
        "note": note[:300],
        "on_shortlist": on_shortlist,
        "coverage": result["coverage"]["ratio"],
        "weights": result["weights"],
        "shortlist": [[row["uid"], row["score"]] for row in result["shortlist"]],
    }
    msg = f"Recorded <@{uid}> as Member of the Week for {wk} ({cfg.award_amount} {cfg.award_currency}, payout is manual)."
    if not on_shortlist:
        msg += " Note: this member was not on the computed shortlist."
    return True, msg


def add_nomination(data: dict[str, Any], week: str, uid: int, staff_id: int, reason: str) -> int:
    noms = data["nominations"].setdefault(week, {})
    rec = noms.setdefault(str(uid), {"c": 0, "by": [], "r": []})
    if str(staff_id) in rec["by"]:
        return rec["c"]
    rec["by"].append(str(staff_id))
    rec["c"] += 1
    if reason:
        rec["r"].append(reason[:200])
        del rec["r"][:-5]
    return rec["c"]
