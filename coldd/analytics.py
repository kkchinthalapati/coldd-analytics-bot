"""Deterministic analytics over stored aggregates (no AI, no Discord API calls).

Definitions used throughout (also printed in reports):
* Qualifying message: a message by a non-bot, non-webhook account in an included,
  non-excluded channel the bot can read, observed live (or imported, where shown).
  Deleted messages are subtracted when the deletion is seen within
  RECENT_MESSAGE_HOURS of posting.
* Active member (for a day): posted at least one qualifying message that day.
* Unique active members (for a period): distinct user IDs active on any day of the
  period; each ID counts once.
* Effective messages: qualifying messages minus duplicate and burst messages,
  capped per day at DAILY_MESSAGE_CAP.
* Coverage: seconds the bot was connected and collecting / seconds in the period.
"""

from __future__ import annotations

import csv
import io
import math
import statistics
import time
from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

from .config import Config
from .scoring import resolve_week, score_week
from .timeutil import day_start_ts, days_back, fmt_ts, now_ts, week_start

DEFINITIONS = [
    "Qualifying message: non-bot, non-webhook message in an included channel the bot can read, observed while online "
    "(imported history is labelled separately). Deletions seen within the recent-message window are subtracted.",
    "Active member: posted >= 1 qualifying message on that day (configured timezone).",
    "Unique active members: distinct user IDs active on any day in the period; each ID counts once.",
    "Effective messages: qualifying messages minus duplicates and burst messages, capped per day.",
    "Coverage: time the bot was connected and collecting divided by the time in the period.",
    "Returning member: active in both this period and the immediately preceding period of equal length.",
]


# --- small helpers ------------------------------------------------------------

def period(today: date, n: int) -> list[str]:
    return days_back(today, n)


def previous_period(today: date, n: int) -> list[str]:
    return days_back(today - timedelta(days=n), n)


def gini(values: list[int]) -> float:
    vals = sorted(v for v in values if v > 0)
    n = len(vals)
    if n == 0:
        return 0.0
    cum = sum((i + 1) * v for i, v in enumerate(vals))
    return (2 * cum) / (n * sum(vals)) - (n + 1) / n


def top_share(values: list[int], fraction: float) -> float:
    vals = sorted((v for v in values if v > 0), reverse=True)
    if not vals:
        return 0.0
    k = max(1, math.ceil(len(vals) * fraction))
    return sum(vals[:k]) / sum(vals)


def pct(a: float, b: float) -> str:
    return f"{100 * a / b:.1f}%" if b else "n/a"


def change(cur: float, prev: float) -> str:
    if not prev:
        return "n/a (no prior data)" if cur else "0"
    return f"{100 * (cur - prev) / prev:+.1f}%"


def md_table(headers: list[str], rows: Iterable[Iterable[Any]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    for row in rows:
        out.append("| " + " | ".join(str(c).replace("|", "/") for c in row) + " |")
    return "\n".join(out)


def name_of(data: dict[str, Any], uid: str) -> str:
    return data["members"].get(uid, {}).get("n") or uid


def channel_name(data: dict[str, Any], cid: str) -> str:
    return "#" + (data["channels"].get(cid, {}).get("n") or cid)


def coverage_for(data: dict[str, Any], days: list[str], today: date, now: int, cfg: Config) -> dict[str, Any]:
    observed = sum(data["server"].get(d, {}).get("cov", 0) for d in days)
    start = day_start_ts(date.fromisoformat(days[0]), cfg.tz)
    elapsed = max(1, min(now, day_start_ts(date.fromisoformat(days[-1]) + timedelta(days=1), cfg.tz)) - start)
    zero_days = [d for d in days if data["server"].get(d, {}).get("cov", 0) == 0 and d <= today.isoformat()]
    cs = data["meta"].get("collection_start")
    return {
        "ratio": min(1.0, observed / elapsed),
        "observed_hours": observed / 3600,
        "uncovered_days": zero_days,
        "before_collection": bool(cs and cs > start),
    }


def aggregate_users(data: dict[str, Any], days: list[str]) -> dict[str, dict[str, Any]]:
    """Per-user totals over ``days`` from member detail (bounded by member retention)."""
    out: dict[str, dict[str, Any]] = {}
    for day in days:
        for uid, rec in data["days"].get(day, {}).get("u", {}).items():
            a = out.get(uid)
            if a is None:
                a = out[uid] = {"m": 0, "d": 0, "b": 0, "r": 0, "rx": 0, "v": 0, "i": 0, "days": 0, "c": set()}
            m = rec.get("m", 0)
            a["m"] += m
            a["d"] += rec.get("d", 0)
            a["b"] += rec.get("b", 0)
            a["r"] += rec.get("r", 0)
            a["rx"] += rec.get("rx", 0)
            a["v"] += rec.get("v", 0)
            a["i"] += rec.get("i", 0)
            if m:
                a["days"] += 1
            a["c"].update(rec.get("c", {}))
    return out


def is_human(data: dict[str, Any], uid: str) -> bool:
    return not data["members"].get(uid, {}).get("b")


# --- /analytics ---------------------------------------------------------------

def summary(data: dict[str, Any], cfg: Config, days_n: int, today: date, now: int | None = None) -> dict[str, Any]:
    now = now_ts() if now is None else now
    days = period(today, days_n)
    prev = previous_period(today, days_n)

    def totals(ds: list[str]) -> dict[str, Any]:
        t: dict[str, Any] = {"m": 0, "j": 0, "lv": 0, "bn": 0, "v": 0, "d": 0, "b": 0, "im": 0, "del": 0}
        hours = [0] * 24
        for d in ds:
            s = data["server"].get(d, {})
            for k in t:
                t[k] += s.get(k, 0)
            for i, h in enumerate(s.get("h", [])):
                hours[i] += h
        users = aggregate_users(data, ds)
        t["unique"] = sum(1 for u in users if is_human(data, u))
        t["hours"] = hours
        t["users"] = users
        return t

    cur, before = totals(days), totals(prev)
    daily = [(d, data["server"].get(d, {}).get("m", 0),
              len(data["days"][d]["u"]) if d in data["days"] else data["server"].get(d, {}).get("a"))
             for d in days]
    per_member = [u["m"] for uid, u in cur["users"].items() if is_human(data, uid)]
    busiest = max(daily, key=lambda r: r[1]) if daily else None
    peak_hour = max(range(24), key=lambda h: cur["hours"][h]) if any(cur["hours"]) else None
    returning = len(set(cur["users"]) & set(before["users"]))
    return {
        "days": days,
        "messages": cur["m"],
        "messages_prev": before["m"],
        "unique_active": cur["unique"],
        "unique_active_prev": before["unique"],
        "returning": returning,
        "avg_per_active": statistics.mean(per_member) if per_member else 0,
        "median_per_active": statistics.median(per_member) if per_member else 0,
        "joins": cur["j"], "leaves": cur["lv"], "bans": cur["bn"],
        "joins_prev": before["j"], "leaves_prev": before["lv"],
        "voice_hours": cur["v"] / 3600,
        "duplicates": cur["d"], "bursts": cur["b"], "imported": cur["im"], "deleted": cur["del"],
        "daily": daily,
        "busiest_day": busiest,
        "peak_hour": peak_hour,
        "hours": cur["hours"],
        "top10_share": top_share(per_member, 0.10),
        "gini": gini(per_member),
        "coverage": coverage_for(data, days, today, now, cfg),
        "coverage_prev": coverage_for(data, prev, today, now, cfg)["ratio"],
        "member_detail_limited": days_n > cfg.member_retention_days,
        "users": cur["users"],
        "prev_users": before["users"],
    }


# --- /leaderboard ---------------------------------------------------------------

METRICS = {
    "messages": ("Effective messages", lambda a: a["eff"]),
    "active_days": ("Active days", lambda a: a["days"]),
    "helpful": ("Replies + reactions received", lambda a: a["r"] + a["rx"]),
    "voice": ("Voice minutes", lambda a: round(a["v"] / 60)),
}


def leaderboard(data: dict[str, Any], cfg: Config, days_n: int, today: date, metric: str = "messages",
                limit: int = 15) -> list[dict[str, Any]]:
    days = period(today, days_n)
    agg: dict[str, dict[str, Any]] = {}
    for day in days:
        for uid, rec in data["days"].get(day, {}).get("u", {}).items():
            a = agg.setdefault(uid, {"eff": 0, "m": 0, "days": 0, "r": 0, "rx": 0, "v": 0})
            eff = max(0, rec.get("m", 0) - rec.get("d", 0) - rec.get("b", 0))
            a["eff"] += min(eff, cfg.daily_message_cap)
            a["m"] += rec.get("m", 0)
            a["days"] += 1 if rec.get("m", 0) else 0
            a["r"] += min(rec.get("r", 0), cfg.daily_reply_cap)
            a["rx"] += min(rec.get("rx", 0), cfg.daily_reaction_cap)
            a["v"] += rec.get("v", 0)
    _, key = METRICS[metric]
    rows: list[dict[str, Any]] = [
        {"uid": uid, "name": name_of(data, uid), "value": key(a), **a}
        for uid, a in agg.items() if is_human(data, uid) and key(a) > 0
    ]
    rows.sort(key=lambda r: (-r["value"], -r["days"], r["uid"]))
    for i, r in enumerate(rows):
        r["rank"] = i + 1
    return rows[:limit] if limit else rows


# --- /channels ---------------------------------------------------------------

def channel_stats(data: dict[str, Any], cfg: Config, days_n: int, today: date) -> list[dict[str, Any]]:
    days = period(today, days_n)
    prev = previous_period(today, days_n)
    msgs: dict[str, int] = {}
    prev_msgs: dict[str, int] = {}
    uniq: dict[str, set[str]] = {}
    snap_uniq: dict[str, int] = {}
    for d in days:
        s = data["server"].get(d, {})
        for cid, n in s.get("ch", {}).items():
            msgs[cid] = msgs.get(cid, 0) + n
        if d in data["days"]:
            for uid, rec in data["days"][d]["u"].items():
                for cid in rec.get("c", {}):
                    uniq.setdefault(cid, set()).add(uid)
        else:
            for cid, n in s.get("chu", {}).items():
                snap_uniq[cid] = max(snap_uniq.get(cid, 0), n)  # lower bound only
    for d in prev:
        for cid, n in data["server"].get(d, {}).get("ch", {}).items():
            prev_msgs[cid] = prev_msgs.get(cid, 0) + n
    total = sum(msgs.values()) or 1
    rows: list[dict[str, Any]] = []
    for cid in set(msgs) | set(prev_msgs) | set(data["channels"]):
        m = msgs.get(cid, 0)
        rows.append({
            "cid": cid,
            "name": channel_name(data, cid),
            "messages": m,
            "share": m / total,
            "unique": max(len(uniq.get(cid, ())), snap_uniq.get(cid, 0)),
            "prev": prev_msgs.get(cid, 0),
            "trend": change(m, prev_msgs.get(cid, 0)),
        })
    rows.sort(key=lambda r: (-r["messages"], r["name"]))
    return rows


# --- /member -----------------------------------------------------------------

def member_stats(data: dict[str, Any], cfg: Config, uid: int, days_n: int, today: date) -> dict[str, Any] | None:
    key = str(uid)
    meta = data["members"].get(key)
    days = period(today, days_n)
    daily = []
    channels: dict[str, int] = {}
    totals = {"m": 0, "d": 0, "b": 0, "r": 0, "rx": 0, "v": 0, "vs": 0, "i": 0}
    for d in days:
        rec = data["days"].get(d, {}).get("u", {}).get(key)
        daily.append((d, rec.get("m", 0) if rec else 0))
        if rec:
            for k in totals:
                totals[k] += rec.get(k, 0)
            for cid, n in rec.get("c", {}).items():
                channels[cid] = channels.get(cid, 0) + n
    if meta is None and not totals["m"] and not totals["v"]:
        return None
    board = leaderboard(data, cfg, days_n, today, "messages", limit=0)
    rank = next((r["rank"] for r in board if r["uid"] == key), None)
    return {
        "uid": key,
        "name": (meta or {}).get("n", key),
        "first_seen": (meta or {}).get("f"),
        "last_active": (meta or {}).get("l"),
        "joined": (meta or {}).get("j"),
        "left": (meta or {}).get("x"),
        "excluded": bool((meta or {}).get("e")),
        "totals": totals,
        "active_days": sum(1 for _, n in daily if n),
        "daily": daily,
        "channels": sorted(channels.items(), key=lambda kv: -kv[1])[:8],
        "rank": rank,
        "ranked_members": len(board),
        "wins": [wk for wk, a in data["awards"].items() if str(a.get("winner")) == key],
        "nominations": sum(n.get(key, {}).get("c", 0) for n in data["nominations"].values()),
    }


def insights(data: dict[str, Any], cfg: Config, days_n: int, today: date) -> dict[str, Any]:
    """Server-health view built only from stored aggregates (Discord's own Insights are not exposed to bots).

    * growth: joins, leaves, net, previous net
    * engagement: mean daily active, trailing-7-day active, stickiness (mean DAU / WAU)
    * heatmap: messages by weekday (Mon=0) and 4-hour block, 7 x 6
    * retention: weekly cohorts of new members (from analytics._cohorts)
    """
    days = period(today, days_n)
    prev = previous_period(today, days_n)
    joins = sum(data["server"].get(d, {}).get("j", 0) for d in days)
    leaves = sum(data["server"].get(d, {}).get("lv", 0) for d in days)
    pj = sum(data["server"].get(d, {}).get("j", 0) for d in prev)
    pl = sum(data["server"].get(d, {}).get("lv", 0) for d in prev)

    dau = [len(data["days"].get(d, {}).get("u", {})) for d in days]
    mean_dau = sum(dau) / len(dau) if dau else 0.0
    last7 = days[-7:]
    wau = len({u for d in last7 for u in data["days"].get(d, {}).get("u", {})})
    mean_dau_7 = sum(len(data["days"].get(d, {}).get("u", {})) for d in last7) / max(1, len(last7))

    grid = [[0] * 6 for _ in range(7)]
    for d in days:
        hourly = data["server"].get(d, {}).get("h")
        if not hourly:
            continue
        wd = date.fromisoformat(d).weekday()
        for hour, n in enumerate(hourly[:24]):
            grid[wd][hour // 4] += n

    return {
        "joins": joins, "leaves": leaves, "net": joins - leaves, "net_prev": pj - pl,
        "mean_dau": mean_dau, "wau": wau,
        "stickiness": (mean_dau_7 / wau) if wau else 0.0,
        "heatmap": grid,
        "cohorts": _cohorts(data, cfg, today)[-6:],
        "coverage": coverage_for(data, days, today, now_ts(), cfg),
    }


# --- community boards (/top, /profile) ----------------------------------------

COMMUNITY_METRICS: dict[str, tuple[str, str, Any]] = {
    "messages": ("Most messages", "msgs", lambda a: a["eff"]),
    "consistency": ("Most consistent (active days)", "days", lambda a: a["days"]),
    "streak": ("Longest streak (consecutive days)", "days", lambda a: a["streak"]),
    "reacting": ("Most reactions given", "reactions", lambda a: a["rg"]),
    "interactions": ("Most interactive (replies and reactions, both ways)", "interactions",
                     lambda a: a["r"] + a["rg"] + a["rx"]),
    "helpful": ("Most helpful (replies and reactions received)", "points", lambda a: a["r"] + a["rx"]),
    "voice": ("Most voice time", "min", lambda a: round(a["v"] / 60)),
}


def member_table(data: dict[str, Any], cfg: Config, days_n: int, today: date) -> dict[str, dict[str, Any]]:
    """Per-member figures for one window, computed in a single pass. Bots and excluded roles are kept
    out of rankings by is_human; daily caps apply the same way as the weekly scoring."""
    days = period(today, days_n)
    agg: dict[str, dict[str, Any]] = {}
    run: dict[str, int] = {}
    best: dict[str, int] = {}
    for day in days:
        users = data["days"].get(day, {}).get("u", {})
        active_today = set()
        for uid, rec in users.items():
            if rec.get("m", 0) > 0:
                active_today.add(uid)
            a = agg.setdefault(uid, {"eff": 0, "m": 0, "days": 0, "streak": 0, "r": 0, "rx": 0, "rg": 0, "v": 0})
            m = rec.get("m", 0)
            a["m"] += m
            a["eff"] += min(max(0, m - rec.get("d", 0) - rec.get("b", 0)), cfg.daily_message_cap)
            a["days"] += 1 if m > 0 else 0
            a["r"] += min(rec.get("r", 0), cfg.daily_reply_cap)
            a["rx"] += min(rec.get("rx", 0), cfg.daily_reaction_cap)
            a["rg"] += min(rec.get("rg", 0), cfg.daily_reaction_cap)
            a["v"] += rec.get("v", 0)
        for uid in list(run):
            if uid not in active_today:
                run[uid] = 0
        for uid in active_today:
            run[uid] = run.get(uid, 0) + 1
            best[uid] = max(best.get(uid, 0), run[uid])
    for uid, a in agg.items():
        a["streak"] = best.get(uid, 0)
        a["window_days"] = len(days)
    return {uid: a for uid, a in agg.items() if is_human(data, uid)}


def community_board(data: dict[str, Any], cfg: Config, days_n: int, today: date, metric: str,
                    limit: int = 10) -> list[dict[str, Any]]:
    _, _, key = COMMUNITY_METRICS[metric]
    rows = [{"uid": uid, "name": name_of(data, uid), "value": key(a), **a}
            for uid, a in member_table(data, cfg, days_n, today).items()]
    rows = [r for r in rows if r["value"] > 0]
    rows.sort(key=lambda r: (-r["value"], -r["days"], r["uid"]))
    for i, r in enumerate(rows):
        r["rank"] = i + 1
    return rows[:limit] if limit else rows


def member_profile(data: dict[str, Any], cfg: Config, uid: str, days_n: int, today: date) -> dict[str, Any] | None:
    """One member's figures plus their rank and total ranked members for every community metric."""
    table = member_table(data, cfg, days_n, today)
    if uid not in table:
        return None
    ranks: dict[str, dict[str, int]] = {}
    for metric, (_, _, key) in COMMUNITY_METRICS.items():
        ordered = sorted((key(a) for a in table.values() if key(a) > 0), reverse=True)
        mine = key(table[uid])
        ranks[metric] = {"rank": (ordered.index(mine) + 1) if mine > 0 else 0, "of": len(ordered), "value": mine}
    return {"name": name_of(data, uid), "figures": table[uid], "ranks": ranks, "window_days": len(period(today, days_n))}


def _pick(sorted_vals: list[float], q: float) -> float:
    """Nearest-rank percentile of an ascending list (0 when empty)."""
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, round(q * (len(sorted_vals) - 1))))
    return float(sorted_vals[idx])


def deep_stats(data: dict[str, Any], cfg: Config, today: date) -> dict[str, Any]:
    """Every aggregate the bot holds, for the AI deep scan.

    Contains no member names, member IDs or message text: only counts, distributions
    and channel names. Windows are capped at the member-detail retention period.
    """
    windows = [w for w in (7, 30, 90) if w <= cfg.member_retention_days] or [cfg.member_retention_days]
    out: dict[str, Any] = {
        "as_of": today.isoformat(),
        "timezone": cfg.timezone_name,
        "retention_days": {"member_detail": cfg.member_retention_days, "server": cfg.server_retention_days},
        "storage": {"members_on_file": len(data["members"]), "channels_on_file": len(data["channels"]),
                    "award_weeks_recorded": len(data["awards"]), "nomination_weeks": len(data["nominations"])},
        "windows": {},
    }
    for w in windows:
        s = summary(data, cfg, w, today)
        ins = insights(data, cfg, w, today)
        vals = sorted(float(r["value"]) for r in leaderboard(data, cfg, w, today, "messages", limit=None))
        chans = [r for r in channel_stats(data, cfg, w, today) if r["messages"]][:10]
        out["windows"][f"{w}d"] = {
            "messages": s["messages"], "messages_previous": s["messages_prev"],
            "active_members": s["unique_active"], "active_members_previous": s["unique_active_prev"],
            "returning_members": s["returning"],
            "joins": s["joins"], "leaves": s["leaves"], "net_growth": ins["net"], "net_growth_previous": ins["net_prev"],
            "mean_messages_per_active": round(s["avg_per_active"], 2),
            "median_messages_per_active": round(s["median_per_active"], 2),
            "top10pct_message_share": round(s["top10_share"], 3),
            "distribution_messages_per_member": {"members": len(vals), "p50": _pick(vals, 0.5),
                                                 "p90": _pick(vals, 0.9), "p99": _pick(vals, 0.99),
                                                 "max": vals[-1] if vals else 0},
            "mean_daily_active": round(ins["mean_dau"], 1), "weekly_active": ins["wau"],
            "stickiness": round(ins["stickiness"], 3),
            "voice_hours": round(s["voice_hours"], 1), "peak_hour": s["peak_hour"],
            "heatmap_weekday_x_4h_blocks": ins["heatmap"],
            "retention_cohorts": ins["cohorts"],
            "coverage": round(s["coverage"]["ratio"], 3), "coverage_previous": round(s["coverage_prev"], 3),
            "channels": [{"name": (data["channels"].get(r["cid"], {}).get("n") or "unknown"),
                          "messages": r["messages"], "share": round(r["share"], 3), "unique_members": r["unique"],
                          "trend": r["trend"]} for r in chans],
        }
    daily_days = period(today, min(90, cfg.server_retention_days))
    out["daily_series_90d"] = [
        [d, data["server"].get(d, {}).get("m", 0), data["server"].get(d, {}).get("j", 0),
         data["server"].get(d, {}).get("lv", 0), data["server"].get(d, {}).get("cov", 0)]
        for d in daily_days
    ]
    out["daily_series_columns"] = ["date", "messages", "joins", "leaves", "observed_seconds"]
    return out


def sparkline(values: list[int]) -> str:
    bars = "▁▂▃▄▅▆▇█"
    top = max(values) if values else 0
    if not top:
        return "▁" * len(values)
    return "".join(bars[min(7, int(7 * v / top))] if v else "▁" for v in values)


# --- /audit ------------------------------------------------------------------

def run_audit(data: dict[str, Any], cfg: Config, days_n: int, today: date, now: int | None = None) -> tuple[str, dict[str, Any]]:
    """Build the full Markdown audit report. Returns (markdown, key_figures)."""
    t0 = time.perf_counter()
    now = now_ts() if now is None else now
    days_n = min(days_n, cfg.audit_max_days)
    s = summary(data, cfg, days_n, today, now)
    users, prev_users = s["users"], s["prev_users"]
    humans = {u: a for u, a in users.items() if is_human(data, u)}
    chans = channel_stats(data, cfg, days_n, today)
    cov = s["coverage"]
    tz = cfg.tz
    L: list[str] = []
    add = L.append

    # executive summary
    add("# COLDD Server Analytics Audit\n")
    add(f"Period: **{s['days'][0]} to {s['days'][-1]}** ({days_n} days, timezone {cfg.timezone_name}). "
        f"Generated {fmt_ts(now, tz)}. Compared with the preceding {days_n} days.\n")
    add("## Executive summary\n")
    findings = []
    findings.append(f"{s['messages']:,} qualifying messages from {s['unique_active']:,} unique active members "
                    f"({change(s['messages'], s['messages_prev'])} messages, "
                    f"{change(s['unique_active'], s['unique_active_prev'])} active members vs previous period).")
    findings.append(f"Activity concentration: the top 10% of active members wrote {s['top10_share']:.0%} of messages (Gini {s['gini']:.2f}).")
    findings.append(f"Membership observed: {s['joins']:,} joins, {s['leaves']:,} departures, net {s['joins'] - s['leaves']:+,}.")
    if s["busiest_day"]:
        findings.append(f"Busiest day: {s['busiest_day'][0]} ({s['busiest_day'][1]:,} messages). Peak hour: "
                        f"{s['peak_hour']:02d}:00 {cfg.timezone_name}." if s["peak_hour"] is not None else "")
    findings.append(f"Collection coverage: {cov['ratio']:.0%} of the period.")
    for f in findings:
        if f:
            add(f"- {f}")
    add("")
    quality = []
    if cov["ratio"] < 0.9:
        quality.append(f"Coverage is {cov['ratio']:.0%}; activity while the bot was offline is not included and was not estimated.")
    if cov["before_collection"]:
        quality.append("Collection began during this period; earlier days have no data.")
    if s["coverage_prev"] < 0.9:
        quality.append(f"The comparison period was only {s['coverage_prev']:.0%} observed, so period-over-period changes "
                       "overstate growth. Compare only fully covered periods.")
    if s["member_detail_limited"]:
        quality.append(f"Per-member detail is kept for {cfg.member_retention_days} days; older days use server-level snapshots only.")
    if s["imported"]:
        quality.append(f"{s['imported']:,} messages came from retrospective history import, not live observation.")
    unreadable = data["meta"].get("unreadable_channels") or []
    if unreadable:
        quality.append(f"{len(unreadable)} channel(s) are unreadable by the bot: " + ", ".join(channel_name(data, c) for c in unreadable[:15]))
    if quality:
        add("**Data quality warnings**\n")
        for q in quality:
            add(f"- {q}")
        add("")

    # overall activity
    add("## 1. Overall activity\n")
    add(md_table(
        ["Metric", "This period", "Previous period", "Change"],
        [
            ["Qualifying messages", f"{s['messages']:,}", f"{s['messages_prev']:,}", change(s["messages"], s["messages_prev"])],
            ["Unique active members", f"{s['unique_active']:,}", f"{s['unique_active_prev']:,}", change(s["unique_active"], s["unique_active_prev"])],
            ["Returning members (active in both)", f"{s['returning']:,}", "", ""],
            ["Mean messages per active member", f"{s['avg_per_active']:.1f}", "", ""],
            ["Median messages per active member", f"{s['median_per_active']:.1f}", "", ""],
            ["Observed voice hours", f"{s['voice_hours']:.1f}", "", ""],
            ["Deletions subtracted", f"{s['deleted']:,}", "", ""],
        ],
    ))
    add("\n### Messages and active members by day\n")
    add(md_table(["Day", "Messages", "Active members", "Coverage"],
                 [(d, f"{m:,}", "n/a" if a is None else f"{a:,}",
                   f"{min(1, data['server'].get(d, {}).get('cov', 0) / 86400):.0%}") for d, m, a in s["daily"]]))
    dist = {"1 day": 0, "2-3 days": 0, "4-7 days": 0, "8-14 days": 0, "15+ days": 0}
    for a in humans.values():
        n = a["days"]
        k = "1 day" if n <= 1 else "2-3 days" if n <= 3 else "4-7 days" if n <= 7 else "8-14 days" if n <= 14 else "15+ days"
        dist[k] += 1
    add("\n### Active-day distribution (how many days each active member posted)\n")
    add(md_table(["Active days", "Members", "Share"], [(k, v, pct(v, len(humans))) for k, v in dist.items()]))
    add("\n### Activity by hour of day (" + cfg.timezone_name + ")\n")
    add("`" + sparkline(s["hours"]) + "`  (00:00 → 23:00)\n")
    add(md_table(["Concentration", "Value"], [
        ["Top 1% share of messages", f"{top_share([a['m'] for a in humans.values()], 0.01):.0%}"],
        ["Top 10% share of messages", f"{s['top10_share']:.0%}"],
        ["Gini coefficient (0 = even, 1 = one person)", f"{s['gini']:.2f}"],
    ]))

    # channels
    add("\n## 2. Channel performance\n")
    total_msgs = sum(c["messages"] for c in chans) or 1
    add(md_table(["Channel", "Messages", "Share", "Unique members", "Previous", "Trend"],
                 [(c["name"], f"{c['messages']:,}", f"{c['messages'] / total_msgs:.1%}", c["unique"], c["prev"], c["trend"])
                  for c in chans[:40]]))
    if len(chans) > 40:
        add(f"\n({len(chans) - 40} more channels omitted; use /export channels for the full list.)")
    quiet = [c["name"] for c in chans if c["messages"] and c["share"] < 0.01]
    dead = [c["name"] for c in chans if not c["messages"] and c["prev"]]
    if quiet:
        add(f"\nUnderused (<1% of messages): {', '.join(quiet[:20])}")
    if dead:
        add(f"\nActive last period but silent now: {', '.join(dead[:20])}")
    if chans and chans[0]["messages"]:
        add(f"\nMost active: {chans[0]['name']} ({chans[0]['share']:.0%} of messages).")

    # community participation
    add("\n## 3. Community participation\n")
    multi = sum(1 for a in humans.values() if len(a["c"]) >= 2)
    period_start = s["days"][0]
    newcomers = [uid for uid, m in data["members"].items()
                 if m.get("j") and m["j"] >= _ts(period_start, cfg) and not m.get("b")]
    posting_new = [uid for uid in newcomers if uid in humans]
    add(md_table(["Metric", "Value"], [
        ["Members active in 2+ channels", f"{multi:,} ({pct(multi, len(humans))})"],
        ["Returning from previous period", f"{s['returning']:,} ({pct(s['returning'], len(prev_users))} of previous active)"],
        ["Newcomers observed joining", f"{len(newcomers):,}"],
        ["Newcomers who posted", f"{len(posting_new):,} ({pct(len(posting_new), len(newcomers))})"],
        ["Median messages per posting newcomer",
         f"{statistics.median([humans[u]['m'] for u in posting_new]):.0f}" if posting_new else "n/a"],
        ["Members active on 4+ days (consistent)", f"{sum(1 for a in humans.values() if a['days'] >= 4):,}"],
    ]))

    # membership
    add("\n## 4. Membership trends (observed while online)\n")
    weekly = _weekly_membership(data, s["days"])
    add(md_table(["Week starting", "Joins", "Departures", "Net", "Bans"], weekly))
    cohorts = _cohorts(data, cfg, today)
    if cohorts:
        add("\n### Newcomer retention cohorts\n")
        add("Share of members who joined in a week (observed) and posted in later weeks. Only weeks with complete member detail are shown.\n")
        add(md_table(["Join week", "Joined", "Posted in join week", "Posted week +1", "Posted week +2-4"], cohorts))
    else:
        add("\nNot enough longitudinal data yet for retention cohorts (needs observed joins plus 2+ following weeks).")
    add("\nThe bot only knows joins and departures it observed while online; it does not know the server's membership history from before installation.")

    # moderation & spam
    add("\n## 5. Moderation and spam indicators (for staff review, not proof)\n")
    add(md_table(["Indicator", "Count"], [
        ["Duplicate messages (same text by same member within window)", f"{s['duplicates']:,}"],
        ["Burst messages (above burst threshold)", f"{s['bursts']:,}"],
        ["Bans observed", f"{s['bans']:,}"],
        ["Deleted messages subtracted", f"{s['deleted']:,}"],
    ]))
    suspicious = sorted(
        ((uid, a) for uid, a in humans.items() if a["m"] >= 30 and (a["d"] + a["b"]) / a["m"] >= cfg.spam_flag_ratio),
        key=lambda kv: -(kv[1]["d"] + kv[1]["b"]) / kv[1]["m"],
    )[:15]
    if suspicious:
        add("\nMembers whose duplicate/burst share is at or above SPAM_FLAG_RATIO (minimum 30 messages):\n")
        add(md_table(["Member", "Messages", "Duplicates", "Bursts", "Share"],
                     [(f"{name_of(data, u)} ({u})", a["m"], a["d"], a["b"], pct(a["d"] + a["b"], a["m"])) for u, a in suspicious]))
    farmers = sorted(((u, a) for u, a in humans.items() if len(a["c"]) == 1 and a["m"] >= 200),
                     key=lambda kv: -kv[1]["m"])[:10]
    if farmers:
        add("\nHigh volume concentrated in a single channel (possible activity farming; often legitimate):\n")
        add(md_table(["Member", "Messages", "Channel"],
                     [(f"{name_of(data, u)} ({u})", a["m"], channel_name(data, next(iter(a["c"])))) for u, a in farmers]))
    vols = [m for _, m, _ in s["daily"] if m]
    if len(vols) >= 7:
        mu, sd = statistics.mean(vols), statistics.pstdev(vols)
        spikes = [(d, m) for d, m, _ in s["daily"] if sd and m > mu + 3 * sd]
        if spikes:
            add("\nVolume spikes (> mean + 3 standard deviations): " + ", ".join(f"{d} ({m:,})" for d, m in spikes))
    add("\nNo automatic action is taken on any indicator.")

    # coverage
    add("\n## 6. Collection coverage and data quality\n")
    gaps = [g for g in data["meta"].get("gaps", []) if g[1] >= _ts(period_start, cfg)]
    add(md_table(["Item", "Value"], [
        ["Coverage of period", f"{cov['ratio']:.1%} ({cov['observed_hours']:.0f} observed hours)"],
        ["Collection started", fmt_ts(data["meta"].get("collection_start"), tz)],
        ["Gaps in this period", len(gaps)],
        ["Days with no observation", len(cov["uncovered_days"])],
        ["Retrospectively imported messages", f"{s['imported']:,}"],
        ["Unreadable channels", len(unreadable)],
    ]))
    if gaps:
        add("\nLargest gaps:\n")
        for g in sorted(gaps, key=lambda g: g[0] - g[1])[:10]:
            add(f"- {fmt_ts(g[0], tz)} → {fmt_ts(g[1], tz)} ({(g[1] - g[0]) / 3600:.1f}h)")

    # weekly snapshot
    try:
        wk = score_week(data, cfg, resolve_week(cfg, today, -1), now)
        add("\n## 7. Last completed week: Member of the Week shortlist (top 5)\n")
        add(md_table(["Rank", "Member", "Score", "Eff. msgs", "Active days"],
                     [(r["rank"], f"{r['name']} ({r['uid']})", f"{r['score']:.1f}", r["effective_messages"], r["active_days"])
                      for r in wk["shortlist"][:5]]) if wk["shortlist"] else "No eligible candidates.")
    except Exception:  # the audit must not fail because of the optional weekly section
        pass

    # recommendations
    add("\n## 8. Recommendations\n")
    recs = []
    if cov["ratio"] < 0.95:
        recs.append("Improve uptime (check host restarts) so weekly awards rest on complete data.")
    if s["top10_share"] > 0.6:
        recs.append("Activity is concentrated in a small group; consider prompts or events that invite quieter members.")
    if newcomers and len(posting_new) / len(newcomers) < 0.2:
        recs.append("Fewer than 20% of observed newcomers posted; review onboarding and welcome channels.")
    if quiet:
        recs.append(f"{len(quiet)} channel(s) receive <1% of messages; consider merging or archiving them.")
    if s["duplicates"] + s["bursts"] > 0.05 * max(1, s["messages"]):
        recs.append("Duplicate/burst messages exceed 5% of volume; check slow-mode on the busiest channels.")
    if unreadable:
        recs.append("Grant the bot View Channel + Read Message History on channels that should be counted.")
    if not recs:
        recs.append("No significant issues detected by the deterministic checks.")
    for r in recs:
        add(f"- {r}")

    add("\n## Definitions\n")
    for d in DEFINITIONS:
        add(f"- {d}")
    add("\n## Limitations\n")
    for lim in LIMITATIONS:
        add(f"- {lim}")
    duration = time.perf_counter() - t0
    add(f"\n_Audit computed in {duration * 1000:.0f} ms from stored aggregates._")
    figures = {k: s[k] for k in ("messages", "unique_active", "joins", "leaves", "returning")}
    figures["coverage"] = cov["ratio"]
    figures["duration_ms"] = round(duration * 1000)
    return "\n".join(L), figures


LIMITATIONS = [
    "Only activity observed while the bot was online is counted; offline periods are reported as gaps, never estimated.",
    "Message content is not stored. Duplicate detection uses short-lived in-memory fingerprints only.",
    "Deletions and reactions are attributed only for messages seen within the recent-message window (default 24h, in memory; reset on restart).",
    "Joins/departures before installation or during downtime are unknown.",
    "Voice time is sampled once per save interval and only counted with the configured minimum number of humans present.",
    "Heuristic spam indicators can flag legitimate behaviour; they are for staff review only.",
]


def _ts(day: str, cfg: Config) -> int:
    return day_start_ts(date.fromisoformat(day), cfg.tz)


def _weekly_membership(data: dict[str, Any], days: list[str]) -> list[tuple[Any, ...]]:
    rows = []
    for i in range(0, len(days), 7):
        chunk = days[i:i + 7]
        j = sum(data["server"].get(d, {}).get("j", 0) for d in chunk)
        lv = sum(data["server"].get(d, {}).get("lv", 0) for d in chunk)
        bn = sum(data["server"].get(d, {}).get("bn", 0) for d in chunk)
        rows.append((chunk[0], j, lv, f"{j - lv:+}", bn))
    return rows


def _cohorts(data: dict[str, Any], cfg: Config, today: date) -> list[tuple[Any, ...]]:
    oldest = min(data["days"], default=None)
    if not oldest:
        return []
    first_week = week_start(date.fromisoformat(oldest), cfg.week_start_day) + timedelta(weeks=1)
    this_week = week_start(today, cfg.week_start_day)
    rows = []
    wk = first_week
    while wk + timedelta(weeks=2) <= this_week:
        begin, end = _ts(wk.isoformat(), cfg), _ts((wk + timedelta(weeks=1)).isoformat(), cfg)
        joined = {u for u, m in data["members"].items() if m.get("j") and begin <= m["j"] < end and not m.get("b")}
        if joined:
            def active_in(start: date, weeks: int, joined: set[str] = joined) -> int:
                days = [(start + timedelta(days=i)).isoformat() for i in range(7 * weeks)]
                seen: set[str] = set()
                for d in days:
                    seen.update(set(data["days"].get(d, {}).get("u", {})) & joined)
                return len(seen)
            w0 = active_in(wk, 1)
            w1 = active_in(wk + timedelta(weeks=1), 1)
            later = active_in(wk + timedelta(weeks=2), 3) if wk + timedelta(weeks=5) <= this_week else None
            rows.append((wk.isoformat(), len(joined), pct(w0, len(joined)), pct(w1, len(joined)),
                         "pending" if later is None else pct(later, len(joined))))
        wk += timedelta(weeks=1)
    return rows[-12:]


# --- exports -----------------------------------------------------------------

def to_csv(headers: list[str], rows: Iterable[Iterable[Any]]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for row in rows:
        w.writerow([_csv_safe(c) for c in row])
    return buf.getvalue().encode("utf-8")


def _csv_safe(value: Any) -> Any:
    """Neutralise spreadsheet formula injection from user-controlled names."""
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + value
    return value


def weekly_markdown(result: dict[str, Any], cfg: Config) -> str:
    L = [f"# Member of the Week: week starting {result['week']}\n"]
    L.append(f"Award: {cfg.award_amount} {cfg.award_currency} (manual decision and manual payout).")
    cov = result["coverage"]
    L.append(f"Coverage: {cov['ratio']:.0%} ({cov['observed_hours']}h of {cov['elapsed_hours']}h). "
             f"Eligible: {result['eligible_count']} of {result['considered_count']} members with activity.\n")
    if result["warnings"]:
        L.append("**Warnings**\n")
        L.extend(f"- {w}" for w in result["warnings"])
        L.append("")
    L.append("Weights used (after redistribution): " + ", ".join(f"{k} {v:.0%}" for k, v in result["weights"].items()) + "\n")
    L.append(md_table(
        ["Rank", "Member", "Score", "Particip.", "Consist.", "Helpful", "Extra", "Eff. msgs", "Days", "Replies", "Reacts", "Noms", "Voice min", "Flags"],
        [(f"{r['rank']}{' (tie)' if r['tie'] else ''}", f"{r['name']} ({r['uid']})", f"{r['score']:.1f}",
          r["components"].get("participation", "-"), r["components"].get("consistency", "-"),
          r["components"].get("helpfulness", "-"), r["components"].get("extra", "-"),
          r["effective_messages"], r["active_days"], r["replies"], r["reactions"], r["nominations"],
          round(r["voice_minutes"]), "; ".join(r["flags"]) or "")
         for r in result["shortlist"]],
    ))
    if any(r.get("ai_advisory") for r in result["shortlist"]):
        L.append("\n## AI advisory notes (not part of the score)\n")
        for r in result["shortlist"]:
            if r.get("ai_advisory"):
                a = r["ai_advisory"]
                L.append(f"- {r['name']}: rating {a.get('rating')}/5 on {a.get('n')} staff-selected message(s). {a.get('summary', '')}")
    if result["ineligible"]:
        L.append("\n## Not eligible\n")
        L.extend(f"- {reason}: {n}" for reason, n in sorted(result["ineligible"].items()))
    L.append("\n## How the score is calculated\n")
    L.append("participation = sqrt(effective messages) / sqrt(best); consistency = active days / days elapsed; "
             "helpfulness = (capped replies + 0.5 x capped reactions received + nomination weight x nominations) / best; "
             "extra = sqrt(voice minutes) / sqrt(best). Unavailable components have their weight redistributed. "
             "Duplicate and burst messages are excluded before capping.")
    return "\n".join(L)
