"""Synthetic load test: 10,000 members, full retention window, real code paths.

    python tools/benchmark.py                 # typical: ~600 active members/day
    python tools/benchmark.py --active 2500   # stress

Writes into a temporary directory; never touches your real data.json.
Process RSS is read from the OS (Linux /proc or Windows psapi). Pass --trace to
also report Python heap allocations via tracemalloc (slows every timing ~2-3x).
"""

from __future__ import annotations

import argparse
import random
import sys
import tempfile
import time
import tracemalloc
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from coldd import analytics  # noqa: E402
from coldd.collector import Collector  # noqa: E402
from coldd.config import Config  # noqa: E402
from coldd.scoring import resolve_week, score_week  # noqa: E402
from coldd.storage import Store  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--members", type=int, default=10_000)
    ap.add_argument("--active", type=int, default=600, help="active members per day")
    ap.add_argument("--days", type=int, default=90, help="days of member detail to generate")
    ap.add_argument("--channels", type=int, default=40)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--trace", action="store_true", help="measure Python heap with tracemalloc (slow)")
    args = ap.parse_args()
    rng = random.Random(args.seed)
    cfg = Config(token="x.y.z", guild_id=1, timezone_name="UTC", recent_message_cache=20000)
    today = date(2026, 10, 12)

    with tempfile.TemporaryDirectory() as tmp:
        if args.trace:
            tracemalloc.start()
        print(f"{'process RSS at start':<38} {rss_mb():9.1f} MB")
        store = Store(Path(tmp) / "data.json", warn_bytes=10**10)
        data = store.open()
        c = Collector(data, cfg)
        weights = [1 / (i + 1) ** 0.8 for i in range(args.members)]  # long-tail activity
        uids = list(range(10**17, 10**17 + args.members))
        chans = list(range(5 * 10**17, 5 * 10**17 + args.channels))
        for u in uids[:2000]:
            data["members"][str(u)] = {"n": f"member{u % 100000}", "f": 1}

        events = 0
        t0 = time.perf_counter()
        mid = 0
        for back in range(args.days, 0, -1):
            day = today - timedelta(days=back - 1)
            base = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
            active = set(rng.choices(range(args.members), weights=weights, k=args.active * 2))
            active_list = list(active)[: args.active]
            for idx in active_list:
                n = max(1, int(rng.expovariate(1 / 8)))
                fav = rng.sample(chans, k=min(3, len(chans)))
                for _ in range(n):
                    ts = base + rng.randrange(86400)
                    mid += 1
                    c.record_message(uids[idx], rng.choice(fav), ts, name=f"member{idx}",
                                     fingerprint=f"{rng.randrange(50)}", reply_to_other=rng.random() < 0.2, message_id=mid)
                    events += 1
            for _ in range(rng.randrange(5, 40)):
                c.record_join(uids[rng.randrange(args.members)], base + rng.randrange(86400))
            data["server"].setdefault(day.isoformat(), {})["cov"] = 86400
        ingest = time.perf_counter() - t0
        # Older compact server days (beyond member retention) as they would exist after pruning.
        for back in range(args.days, cfg.server_retention_days):
            d = (today - timedelta(days=back)).isoformat()
            data["server"][d] = {"m": 4000, "a": args.active, "cov": 86400, "h": [160] * 24,
                                 "ch": {str(ch): 100 for ch in chans}, "chu": {str(ch): 40 for ch in chans}}
        _, peak_ingest = tracemalloc.get_traced_memory() if args.trace else (0, 0)
        print(f"{'process RSS after ingest':<38} {rss_mb():9.1f} MB")

        def timed(label: str, fn):  # type: ignore[no-untyped-def]
            s = time.perf_counter()
            out = fn()
            print(f"{label:<38} {1000 * (time.perf_counter() - s):9.1f} ms")
            return out

        print(f"members={args.members:,} active/day={args.active:,} detail days={args.days} channels={args.channels}")
        print(f"{'ingest ' + format(events, ',') + ' message events':<38} {ingest * 1000:9.1f} ms "
              f"({events / ingest:,.0f} events/s)")
        store.mark_dirty()
        timed("save data.json (serialize+fsync)", lambda: store.save())
        size = store.path.stat().st_size
        print(f"{'data.json size':<38} {size / 1048576:9.2f} MB")
        member_days = sum(len(d["u"]) for d in data["days"].values())
        print(f"{'member-day records':<38} {member_days:9,}")
        print(f"{'members stored':<38} {len(data['members']):9,}")
        store.close()
        s2 = Store(store.path)
        timed("load + validate data.json", s2.open)
        s2.close()
        week = resolve_week(cfg, today, -1)
        r = timed("weekly shortlist (last week)", lambda: score_week(data, cfg, week, now=int(time.time())))
        print(f"{'  eligible candidates':<38} {r['eligible_count']:9,}")
        timed("/analytics 30 days", lambda: analytics.summary(data, cfg, 30, today))
        timed("/leaderboard 30 days", lambda: analytics.leaderboard(data, cfg, 30, today))
        timed("/channels 30 days", lambda: analytics.channel_stats(data, cfg, 30, today))
        timed("/audit 30 days", lambda: analytics.run_audit(data, cfg, 30, today))
        timed("/audit 90 days", lambda: analytics.run_audit(data, cfg, 90, today))
        timed("/audit 180 days", lambda: analytics.run_audit(data, cfg, 180, today))
        timed("daily retention prune", lambda: c.prune(today + timedelta(days=1)))
        print(f"{'process RSS at end (incl. reload copy)':<38} {rss_mb():9.1f} MB")
        if args.trace:
            current, peak = tracemalloc.get_traced_memory()
            print(f"{'python heap now / peak (tracemalloc)':<38} {current / 1048576:6.1f} / {peak / 1048576:.1f} MB "
                  f"(peak during ingest {peak_ingest / 1048576:.1f} MB)")


def rss_mb() -> float:
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class PMC(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                (n, ctypes.c_size_t) for n in ("PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                                               "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                                               "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]

        pmc = PMC()
        pmc.cb = ctypes.sizeof(PMC)
        k32, psapi = ctypes.windll.kernel32, ctypes.windll.psapi
        k32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(PMC), wintypes.DWORD]
        if psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb):
            return pmc.WorkingSetSize / 1048576
    return float("nan")


if __name__ == "__main__":
    main()
