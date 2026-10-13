import csv
import io
from datetime import date, datetime, timedelta, timezone

from conftest import make_cfg

from coldd import analytics
from coldd.collector import Collector
from coldd.scoring import score_week
from coldd.storage import empty_dataset

TODAY = date(2026, 10, 12)


def ts(d: date, h=12):
    return int(datetime(d.year, d.month, d.day, h, tzinfo=timezone.utc).timestamp())


def build():
    cfg = make_cfg(min_weekly_messages=1, min_active_days=1)
    data = empty_dataset()
    c = Collector(data, cfg)
    for i in range(14):
        day = TODAY - timedelta(days=i)
        data["server"].setdefault(day.isoformat(), {})["cov"] = 86400
        c.record_message(1, 500, ts(day), name="alice")
        c.record_message(1, 501, ts(day, 13), name="alice")
        if i < 7:
            c.record_message(2, 500, ts(day), name="=cmd|' /C calc'!A0")
    c.record_message(3, 502, ts(TODAY - timedelta(days=10)), name="carol")
    c.record_join(4, ts(TODAY - timedelta(days=2)), "newbie")
    c.record_message(4, 500, ts(TODAY - timedelta(days=1)), name="newbie")
    c.record_leave(5, ts(TODAY - timedelta(days=1)))
    data["channels"] = {"500": {"n": "general"}, "501": {"n": "help"}, "502": {"n": "memes"}}
    return cfg, data


def test_summary_unique_counts_once():
    cfg, data = build()
    s = analytics.summary(data, cfg, 7, TODAY, now=ts(TODAY, 23))
    assert s["unique_active"] == 3  # users 1, 2, 4 each counted once despite many days
    assert s["messages"] == 7 * 2 + 7 + 1
    assert s["unique_active_prev"] == 2  # users 1 and 3
    assert s["returning"] == 1
    assert s["joins"] == 1 and s["leaves"] == 1


def test_leaderboard_orders_and_excludes_bots():
    cfg, data = build()
    data["members"]["2"]["b"] = 1
    rows = analytics.leaderboard(data, cfg, 7, TODAY)
    assert [r["uid"] for r in rows] == ["1", "4"]
    assert rows[0]["value"] == 14 and rows[0]["days"] == 7


def test_channel_stats_unique_members():
    cfg, data = build()
    rows = {r["cid"]: r for r in analytics.channel_stats(data, cfg, 7, TODAY)}
    assert rows["500"]["messages"] == 15 and rows["500"]["unique"] == 3
    assert rows["501"]["unique"] == 1
    assert rows["502"]["messages"] == 0 and rows["502"]["prev"] == 1


def test_member_stats():
    cfg, data = build()
    st = analytics.member_stats(data, cfg, 1, 14, TODAY)
    assert st["totals"]["m"] == 28 and st["active_days"] == 14 and st["rank"] == 1
    assert analytics.member_stats(data, cfg, 999, 14, TODAY) is None


def test_audit_report_sections_and_definitions():
    cfg, data = build()
    report, fig = analytics.run_audit(data, cfg, 7, TODAY, now=ts(TODAY, 23))
    for heading in ("Executive summary", "Overall activity", "Channel performance", "Community participation",
                    "Membership trends", "Moderation and spam", "coverage", "Recommendations", "Definitions", "Limitations"):
        assert heading.lower() in report.lower()
    assert fig["unique_active"] == 3
    assert "#general" in report


def test_audit_flags_low_coverage():
    cfg, data = build()
    for k in list(data["server"]):
        data["server"][k]["cov"] = 3600
    report, fig = analytics.run_audit(data, cfg, 7, TODAY, now=ts(TODAY, 23))
    assert fig["coverage"] < 0.2
    assert "not included and was not estimated" in report


def test_audit_warns_when_previous_period_not_covered():
    cfg, data = build()  # coverage only exists for the last 14 days
    report, _ = analytics.run_audit(data, cfg, 14, TODAY, now=ts(TODAY, 23))
    assert "comparison period was only" in report


def test_audit_empty_dataset_does_not_crash():
    report, fig = analytics.run_audit(empty_dataset(), make_cfg(), 30, TODAY)
    assert fig["messages"] == 0 and "Executive summary" in report


def test_csv_export_neutralises_formulas():
    raw = analytics.to_csv(["name", "n"], [["=cmd|' /C calc'!A0", 1], ["@SUM(A1)", 2], ["normal", 3]])
    rows = list(csv.reader(io.StringIO(raw.decode())))
    assert rows[1][0].startswith("'=") and rows[2][0].startswith("'@") and rows[3][0] == "normal"


def test_weekly_markdown_formatting():
    cfg, data = build()
    r = score_week(data, cfg, date(2026, 10, 5), now=ts(TODAY, 23))
    md = analytics.weekly_markdown(r, cfg)
    assert "100 Robux" in md and "| Rank |" in md and "How the score is calculated" in md


def test_helpers():
    assert analytics.gini([5, 5, 5]) == 0
    assert analytics.gini([0, 0, 10]) == 0  # one active member: zero inequality among actives
    assert 0 < analytics.gini([1, 1, 10]) < 1
    assert analytics.top_share([10, 1, 1, 1, 1, 1, 1, 1, 1, 1], 0.1) == 10 / 19
    assert analytics.change(110, 100) == "+10.0%"
    assert analytics.sparkline([0, 1, 2]) [0] == "▁"
