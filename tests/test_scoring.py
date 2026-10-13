from datetime import date, datetime, timedelta, timezone

import pytest
from conftest import make_cfg

from coldd.scoring import add_nomination, approve_winner, resolve_week, score_week
from coldd.storage import empty_dataset

WEEK = date(2026, 10, 5)  # Monday
AFTER = int(datetime(2026, 10, 13, tzinfo=timezone.utc).timestamp())  # week complete


def put(data, uid, day_offset, **rec):
    day = (WEEK + timedelta(days=day_offset)).isoformat()
    data["days"].setdefault(day, {"u": {}})["u"][str(uid)] = rec
    data["members"].setdefault(str(uid), {"n": f"user{uid}", "f": 1})


def full_coverage(data):
    for i in range(7):
        data["server"][(WEEK + timedelta(days=i)).isoformat()] = {"cov": 86400}


@pytest.fixture
def data():
    d = empty_dataset()
    full_coverage(d)
    return d


def test_resolve_week():
    cfg = make_cfg()
    assert resolve_week(cfg, date(2026, 10, 8), 0) == WEEK
    assert resolve_week(cfg, date(2026, 10, 8), -1) == WEEK - timedelta(weeks=1)
    sunday_cfg = make_cfg(week_start_day=6)
    assert resolve_week(sunday_cfg, date(2026, 10, 8), 0) == date(2026, 10, 4)


def test_scores_normalised_and_weights_redistributed(data):
    cfg = make_cfg(min_weekly_messages=1, min_active_days=1)
    for d in range(7):
        put(data, 1, d, m=20)
    for d in range(3):
        put(data, 2, d, m=10)
    r = score_week(data, cfg, WEEK, AFTER)
    # No replies/reactions/nominations/voice => only participation + consistency, renormalised.
    assert set(r["available_components"]) == {"participation", "consistency"}
    assert r["weights"]["participation"] == pytest.approx(0.5 / 0.7, abs=1e-3)
    assert r["weights"]["helpfulness"] == 0 and r["weights"]["extra"] == 0
    top = r["shortlist"][0]
    assert top["uid"] == "1" and top["score"] == pytest.approx(100.0)
    second = r["shortlist"][1]
    expected = 100 * ((0.5 / 0.7) * (30 ** 0.5 / 140 ** 0.5) + (0.2 / 0.7) * (3 / 7))
    assert second["score"] == pytest.approx(expected, abs=0.01)
    assert any("helpfulness" in w for w in r["warnings"])


def test_helpfulness_and_voice_used_when_available(data):
    cfg = make_cfg(min_weekly_messages=1, min_active_days=1)
    put(data, 1, 0, m=10, r=4, rx=2, v=600)
    put(data, 2, 0, m=10)
    r = score_week(data, cfg, WEEK, AFTER)
    assert set(r["available_components"]) == {"participation", "consistency", "helpfulness", "extra"}
    by = {c["uid"]: c for c in r["shortlist"]}
    assert by["1"]["components"]["helpfulness"] == 1.0
    assert by["2"]["components"]["helpfulness"] == 0.0
    assert by["1"]["score"] > by["2"]["score"]


def test_daily_cap_and_spam_excluded(data):
    cfg = make_cfg(min_weekly_messages=1, min_active_days=1, daily_message_cap=50)
    put(data, 1, 0, m=500, d=100, b=300)  # effective = 100, capped to 50
    r = score_week(data, cfg, WEEK, AFTER)
    c = r["shortlist"][0]
    assert c["effective_messages"] == 50
    assert any("duplicates/bursts" in f for f in c["flags"])


def test_eligibility_rules(data):
    cfg = make_cfg(min_weekly_messages=10, min_active_days=2, winner_cooldown_weeks=2)
    put(data, 1, 0, m=20); put(data, 1, 1, m=20)                      # eligible
    put(data, 2, 0, m=5); put(data, 2, 1, m=4)                        # too few messages
    put(data, 3, 0, m=50)                                             # one active day
    put(data, 4, 0, m=20); put(data, 4, 1, m=20); data["members"]["4"]["b"] = 1
    put(data, 5, 0, m=20); put(data, 5, 1, m=20); data["members"]["5"]["e"] = 1
    put(data, 6, 0, m=20); put(data, 6, 1, m=20); data["members"]["6"].update(j=10, x=20)
    put(data, 7, 0, m=20); put(data, 7, 1, m=20)
    data["awards"][(WEEK - timedelta(weeks=1)).isoformat()] = {"winner": "7"}
    r = score_week(data, cfg, WEEK, AFTER)
    assert [c["uid"] for c in r["shortlist"]] == ["1"]
    assert r["eligible_count"] == 1 and r["considered_count"] == 7
    reasons = " ".join(r["ineligible"])
    for needle in ("bot", "excluded role", "left", "won within", "effective messages", "active on fewer"):
        assert needle in reasons


def test_tie_detection_and_deterministic_order(data):
    cfg = make_cfg(min_weekly_messages=1, min_active_days=1)
    put(data, 9, 0, m=10)
    put(data, 3, 0, m=10)
    r = score_week(data, cfg, WEEK, AFTER)
    assert [c["uid"] for c in r["shortlist"]] == ["3", "9"]
    assert all(c["tie"] for c in r["shortlist"])
    assert any("tied" in w for w in r["warnings"])


def test_shortlist_size(data):
    cfg = make_cfg(min_weekly_messages=1, min_active_days=1, shortlist_size=5)
    for u in range(20):
        put(data, u + 1, 0, m=u + 1)
    r = score_week(data, cfg, WEEK, AFTER)
    assert len(r["shortlist"]) == 5 and r["eligible_count"] == 20


def test_coverage_warnings_for_incomplete_week():
    data = empty_dataset()
    data["server"][WEEK.isoformat()] = {"cov": 3600}
    data["meta"]["collection_start"] = int(datetime(2026, 10, 5, 12, tzinfo=timezone.utc).timestamp())
    data["meta"]["gaps"] = [[int(datetime(2026, 10, 6, tzinfo=timezone.utc).timestamp()),
                             int(datetime(2026, 10, 7, tzinfo=timezone.utc).timestamp())]]
    now = int(datetime(2026, 10, 7, 12, tzinfo=timezone.utc).timestamp())
    r = score_week(data, make_cfg(), WEEK, now)
    text = " ".join(r["warnings"])
    assert "still in progress" in text and "coverage" in text.lower()
    assert "part-way" in text and "gap" in text
    assert r["elapsed_days"] == 3


def test_min_active_days_relaxed_for_partial_week():
    data = empty_dataset()
    cfg = make_cfg(min_weekly_messages=1, min_active_days=3)
    put(data, 1, 0, m=5)
    now = int(datetime(2026, 10, 5, 18, tzinfo=timezone.utc).timestamp())  # only day 1 elapsed
    r = score_week(data, cfg, WEEK, now)
    assert r["eligible_count"] == 1


def test_nominations_count_once_per_staff(data):
    cfg = make_cfg(min_weekly_messages=1, min_active_days=1)
    put(data, 1, 0, m=10)
    put(data, 2, 0, m=10)
    wk = WEEK.isoformat()
    assert add_nomination(data, wk, 2, 900, "helped newcomers") == 1
    assert add_nomination(data, wk, 2, 900, "again") == 1
    assert add_nomination(data, wk, 2, 901, "") == 2
    r = score_week(data, cfg, WEEK, AFTER)
    assert r["shortlist"][0]["uid"] == "2" and r["shortlist"][0]["nominations"] == 2


def test_approve_winner_manual_and_protected(data):
    cfg = make_cfg(min_weekly_messages=1, min_active_days=1)
    put(data, 1, 0, m=10)
    r = score_week(data, cfg, WEEK, AFTER)
    ok, msg = approve_winner(data, cfg, r, 1, 900, "great week")
    assert ok and "manual" in msg
    a = data["awards"][WEEK.isoformat()]
    assert a["winner"] == "1" and a["paid"] is False and a["amount"] == 100 and a["on_shortlist"]
    ok, msg = approve_winner(data, cfg, r, 2, 900)
    assert not ok and "already" in msg
    ok, msg = approve_winner(data, cfg, r, 2, 900, replace=True)
    assert ok and "not on the computed shortlist" in msg


def test_weights_configurable(data):
    cfg = make_cfg(min_weekly_messages=1, min_active_days=1)
    cfg.weights.participation, cfg.weights.consistency = 0.0, 1.0
    put(data, 1, 0, m=60)
    put(data, 2, 0, m=1); put(data, 2, 1, m=1)
    r = score_week(data, cfg, WEEK, AFTER)
    assert r["shortlist"][0]["uid"] == "2"
