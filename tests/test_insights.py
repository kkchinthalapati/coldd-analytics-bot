from datetime import date

from coldd import analytics


def test_insights_heatmap_stickiness_and_growth(cfg, data):
    today = date(2026, 10, 14)  # a Wednesday
    d = today.isoformat()
    # one busy hour on today (hour 9 -> block 2) and two users active
    data["server"][d] = {"j": 3, "lv": 1, "h": [0] * 9 + [5] + [0] * 14}
    data["days"][d] = {"u": {"1": {"m": 5}, "2": {"m": 1}}}
    ins = analytics.insights(data, cfg, 7, today)
    assert ins["joins"] == 3 and ins["leaves"] == 1 and ins["net"] == 2
    assert ins["wau"] == 2
    assert ins["mean_dau"] == 2 / 7
    assert ins["heatmap"][today.weekday()][2] == 5
    assert sum(sum(r) for r in ins["heatmap"]) == 5
    assert 0 <= ins["stickiness"] <= 1


def test_deep_stats_has_all_windows_and_no_identities(cfg, data):
    today = date(2026, 10, 14)
    d = today.isoformat()
    data["days"][d] = {"u": {"111": {"m": 4}, "222": {"m": 2}}}
    data["server"][d] = {"m": 6, "j": 1, "lv": 0, "h": [0] * 24, "cov": 86400, "ch": {"999": 6}}
    data["channels"]["999"] = {"n": "general"}
    out = analytics.deep_stats(data, cfg, today)
    assert set(out["windows"]) == {"7d", "30d", "90d"}
    w = out["windows"]["7d"]
    assert w["channels"][0]["name"] == "general"
    assert w["distribution_messages_per_member"]["members"] == 2
    text = repr(out)
    assert "111" not in text and "222" not in text  # no member IDs leak into the AI payload
