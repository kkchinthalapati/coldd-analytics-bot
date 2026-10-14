from datetime import date, timedelta

from coldd import analytics


def _day(today, back):
    return (today - timedelta(days=back)).isoformat()


def test_streak_and_consistency(cfg, data):
    today = date(2026, 10, 14)
    # member 1: active 5 days in a row, then a gap, then 1 more day
    for back in [0, 1, 2, 3, 4, 6]:
        data["days"].setdefault(_day(today, back), {"u": {}})["u"]["1"] = {"m": 3}
    # member 2: active on 2 non-consecutive days
    for back in [0, 5]:
        data["days"].setdefault(_day(today, back), {"u": {}})["u"]["2"] = {"m": 1}
    table = analytics.member_table(data, cfg, 7, today)
    assert table["1"]["streak"] == 5
    assert table["1"]["days"] == 6
    assert table["2"]["streak"] == 1
    assert table["2"]["days"] == 2


def test_boards_rank_reactions_and_interactions(cfg, data):
    today = date(2026, 10, 14)
    d = today.isoformat()
    data["days"][d] = {"u": {
        "1": {"m": 10, "rg": 2, "r": 1},
        "2": {"m": 4, "rg": 9, "rx": 5},
        "3": {"m": 1, "rx": 12},
    }}
    board = analytics.community_board(data, cfg, 7, today, "reacting")
    assert [r["uid"] for r in board] == ["2", "1"]
    assert board[0]["value"] == 9 and board[0]["rank"] == 1
    board = analytics.community_board(data, cfg, 7, today, "messages")
    assert board[0]["uid"] == "1"
    board = analytics.community_board(data, cfg, 7, today, "interactions")
    assert board[0]["uid"] == "2"  # member 2: 9 given + 5 received = 14; member 3: 12; member 1: 3


def test_profile_ranks(cfg, data):
    today = date(2026, 10, 14)
    data["days"][today.isoformat()] = {"u": {"1": {"m": 10}, "2": {"m": 4}}}
    p = analytics.member_profile(data, cfg, "2", 7, today)
    assert p["ranks"]["messages"] == {"rank": 2, "of": 2, "value": 4}
    assert analytics.member_profile(data, cfg, "999", 7, today) is None
