"""Scoring scope (general chat + tickets), conversation partners and presence."""

import copy
from datetime import date

from conftest import make_cfg

from coldd import analytics, recount, scoring
from coldd.collector import Collector, scored_counts
from coldd.recount import Msg
from coldd.storage import empty_dataset

GENERAL, COUNTING, TICKET = 100, 200, 300
T = 1_791_800_000  # 2026-10-12 12:53 UTC
DAY = "2026-10-12"


def scoped_cfg(**kw):
    return make_cfg(score_channel_ids={GENERAL}, score_channel_prefixes=("ticket-", "closed-"), **kw)


def collector(**kw):
    cfg = scoped_cfg(**kw)
    data = empty_dataset(now=1_700_000_000)
    col = Collector(data, cfg)
    col.note_channel(GENERAL, "general-chat")
    col.note_channel(COUNTING, "counting")
    col.note_channel(TICKET, "ticket-0042")
    return col, data, cfg


def test_only_general_chat_and_tickets_are_scored():
    col, _, _ = collector()
    assert col.is_scored(GENERAL)
    assert col.is_scored(TICKET)  # by name prefix
    assert not col.is_scored(COUNTING)
    col.note_channel(TICKET, "closed-0042")  # a closed ticket is renamed and still counts
    assert col.is_scored(TICKET)
    assert collector()[0].is_scored(999, "Ticket-777")  # prefix match ignores case


def test_everything_is_scored_without_a_scope():
    cfg = make_cfg()
    col = Collector(empty_dataset(now=1), cfg)
    assert not cfg.score_scope_active and col.is_scored(COUNTING)


def test_scored_counters_ignore_other_channels():
    col, data, cfg = collector()
    for i in range(5):
        col.record_message(1, COUNTING, T + i, fingerprint=f"n{i}")
    col.record_message(1, GENERAL, T + 10, fingerprint="a")
    col.record_message(1, TICKET, T + 20, fingerprint="b")
    rec = data["days"][DAY]["u"]["1"]
    assert rec["m"] == 7 and rec["sm"] == 2  # seven sent, two counted
    assert scored_counts(rec, cfg)[0] == 2


def test_unrebuilt_days_fall_back_to_all_channels():
    cfg = scoped_cfg()
    old = {"m": 9, "d": 1, "b": 0, "r": 2}  # written before the scope existed: no "sv" marker
    assert scored_counts(old, cfg) == (9, 1, 0, 2)
    assert scored_counts({**old, "sv": 1}, cfg) == (0, 0, 0, 0)


def test_partners_are_symmetric_and_distinct():
    col, data, _ = collector()
    col.record_message(1, GENERAL, T, partners=(2, 3))
    col.record_message(1, GENERAL, T + 5, partners=(2,))  # same person again: still one partner
    u = data["days"][DAY]["u"]
    assert len(u["1"]["pp"]) == 2
    assert len(u["2"]["pp"]) == 1 and len(u["3"]["pp"]) == 1  # the other side counts too


def test_partners_only_count_in_scored_channels():
    col, data, _ = collector()
    col.record_message(1, COUNTING, T, partners=(2,))
    assert "pp" not in data["days"][DAY]["u"]["1"]


def test_presence_tracks_distinct_hours():
    col, data, _ = collector()
    for hour in (9, 9, 14, 20):
        col.record_message(1, GENERAL, T - 12 * 3600 - 53 * 60 + hour * 3600)
    assert bin(data["days"][DAY]["u"]["1"]["sh"]).count("1") == 3


def test_deleting_a_scored_message_undoes_the_scored_counters():
    col, data, _ = collector()
    col.record_message(1, GENERAL, T, message_id=11, fingerprint="x")
    col.record_message(1, GENERAL, T + 1, message_id=12, fingerprint="y")
    col.record_delete(11)
    rec = data["days"][DAY]["u"]["1"]
    assert rec["m"] == 1 and rec["sm"] == 1


def test_rebuild_carries_scope_partners_and_is_repeatable():
    data = empty_dataset(now=1_700_000_000)
    cfg = scoped_cfg()
    msgs = [Msg(T, 1, GENERAL, "a", False, "A", False, (), True, (2,)),
            Msg(T + 1, 1, COUNTING, "b", False, "A", False, (), False, ()),
            Msg(T + 2, 2, GENERAL, "c", False, "B", False, (), True, ())]
    recount.rebuild_days(data, cfg, msgs, [DAY], now=T + 100_000)
    u = data["days"][DAY]["u"]
    assert u["1"]["m"] == 2 and u["1"]["sm"] == 1 and u["1"]["sv"] == 1
    assert len(u["1"]["pp"]) == 1 and len(u["2"]["pp"]) == 1
    snap = copy.deepcopy(data["days"])
    recount.rebuild_days(data, cfg, msgs, [DAY], now=T + 200_000)
    assert data["days"] == snap


def week_data(cfg):
    """Two members chatting on 3 days: member 1 talks to many people, member 2 only posts the same volume alone."""
    data = empty_dataset(now=1_700_000_000)
    start = date(2026, 10, 5)
    col = Collector(data, cfg)
    col.note_channel(GENERAL, "general-chat")
    for i in range(3):
        day_ts = 1_791_200_000 + i * 86_400  # a Monday onward, noon-ish UTC
        for j in range(10):
            col.record_message(1, GENERAL, day_ts + j * 3_600, fingerprint=f"1-{i}-{j}", partners=(10 + j,))
            col.record_message(2, GENERAL, day_ts + j * 60 + 7, fingerprint=f"2-{i}-{j}")
    return data, start


def test_conversation_and_presence_beat_solo_posting():
    cfg = scoped_cfg(min_weekly_messages=1, min_active_days=1)
    data, start = week_data(cfg)
    r = scoring.score_week(data, cfg, start, now=2_000_000_000)
    rows = {c["uid"]: c for c in r["shortlist"]}
    assert rows["1"]["partners"] > rows["2"]["partners"] == 0
    assert rows["1"]["presence_hours"] > rows["2"]["presence_hours"]
    assert {"conversation", "presence"} <= set(r["available_components"])
    assert r["shortlist"][0]["uid"] == "1"  # equal volume, but member 1 actually talks with people over more hours


def test_scored_scope_removes_counting_farm_from_the_ranking():
    cfg = scoped_cfg(min_weekly_messages=1, min_active_days=1)
    data = empty_dataset(now=1_700_000_000)
    col = Collector(data, cfg)
    col.note_channel(GENERAL, "general-chat")
    col.note_channel(COUNTING, "counting")
    base = 1_791_200_000
    for i in range(3):
        for j in range(5):
            col.record_message(1, GENERAL, base + i * 86_400 + j * 100, fingerprint=f"g{i}{j}")
        for j in range(100):  # member 2 farms the counting channel
            col.record_message(2, COUNTING, base + i * 86_400 + 3_000 + j * 100, fingerprint=f"c{i}{j}")
        col.record_message(2, GENERAL, base + i * 86_400 + 9_000, fingerprint=f"s{i}")
    r = scoring.score_week(data, cfg, date(2026, 10, 5), now=2_000_000_000)
    rows = {c["uid"]: c for c in r["shortlist"]}
    assert rows["1"]["effective_messages"] == 15 and rows["2"]["effective_messages"] == 3
    assert any("outside the scored channels" in f for f in rows["2"]["flags"])


def test_top_boards_expose_the_new_signals():
    cfg = scoped_cfg()
    data, _ = week_data(cfg)
    today = date(2026, 10, 14)
    board = analytics.community_board(data, cfg, 14, today, "conversation")
    assert board[0]["uid"] == "1" and board[0]["value"] > 0
    assert analytics.community_board(data, cfg, 14, today, "presence")[0]["value"] > 0
