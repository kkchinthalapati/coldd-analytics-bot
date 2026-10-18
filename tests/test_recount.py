from datetime import date

from conftest import make_cfg

from coldd import recount
from coldd.recount import Msg
from coldd.storage import empty_dataset

DAY1, DAY2 = "2026-10-12", "2026-10-13"
T1 = 1_791_800_000  # falls on 2026-10-12 UTC
T2 = T1 + 86_400  # 2026-10-13 UTC


def fresh():
    return empty_dataset(now=1_700_000_000), make_cfg()


def msg(ts, uid, cid=10, fp=None, reply=False, name=None, bot=False, reactors=()):
    return Msg(ts, uid, cid, fp, reply, name, bot, tuple(reactors))


def test_rebuild_matches_the_messages_and_is_idempotent():
    data, cfg = fresh()
    msgs = [msg(T1 + i * 100, 1, name="Ann") for i in range(5)] + [msg(T1 + 700, 2, cid=11, name="Bob")]
    recount.rebuild_days(data, cfg, msgs, [DAY1, DAY2], now=T2 + 10)
    assert data["days"][DAY1]["u"]["1"]["m"] == 5
    assert data["days"][DAY1]["u"]["2"]["m"] == 1
    assert data["server"][DAY1]["m"] == 6 and sum(data["server"][DAY1]["h"]) == 6
    first = (dict(data["days"]), dict(data["server"]))
    import copy
    snap = copy.deepcopy(first)
    recount.rebuild_days(data, cfg, msgs, [DAY1, DAY2], now=T2 + 20)
    assert (data["days"], data["server"]) == snap  # same counts the second time


def test_rebuild_removes_stale_and_deleted_messages():
    data, cfg = fresh()
    data["days"][DAY1] = {"u": {"1": {"m": 50, "c": {"10": 50}}, "9": {"m": 7, "rx": 3}}}
    data["server"][DAY1] = {"m": 57, "h": [57] + [0] * 23, "ch": {"10": 57}, "cov": 86400, "j": 4}
    recount.rebuild_days(data, cfg, [msg(T1, 1)], [DAY1], now=T2)
    assert data["days"][DAY1]["u"]["1"]["m"] == 1  # was 50: the other messages no longer exist in Discord
    assert "9" not in data["days"][DAY1]["u"]  # a member with no remaining messages and no reactions disappears
    assert data["server"][DAY1]["m"] == 1
    assert data["server"][DAY1]["cov"] == 86400 and data["server"][DAY1]["j"] == 4  # non-message fields kept


def test_voice_and_other_days_are_untouched():
    data, cfg = fresh()
    data["days"][DAY1] = {"u": {"1": {"m": 3, "v": 600, "vs": 2}}}
    data["days"][DAY2] = {"u": {"1": {"m": 9}}}
    recount.rebuild_days(data, cfg, [msg(T1, 1)], [DAY1], now=T2)
    assert data["days"][DAY1]["u"]["1"]["v"] == 600 and data["days"][DAY1]["u"]["1"]["vs"] == 2
    assert data["days"][DAY2]["u"]["1"]["m"] == 9  # not part of the rebuilt days


def test_duplicates_and_bursts_are_flagged_chronologically():
    data, cfg = fresh()
    msgs = [msg(T1 + 10, 1, fp="same"), msg(T1 + 20, 1, fp="same"), msg(T1 + 30, 1, fp="other")]
    recount.rebuild_days(data, cfg, msgs, [DAY1], now=T2)
    rec = data["days"][DAY1]["u"]["1"]
    assert rec["m"] == 3 and rec["d"] == 1


def test_bot_messages_do_not_count_as_members():
    data, cfg = fresh()
    recount.rebuild_days(data, cfg, [msg(T1, 5, bot=True), msg(T1 + 1, 1)], [DAY1], now=T2)
    assert "5" not in data["days"][DAY1]["u"]
    assert data["server"][DAY1]["bm"] == 1 and data["server"][DAY1]["m"] == 1


def test_reactions_given_and_received_are_rebuilt_per_message_day():
    data, cfg = fresh()
    data["days"][DAY1] = {"u": {"1": {"m": 1, "rx": 99, "rg": 99}}}  # stale live values
    msgs = [msg(T1, 1, reactors=[2, 3]), msg(T1 + 5, 2, reactors=[1])]
    recount.rebuild_days(data, cfg, msgs, [DAY1], now=T2)
    u = data["days"][DAY1]["u"]
    assert u["1"]["rx"] == 2 and u["1"]["rg"] == 1
    assert u["2"]["rx"] == 1 and u["2"]["rg"] == 1
    assert u["3"]["rg"] == 1 and "m" not in u["3"]


def test_exclusion_flags_and_last_event_survive():
    data, cfg = fresh()
    data["members"]["1"] = {"f": 1, "e": 1}
    data["meta"]["last_event"] = 1_791_999_999
    recount.rebuild_days(data, cfg, [msg(T1, 1), msg(T1 + 1, 2)], [DAY1], now=T2)
    assert data["members"]["1"].get("e") == 1 and "e" not in data["members"]["2"]
    assert data["meta"]["last_event"] == 1_791_999_999


def test_verification_tracks_only_completed_days():
    data, cfg = fresh()
    recount.rebuild_days(data, cfg, [], [DAY1], now=T2)
    v = recount.verification(data, [DAY1, DAY2], today=date(2026, 10, 13))
    assert v["complete_days"] == 1 and v["verified_days"] == 1 and not v["all_verified"]
    recount.rebuild_days(data, cfg, [], [DAY2], now=T2 + 86_400)
    v = recount.verification(data, [DAY1, DAY2], today=date(2026, 10, 14))
    assert v["all_verified"] and v["oldest_verified_at"] == T2


def test_last_complete_days_ends_yesterday():
    assert recount.last_complete_days(date(2026, 10, 14), 3) == ["2026-10-11", "2026-10-12", "2026-10-13"]
