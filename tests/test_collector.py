from datetime import date, datetime, timezone

from conftest import make_cfg

from coldd.collector import Collector, SpamTracker, content_fingerprint
from coldd.storage import Store


def ts(y, mo, d, h=12, mi=0, s=0):
    return int(datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc).timestamp())


T = ts(2026, 10, 12)  # Monday 12:00 UTC


def test_message_aggregation(collector, data):
    for i in range(3):
        collector.record_message(10, 500, T + i * 100, name="alice", fingerprint=f"f{i}", message_id=i)
    collector.record_message(11, 501, T, name="bob", message_id=99)
    u = data["days"]["2026-10-12"]["u"]
    assert u["10"]["m"] == 3 and u["10"]["c"] == {"500": 3}
    assert u["11"]["m"] == 1
    srv = data["server"]["2026-10-12"]
    assert srv["m"] == 4 and srv["ch"] == {"500": 3, "501": 1} and srv["h"][12] == 4
    assert data["members"]["10"]["n"] == "alice" and data["members"]["10"]["l"] == T + 200
    assert "d" not in u["10"]  # zero counters are omitted


def test_bots_not_counted_by_default(collector, data):
    collector.record_message(20, 500, T, is_bot=True)
    assert "20" not in data["members"]
    assert data["days"] == {}
    assert data["server"]["2026-10-12"]["bm"] == 1


def test_bots_counted_when_configured(data):
    c = Collector(data, make_cfg(count_bots=True))
    c.record_message(20, 500, T, is_bot=True)
    assert data["days"]["2026-10-12"]["u"]["20"]["m"] == 1
    assert data["members"]["20"]["b"] == 1


def test_timezone_day_boundary(data):
    c = Collector(data, make_cfg(timezone_name="America/New_York"))
    # 2026-10-13 02:30 UTC is still 2026-10-12 22:30 in New York.
    c.record_message(1, 5, ts(2026, 10, 13, 2, 30))
    assert "2026-10-12" in data["days"] and "2026-10-13" not in data["days"]
    assert data["server"]["2026-10-12"]["h"][22] == 1


def test_utc_boundary(collector, data):
    collector.record_message(1, 5, ts(2026, 10, 12, 23, 59, 59))
    collector.record_message(1, 5, ts(2026, 10, 13, 0, 0, 0))
    assert data["days"]["2026-10-12"]["u"]["1"]["m"] == 1
    assert data["days"]["2026-10-13"]["u"]["1"]["m"] == 1


def test_duplicate_detection(collector, data):
    fp = content_fingerprint("Free nitro!!")
    assert fp == content_fingerprint("  free   NITRO!! ")
    collector.record_message(1, 5, T, fingerprint=fp)
    collector.record_message(1, 5, T + 30, fingerprint=fp)
    collector.record_message(1, 5, T + 30 + 700, fingerprint=fp)  # outside 600s window
    collector.record_message(2, 5, T + 31, fingerprint=fp)  # other user: not a duplicate
    assert data["days"]["2026-10-12"]["u"]["1"]["d"] == 1
    assert "d" not in data["days"]["2026-10-12"]["u"]["2"]


def test_burst_detection():
    t = SpamTracker(dup_window=600, burst_window=60, burst_threshold=5)
    flags = [t.check(1, T + i, None)[1] for i in range(8)]
    assert flags == [False] * 5 + [True] * 3
    assert t.check(1, T + 500, None) == (False, False)


def test_spam_tracker_bounded_and_swept():
    t = SpamTracker(600, 60, 5)
    for u in range(100):
        t.check(u, T, None)
    assert len(t) == 100
    t.sweep(T + 10_000)
    assert len(t) == 0


def test_delete_reverses_counts(collector, data):
    fp = content_fingerprint("same")
    collector.record_message(1, 5, T, fingerprint=fp, message_id=100, reply_to_other=True)
    collector.record_message(1, 5, T + 1, fingerprint=fp, message_id=101)
    assert collector.record_delete(101)
    u = data["days"]["2026-10-12"]["u"]["1"]
    assert u["m"] == 1 and "d" not in u and u["r"] == 1
    assert collector.record_delete(100)
    assert "m" not in u and "c" not in u and "r" not in u
    srv = data["server"]["2026-10-12"]
    assert "m" not in srv and srv["del"] == 2 and srv["h"][12] == 0
    assert not collector.record_delete(100)  # double delete is a no-op
    assert not collector.record_delete(999)  # unknown message


def test_recent_cache_bounded(data):
    c = Collector(data, make_cfg(recent_message_cache=10))
    for i in range(50):
        c.record_message(1, 5, T + i, message_id=i)
    assert len(c.recent) == 10 and 49 in c.recent and 0 not in c.recent


def test_recent_cache_expires(data):
    c = Collector(data, make_cfg(recent_message_hours=1))
    c.record_message(1, 5, T, message_id=1)
    c.record_message(1, 5, T + 7000, message_id=2)
    c.expire_recent(T + 7200)
    assert list(c.recent) == [2]


def test_reactions(collector, data):
    collector.record_message(1, 5, T, message_id=100)
    assert collector.record_reaction(100, 2, T, True)
    assert not collector.record_reaction(100, 2, T, True)  # same member twice: once
    assert not collector.record_reaction(100, 1, T, True)  # self-reaction ignored
    assert collector.record_reaction(100, 3, T, True)
    assert data["days"]["2026-10-12"]["u"]["1"]["rx"] == 2
    assert collector.record_reaction(100, 2, T, False)
    assert data["days"]["2026-10-12"]["u"]["1"]["rx"] == 1
    # reactions given: member 2 added then removed (back to 0), member 3 added once, self-reaction counts for nothing
    assert "rg" not in data["days"]["2026-10-12"]["u"]["2"]
    assert data["days"]["2026-10-12"]["u"]["3"]["rg"] == 1
    assert "rg" not in data["days"]["2026-10-12"]["u"]["1"]
    # uncached message: no rx credit to anyone, but the reaction still counts as given
    assert collector.record_reaction(555, 2, T, True)
    assert data["days"]["2026-10-12"]["u"]["2"]["rg"] == 1
    assert data["days"]["2026-10-12"]["u"]["1"]["rx"] == 1


def test_reactions_disabled(data):
    c = Collector(data, make_cfg(reactions_enabled=False))
    c.record_message(1, 5, T, message_id=100)
    assert not c.record_reaction(100, 2, T, True)


def test_join_leave_rejoin(collector, data):
    collector.record_join(5, T, "eve")
    collector.record_leave(5, T + 10)
    assert data["members"]["5"]["x"] == T + 10
    collector.record_join(5, T + 20)
    assert "x" not in data["members"]["5"]
    srv = data["server"]["2026-10-12"]
    assert srv["j"] == 2 and srv["lv"] == 1
    collector.record_join(6, T, "bot", is_bot=True)
    assert srv["jb"] == 1 and srv["j"] == 2


def test_channel_filters():
    c = Collector({}, make_cfg(exclude_channel_ids={9}))
    assert c.channel_allowed(1, None) and not c.channel_allowed(1, 9)  # 9 may be a category
    c2 = Collector({}, make_cfg(include_channel_ids={7}))
    assert c2.channel_allowed(7) and c2.channel_allowed(3, 7) and not c2.channel_allowed(3)


def test_heartbeat_coverage_and_gaps(collector, data):
    collector.heartbeat(T, True, 60)
    assert data["meta"]["collection_start"] == T
    collector.heartbeat(T + 60, True, 60)
    collector.heartbeat(T + 120, True, 60)
    assert data["server"]["2026-10-12"]["cov"] == 120
    collector.heartbeat(T + 180, False, 60)  # disconnected: nothing credited
    collector.heartbeat(T + 3600, True, 60)  # long silence => gap
    assert data["meta"]["gaps"] == [[T + 120, T + 3600]]
    assert data["server"]["2026-10-12"]["cov"] == 120


def test_mark_reconnected_records_gap(collector, data):
    collector.mark_reconnected(None, T)
    assert data["meta"]["collection_start"] == T and data["meta"]["gaps"] == []
    collector.mark_reconnected(T + 100, T + 130)  # short blip: no gap
    assert data["meta"]["gaps"] == []
    collector.mark_reconnected(T + 200, T + 900)
    assert data["meta"]["gaps"] == [[T + 200, T + 900]]
    assert data["meta"]["last_heartbeat"] == T + 900


def test_voice_sessions(collector, data):
    collector.credit_voice({1: "a", 2: "b"}, 0, T)
    collector.credit_voice({1: "a", 2: "b"}, 60, T + 60)
    collector.credit_voice({1: "a"}, 60, T + 120)
    collector.credit_voice({}, 60, T + 180)
    collector.credit_voice({1: "a"}, 60, T + 240)
    u = data["days"]["2026-10-12"]["u"]
    assert u["1"]["v"] == 180 and u["1"]["vs"] == 2
    assert u["2"]["v"] == 60 and u["2"]["vs"] == 1


def test_voice_disabled(data):
    c = Collector(data, make_cfg(voice_enabled=False))
    c.credit_voice({1: "a"}, 60, T)
    assert data["days"] == {}


def test_prune_snapshots_and_removes(collector, data):
    collector.record_message(1, 5, ts(2026, 6, 1))
    collector.record_message(2, 5, ts(2026, 6, 1))
    collector.record_message(2, 6, ts(2026, 6, 1))
    collector.record_message(1, 5, ts(2026, 1, 1))
    collector.record_message(1, 5, T)
    data["awards"]["2020-01-06"] = {"winner": "1"}
    removed = collector.prune(date(2026, 10, 12))
    assert "2026-06-01" not in data["days"] and "2026-10-12" in data["days"]
    assert data["server"]["2026-06-01"]["a"] == 2
    assert data["server"]["2026-06-01"]["chu"] == {"5": 2, "6": 1}
    assert "2026-01-01" not in data["server"]  # older than 180 days
    assert "2020-01-06" not in data["awards"]
    assert removed["member_days"] == 2 and removed["server_days"] == 1
    assert collector.prune(date(2026, 10, 12)) == {}  # once per day


def test_prune_inactive_members_but_keep_winners(collector, data):
    data["members"]["1"] = {"f": 1000, "l": 1000}
    data["members"]["2"] = {"f": 1000, "l": 1000}
    data["awards"]["2026-10-05"] = {"winner": "2"}
    collector.prune(date(2026, 10, 12), force=True)
    assert "1" not in data["members"] and "2" in data["members"]


def test_forget_member(collector, data):
    collector.record_message(1, 5, T, message_id=1)
    data["nominations"]["2026-10-12"] = {"1": {"c": 1}}
    data["awards"]["2026-10-05"] = {"winner": "1", "shortlist": [["1", 90], ["2", 80]]}
    n = collector.forget_member(1)
    assert n >= 3
    assert "1" not in data["members"] and "1" not in data["days"]["2026-10-12"]["u"]
    assert data["awards"]["2026-10-05"]["winner"] == "forgotten"
    assert data["awards"]["2026-10-05"]["shortlist"] == [["2", 80]]
    assert not collector.recent


def test_imported_messages_labelled(collector, data):
    collector.record_message(1, 5, T, imported=True, flags_override=(False, False), message_id=7)
    assert data["days"]["2026-10-12"]["u"]["1"]["i"] == 1
    assert data["server"]["2026-10-12"]["im"] == 1
    assert 7 not in collector.recent  # imports never enter the live delete map
    assert data["meta"]["last_event"] is None


def test_restart_recovery_continues_counts(tmp_path):
    cfg = make_cfg()
    s = Store(tmp_path / "data.json")
    data = s.open()
    c = Collector(data, cfg)
    c.record_message(1, 5, T)
    s.mark_dirty()
    s.save()
    s.close()  # simulated restart
    s2 = Store(tmp_path / "data.json")
    data2 = s2.open()
    c2 = Collector(data2, cfg)
    c2.record_message(1, 5, T + 10)
    assert data2["days"]["2026-10-12"]["u"]["1"]["m"] == 2
    s2.close()
