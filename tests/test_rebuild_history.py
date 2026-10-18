"""The Discord-facing rebuild, exercised with fake channels: no network."""

import asyncio
import copy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import discord
import pytest
from conftest import make_cfg

from coldd import recount
from coldd.bot import ColddBot
from coldd.collector import Collector
from coldd.storage import Store
from coldd.timeutil import now_ts, to_local


class FakeUser:
    def __init__(self, uid, bot=False):
        self.id, self.bot = uid, bot


class FakeReaction:
    def __init__(self, users):
        self._users = users

    async def users(self, limit=None):
        for u in self._users:
            yield u


def make_msg(uid, when, reactions=(), bot=False, content="hello"):
    return SimpleNamespace(
        is_system=lambda: False, author=SimpleNamespace(id=uid, bot=bot, name=f"u{uid}", display_name=f"U{uid}"),
        webhook_id=None, reference=None, content=content, created_at=when, reactions=list(reactions),
    )


class FakeChannel:
    def __init__(self, cid, msgs, error=None):
        self.id, self.category_id, self.msgs, self.error = cid, None, msgs, error

    async def history(self, after=None, before=None, limit=None, oldest_first=True):
        if self.error:
            raise self.error
        for m in sorted(self.msgs, key=lambda x: x.created_at):
            if after < m.created_at < before:
                yield m


def http_error(cls):
    return cls(SimpleNamespace(status=403 if cls is discord.Forbidden else 500, reason="x"), "boom")


@pytest.fixture
def bot(tmp_path):
    cfg = make_cfg(data_dir=tmp_path)
    store = Store(tmp_path / "data.json")
    data = store.open()
    b = ColddBot(cfg, store, Collector(data, cfg))
    b.get_guild = lambda _gid: SimpleNamespace(id=cfg.guild_id)  # type: ignore[method-assign]
    yield b
    store.close()


def yesterday_noon(cfg):
    today = to_local(now_ts(), cfg.tz).date()
    d = today - timedelta(days=1)
    return datetime(d.year, d.month, d.day, 12, tzinfo=timezone.utc), d.isoformat(), today.isoformat()


def run(bot, channels, **kw):
    async def fake_targets(_guild):
        return channels

    bot.readable_targets = fake_targets  # type: ignore[method-assign]
    return asyncio.run(bot.rebuild_history(**kw))


def test_rebuild_counts_complete_days_and_leaves_today_alone(bot):
    when, yday, today = yesterday_noon(bot.cfg)
    bot.store.data["days"][yday] = {"u": {"1": {"m": 99}}}  # stale live value
    bot.store.data["days"][today] = {"u": {"1": {"m": 7}}}  # today belongs to live counting
    chans = [FakeChannel(10, [make_msg(1, when), make_msg(1, when + timedelta(minutes=5)), make_msg(2, when)]),
             FakeChannel(11, [make_msg(2, when + timedelta(minutes=1))])]
    res = run(bot, chans, days=1)
    d = bot.store.data
    assert d["days"][yday]["u"]["1"]["m"] == 2 and d["days"][yday]["u"]["2"]["m"] == 2
    assert d["days"][today]["u"]["1"]["m"] == 7
    assert yday in d["meta"]["recounted"] and today not in d["meta"]["recounted"]
    assert res["days_rebuilt"] == 1 and res["messages"] == 4 and res["chunks_failed"] == 0


def test_unreadable_channel_is_skipped_but_the_rest_apply(bot):
    when, yday, _ = yesterday_noon(bot.cfg)
    chans = [FakeChannel(10, [make_msg(1, when)]), FakeChannel(11, [], error=http_error(discord.Forbidden))]
    res = run(bot, chans, days=1)
    assert bot.store.data["days"][yday]["u"]["1"]["m"] == 1
    assert res["skipped_channels"] == ["11"] and res["chunks_failed"] == 0


def test_discord_error_leaves_the_data_untouched(bot):
    when, yday, _ = yesterday_noon(bot.cfg)
    bot.store.data["days"][yday] = {"u": {"1": {"m": 5}}}
    before = copy.deepcopy(bot.store.data["days"])
    chans = [FakeChannel(10, [make_msg(1, when)]), FakeChannel(11, [], error=http_error(discord.HTTPException))]
    res = run(bot, chans, days=1)
    assert bot.store.data["days"] == before  # nothing half-applied
    assert yday not in bot.store.data["meta"].get("recounted", {})
    assert res["chunks_failed"] == 1 and res["days_rebuilt"] == 0


def test_rebuild_is_repeatable(bot):
    when, yday, _ = yesterday_noon(bot.cfg)
    chans = [FakeChannel(10, [make_msg(1, when), make_msg(2, when + timedelta(minutes=2))])]
    run(bot, chans, days=1)
    first = copy.deepcopy(bot.store.data["days"])
    run(bot, chans, days=1)
    assert bot.store.data["days"] == first


def test_reactions_skip_bots_and_the_author(bot):
    when, yday, _ = yesterday_noon(bot.cfg)
    reactions = [FakeReaction([FakeUser(2), FakeUser(3), FakeUser(1), FakeUser(99, bot=True)]),
                 FakeReaction([FakeUser(2)])]  # member 2 reacted twice with different emoji: counts once
    chans = [FakeChannel(10, [make_msg(1, when, reactions=reactions)])]
    res = run(bot, chans, days=1, reaction_days=30)
    u = bot.store.data["days"][yday]["u"]
    assert u["1"]["rx"] == 2 and "rg" not in u["1"]  # received from 2 and 3 only
    assert u["2"]["rg"] == 1 and u["3"]["rg"] == 1 and "99" not in u
    assert res["reaction_warnings"] == 0


def test_a_backup_is_taken_before_counters_change(bot):
    when, yday, _ = yesterday_noon(bot.cfg)
    bot.store.write(bot.store.serialize())
    run(bot, [FakeChannel(10, [make_msg(1, when)])], days=1)
    assert bot.store.backups(), "expected a safety backup"


def test_days_are_capped_at_retention(bot):
    when, yday, _ = yesterday_noon(bot.cfg)
    res = run(bot, [FakeChannel(10, [make_msg(1, when)])], days=100000)
    assert res["days_requested"] == bot.cfg.member_retention_days
    assert recount.verification(bot.store.data, [yday], to_local(now_ts(), bot.cfg.tz).date())["all_verified"]
