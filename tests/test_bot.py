"""Discord-layer tests using fake objects: no network, no real server."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from conftest import make_cfg

from coldd.bot import ColddBot, build_intents
from coldd.collector import Collector
from coldd.storage import Store


class FakeResponse:
    def __init__(self):
        self.sent = []
        self._done = False

    async def send_message(self, content=None, **kw):
        self.sent.append((content, kw))
        self._done = True

    async def defer(self, **kw):
        self._done = True

    def is_done(self):
        return self._done


class FakeFollowup:
    def __init__(self):
        self.sent = []

    async def send(self, content=None, **kw):
        self.sent.append((content, kw))


def interaction(user_id, guild_owner=999):
    return SimpleNamespace(
        user=SimpleNamespace(id=user_id, roles=[], guild_permissions=SimpleNamespace(manage_guild=False)),
        guild=SimpleNamespace(owner_id=guild_owner),
        response=FakeResponse(),
        followup=FakeFollowup(),
        command=None,
    )


@pytest.fixture
def bot(tmp_path):
    cfg = make_cfg(data_dir=tmp_path)
    store = Store(tmp_path / "data.json")
    data = store.open()
    b = ColddBot(cfg, store, Collector(data, cfg))
    yield b
    store.close()


def test_intents_least_privilege():
    i = build_intents(make_cfg(message_content_intent=False, voice_enabled=False, reactions_enabled=False))
    assert i.members and i.guild_messages and not i.message_content and not i.voice_states
    assert not i.presences and not i.typing and not i.dm_messages and not i.guild_reactions


def test_expected_commands_registered(bot):
    names = {c.name for c in bot.tree.get_commands()}
    assert names == {"help", "analytics", "insights", "top", "profile", "motw", "weekly", "deepscan", "health",
                     "export", "import_history", "forget"}
    weekly = bot.tree.get_command("weekly")
    assert {c.name for c in weekly.commands} == {"show", "approve", "paid", "history", "nominate", "ai_review"}


@pytest.mark.parametrize("name,kwargs", [
    ("health", {}), ("analytics", {"days": 7}), ("insights", {"days": 30}), ("top", {"metric": "messages", "days": 7}),
])
def test_commands_run_without_role_or_owner_gate(bot, name, kwargs):
    """Access is controlled by Discord's integration settings, not by checks in the bot."""
    inter = interaction(2)
    asyncio.run(bot.tree.get_command(name).callback(inter, **kwargs))
    assert inter.response.sent, "command produced no response"
    assert "restricted" not in str(inter.response.sent[0][0])


def test_health_reports_verification_and_stays_ephemeral(bot):
    inter = interaction(1)
    asyncio.run(bot.tree.get_command("health").callback(inter))
    _, kw = inter.response.sent[0]
    assert kw["ephemeral"] and "Bot health" in kw["embed"].title
    assert "Verified against Discord" in kw["embed"].description


def test_owner_data_export_is_gzip(bot):
    import gzip
    import json

    inter = interaction(1)
    bot.collector.dirty = True
    asyncio.run(bot.tree.get_command("export").callback(inter, kind="data", days=30, week=-1))
    f = inter.followup.sent[0][1]["file"]
    payload = json.loads(gzip.decompress(f.fp.read()))
    assert payload["schema_version"] == 1


def fake_message(author_id=10, bot_author=False, webhook=None, guild_id=111, channel_id=500, system=False, content="hi"):
    author = SimpleNamespace(id=author_id, bot=bot_author, name="n", display_name="Name")
    return SimpleNamespace(
        guild=SimpleNamespace(id=guild_id),
        is_system=lambda: system,
        channel=SimpleNamespace(id=channel_id, category_id=None, name="general"),
        author=author, webhook_id=webhook, reference=None, content=content, id=12345,
        created_at=datetime(2026, 10, 12, 12, tzinfo=timezone.utc),
    )


def test_on_message_filters(bot):
    data = bot.store.data
    asyncio.run(bot.on_message(fake_message()))
    asyncio.run(bot.on_message(fake_message(author_id=11, guild_id=222)))  # other server
    asyncio.run(bot.on_message(fake_message(author_id=12, system=True)))  # join/pin notices
    asyncio.run(bot.on_message(fake_message(author_id=13, webhook=77)))  # webhook = bot
    asyncio.run(bot.on_message(fake_message(author_id=14, bot_author=True)))
    users = data["days"]["2026-10-12"]["u"]
    assert set(users) == {"10"}
    assert data["server"]["2026-10-12"]["bm"] == 2
    assert data["channels"]["500"]["n"] == "general"
    assert 12345 in bot.collector.recent


def test_on_message_excluded_channel(tmp_path):
    cfg = make_cfg(exclude_channel_ids={500})
    store = Store(tmp_path / "data.json")
    data = store.open()
    b = ColddBot(cfg, store, Collector(data, cfg))
    asyncio.run(b.on_message(fake_message()))
    assert data["days"] == {}
    store.close()


def test_raw_delete_and_reaction_events(bot):
    asyncio.run(bot.on_message(fake_message()))
    asyncio.run(bot.on_raw_reaction_add(SimpleNamespace(guild_id=111, member=SimpleNamespace(bot=False), message_id=12345, user_id=20)))
    asyncio.run(bot.on_raw_reaction_add(SimpleNamespace(guild_id=111, member=SimpleNamespace(bot=True), message_id=12345, user_id=21)))
    assert bot.store.data["days"]["2026-10-12"]["u"]["10"]["rx"] == 1
    asyncio.run(bot.on_raw_message_delete(SimpleNamespace(guild_id=111, message_id=12345)))
    assert "m" not in bot.store.data["days"]["2026-10-12"]["u"]["10"]


def test_flush_failure_is_retried(bot, monkeypatch):
    bot.collector.dirty = True

    def fail(payload):
        raise OSError("no space")

    monkeypatch.setattr(bot.store, "write", fail)
    assert not asyncio.run(bot.flush())
    assert bot.store.dirty and "no space" in bot.store.last_error
    monkeypatch.undo()
    assert asyncio.run(bot.flush())
    assert bot.store.last_error is None


def test_disconnect_reconnect_records_gap(bot, monkeypatch):
    import coldd.bot as botmod

    t = [1_800_000_000]
    monkeypatch.setattr(botmod, "now_ts", lambda: t[0])
    bot.connected = True
    asyncio.run(bot.on_disconnect())
    t[0] += 600
    asyncio.run(bot.on_resumed())
    assert bot.store.data["meta"]["gaps"] == [[1_800_000_000, 1_800_000_600]]
    assert bot.connected


def test_health_before_connect_does_not_crash(bot):
    h = bot.health()
    assert h["latency_ms"] is None and h["connected"] is False


def test_voice_sampling_counts_sessions_present_at_startup(bot, monkeypatch):
    def member(i, deaf=False, is_bot=False):
        return SimpleNamespace(id=i, bot=is_bot, display_name=f"m{i}", voice=SimpleNamespace(self_deaf=deaf, deaf=False))

    vc = SimpleNamespace(id=700, category_id=None, members=[member(1), member(2), member(3, deaf=True), member(4, is_bot=True)])
    lonely = SimpleNamespace(id=701, category_id=None, members=[member(5)])
    guild = SimpleNamespace(voice_channels=[vc, lonely], stage_channels=[], afk_channel=None)
    monkeypatch.setattr(bot, "get_guild", lambda gid: guild)
    ts = int(datetime(2026, 10, 12, 12, tzinfo=timezone.utc).timestamp())
    bot._sample_voice(ts)
    bot._last_voice_sample -= 60
    bot._sample_voice(ts + 60)
    u = bot.store.data["days"]["2026-10-12"]["u"]
    assert set(u) == {"1", "2"}  # deafened, bot and alone-in-channel members are not credited
    assert u["1"]["vs"] == 1 and 59 <= u["1"]["v"] <= 61


def weekly_approve(bot, **kw):
    inter = interaction(1)
    cmd = bot.tree.get_command("weekly").get_command("approve")
    asyncio.run(cmd.callback(inter, member=SimpleNamespace(id=5, mention="<@5>"), week=-1, note="", replace=False, **kw))
    return inter.response.sent[0][0]


def test_approve_requires_a_verified_week(bot):
    assert "isn't verified" in weekly_approve(bot, unverified=False)
    assert "isn't verified" not in weekly_approve(bot, unverified=True)


def test_approve_passes_the_gate_once_the_week_is_verified(bot):
    from datetime import timedelta

    from coldd import scoring
    from coldd.bot import today_for
    from coldd.timeutil import now_ts

    wk = scoring.resolve_week(bot.cfg, today_for(bot.cfg), -1)
    stamps = bot.store.data["meta"].setdefault("recounted", {})
    for i in range(7):
        stamps[(wk + timedelta(days=i)).isoformat()] = now_ts()
    assert "isn't verified" not in weekly_approve(bot, unverified=False)
