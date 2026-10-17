"""Discord client: event collection, background tick, slash commands."""

from __future__ import annotations

import asyncio
import gzip
import io
import logging
import math
import os
import re
import time
from datetime import date, datetime, timezone
from typing import Any

import discord
from discord import app_commands

from . import __version__, ai, analytics, scoring, ui
from .collector import Collector, SpamTracker, content_fingerprint
from .config import Config
from .storage import Store
from .timeutil import fmt_ts, now_ts, to_local

log = logging.getLogger("coldd.bot")

EMBED_DESC_LIMIT = 3900
DEEPSCAN_COOLDOWN_S = 6 * 3600   # one deep scan per six hours, server-wide
DEEPSCAN_TIMEOUT_S = 240         # long-form reports take far longer than a chat reply
DEEPSCAN_KEEP = 6                # previous reports kept in data.json as the bot's memory
UPLOAD_LIMIT = 8 * 1024 * 1024
LINK_RE = re.compile(r"discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)")
BRAND = 0x5DADE2


def build_intents(cfg: Config) -> discord.Intents:
    intents = discord.Intents.none()
    intents.guilds = True
    intents.guild_messages = True
    intents.members = True  # privileged: joins/departures
    intents.message_content = cfg.message_content_intent  # privileged: duplicate detection + AI evidence only
    intents.voice_states = cfg.voice_enabled
    intents.guild_reactions = cfg.reactions_enabled
    intents.moderation = True  # ban counts
    return intents


class Access:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def is_owner(self, inter: discord.Interaction) -> bool:
        if self.cfg.owner_ids:
            return inter.user.id in self.cfg.owner_ids
        return bool(inter.guild and inter.guild.owner_id == inter.user.id)

    def is_staff(self, inter: discord.Interaction) -> bool:
        if self.is_owner(inter):
            return True
        user = inter.user
        if not isinstance(user, discord.Member):
            return False
        if any(r.id in self.cfg.staff_role_ids for r in user.roles):
            return True
        return self.cfg.allow_manage_guild and user.guild_permissions.manage_guild


class ColddBot(discord.Client):
    def __init__(self, cfg: Config, store: Store, collector: Collector):
        super().__init__(
            intents=build_intents(cfg),
            max_messages=None,  # no message cache: we only need raw events
            member_cache_flags=discord.MemberCacheFlags(voice=cfg.voice_enabled, joined=False),
            chunk_guilds_at_startup=False,  # never download the full member list
        )
        self.cfg = cfg
        self.store = store
        self.collector = collector
        self.access = Access(cfg)
        self.tree = app_commands.CommandTree(self)
        self.started_at = time.time()
        self.connected = False
        self._ticker: asyncio.Task[None] | None = None
        self._save_lock = asyncio.Lock()
        self._last_voice_sample: float | None = None
        self._import_running = False
        self._last_tick = 0.0
        self._disconnected_at: int | None = None
        register_commands(self)

    # lifecycle ----------------------------------------------------------------

    async def setup_hook(self) -> None:
        guild = discord.Object(id=self.cfg.guild_id)
        self.tree.copy_global_to(guild=guild)
        self.tree.clear_commands(guild=None)
        try:
            synced = await self.tree.sync(guild=guild)
            log.info("Synced %d slash commands to the server.", len(synced))
        except discord.Forbidden:
            log.error("Cannot register slash commands: re-invite the bot with the 'applications.commands' scope.")
        except discord.HTTPException as exc:
            log.error("Slash command sync failed: %s", exc)
        if self._ticker is None or self._ticker.done():
            self._ticker = asyncio.create_task(self._tick_loop(), name="coldd-ticker")

    async def on_ready(self) -> None:
        guild = self.get_guild(self.cfg.guild_id)
        if guild is None:
            log.error("Bot is not in server %s. Invite it with the URL in README.md, then restart.", self.cfg.guild_id)
            return
        meta = self.store.data["meta"]
        if not self.connected:
            offline_since = self._disconnected_at or meta.get("last_heartbeat")
            self.collector.mark_reconnected(offline_since, now_ts())
            self._disconnected_at = None
        self.connected = True
        meta.update(guild_id=str(guild.id), guild_name=guild.name, timezone=self.cfg.timezone_name)
        self._check_permissions(guild)
        for ch in guild.channels:
            self.collector.note_channel(ch.id, ch.name)
        log.info("Connected as %s to %s (%s members). Collecting activity.", self.user, guild.name, guild.member_count)

    async def on_resumed(self) -> None:
        if not self.connected:
            self.collector.mark_reconnected(self._disconnected_at, now_ts())
            self._disconnected_at = None
        self.connected = True
        log.info("Gateway session resumed.")

    async def on_disconnect(self) -> None:
        if self.connected:
            log.warning("Disconnected from Discord; discord.py will reconnect automatically.")
            self.collector.note_disconnected_since(now_ts())
            self._disconnected_at = now_ts()
        self.connected = False

    def _check_permissions(self, guild: discord.Guild) -> None:
        me = guild.me
        unreadable = []
        for ch in guild.channels:
            if isinstance(ch, discord.CategoryChannel):
                continue
            if not self.collector.channel_allowed(ch.id, ch.category_id):
                continue
            perms = ch.permissions_for(me)
            if not (perms.view_channel and perms.read_message_history):
                unreadable.append(ch.id)
        self.collector.unreadable_channels(unreadable)
        if unreadable:
            log.warning("%d channel(s) are not readable by the bot and will not be counted (see /health).", len(unreadable))
        if not self.intents.message_content:
            log.info("Message Content intent disabled: duplicate detection and AI evidence review are off.")

    async def close(self) -> None:
        if self._ticker:
            self._ticker.cancel()
            try:
                await self._ticker
            except (asyncio.CancelledError, Exception):
                pass
        self.collector.note_disconnected_since(now_ts())
        await self.flush(reason="shutdown")
        await super().close()

    # persistence ----------------------------------------------------------------

    async def flush(self, reason: str = "interval") -> bool:
        if not (self.collector.dirty or self.store.dirty):
            return True
        async with self._save_lock:
            self.collector.dirty = False
            self.store.dirty = False
            try:
                payload = self.store.serialize()  # on the event loop: consistent snapshot
                await asyncio.to_thread(self.store.write, payload)
            except (OSError, TypeError, ValueError) as exc:
                self.store.on_save_failed(exc)
                return False
            self.store.on_save_ok()
            if reason != "interval":
                log.info("Saved %s (%s, %.1f KB).", self.store.path.name, reason, self.store.last_size / 1024)
            return True

    async def _tick_loop(self) -> None:
        interval = self.cfg.save_interval
        while True:
            try:
                await asyncio.sleep(interval)
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Background tick failed; will retry next interval.")

    async def _tick(self) -> None:
        ts = now_ts()
        self._last_tick = time.time()
        self.collector.heartbeat(ts, self.connected, self.cfg.save_interval)
        if self.connected:
            self._sample_voice(ts)
        self.collector.expire_recent(ts)
        self.collector.spam.sweep(ts)
        self.collector.prune(to_local(ts, self.cfg.tz).date())
        await self.flush()

    def _sample_voice(self, ts: int) -> None:
        if not self.cfg.voice_enabled:
            return
        guild = self.get_guild(self.cfg.guild_id)
        if guild is None:
            return
        now = time.monotonic()
        last = self._last_voice_sample
        self._last_voice_sample = now
        if last is None:
            seconds = 0
        else:
            seconds = int(min(now - last, 2 * self.cfg.save_interval))
        present: dict[int, str | None] = {}
        for ch in list(guild.voice_channels) + list(guild.stage_channels):
            if guild.afk_channel and ch.id == guild.afk_channel.id:
                continue
            if not self.collector.channel_allowed(ch.id, ch.category_id):
                continue
            humans = [m for m in ch.members if not m.bot and m.voice and not (m.voice.self_deaf or m.voice.deaf)]
            if len(humans) >= self.cfg.voice_min_members:
                for m in humans:
                    present[m.id] = m.display_name
        self.collector.credit_voice(present, seconds, ts)

    # collection -----------------------------------------------------------------

    def _resolve_channel(self, channel: Any) -> tuple[int, tuple[int | None, ...]]:
        """Return (channel id used for counting, ids used for include/exclude checks)."""
        parent_id = getattr(channel, "parent_id", None)
        if isinstance(channel, discord.Thread) and parent_id:
            parent = channel.parent
            cat = getattr(parent, "category_id", None) if parent else None
            return parent_id, (channel.id, parent_id, cat)
        return channel.id, (channel.id, getattr(channel, "category_id", None))

    def message_qualifies(self, message: discord.Message) -> bool:
        if message.guild is None or message.guild.id != self.cfg.guild_id:
            return False
        if message.is_system():
            return False
        return True

    async def on_message(self, message: discord.Message) -> None:
        if not self.message_qualifies(message):
            return
        count_id, check_ids = self._resolve_channel(message.channel)
        if not self.collector.channel_allowed(*check_ids):
            return
        author = message.author
        is_bot = author.bot or message.webhook_id is not None
        excluded = isinstance(author, discord.Member) and any(r.id in self.cfg.exclude_role_ids for r in author.roles)
        ref = message.reference.resolved if message.reference else None
        reply_to_other = isinstance(ref, discord.Message) and ref.author.id != author.id and not ref.author.bot
        fp = content_fingerprint(message.content) if message.content else None
        self.collector.record_message(
            author.id, count_id, int(message.created_at.timestamp()),
            name=getattr(author, "display_name", None) or author.name,
            is_bot=is_bot, fingerprint=fp, reply_to_other=reply_to_other,
            excluded_role=excluded, message_id=message.id,
        )
        if getattr(message.channel, "name", None):
            ch = message.channel.parent if isinstance(message.channel, discord.Thread) and message.channel.parent else message.channel
            self.collector.note_channel(count_id, getattr(ch, "name", None))

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent) -> None:
        if payload.guild_id == self.cfg.guild_id:
            self.collector.record_delete(payload.message_id)

    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent) -> None:
        if payload.guild_id == self.cfg.guild_id:
            for mid in payload.message_ids:
                self.collector.record_delete(mid)

    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent) -> None:
        if payload.guild_id != self.cfg.guild_id:
            return
        if payload.member is not None and payload.member.bot:
            return
        self.collector.record_reaction(payload.message_id, payload.user_id, now_ts(), added=True)

    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent) -> None:
        if payload.guild_id == self.cfg.guild_id:
            self.collector.record_reaction(payload.message_id, payload.user_id, now_ts(), added=False)

    async def on_member_join(self, member: discord.Member) -> None:
        if member.guild.id == self.cfg.guild_id:
            self.collector.record_join(member.id, now_ts(), member.display_name, member.bot)

    async def on_raw_member_remove(self, payload: discord.RawMemberRemoveEvent) -> None:
        if payload.guild_id == self.cfg.guild_id:
            self.collector.record_leave(payload.user.id, now_ts(), getattr(payload.user, "bot", False))

    async def on_member_ban(self, guild: discord.Guild, user: discord.User | discord.Member) -> None:
        if guild.id == self.cfg.guild_id:
            self.collector.record_ban(now_ts())

    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel) -> None:
        if channel.guild.id == self.cfg.guild_id:
            self.collector.note_channel(channel.id, channel.name)

    async def on_guild_channel_update(self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel) -> None:
        if after.guild.id == self.cfg.guild_id:
            self.collector.note_channel(after.id, after.name)
            self._check_permissions(after.guild)

    # history import ---------------------------------------------------------------

    async def import_history(self, channel: discord.TextChannel, days: int,
                             progress: Any = None) -> dict[str, Any]:
        """Owner-triggered, rate-limited import of messages the bot did not observe live."""
        meta = self.store.data["meta"]
        now = now_ts()
        start = now - days * 86400
        cs = meta.get("collection_start") or now
        windows = [[start, min(cs, now)]] if start < cs else []
        windows += [[max(g[0], start), g[1]] for g in meta.get("gaps", []) if g[1] > start]
        done = meta.setdefault("imports", {}).get(str(channel.id), [])
        windows = subtract_ranges(windows, done)
        tracker = SpamTracker(self.cfg.duplicate_window_seconds, self.cfg.burst_window_seconds, self.cfg.burst_threshold)
        count = skipped = 0
        count_id, check_ids = self._resolve_channel(channel)
        if not self.collector.channel_allowed(*check_ids):
            return {"imported": 0, "skipped": 0, "windows": 0, "note": "channel is excluded by configuration"}
        for w0, w1 in windows:
            if count >= self.cfg.history_import_max_messages:
                break
            last_ts = w0
            after = datetime.fromtimestamp(w0, tz=timezone.utc)
            before = datetime.fromtimestamp(w1, tz=timezone.utc)
            async for msg in channel.history(after=after, before=before, oldest_first=True,
                                             limit=self.cfg.history_import_max_messages - count):
                if msg.is_system():
                    continue
                if msg.author.bot or msg.webhook_id:
                    skipped += 1
                    continue
                ts = int(msg.created_at.timestamp())
                fp = content_fingerprint(msg.content) if msg.content else None
                ref = msg.reference.resolved if msg.reference else None
                reply = isinstance(ref, discord.Message) and ref.author.id != msg.author.id and not ref.author.bot
                excluded = isinstance(msg.author, discord.Member) and any(
                    r.id in self.cfg.exclude_role_ids for r in msg.author.roles)
                self.collector.record_message(
                    msg.author.id, count_id, ts, name=getattr(msg.author, "display_name", None),
                    fingerprint=fp, reply_to_other=reply, excluded_role=excluded, imported=True,
                    flags_override=tracker.check(msg.author.id, ts, fp),
                )
                count += 1
                last_ts = ts
                if count % 500 == 0:
                    await asyncio.sleep(0)
                    if progress:
                        await progress(count)
            # Only mark the part actually read as done if the message limit cut the window short.
            done.append([w0, w1] if count < self.cfg.history_import_max_messages else [w0, last_ts])
        meta["imports"][str(channel.id)] = merge_ranges(done)
        self.collector.dirty = True
        await self.flush(reason="history import")
        return {"imported": count, "skipped": skipped, "windows": len(windows)}

    # health ------------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        data = self.store.data
        ts = now_ts()
        today = to_local(ts, self.cfg.tz).date()
        cov7 = analytics.coverage_for(data, analytics.period(today, 7), today, ts, self.cfg)
        member_days = sum(len(d["u"]) for d in data["days"].values())
        week_ago = ts - 7 * 86400
        return {
            "version": __version__,
            "connected": self.connected,
            "latency_ms": round(self.latency * 1000) if math.isfinite(self.latency) else None,
            "uptime_h": (time.time() - self.started_at) / 3600,
            "last_save": data["meta"].get("last_save"),
            "save_ms": round(self.store.last_save_duration * 1000, 1),
            "file_kb": self.store.last_size / 1024,
            "warn_mb": self.cfg.storage_warn_mb,
            "size_warning": self.store.size_warning,
            "save_error": self.store.last_error,
            "dirty": self.collector.dirty or self.store.dirty,
            "members": len(data["members"]),
            "member_days": member_days,
            "detail_days": len(data["days"]),
            "server_days": len(data["server"]),
            "channels": len(data["channels"]),
            "recent_cache": len(self.collector.recent),
            "spam_tracked": len(self.collector.spam),
            "events": self.collector.events_seen,
            "coverage_7d": cov7["ratio"],
            "gaps_7d": sum(1 for g in data["meta"].get("gaps", []) if g[1] >= week_ago),
            "collection_start": data["meta"].get("collection_start"),
            "last_event": data["meta"].get("last_event"),
            "unreadable": len(data["meta"].get("unreadable_channels") or []),
            "rss_mb": rss_mb(),
            "backups": len(self.store.backups()),
        }


def rss_mb() -> float | None:
    """Current resident memory where the platform exposes it cheaply (Linux)."""
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return None


def merge_ranges(ranges: list[list[int]]) -> list[list[int]]:
    out: list[list[int]] = []
    for a, b in sorted(ranges):
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return out


def subtract_ranges(windows: list[list[int]], done: list[list[int]]) -> list[list[int]]:
    result = []
    for a, b in merge_ranges([w for w in windows if w[1] > w[0]]):
        pieces = [[a, b]]
        for d0, d1 in done:
            nxt = []
            for p0, p1 in pieces:
                if d1 <= p0 or d0 >= p1:
                    nxt.append([p0, p1])
                    continue
                if p0 < d0:
                    nxt.append([p0, d0])
                if d1 < p1:
                    nxt.append([d1, p1])
            pieces = nxt
        result.extend(pieces)
    return result


# --- command helpers ----------------------------------------------------------------

def embed(title: str, description: str = "", color: int = BRAND) -> discord.Embed:
    if len(description) > EMBED_DESC_LIMIT:
        description = description[: EMBED_DESC_LIMIT - 40] + "\n… (truncated; see attached file)"
    return discord.Embed(title=title[:256], description=description, color=color)


def text_file(name: str, content: str | bytes) -> discord.File:
    raw = content.encode("utf-8") if isinstance(content, str) else content
    return discord.File(io.BytesIO(raw), filename=name)


async def deny(inter: discord.Interaction, what: str = "staff") -> None:
    await inter.response.send_message(f"This command is restricted to {what}.", ephemeral=True)


def today_for(cfg: Config) -> date:
    return to_local(now_ts(), cfg.tz).date()


def clamp_days(days: int, cfg: Config) -> int:
    return max(1, min(days, cfg.audit_max_days))


def register_commands(bot: ColddBot) -> None:
    cfg, tree, access = bot.cfg, bot.tree, bot.access
    data = lambda: bot.store.data  # noqa: E731 - always the live dataset

    @tree.error
    async def on_error(inter: discord.Interaction, error: app_commands.AppCommandError) -> None:
        log.error("Command /%s failed: %s", inter.command.qualified_name if inter.command else "?", error,
                  exc_info=getattr(error, "original", error))
        msg = "Something went wrong running that command. The error was logged."
        if isinstance(getattr(error, "original", None), discord.Forbidden):
            msg = "The bot lacks a Discord permission needed for that command (see logs)."
        try:
            if inter.response.is_done():
                await inter.followup.send(msg, ephemeral=True)
            else:
                await inter.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass

    @tree.command(name="help", description="Explain the analytics bot's commands")
    async def help_cmd(inter: discord.Interaction) -> None:
        lines = [
            "**Staff commands** (results are only visible to you):",
            "`/analytics [days]` server activity dashboard",
            "`/insights [days]` server health: growth, engagement, peak times, retention",
            "`/summary [days]` AI-written summary of the same figures (aggregate only)",
            "`/audit [days]` full analytics audit (Markdown report attached)",
            "`/weekly show [week]` Member of the Week shortlist (0 = this week, -1 = last week)",
            "`/weekly nominate` record a staff nomination with a reason",
            "`/weekly approve` record the chosen winner (manual; nothing is paid or announced)",
            "`/weekly paid` mark a recorded award as paid out",
            "`/weekly history` previous winners",
            "`/weekly ai_review` optional advisory AI read of staff-selected messages",
            "`/leaderboard [days] [metric]` top members",
            "`/channels [days]` channel activity",
            "`/member <user> [days]` one member's stored statistics",
            "`/health` uptime, coverage, storage and save status",
            "`/export <kind>` CSV/Markdown reports, or a data backup (owner)",
            "`/config` current non-secret settings",
            "**Owner only:** `/import_history`, `/forget`",
            "",
            "The bot counts activity only while online, stores no message text, and never awards anything automatically.",
        ]
        await inter.response.send_message(embed=embed("COLDD Analytics: help", "\n".join(lines)), ephemeral=True)

    @tree.command(name="analytics", description="Server activity summary")
    @app_commands.describe(days="Number of days (default 7)")
    async def analytics_cmd(inter: discord.Interaction, days: app_commands.Range[int, 1, 365] = 7) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        days = clamp_days(days, cfg)
        s = analytics.summary(data(), cfg, days, today_for(cfg))
        cov = s["coverage"]
        net = s["joins"] - s["leaves"]
        e = ui.card(f"Activity · last {days} days",
                    f"{s['days'][0]} → {s['days'][-1]} · {cfg.timezone_name}\n"
                    f"`{ui.spark([m for _, m, _ in s['daily']])}`  daily messages",
                    inter.guild, ui.color_for(cov["ratio"]),
                    footer=f"Coverage {ui.pct(cov['ratio'])} observed · counts are never estimated")
        e.add_field(name="💬 Messages", value=f"**{s['messages']:,}**\n{ui.trend(s['messages'], s['messages_prev'])} vs prior {days}d", inline=True)
        e.add_field(name="👥 Active members", value=f"**{s['unique_active']:,}**\n{ui.trend(s['unique_active'], s['unique_active_prev'])}", inline=True)
        e.add_field(name="🔁 Returning", value=f"**{s['returning']:,}**\nfrom previous period", inline=True)
        e.add_field(name="📥 Joins / 📤 leaves", value=f"**{s['joins']:,}** / **{s['leaves']:,}**\nnet {net:+,}", inline=True)
        e.add_field(name="⏰ Peak hour", value=f"**{s['peak_hour']:02d}:00**" if s["peak_hour"] is not None else "n/a", inline=True)
        e.add_field(name="🎙️ Voice", value=f"**{s['voice_hours']:.1f} h**\nobserved", inline=True)
        e.add_field(name="📊 Per active member",
                    value=f"mean **{s['avg_per_active']:.1f}** · median **{s['median_per_active']:.1f}**\n"
                          f"top 10% send {ui.pct(s['top10_share'])} of messages", inline=False)
        notes = []
        if s["coverage_prev"] < 0.9:
            notes.append(f"Previous period only {ui.pct(s['coverage_prev'])} observed: changes are not like-for-like.")
        if cov["ratio"] < 0.9:
            notes.append("Offline periods are not estimated, so totals are a floor.")
        if s["member_detail_limited"]:
            notes.append(f"Member detail only covers {cfg.member_retention_days} days.")
        if notes:
            e.add_field(name="⚠️ Notes", value="\n".join(notes), inline=False)
        e.set_image(url=ui.banner_url())
        await inter.response.send_message(embed=e, file=ui.banner_file(), ephemeral=True)

    community_choices = [app_commands.Choice(name=label, value=key) for key, (label, _, _) in analytics.COMMUNITY_METRICS.items()]

    @tree.command(name="top", description="Community leaderboard: messages, consistency, streaks, reactions, interactions")
    @app_commands.describe(metric="What to rank by", days="Window in days (default 7)", ai_highlight="Add a short AI highlight (uses AI budget)")
    @app_commands.choices(metric=community_choices)
    async def top_cmd(inter: discord.Interaction, metric: str = "messages",
                      days: app_commands.Range[int, 1, 90] = 7, ai_highlight: bool = False) -> None:
        days = min(clamp_days(days, cfg), cfg.member_retention_days)
        today = today_for(cfg)
        label, unit, _ = analytics.COMMUNITY_METRICS[metric]
        rows = analytics.community_board(data(), cfg, days, today, metric, limit=10)
        top = rows[0]["value"] if rows else 0
        lines = [f"{ui.medal(r['rank'])} <@{r['uid']}>\n"
                 f"┗ `{ui.bar(r['value'], top)}` **{r['value']:,}** {unit}" for r in rows]
        e = ui.card(f"{label} · last {days} days", "\n".join(lines) or "No activity recorded in this window.", inter.guild,
                    footer=f"Top {len(rows)} · bars scaled to #1 · {cfg.timezone_name}")
        if ai_highlight and rows and cfg.ai_enabled and ai.budget_ok(data(), cfg, today.isoformat()):
            await inter.response.defer(thinking=True)
            facts = {"metric": label, "unit": unit, "window_days": days, "ranked_members": len(rows),
                     "values": [r["value"] for r in rows], "leader_value": top}
            ai.consume_budget(data())
            try:
                e.add_field(name="✨ Highlight", value=await asyncio.to_thread(ai.brief, cfg, facts), inline=False)
            except ai.AIError as exc:
                log.warning("AI highlight failed: %s", ai.redact(str(exc), cfg.secrets()))
            return await inter.followup.send(embed=e)
        await inter.response.send_message(embed=e)

    @tree.command(name="motw", description="Member of the Week: current leaders (public)")
    async def motw_cmd(inter: discord.Interaction) -> None:
        await inter.response.defer(thinking=True)
        r = week_result(0)
        rows = r["shortlist"][:5]
        if not rows:
            e = ui.card("Member of the Week", "Nobody meets the minimums yet this week. Keep chatting!", inter.guild)
        else:
            lines = [f"{ui.medal(row['rank'])} <@{row['uid']}> · **{row['score']:.1f}** pts · "
                     f"{row['effective_messages']} msgs · {row['active_days']} active days · "
                     f"{row['reactions']} reactions received" for row in rows]
            e = ui.card(f"Member of the Week · week of {r['week']}", "\n".join(lines), inter.guild,
                        footer=f"Winner announced by staff · award {cfg.award_amount} {cfg.award_currency} · coverage {ui.pct(r['coverage']['ratio'])}")
        await inter.followup.send(embed=e)

    @tree.command(name="profile", description="Your community profile (staff can view anyone)")
    @app_commands.describe(member="Member to view (staff only for others)", days="Window in days (default 30)")
    async def profile_cmd(inter: discord.Interaction, member: discord.Member | None = None,
                          days: app_commands.Range[int, 1, 90] = 30) -> None:
        target = member or inter.user
        if target.id != inter.user.id and not access.is_staff(inter):
            return await deny(inter, "staff (you can view your own profile)")
        days = min(clamp_days(days, cfg), cfg.member_retention_days)
        p = analytics.member_profile(data(), cfg, str(target.id), days, today_for(cfg))
        if p is None:
            return await inter.response.send_message("No activity recorded for that member in this window.", ephemeral=True)
        f, rk = p["figures"], p["ranks"]

        def place(metric: str) -> str:
            r = rk[metric]
            return f"#{r['rank']} of {r['of']}" if r["rank"] else "unranked"

        e = ui.card(f"Profile · {target.display_name}", f"Last {days} days · {cfg.timezone_name}", inter.guild)
        e.set_thumbnail(url=target.display_avatar.url)
        e.add_field(name="💬 Messages", value=f"**{f['eff']:,}** · {place('messages')}", inline=True)
        e.add_field(name="📅 Active days", value=f"**{f['days']}** of {p['window_days']} · {place('consistency')}", inline=True)
        e.add_field(name="🔥 Best streak", value=f"**{f['streak']}** days · {place('streak')}", inline=True)
        e.add_field(name="❤️ Reactions given", value=f"**{f['rg']:,}** · {place('reacting')}", inline=True)
        e.add_field(name="🤝 Interactions", value=f"**{f['r'] + f['rg'] + f['rx']:,}** · {place('interactions')}", inline=True)
        e.add_field(name="⭐ Helpful", value=f"replies {f['r']} · reactions received {f['rx']} · {place('helpful')}", inline=True)
        e.add_field(name="🎙️ Voice", value=f"**{round(f['v'] / 60)} min** · {place('voice')}", inline=True)
        await inter.response.send_message(embed=e, ephemeral=True)

    @tree.command(name="insights", description="Server health: growth, engagement, peak times, retention (staff)")
    @app_commands.describe(days="Number of days (default 30)")
    async def insights_cmd(inter: discord.Interaction, days: app_commands.Range[int, 7, 365] = 30) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        days = clamp_days(days, cfg)
        today = today_for(cfg)
        ins = analytics.insights(data(), cfg, days, today)
        top_ch = [r for r in analytics.channel_stats(data(), cfg, days, today) if r["messages"]][:5]
        cov = ins["coverage"]["ratio"]

        e = ui.card(f"Server insights · last {days} days", "", inter.guild, ui.color_for(cov),
                    footer=f"Coverage {ui.pct(cov)} · timezone {cfg.timezone_name} · Discord's own Insights aren't exposed to bots")
        e.add_field(name="📈 Growth",
                    value=f"+**{ins['joins']:,}** joined · −**{ins['leaves']:,}** left\n"
                          f"net **{ins['net']:+,}** (prior period {ins['net_prev']:+,})",
                    inline=True)
        e.add_field(name="🔥 Engagement",
                    value=f"**{ins['mean_dau']:.0f}** avg daily active\n"
                          f"**{ins['wau']:,}** active in the last 7 days\n"
                          f"stickiness **{ui.pct(ins['stickiness'])}**",
                    inline=True)

        grid = ins["heatmap"]
        peak = max((v for row in grid for v in row), default=0)
        shades = " ░▒▓█"

        def shade(v: int) -> str:
            if not peak or not v:
                return "   "
            return shades[min(4, 1 + int(3 * v / peak))] * 3

        blocks = ["0-4", "4-8", "8-12", "12-16", "16-20", "20-24"]
        rows = ["    " + "".join(f"{b:^5}" for b in blocks)]
        for name, row in zip(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], grid, strict=True):
            rows.append(f"{name} " + "".join(f"{shade(v):^5}" for v in row))
        e.add_field(name="🗓️ When people talk (local time, darker = busier)",
                    value="```\n" + "\n".join(rows) + "\n```", inline=False)

        if ins["cohorts"]:
            lines = ["week         new  wk0   wk1   wk2-4"]
            for wk, n, w0, w1, later in ins["cohorts"]:
                lines.append(f"{wk}  {n:>3}  {w0:>4}  {w1:>4}  {later:>5}")
            e.add_field(name="🧲 New-member retention (share still active)",
                        value="```\n" + "\n".join(lines) + "\n```", inline=False)

        if top_ch:
            top = top_ch[0]["messages"]
            e.add_field(name="📍 Top channels",
                        value="\n".join(f"<#{r['cid']}> `{ui.bar(r['messages'], top, 8)}` {ui.pct(r['share'])}" for r in top_ch),
                        inline=False)
        await inter.response.send_message(embed=e, ephemeral=True)

    @tree.command(name="deepscan", description="Deep AI analysis of every aggregate metric (staff, slow, rate-limited)")
    async def deepscan_cmd(inter: discord.Interaction) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        if not cfg.ai_enabled:
            return await inter.response.send_message("AI is not configured (set AI_PROVIDER, AI_API_KEY, AI_MODEL).", ephemeral=True)
        meta = data()["meta"]
        last = meta.get("last_deepscan") or 0
        wait = DEEPSCAN_COOLDOWN_S - (now_ts() - last)
        if wait > 0:
            return await inter.response.send_message(
                f"A deep scan ran recently. Next one available in about {math.ceil(wait / 3600)} h.", ephemeral=True)
        today = today_for(cfg)
        if not ai.budget_ok(data(), cfg, today.isoformat()):
            return await inter.response.send_message("Daily AI request budget reached (AI_MAX_REQUESTS_PER_DAY).", ephemeral=True)
        await inter.response.defer(ephemeral=True, thinking=True)
        ai.consume_budget(data())
        meta["last_deepscan"] = now_ts()
        stats = analytics.deep_stats(data(), cfg, today)
        reports = meta.setdefault("ai_reports", [])
        previous = reports[-1]["text"] if reports else None
        try:
            text = await asyncio.to_thread(ai.deep_report, cfg.with_ai_timeout(DEEPSCAN_TIMEOUT_S), stats, previous)
        except ai.AIError as exc:
            bot.collector.dirty = True
            log.warning("Deep scan failed: %s", ai.redact(str(exc), cfg.secrets()))
            return await inter.followup.send(f"Deep scan unavailable ({ai.redact(str(exc), cfg.secrets())}). Try again later.",
                                             ephemeral=True)
        reports.append({"at": now_ts(), "text": text})
        del reports[:-DEEPSCAN_KEEP]
        bot.collector.dirty = True
        await bot.flush(reason="deep scan saved")
        head = text[:3600] + ("\n…" if len(text) > 3600 else "")
        e = ui.card(f"Deep scan · {today.isoformat()}", head, inter.guild,
                    footer="AI-written · aggregate figures only · full report attached")
        await inter.followup.send(embed=e, file=text_file(f"coldd-deepscan-{today.isoformat()}.md", text), ephemeral=True)

    @tree.command(name="summary", description="AI-written summary of recent activity (staff, aggregate figures only)")
    @app_commands.describe(days="Number of days (default 7)")
    async def summary_cmd(inter: discord.Interaction, days: app_commands.Range[int, 1, 365] = 7) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        if not cfg.ai_enabled:
            return await inter.response.send_message("AI is not configured (set AI_PROVIDER, AI_API_KEY, AI_MODEL).", ephemeral=True)
        today = today_for(cfg).isoformat()
        if not ai.budget_ok(data(), cfg, today):
            return await inter.response.send_message("Daily AI request budget reached (AI_MAX_REQUESTS_PER_DAY).", ephemeral=True)
        await inter.response.defer(ephemeral=True, thinking=True)
        days = clamp_days(days, cfg)
        s = analytics.summary(data(), cfg, days, today_for(cfg))
        stats = {
            "period_days": days,
            "messages": s["messages"], "messages_previous": s["messages_prev"],
            "active_members": s["unique_active"], "active_members_previous": s["unique_active_prev"],
            "returning_members": s["returning"],
            "mean_messages_per_active": round(s["avg_per_active"], 2),
            "median_messages_per_active": round(s["median_per_active"], 2),
            "top10pct_message_share": round(s["top10_share"], 3),
            "joins": s["joins"], "leaves": s["leaves"], "voice_hours": round(s["voice_hours"], 1),
            "peak_hour": s["peak_hour"],
            "coverage_ratio": round(s["coverage"]["ratio"], 3),
            "coverage_ratio_previous": round(s["coverage_prev"], 3),
            "daily_messages": [[d, m] for d, m, _ in s["daily"]],
        }
        ai.consume_budget(data())
        try:
            text = await asyncio.to_thread(ai.narrate, cfg, stats)
        except ai.AIError as exc:
            log.warning("AI summary failed: %s", ai.redact(str(exc), cfg.secrets()))
            return await inter.followup.send(f"AI summary unavailable ({ai.redact(str(exc), cfg.secrets())}).", ephemeral=True)
        e = ui.card(f"Summary · last {days} days", text, inter.guild, footer="AI-written from aggregate counts")
        await inter.followup.send(embed=e, ephemeral=True)

    @tree.command(name="audit", description="Run a full analytics audit (staff)")
    @app_commands.describe(days="Number of days to audit (default 30)")
    async def audit_cmd(inter: discord.Interaction, days: app_commands.Range[int, 7, 365] = 30) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        await inter.response.defer(ephemeral=True, thinking=True)
        days = clamp_days(days, cfg)
        report, fig = analytics.run_audit(data(), cfg, days, today_for(cfg))
        summary = (f"Messages: {fig['messages']:,} · Unique active: {fig['unique_active']:,} · "
                   f"Joins/leaves: {fig['joins']:,}/{fig['leaves']:,} · Coverage: {fig['coverage']:.0%}\n"
                   f"Computed in {fig['duration_ms']} ms. Full report attached.")
        await inter.followup.send(embed=embed(f"Audit: last {days} days", summary),
                                  file=text_file(f"coldd-audit-{today_for(cfg)}.md", report), ephemeral=True)

    weekly = app_commands.Group(name="weekly", description="Member of the Week")

    def week_result(offset: int) -> dict[str, Any]:
        start = scoring.resolve_week(cfg, today_for(cfg), offset)
        return scoring.score_week(data(), cfg, start)

    @weekly.command(name="show", description="Generate the Member of the Week shortlist")
    @app_commands.describe(week="0 = current week, -1 = last week, -2 = two weeks ago…")
    async def weekly_show(inter: discord.Interaction, week: app_commands.Range[int, -52, 0] = 0) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        await inter.response.defer(ephemeral=True, thinking=True)
        r = week_result(week)
        rows = r["shortlist"]
        warn = [f"⚠️ {w}" for w in r["warnings"]]
        if r["award"]:
            paid = "paid ✅" if r["award"].get("paid") else "unpaid"
            warn.insert(0, f"🏆 Recorded winner: <@{r['award']['winner']}> · {paid}")
        e = ui.card(f"Member of the Week · {r['week']}",
                    f"Award **{cfg.award_amount} {cfg.award_currency}** (paid manually)\n"
                    f"Eligible **{r['eligible_count']}** of {r['considered_count']} · "
                    f"coverage **{ui.pct(r['coverage']['ratio'])}**",
                    inter.guild, ui.color_for(r["coverage"]["ratio"]),
                    footer="Use /weekly approve to record a decision · full breakdown attached")
        for row in rows[:3]:
            flag = " ⚠️" if row["flags"] else ""
            tie = " (tie)" if row["tie"] else ""
            e.add_field(name=f"{ui.medal(row['rank'])} {tie.strip() or 'Rank ' + str(row['rank'])}{flag}",
                        value=f"<@{row['uid']}>\n**{row['score']:.1f}** pts\n"
                              f"{row['effective_messages']} msgs · {row['active_days']}d\n"
                              f"{row['replies']} replies · {row['reactions']} ❤ · {row['nominations']} noms",
                        inline=True)
        if rows[3:]:
            table = ["rank  score  msgs  days  r/❤   noms"]
            for row in rows[3:]:
                flag = "!" if row["flags"] else " "
                table.append(f"{row['rank']:>4}  {row['score']:>5.1f}  {row['effective_messages']:>4}  "
                             f"{row['active_days']:>4}  {row['replies']}/{row['reactions']:<4}  {row['nominations']:>4}{flag}")
            e.add_field(name="More candidates", value="```\n" + "\n".join(table) + "\n```", inline=False)
        if not rows:
            e.add_field(name="No eligible candidates yet", value="Nothing met the minimums for this week.", inline=False)
        if warn:
            e.add_field(name="Notes", value="\n".join(warn)[:1024], inline=False)
        e.set_image(url=ui.banner_url())
        await inter.followup.send(embed=e,
                                  files=[ui.banner_file(), text_file(f"weekly-{r['week']}.md", analytics.weekly_markdown(r, cfg))],
                                  ephemeral=True)

    @weekly.command(name="approve", description="Record the staff-chosen Member of the Week (no automatic payout)")
    @app_commands.describe(member="Chosen winner", week="0 = current, -1 = last week (default)", note="Optional reason",
                           replace="Replace an already recorded winner")
    async def weekly_approve(inter: discord.Interaction, member: discord.Member,
                             week: app_commands.Range[int, -52, 0] = -1, note: str = "", replace: bool = False) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        r = week_result(week)
        ok, msg = scoring.approve_winner(data(), cfg, r, member.id, inter.user.id, note, replace)
        if ok:
            bot.collector.dirty = True
            await bot.flush(reason="award approved")
            log.info("Award for week %s recorded by staff %s.", r["week"], inter.user.id)
        await inter.response.send_message(msg, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @weekly.command(name="paid", description="Mark a recorded award as paid out")
    async def weekly_paid(inter: discord.Interaction, week: app_commands.Range[int, -52, 0] = -1) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        wk = scoring.resolve_week(cfg, today_for(cfg), week).isoformat()
        award = data()["awards"].get(wk)
        if not award:
            return await inter.response.send_message(f"No winner recorded for week {wk}.", ephemeral=True)
        award["paid"] = True
        award["paid_by"] = str(inter.user.id)
        award["paid_at"] = now_ts()
        bot.collector.dirty = True
        await bot.flush(reason="award paid")
        await inter.response.send_message(f"Marked week {wk} award as paid.", ephemeral=True)

    @weekly.command(name="history", description="Previous Member of the Week decisions")
    async def weekly_history(inter: discord.Interaction) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        rows = sorted(data()["awards"].items(), reverse=True)[:25]
        lines = [f"`{wk}` <@{a['winner']}> · {a.get('amount')} {a.get('currency')} · "
                 f"{'paid' if a.get('paid') else 'unpaid'} · by <@{a.get('approved_by')}>" for wk, a in rows]
        await inter.response.send_message(embed=embed("Member of the Week history", "\n".join(lines) or "No awards recorded."),
                                          ephemeral=True)

    @weekly.command(name="nominate", description="Record a staff nomination (counts toward helpfulness)")
    @app_commands.describe(member="Member to nominate", reason="Short reason (stored)", week="0 = current week")
    async def weekly_nominate(inter: discord.Interaction, member: discord.Member, reason: str,
                              week: app_commands.Range[int, -52, 0] = 0) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        if member.bot:
            return await inter.response.send_message("Bots cannot be nominated.", ephemeral=True)
        wk = scoring.resolve_week(cfg, today_for(cfg), week).isoformat()
        n = scoring.add_nomination(data(), wk, member.id, inter.user.id, reason)
        bot.collector.dirty = True
        await inter.response.send_message(f"Nomination recorded for {member.mention} (week {wk}, {n} total).",
                                          ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @weekly.command(name="ai_review", description="Optional advisory AI read of up to 5 staff-selected messages")
    @app_commands.describe(member="Candidate", message_links="Message links separated by spaces", week="0 = current week")
    async def weekly_ai(inter: discord.Interaction, member: discord.Member, message_links: str,
                        week: app_commands.Range[int, -52, 0] = 0) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        if not cfg.ai_enabled:
            return await inter.response.send_message("AI is not configured (set AI_PROVIDER, AI_API_KEY, AI_MODEL). "
                                                     "Deterministic scoring works without it.", ephemeral=True)
        today = today_for(cfg).isoformat()
        if not ai.budget_ok(data(), cfg, today):
            return await inter.response.send_message("Daily AI request budget reached (AI_MAX_REQUESTS_PER_DAY).", ephemeral=True)
        links = LINK_RE.findall(message_links)[: cfg.ai_max_messages_per_request]
        if not links:
            return await inter.response.send_message("Provide Discord message links (right-click a message → Copy Message Link).",
                                                     ephemeral=True)
        await inter.response.defer(ephemeral=True, thinking=True)
        texts = []
        for gid, cid, mid in links:
            if int(gid) != cfg.guild_id:
                continue
            channel = bot.get_channel(int(cid))
            if channel is None or not hasattr(channel, "fetch_message"):
                continue
            try:
                msg = await channel.fetch_message(int(mid))
            except discord.HTTPException:
                continue
            if msg.author.id == member.id and msg.content:
                texts.append(msg.content)
        if not texts:
            return await inter.followup.send("None of the links pointed to readable text messages by that member.", ephemeral=True)
        ai.consume_budget(data())
        try:
            result = await asyncio.to_thread(ai.assess, cfg, texts)
        except ai.AIError as exc:
            log.warning("AI review failed: %s", ai.redact(str(exc), cfg.secrets()))
            return await inter.followup.send(f"AI review unavailable ({ai.redact(str(exc), cfg.secrets())}). "
                                             "Deterministic scoring is unaffected.", ephemeral=True)
        wk = scoring.resolve_week(cfg, today_for(cfg), week).isoformat()
        rec = data()["nominations"].setdefault(wk, {}).setdefault(str(member.id), {"c": 0, "by": [], "r": []})
        rec["ai"] = {**result, "at": now_ts(), "by": str(inter.user.id)}
        bot.collector.dirty = True
        await inter.followup.send(
            f"**Advisory only** (sent {len(texts)} message(s) to an AI provider; this used 1 external API request).\n"
            f"Rating: **{result['rating']}/5**. {result['summary']}\nThis does not change counts or scores.",
            ephemeral=True,
        )

    tree.add_command(weekly)

    @tree.command(name="leaderboard", description="Top members for a period")
    @app_commands.describe(days="Number of days (default 7)", metric="Ranking metric")
    @app_commands.choices(metric=[app_commands.Choice(name=v[0], value=k) for k, v in analytics.METRICS.items()])
    async def leaderboard_cmd(inter: discord.Interaction, days: app_commands.Range[int, 1, 365] = 7,
                              metric: str = "messages") -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        days = min(clamp_days(days, cfg), cfg.member_retention_days)
        rows = analytics.leaderboard(data(), cfg, days, today_for(cfg), metric, limit=20)
        label = analytics.METRICS[metric][0]
        top = rows[0]["value"] if rows else 0
        lines = [f"{ui.medal(r['rank'])} <@{r['uid']}>\n"
                 f"┗ `{ui.bar(r['value'], top)}` **{r['value']:,}** · {r['days']} active days" for r in rows]
        e = ui.card(f"Leaderboard · {label}", "\n".join(lines) or "No activity recorded.", inter.guild,
                    footer=f"Last {days} days · top {len(rows)} · bars scaled to #1")
        await inter.response.send_message(embed=e, ephemeral=True)

    @tree.command(name="channels", description="Channel activity analysis")
    async def channels_cmd(inter: discord.Interaction, days: app_commands.Range[int, 1, 365] = 7) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        days = clamp_days(days, cfg)
        rows = [r for r in analytics.channel_stats(data(), cfg, days, today_for(cfg)) if r["messages"] or r["prev"]][:25]
        top = rows[0]["messages"] if rows else 0
        lines = [f"<#{r['cid']}>\n┗ `{ui.bar(r['messages'], top)}` **{r['messages']:,}** "
                 f"({ui.pct(r['share'])}) · {r['unique']} members · {r['trend']}" for r in rows]
        unread = data()["meta"].get("unreadable_channels") or []
        footer = f"Last {days} days"
        if unread:
            footer += f" · ⚠️ {len(unread)} channel(s) unreadable, not counted"
        e = ui.card(f"Channels · last {days} days", "\n".join(lines) or "No activity recorded.", inter.guild,
                    ui.WARN if unread else ui.BRAND, footer=footer)
        await inter.response.send_message(embed=e, ephemeral=True)

    @tree.command(name="member", description="A member's stored aggregate statistics")
    async def member_cmd(inter: discord.Interaction, user: discord.User, days: app_commands.Range[int, 1, 365] = 30) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        days = min(clamp_days(days, cfg), cfg.member_retention_days)
        st = analytics.member_stats(data(), cfg, user.id, days, today_for(cfg))
        if st is None:
            return await inter.response.send_message("No statistics stored for that member.", ephemeral=True)
        t, tz = st["totals"], cfg.tz
        lines = [
            f"**Messages:** {t['m']:,} ({t['d']} duplicates, {t['b']} burst, {t['i']} imported)",
            f"**Active days:** {st['active_days']} of {days}",
            f"**Replies to others:** {t['r']} · **Reactions received:** {t['rx']}",
            f"**Voice:** {t['v'] / 60:.0f} min in {t['vs']} sessions",
            f"**Rank (effective messages):** {st['rank'] or 'unranked'} of {st['ranked_members']}",
            f"**Activity:** `{analytics.sparkline([n for _, n in st['daily']][-30:])}`",
            "**Top channels:** " + (", ".join(f"<#{c}> {n}" for c, n in st["channels"]) or "none"),
            f"**First seen:** {fmt_ts(st['first_seen'], tz)} · **Last active:** {fmt_ts(st['last_active'], tz)}",
            f"**Joined (observed):** {fmt_ts(st['joined'], tz)}" + (f" · **Left:** {fmt_ts(st['left'], tz)}" if st["left"] else ""),
            f"**Awards:** {', '.join(st['wins']) or 'none'} · **Nominations:** {st['nominations']}",
        ]
        if st["excluded"]:
            lines.append("ℹ️ Has an excluded role: not eligible for Member of the Week.")
        await inter.response.send_message(embed=embed(f"Member: {st['name']}", "\n".join(lines)), ephemeral=True)

    @tree.command(name="health", description="Collection coverage, uptime and storage status")
    async def health_cmd(inter: discord.Interaction) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        h, tz = bot.health(), cfg.tz
        lines = [
            f"**Version:** {h['version']} · **Connected:** {'yes' if h['connected'] else 'no'} · **Latency:** {h['latency_ms']} ms",
            f"**Uptime:** {h['uptime_h']:.1f} h · **Events this run:** {h['events']:,}",
            f"**Collecting since:** {fmt_ts(h['collection_start'], tz)} · **Last event:** {fmt_ts(h['last_event'], tz)}",
            f"**Coverage (7d):** {h['coverage_7d']:.0%} · **Gaps (7d):** {h['gaps_7d']}",
            f"**Last save:** {fmt_ts(h['last_save'], tz)} ({h['save_ms']} ms) · pending changes: {'yes' if h['dirty'] else 'no'}",
            f"**data.json:** {h['file_kb']:.0f} KB (warning at {h['warn_mb']} MB){' ⚠️' if h['size_warning'] else ''} · backups: {h['backups']}",
            f"**Stored:** {h['members']:,} members · {h['member_days']:,} member-day records over {h['detail_days']} days · "
            f"{h['server_days']} server days · {h['channels']} channels",
            f"**In memory:** {h['recent_cache']:,} recent message refs · {h['spam_tracked']:,} spam trackers"
            + (f" · RSS {h['rss_mb']:.0f} MB" if h["rss_mb"] else ""),
            f"**Unreadable channels:** {h['unreadable']}",
        ]
        if h["save_error"]:
            lines.append(f"⚠️ **Last save failed:** {h['save_error']}")
        await inter.response.send_message(embed=embed("Bot health", "\n".join(lines)), ephemeral=True)

    @tree.command(name="export", description="Export a report (staff) or a data backup (owner)")
    @app_commands.choices(kind=[
        app_commands.Choice(name="Audit report (Markdown)", value="audit"),
        app_commands.Choice(name="Weekly shortlist (CSV)", value="weekly"),
        app_commands.Choice(name="Leaderboard (CSV)", value="leaderboard"),
        app_commands.Choice(name="Channels (CSV)", value="channels"),
        app_commands.Choice(name="Daily server totals (CSV)", value="daily"),
        app_commands.Choice(name="Full data backup (owner, gzip)", value="data"),
    ])
    async def export_cmd(inter: discord.Interaction, kind: str, days: app_commands.Range[int, 1, 365] = 30,
                         week: app_commands.Range[int, -52, 0] = -1) -> None:
        if kind == "data":
            if not access.is_owner(inter):
                return await deny(inter, "the owner")
        elif not access.is_staff(inter):
            return await deny(inter)
        await inter.response.defer(ephemeral=True, thinking=True)
        days = clamp_days(days, cfg)
        today = today_for(cfg)
        if kind == "audit":
            report, _ = analytics.run_audit(data(), cfg, days, today)
            f = text_file(f"coldd-audit-{today}.md", report)
        elif kind == "weekly":
            r = week_result(week)
            rows = [(c.rank, c.uid, c.name, round(c.score, 2), c.tie, c.effective_messages, c.active_days, c.replies,
                     c.reactions, c.nominations, round(c.voice_minutes), c.duplicates, c.bursts, "; ".join(c.flags))
                    for c in r["all_eligible"][: cfg.export_max_rows]]
            f = text_file(f"weekly-{r['week']}.csv", analytics.to_csv(
                ["rank", "user_id", "name", "score", "tie", "effective_messages", "active_days", "replies",
                 "reactions_received", "nominations", "voice_minutes", "duplicates", "bursts", "flags"], rows))
        elif kind == "leaderboard":
            board = analytics.leaderboard(data(), cfg, min(days, cfg.member_retention_days), today, "messages", limit=cfg.export_max_rows)
            f = text_file(f"leaderboard-{days}d.csv", analytics.to_csv(
                ["rank", "user_id", "name", "effective_messages", "messages", "active_days", "replies", "reactions", "voice_seconds"],
                [(r["rank"], r["uid"], r["name"], r["eff"], r["m"], r["days"], r["r"], r["rx"], r["v"]) for r in board]))
        elif kind == "channels":
            chans = analytics.channel_stats(data(), cfg, days, today)
            f = text_file(f"channels-{days}d.csv", analytics.to_csv(
                ["channel_id", "name", "messages", "share", "unique_members", "previous_period", "trend"],
                [(r["cid"], r["name"], r["messages"], round(r["share"], 4), r["unique"], r["prev"], r["trend"]) for r in chans]))
        elif kind == "daily":
            ds = analytics.period(today, days)
            srv = data()["server"]
            f = text_file(f"daily-{days}d.csv", analytics.to_csv(
                ["day", "messages", "active_members", "joins", "departures", "bans", "voice_minutes", "duplicates",
                 "bursts", "imported", "coverage_hours"],
                [(d, srv.get(d, {}).get("m", 0),
                  len(data()["days"][d]["u"]) if d in data()["days"] else srv.get(d, {}).get("a", ""),
                  srv.get(d, {}).get("j", 0), srv.get(d, {}).get("lv", 0), srv.get(d, {}).get("bn", 0),
                  round(srv.get(d, {}).get("v", 0) / 60), srv.get(d, {}).get("d", 0), srv.get(d, {}).get("b", 0),
                  srv.get(d, {}).get("im", 0), round(srv.get(d, {}).get("cov", 0) / 3600, 2)) for d in ds]))
        else:
            await bot.flush(reason="export")
            raw = gzip.compress(bot.store.path.read_bytes())
            if len(raw) > UPLOAD_LIMIT:
                return await inter.followup.send(
                    f"Compressed backup is {len(raw) / 1048576:.1f} MB, above Discord's upload limit. "
                    "Download data.json or the backups/ folder from the hosting panel instead.", ephemeral=True)
            f = text_file(f"coldd-data-{today}.json.gz", raw)
            log.info("Data backup exported by owner %s.", inter.user.id)
        await inter.followup.send(f"Export ready: `{f.filename}`", file=f, ephemeral=True)

    @tree.command(name="config", description="Show non-secret configuration")
    async def config_cmd(inter: discord.Interaction) -> None:
        if not access.is_staff(inter):
            return await deny(inter)
        lines = [f"**{k}:** {v}" for k, v in cfg.safe_summary().items()]
        await inter.response.send_message(embed=embed("Configuration (secrets hidden)", "\n".join(lines)), ephemeral=True)

    def readable_targets(guild: discord.Guild) -> list[Any]:
        """Every text channel and active thread the bot can read and that isn't excluded by configuration."""
        me = guild.me
        out: list[Any] = []
        for ch in [*guild.text_channels, *guild.threads]:
            perms = ch.permissions_for(me)
            if not (perms.view_channel and perms.read_message_history):
                continue
            _, check_ids = bot._resolve_channel(ch)
            if bot.collector.channel_allowed(*check_ids):
                out.append(ch)
        return out

    async def run_import(inter: discord.Interaction, targets: list[Any], days: int) -> None:
        totals = {"imported": 0, "skipped": 0, "windows": 0}
        failed: list[str] = []
        try:
            for ch in targets:
                try:
                    res = await bot.import_history(ch, days, None)
                except discord.Forbidden:
                    failed.append(f"{ch.mention}: no access")
                    continue
                except Exception as exc:  # keep going: one bad channel must not stop the rest
                    log.exception("History import failed for channel %s", ch.id)
                    failed.append(f"{ch.mention}: {type(exc).__name__}")
                    continue
                for k in totals:
                    totals[k] += res.get(k, 0)
                if res.get("note"):
                    failed.append(f"{ch.mention}: {res['note']}")
        finally:
            bot._import_running = False
        e = ui.card(f"History import · last {days} days",
                    f"Channels processed: **{len(targets)}**\n"
                    f"Messages imported: **{totals['imported']:,}** · bot messages skipped: **{totals['skipped']:,}**\n"
                    f"Uncovered windows filled: **{totals['windows']}**",
                    inter.guild, ui.WARN if failed else ui.BRAND,
                    footer="Imported messages are labelled separately · reactions are not back-filled")
        if failed:
            e.add_field(name="Skipped or failed", value="\n".join(failed)[:1024], inline=False)
        try:
            await inter.channel.send(content=inter.user.mention, embed=e)
        except discord.HTTPException:
            log.warning("Could not post the import report in the invoking channel; see logs for totals.")
            log.info("History import finished: %s; failed: %s", totals, failed)

    @tree.command(name="import_history", description="Owner: import past messages for one channel, or every readable channel")
    @app_commands.describe(channel="One channel (leave empty for every channel the bot can read)",
                           days="How far back (max = member retention)")
    async def import_cmd(inter: discord.Interaction, channel: discord.TextChannel | None = None,
                         days: app_commands.Range[int, 1, 365] = 90) -> None:
        if not access.is_owner(inter):
            return await deny(inter, "the owner")
        if not cfg.history_import_enabled:
            return await inter.response.send_message("History import is disabled. Set HISTORY_IMPORT_ENABLED=true to allow it.",
                                                     ephemeral=True)
        if bot._import_running:
            return await inter.response.send_message("An import is already running.", ephemeral=True)
        days = min(days, cfg.member_retention_days)
        targets = [channel] if channel else readable_targets(inter.guild)
        if not targets:
            return await inter.response.send_message("The bot can't read any channels to import.", ephemeral=True)
        bot._import_running = True
        # The import can run for a long time, longer than Discord allows for a reply, so it runs in the background
        # and the report is posted to the channel where the command was run.
        asyncio.create_task(run_import(inter, targets, days))
        await inter.response.send_message(
            f"Importing {days} days from {len(targets)} channel(s) in the background. "
            f"The report will be posted here when it finishes.", ephemeral=True)

    @tree.command(name="forget", description="Owner: delete a member's stored statistics (privacy request)")
    async def forget_cmd(inter: discord.Interaction, user: discord.User, confirm: bool = False) -> None:
        if not access.is_owner(inter):
            return await deny(inter, "the owner")
        if not confirm:
            return await inter.response.send_message("Re-run with `confirm: True` to permanently delete this member's statistics.",
                                                     ephemeral=True)
        n = bot.collector.forget_member(user.id)
        await bot.flush(reason="forget member")
        log.info("Deleted %d stored record(s) for a member at owner request.", n)
        await inter.response.send_message(f"Deleted {n} stored record(s). Older backups still contain them until they rotate out.",
                                          ephemeral=True)


def describe_start(cfg: Config) -> str:
    return f"COLDD analytics bot {__version__} (pid {os.getpid()}, tz {cfg.timezone_name}, data {cfg.data_path})"

