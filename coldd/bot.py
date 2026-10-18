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
from collections.abc import Awaitable, Callable
from datetime import date, datetime, timedelta, timezone
from typing import Any, cast

import discord
from discord import app_commands

from . import __version__, ai, analytics, recount, scoring, ui
from .collector import Collector, content_fingerprint
from .config import Config
from .storage import Store
from .timeutil import day_start_ts, fmt_ts, now_ts, to_local

log = logging.getLogger("coldd.bot")

EMBED_DESC_LIMIT = 3900
DEEPSCAN_COOLDOWN_S = 6 * 3600   # one deep scan per six hours, server-wide
DEEPSCAN_TIMEOUT_S = 240         # long-form reports take far longer than a chat reply
DEEPSCAN_KEEP = 6                # previous reports kept in data.json as the bot's memory
RECOUNT_CHUNK_DAYS = 30          # days rebuilt per batch: bounds memory, and each batch is applied atomically
RECOUNT_REACTION_DAYS = 30        # reactions are rebuilt only for this many recent days (each costs a request)
RECOUNT_CONCURRENCY = 4          # channels read at once: faster, and still well inside Discord's per-route limits
RECOUNT_MAX_CHUNK_MESSAGES = 500_000  # safety valve for a 1 GB host; a batch beyond this is skipped, not half-applied
UPLOAD_LIMIT = 8 * 1024 * 1024
LINK_RE = re.compile(r"discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)")
BRAND = 0xD61F26  # COLDD red


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
        self.tree = app_commands.CommandTree(self)
        self.started_at = time.time()
        self.connected = False
        self._ticker: asyncio.Task[None] | None = None
        self._auto_task: asyncio.Task[None] | None = None
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
        if self._auto_task is None or self._auto_task.done():
            self._auto_task = asyncio.create_task(self._auto_recount_loop(), name="coldd-auto-recount")

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
        # Record the channel's name first: whether a message is scored can depend on it (e.g. "ticket-" channels).
        ch = message.channel.parent if isinstance(message.channel, discord.Thread) and message.channel.parent else message.channel
        ch_name = getattr(ch, "name", None)
        if ch_name:
            self.collector.note_channel(count_id, ch_name)
        self.collector.record_message(
            author.id, count_id, int(message.created_at.timestamp()),
            name=getattr(author, "display_name", None) or author.name,
            is_bot=is_bot, fingerprint=fp, reply_to_other=reply_to_other,
            excluded_role=excluded, message_id=message.id, partners=self._partners(message),
        )

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

    async def readable_targets(self, guild: discord.Guild) -> list[Any]:
        """Every place people can post that the bot can read and that isn't excluded by configuration:
        text channels, voice-channel text chats, stage text, forum posts, and active and archived threads."""
        me = guild.me
        candidates: list[Any] = [
            ch for ch in guild.channels
            if isinstance(ch, (discord.TextChannel, discord.VoiceChannel, discord.StageChannel, discord.ForumChannel))
        ]
        candidates += list(guild.threads)
        for ch in [c for c in candidates if isinstance(c, (discord.TextChannel, discord.ForumChannel))]:
            if not ch.permissions_for(me).read_message_history:
                continue
            try:
                async for thread in ch.archived_threads(limit=None):
                    candidates.append(thread)
                if isinstance(ch, discord.TextChannel):  # private archived threads, visible to an administrator
                    async for thread in ch.archived_threads(private=True, limit=None):
                        candidates.append(thread)
            except discord.HTTPException:
                log.warning("Could not list archived threads in %s", ch.id)
        seen: set[int] = set()
        out: list[Any] = []
        for ch in candidates:
            if ch.id in seen or isinstance(ch, discord.ForumChannel):
                continue
            seen.add(ch.id)
            perms = ch.permissions_for(me)
            if not (perms.view_channel and perms.read_message_history):
                continue
            _, check_ids = self._resolve_channel(ch)
            if self.collector.channel_allowed(*check_ids):
                out.append(ch)
        return out

    @staticmethod
    def _partners(message: discord.Message) -> tuple[int, ...]:
        """Distinct humans this message is directed at: the author it replies to and anyone mentioned."""
        author = message.author
        if author.bot or message.webhook_id is not None:
            return ()
        found: set[int] = set()
        ref = message.reference.resolved if message.reference else None
        if isinstance(ref, discord.Message) and not ref.author.bot and ref.author.id != author.id:
            found.add(ref.author.id)
        for user in getattr(message, "mentions", ()):
            if not user.bot and user.id != author.id:
                found.add(user.id)
        return tuple(found)

    def _channel_scored(self, ch: Any, count_id: int) -> bool:
        parent = ch.parent if isinstance(ch, discord.Thread) else None
        return self.collector.is_scored(count_id, (parent.name if parent else getattr(ch, "name", None)))

    async def _to_msg(self, m: discord.Message, count_id: int, scored: bool, with_reactions: bool,
                      warnings: list[int]) -> recount.Msg | None:
        """Turn a history message into the compact record the rebuild needs (message text is not kept)."""
        if m.is_system():
            return None
        author = m.author
        is_bot = author.bot or m.webhook_id is not None
        ref = m.reference.resolved if m.reference else None
        reply = isinstance(ref, discord.Message) and ref.author.id != author.id and not ref.author.bot
        fp = content_fingerprint(m.content) if m.content else None
        reactors: set[int] = set()
        if with_reactions and not is_bot and m.reactions:
            for reaction in m.reactions:
                try:
                    async for user in reaction.users(limit=None):
                        if not user.bot and user.id != author.id:
                            reactors.add(user.id)
                except discord.HTTPException:
                    warnings[0] += 1  # this message's reactions may be incomplete
        return recount.Msg(int(m.created_at.timestamp()), author.id, count_id, fp, reply,
                           getattr(author, "display_name", None) or author.name, is_bot, tuple(reactors),
                           scored, self._partners(m))

    @staticmethod
    def _thread_has_window_activity(ch: Any, start: datetime) -> bool:
        """A thread archived before the window started cannot have posts inside it, so it is skipped."""
        if isinstance(ch, discord.Thread) and ch.archived and ch.archive_timestamp is not None:
            return ch.archive_timestamp >= start
        return True

    async def _read_channel(self, ch: Any, after: datetime, before: datetime, scored: bool, with_reactions: bool,
                            warnings: list[int]) -> tuple[list[recount.Msg], str | None]:
        """Every qualifying message in one channel for the window. Returns (messages, error or None)."""
        count_id, _ = self._resolve_channel(ch)
        msgs: list[recount.Msg] = []
        try:
            async for m in ch.history(after=after, before=before, limit=None, oldest_first=True):
                rec = await self._to_msg(m, count_id, scored, with_reactions, warnings)
                if rec is not None:
                    msgs.append(rec)
                if len(msgs) > RECOUNT_MAX_CHUNK_MESSAGES:
                    raise MemoryError("batch too large")
        except discord.Forbidden:
            return [], "forbidden"
        except (discord.HTTPException, MemoryError) as exc:
            return [], type(exc).__name__
        return msgs, None

    async def rebuild_history(self, days: int, reaction_days: int = 0,
                              progress: Callable[[int, int, int], Awaitable[None]] | None = None) -> dict[str, Any]:
        """Rebuild exact message counts for the last ``days`` complete days from Discord's own history.

        Reactions are rebuilt only for the most recent ``reaction_days`` (0 = none), because each reaction costs a
        separate request. Channels are read a few at a time. A batch is applied only if every channel it needs was
        read successfully, so an error never leaves a half-updated day. Today is never touched.
        """
        guild = self.get_guild(self.cfg.guild_id)
        if guild is None:
            raise RuntimeError("not connected to the server")
        tz = self.cfg.tz
        today = to_local(now_ts(), tz).date()
        days = max(1, min(days, self.cfg.member_retention_days))
        reaction_days = max(0, min(reaction_days, days))
        all_days = recount.last_complete_days(today, days)
        reaction_from = all_days[len(all_days) - reaction_days] if reaction_days else None
        targets = await self.readable_targets(guild)
        await asyncio.to_thread(self.store.make_backup, True)  # one safety copy before any counter is replaced
        chunks = [all_days[i:i + RECOUNT_CHUNK_DAYS] for i in range(0, len(all_days), RECOUNT_CHUNK_DAYS)]
        total_steps = max(1, len(chunks) * len(targets))
        step = read = 0
        warnings = [0]
        sem = asyncio.Semaphore(RECOUNT_CONCURRENCY)
        out: dict[str, Any] = {"days_requested": days, "days_rebuilt": 0, "messages": 0, "channels": len(targets),
                               "chunks_failed": 0, "failed": [], "skipped_channels": [], "reaction_warnings": 0,
                               "reaction_days": reaction_days, "threads_skipped": 0}
        for chunk in chunks:
            start_dt = datetime.fromtimestamp(day_start_ts(date.fromisoformat(chunk[0]), tz), tz=timezone.utc)
            end_dt = datetime.fromtimestamp(day_start_ts(date.fromisoformat(chunk[-1]) + timedelta(days=1), tz), tz=timezone.utc)
            after = start_dt - timedelta(milliseconds=1)
            with_reactions = reaction_from is not None and chunk[-1] >= reaction_from
            active = [ch for ch in targets if self._thread_has_window_activity(ch, start_dt)]
            out["threads_skipped"] += len(targets) - len(active)

            async def run(ch: Any, after: datetime = after, end_dt: datetime = end_dt,
                          with_reactions: bool = with_reactions) -> tuple[Any, list[recount.Msg], str | None]:
                count_id, _ = self._resolve_channel(ch)
                async with sem:
                    msgs, err = await self._read_channel(ch, after, end_dt, self._channel_scored(ch, count_id),
                                                         with_reactions, warnings)
                return ch, msgs, err

            msgs: list[recount.Msg] = []
            failed = False
            pending = [asyncio.ensure_future(run(ch)) for ch in active]
            for fut in asyncio.as_completed(pending):
                ch, got, err = await fut
                step += 1
                if err == "forbidden":
                    if str(ch.id) not in out["skipped_channels"]:
                        out["skipped_channels"].append(str(ch.id))
                elif err:
                    failed = True
                    out["failed"].append(f"{chunk[0]} to {chunk[-1]} in {ch.id}: {err}")
                else:
                    msgs.extend(got)
                    read += len(got)
                if progress is not None:
                    await progress(step, total_steps, read)
            step += len(targets) - len(active)
            if failed:
                for fut in pending:
                    fut.cancel()
                out["chunks_failed"] += 1
                continue
            result = recount.rebuild_days(self.store.data, self.cfg, msgs, chunk, now_ts(), reactions=with_reactions)
            self.collector.dirty = True
            await self.flush(reason="history rebuild")
            out["days_rebuilt"] += result["days"]
            out["messages"] += result["messages"]
        out["reaction_warnings"] = warnings[0]
        return out

    async def _auto_recount_loop(self) -> None:
        """Once a day after 04:00 local time, re-verify the last few complete days so deletions and any downtime
        are corrected without anyone having to run a command."""
        while True:
            try:
                await asyncio.sleep(600)
                if self.cfg.auto_recount_days <= 0 or not self.connected or self._import_running:
                    continue
                local = to_local(now_ts(), self.cfg.tz)
                key = local.date().isoformat()
                meta = self.store.data["meta"]
                if local.hour < 4 or meta.get("last_auto_recount") == key:
                    continue
                self._import_running = True
                try:
                    res = await self.rebuild_history(self.cfg.auto_recount_days, reaction_days=0)
                finally:
                    self._import_running = False
                if res["chunks_failed"] == 0:
                    meta["last_auto_recount"] = key
                    self.collector.dirty = True
                log.info("Nightly re-verification: %d day(s), %d messages, %d failed batch(es).",
                         res["days_rebuilt"], res["messages"], res["chunks_failed"])
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Nightly re-verification failed; will retry.")

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


# --- command helpers ----------------------------------------------------------------

def embed(title: str, description: str = "", color: int = BRAND) -> discord.Embed:
    if len(description) > EMBED_DESC_LIMIT:
        description = description[: EMBED_DESC_LIMIT - 40] + "\n… (truncated; see attached file)"
    e = discord.Embed(title=title[:256], description=description, color=color)
    e.set_thumbnail(url=ui.dragon_url())
    return e


def text_file(name: str, content: str | bytes) -> discord.File:
    raw = content.encode("utf-8") if isinstance(content, str) else content
    return discord.File(io.BytesIO(raw), filename=name)


def today_for(cfg: Config) -> date:
    return to_local(now_ts(), cfg.tz).date()


def clamp_days(days: int, cfg: Config) -> int:
    return max(1, min(days, cfg.audit_max_days))


def register_commands(bot: ColddBot) -> None:
    cfg, tree = bot.cfg, bot.tree
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
            "**Activity and rankings**",
            "`/analytics [days] [ai_read]` server dashboard, optionally with an AI read",
            "`/insights [days]` growth, engagement, peak times, retention",
            "`/top [metric] [days]` leaderboards: messages, consistency, streaks, reactions, interactions, helpful, voice",
            "`/profile [member]` one member's stats and ranks",
            "`/deepscan` deep AI analysis of every metric",
            "",
            "**Member of the Week**",
            "`/motw` current leaders · `/weekly show [week] [ai_check]` full shortlist with a fairness read",
            "`/weekly nominate` · `/weekly approve` (needs a verified week) · `/weekly paid` · `/weekly history`",
            "`/weekly ai_review` advisory AI read of staff-selected messages",
            "",
            "**Data**",
            "`/import_history [days]` rebuild exact counts from Discord's own history (also runs nightly for recent days)",
            "`/export <kind>` reports and CSVs, or a full backup · `/health` status · `/forget` delete a member's data",
            "",
            "Counts match Discord's history for rebuilt days. The bot stores no message text and never pays or awards anything itself.",
        ]
        help_embed = embed("COLDD Analytics: help", "\n".join(lines))
        await inter.response.send_message(embed=help_embed, ephemeral=True)

    @tree.command(name="analytics", description="Server activity dashboard")
    @app_commands.describe(days="Number of days (default 7)", ai_read="Add a short AI-written read of the figures")
    async def analytics_cmd(inter: discord.Interaction, days: app_commands.Range[int, 1, 365] = 7,
                            ai_read: bool = False) -> None:
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
        if ai_read and cfg.ai_enabled and ai.budget_ok(data(), cfg, today_for(cfg).isoformat()):
            await inter.response.defer(ephemeral=True, thinking=True)
            stats = {
                "period_days": days, "messages": s["messages"], "messages_previous": s["messages_prev"],
                "active_members": s["unique_active"], "active_members_previous": s["unique_active_prev"],
                "returning_members": s["returning"], "joins": s["joins"], "leaves": s["leaves"],
                "mean_messages_per_active": round(s["avg_per_active"], 2),
                "median_messages_per_active": round(s["median_per_active"], 2),
                "top10pct_message_share": round(s["top10_share"], 3), "voice_hours": round(s["voice_hours"], 1),
                "peak_hour": s["peak_hour"], "coverage_ratio": round(cov["ratio"], 3),
                "coverage_ratio_previous": round(s["coverage_prev"], 3),
                "daily_messages": [[d, m] for d, m, _ in s["daily"]],
            }
            ai.consume_budget(data())
            try:
                e.add_field(name="✨ AI read", value=(await asyncio.to_thread(ai.narrate, cfg, stats))[:1000], inline=False)
            except ai.AIError as exc:
                log.warning("AI read failed: %s", ai.redact(str(exc), cfg.secrets()))
            return await inter.followup.send(embed=e, file=ui.banner_file(), ephemeral=True)
        await inter.response.send_message(embed=e, file=ui.banner_file(), ephemeral=True)

    community_choices = [app_commands.Choice(name=label, value=key) for key, (label, _, _) in analytics.COMMUNITY_METRICS.items()]

    @tree.command(name="top", description="Community leaderboard: messages, consistency, streaks, reactions, interactions")
    @app_commands.describe(metric="What to rank by", days="Window in days (default 7)", ai_highlight="Add a short AI highlight (uses AI budget)")
    @app_commands.choices(metric=community_choices)
    async def top_cmd(inter: discord.Interaction, metric: str = "messages",
                      days: app_commands.Range[int, 1, 365] = 7, ai_highlight: bool = False) -> None:
        days = min(clamp_days(days, cfg), cfg.member_retention_days)
        today = today_for(cfg)
        label, unit, _ = analytics.COMMUNITY_METRICS[metric]
        rows = analytics.community_board(data(), cfg, days, today, metric, limit=10)
        top = rows[0]["value"] if rows else 0
        def extra(r: dict[str, Any]) -> str:
            return f" · {r['m']:,} sent in total" if metric == "messages" and r["m"] != r["value"] else ""

        lines = [f"{ui.medal(r['rank'])} <@{r['uid']}>\n"
                 f"┗ `{ui.bar(r['value'], top)}` **{r['value']:,}** {unit}{extra(r)}" for r in rows]
        v = recount.verification(data(), analytics.period(today, days), today)
        verified = f"{v['verified_days']}/{v['complete_days']} days verified against Discord"
        e = ui.card(f"{label} · last {days} days", "\n".join(lines) or "No activity recorded in this window.", inter.guild,
                    footer=f"Top {len(rows)} · {verified} · counted messages exclude duplicates, bursts and non-scoring channels")
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

    @tree.command(name="profile", description="A member's community profile and ranks")
    @app_commands.describe(member="Member to view (default: you)", days="Window in days (default 30)")
    async def profile_cmd(inter: discord.Interaction, member: discord.Member | None = None,
                          days: app_commands.Range[int, 1, 365] = 30) -> None:
        target = member or inter.user
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
        e.add_field(name="💬 Messages", value=f"**{f['eff']:,}** counted ({f['m']:,} sent) · {place('messages')}", inline=True)
        e.add_field(name="📅 Active days", value=f"**{f['days']}** of {p['window_days']} · {place('consistency')}", inline=True)
        e.add_field(name="🔥 Best streak", value=f"**{f['streak']}** days · {place('streak')}", inline=True)
        e.add_field(name="❤️ Reactions given", value=f"**{f['rg']:,}** · {place('reacting')}", inline=True)
        e.add_field(name="🤝 Interactions", value=f"**{f['r'] + f['rg'] + f['rx']:,}** · {place('interactions')}", inline=True)
        e.add_field(name="⭐ Helpful", value=f"replies {f['r']} · reactions received {f['rx']} · {place('helpful')}", inline=True)
        e.add_field(name="🎙️ Voice", value=f"**{round(f['v'] / 60)} min** · {place('voice')}", inline=True)
        e.add_field(name="💬 Talks with", value=f"**{f['partners']}** different people · {place('conversation')}", inline=True)
        e.add_field(name="🕒 Present", value=f"**{f['presence']}** active hours · {place('presence')}", inline=True)
        await inter.response.send_message(embed=e, ephemeral=True)

    @tree.command(name="insights", description="Server health: growth, engagement, peak times, retention")
    @app_commands.describe(days="Number of days (default 30)")
    async def insights_cmd(inter: discord.Interaction, days: app_commands.Range[int, 7, 365] = 30) -> None:
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

    @tree.command(name="deepscan", description="Deep AI analysis of every metric (slow, rate-limited)")
    async def deepscan_cmd(inter: discord.Interaction) -> None:
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

    weekly = app_commands.Group(name="weekly", description="Member of the Week")

    def week_result(offset: int) -> dict[str, Any]:
        start = scoring.resolve_week(cfg, today_for(cfg), offset)
        return scoring.score_week(data(), cfg, start)

    @weekly.command(name="show", description="Generate the Member of the Week shortlist")
    @app_commands.describe(week="0 = current week, -1 = last week, -2 = two weeks ago…",
                           ai_check="Add an AI fairness read of the shortlist (uses AI budget; sees no names)")
    async def weekly_show(inter: discord.Interaction, week: app_commands.Range[int, -52, 0] = 0,
                          ai_check: bool = False) -> None:
        await inter.response.defer(ephemeral=True, thinking=True)
        r = week_result(week)
        rows = r["shortlist"]
        warn = [f"⚠️ {w}" for w in r["warnings"]]
        if r["award"]:
            paid = "paid ✅" if r["award"].get("paid") else "unpaid"
            warn.insert(0, f"🏆 Recorded winner: <@{r['award']['winner']}> · {paid}")
        reign_line = ""
        if rows:
            held = scoring.reign_length(data(), str(rows[0]["uid"]), date.fromisoformat(r["week"]))
            if held:
                reign_line = f"\n👑 <@{rows[0]['uid']}> has held the title for **{held}** consecutive week(s) so far"
        cv = r["coverage"]
        if cv["verified"]:
            verify_line = f"\n✅ **Verified** against Discord's history (rebuilt <t:{cv['verified_at']}:R>)"
        else:
            verify_line = f"\n⚠️ **Provisional**: {cv['verified_days']} of 7 days verified. Run `/import_history` before approving"
        e = ui.card(f"Member of the Week · {r['week']}",
                    f"Award **{cfg.award_amount} {cfg.award_currency}** (paid manually)\n"
                    f"Eligible **{r['eligible_count']}** of {r['considered_count']} · "
                    f"coverage **{ui.pct(r['coverage']['ratio'])}**" + reign_line + verify_line,
                    inter.guild, ui.color_for(r["coverage"]["ratio"]),
                    footer="Use /weekly approve to record a decision · full breakdown attached")
        for row in rows[:3]:
            flag = " ⚠️" if row["flags"] else ""
            tie = " (tie)" if row["tie"] else ""
            e.add_field(name=f"{ui.medal(row['rank'])} {tie.strip() or 'Rank ' + str(row['rank'])}{flag}",
                        value=f"<@{row['uid']}>\n**{row['score']:.1f}** pts\n"
                              f"{row['effective_messages']} msgs · {row['active_days']}d\n"
                              f"{row['replies']} replies · {row['reactions']} ❤ · {row['nominations']} noms\n"
                              f"talks with {row['partners']} people · active {row['presence_hours']}h",
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
        if ai_check and rows and cfg.ai_enabled and ai.budget_ok(data(), cfg, today_for(cfg).isoformat()):
            keep = ("rank", "score", "components", "messages_raw", "effective_messages", "duplicates", "bursts",
                    "active_days", "replies", "reactions", "reactions_given", "non_scoring_messages", "nominations",
                    "voice_minutes", "imported", "channels", "flags", "tie")
            facts = {"week": r["week"], "verified": cv["verified"], "weights": r["weights"],
                     "eligible": r["eligible_count"], "candidates": [{k: row[k] for k in keep} for row in rows[:5]],
                     "ineligible": r["ineligible"], "warnings": r["warnings"]}
            ai.consume_budget(data())
            try:
                e.add_field(name="🧠 AI fairness read",
                            value=(await asyncio.to_thread(ai.explain_shortlist, cfg, facts))[:1000], inline=False)
            except ai.AIError as exc:
                log.warning("AI fairness read failed: %s", ai.redact(str(exc), cfg.secrets()))
        e.set_image(url=ui.banner_url())
        await inter.followup.send(embed=e,
                                  files=[ui.banner_file(), text_file(f"weekly-{r['week']}.md", analytics.weekly_markdown(r, cfg))],
                                  ephemeral=True)

    @weekly.command(name="approve", description="Record the staff-chosen Member of the Week (no automatic payout)")
    @app_commands.describe(member="Chosen winner", week="0 = current, -1 = last week (default)", note="Optional reason",
                           replace="Replace an already recorded winner",
                           unverified="Approve even though the week is not fully verified against Discord (not recommended)")
    async def weekly_approve(inter: discord.Interaction, member: discord.Member,
                             week: app_commands.Range[int, -52, 0] = -1, note: str = "", replace: bool = False,
                             unverified: bool = False) -> None:
        r = week_result(week)
        cv = r["coverage"]
        if not cv["verified"] and not unverified:
            need = max(1, (today_for(cfg) - date.fromisoformat(r["days"][0])).days)
            return await inter.response.send_message(
                f"Week {r['week']} isn't verified against Discord's history yet ({cv['verified_days']} of 7 days). "
                f"Run `/import_history days:{need}` first so the counts match what Discord shows, then approve. "
                "To approve anyway, set `unverified: True`.", ephemeral=True)
        ok, msg = scoring.approve_winner(data(), cfg, r, member.id, inter.user.id, note, replace)
        if ok:
            bot.collector.dirty = True
            await bot.flush(reason="award approved")
            log.info("Award for week %s recorded by staff %s.", r["week"], inter.user.id)
        await inter.response.send_message(msg, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @weekly.command(name="paid", description="Mark a recorded award as paid out")
    async def weekly_paid(inter: discord.Interaction, week: app_commands.Range[int, -52, 0] = -1) -> None:
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
        rows = sorted(data()["awards"].items(), reverse=True)[:25]
        lines = [f"`{wk}` <@{a['winner']}> · {a.get('amount')} {a.get('currency')} · "
                 f"{'paid' if a.get('paid') else 'unpaid'} · by <@{a.get('approved_by')}>" for wk, a in rows]
        await inter.response.send_message(embed=embed("Member of the Week history", "\n".join(lines) or "No awards recorded."),
                                          ephemeral=True)

    @weekly.command(name="nominate", description="Record a staff nomination (counts toward helpfulness)")
    @app_commands.describe(member="Member to nominate", reason="Short reason (stored)", week="0 = current week")
    async def weekly_nominate(inter: discord.Interaction, member: discord.Member, reason: str,
                              week: app_commands.Range[int, -52, 0] = 0) -> None:
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

    @tree.command(name="health", description="Collection coverage, uptime and storage status")
    async def health_cmd(inter: discord.Interaction) -> None:
        h, tz = bot.health(), cfg.tz
        lines = [
            f"**Version:** {h['version']} · **Connected:** {'yes' if h['connected'] else 'no'} · **Latency:** {h['latency_ms']} ms",
            f"**Uptime:** {h['uptime_h']:.1f} h · **Events this run:** {h['events']:,}",
            f"**Collecting since:** {fmt_ts(h['collection_start'], tz)} · **Last event:** {fmt_ts(h['last_event'], tz)}",
            f"**Coverage (7d):** {h['coverage_7d']:.0%} · **Gaps (7d):** {h['gaps_7d']}",
            "**Verified against Discord (30d):** {} of {} complete days".format(
                *(lambda v: (v["verified_days"], v["complete_days"]))(
                    recount.verification(data(), analytics.period(today_for(cfg), 30), today_for(cfg)))),
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

    @tree.command(name="export", description="Export a report (Markdown or CSV) or a full data backup")
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

    async def run_rebuild(inter: discord.Interaction, days: int, reaction_days: int) -> None:
        progress_msg: discord.Message | None = None
        try:
            progress_msg = await cast(discord.abc.Messageable, inter.channel).send(
                f"Rebuilding {days} day(s) from Discord's history · collecting channels…")
        except (discord.HTTPException, AttributeError):
            log.warning("Could not post the rebuild progress message; progress goes to the logs only.")
        last_edit = 0.0

        async def progress(done: int, total: int, read: int) -> None:
            nonlocal last_edit
            if progress_msg is None or (time.monotonic() - last_edit < 15 and done < total):
                return
            last_edit = time.monotonic()
            try:
                await progress_msg.edit(content=f"Rebuilding {days} day(s) · `{ui.bar(done, total, 20)}` "
                                                f"{done}/{total} steps · {read:,} messages read so far")
            except discord.HTTPException:
                pass

        res: dict[str, Any] | None = None
        error = ""
        try:
            res = await bot.rebuild_history(days, reaction_days, progress)
        except Exception as exc:  # report it; never leave the import flag stuck
            log.exception("History rebuild failed")
            error = type(exc).__name__
        finally:
            bot._import_running = False
        if res is None:
            e = ui.card("History rebuild failed", f"The rebuild stopped with **{error}**. Nothing was half-applied: "
                        "completed batches are saved and the rest are untouched. Run it again.", inter.guild, ui.WARN)
        else:
            problems = bool(res["chunks_failed"] or res["skipped_channels"] or res["reaction_warnings"])
            e = ui.card(f"History rebuilt · last {res['days_requested']} complete days",
                        f"Days rebuilt: **{res['days_rebuilt']}** of {res['days_requested']}\n"
                        f"Messages counted: **{res['messages']:,}** from **{res['channels']}** channels and threads\n"
                        f"Reactions: **{'rebuilt for the last ' + str(res['reaction_days']) + ' days' if res['reaction_days'] else 'not rebuilt (existing counts kept)'}**",
                        inter.guild, ui.WARN if problems else ui.GOOD,
                        footer="Counts now match Discord's own history for these days · today is counted live · voice time is live-only")
            notes = []
            if res["chunks_failed"]:
                notes.append(f"{res['chunks_failed']} batch(es) failed and were left unchanged; run it again:\n"
                             + "\n".join(res["failed"][:5]))
            if res["skipped_channels"]:
                notes.append(f"{len(res['skipped_channels'])} channel(s) could not be read and are not included.")
            if res["reaction_warnings"]:
                notes.append(f"{res['reaction_warnings']} message(s) had reactions that could not be fully read.")
            if notes:
                e.add_field(name="Needs attention", value="\n".join(notes)[:1024], inline=False)
        try:
            await cast(discord.abc.Messageable, inter.channel).send(content=inter.user.mention, embed=e)
        except discord.HTTPException:
            log.warning("Could not post the rebuild report in the invoking channel.")
            log.info("History rebuild finished: %s", res or error)

    @tree.command(name="import_history", description="Rebuild exact counts from Discord's own history for the last N complete days")
    @app_commands.describe(days="Complete days back, up to the retention limit (default 30)",
                           reactions="Also rebuild reactions for the last 30 days (slower: one request per reaction)")
    async def import_cmd(inter: discord.Interaction, days: app_commands.Range[int, 1, 730] = 30,
                         reactions: bool = False) -> None:
        if not cfg.history_import_enabled:
            return await inter.response.send_message("History import is disabled. Set HISTORY_IMPORT_ENABLED=true to allow it.",
                                                     ephemeral=True)
        if bot._import_running:
            return await inter.response.send_message("A rebuild is already running.", ephemeral=True)
        days = min(days, cfg.member_retention_days)
        bot._import_running = True
        # A full rebuild can take a long time, longer than Discord allows for a reply, so it runs in the background
        # and the report is posted to the channel where the command was run.
        reaction_days = min(days, RECOUNT_REACTION_DAYS) if reactions else 0
        asyncio.create_task(run_rebuild(inter, days, reaction_days))
        await inter.response.send_message(
            f"Rebuilding the last {days} complete day(s) from Discord's history in the background. "
            f"This can take a while on a large server. The report will be posted here when it finishes.", ephemeral=True)

    @tree.command(name="forget", description="Delete a member's stored statistics (privacy request)")
    async def forget_cmd(inter: discord.Interaction, user: discord.User, confirm: bool = False) -> None:
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

