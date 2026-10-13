"""COLDD analytics bot entry point.  Start with:  python main.py"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parent


class RedactSecrets(logging.Filter):
    """Last line of defence: scrub configured secrets from every log record."""

    def __init__(self, secrets: list[str]):
        super().__init__()
        self.secrets = [s for s in secrets if s and len(s) >= 8]

    def filter(self, record: logging.LogRecord) -> bool:
        if self.secrets:
            msg = record.getMessage()
            clean = msg
            for s in self.secrets:
                clean = clean.replace(s, "[REDACTED]")
            if clean != msg:
                record.msg, record.args = clean, ()
        return True


def setup_logging(log_dir: Path, max_kb: int, backups: int, secrets: list[str]) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in list(root.handlers):
        root.removeHandler(h)
    redact = RedactSecrets(secrets)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    console.addFilter(redact)
    root.addHandler(console)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(log_dir / "bot.log", maxBytes=max_kb * 1024, backupCount=backups, encoding="utf-8")
        fh.setFormatter(fmt)
        fh.addFilter(redact)
        root.addHandler(fh)
    except OSError as exc:
        root.warning("File logging disabled (%s); logging to console only.", exc)
    logging.getLogger("discord").setLevel(logging.WARNING)
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)
    logging.getLogger("discord.client").setLevel(logging.ERROR)  # hides "PyNaCl not installed": no audio is used


async def run(cfg, store, collector) -> int:  # type: ignore[no-untyped-def]
    import aiohttp
    import discord

    from coldd.bot import ColddBot

    log = logging.getLogger("coldd")
    delay = 5
    stop = asyncio.Event()
    current: dict[str, ColddBot] = {}

    def request_stop() -> None:
        log.info("Shutdown requested; saving data and disconnecting.")
        stop.set()
        bot = current.get("bot")
        if bot is not None:
            asyncio.ensure_future(bot.close())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, request_stop)
        except (NotImplementedError, RuntimeError, ValueError):
            pass  # Windows: Ctrl+C raises KeyboardInterrupt instead

    while not stop.is_set():
        bot = ColddBot(cfg, store, collector)
        current["bot"] = bot
        try:
            async with bot:
                await bot.start(cfg.token, reconnect=True)
            if stop.is_set():
                return 0
            log.warning("Discord connection ended unexpectedly; restarting in %ds.", delay)
        except discord.LoginFailure:
            log.critical("Discord rejected the token. Reset it in the Developer Portal (Bot > Reset Token) and update DISCORD_TOKEN.")
            return 2
        except discord.PrivilegedIntentsRequired:
            log.critical("Privileged intents are not enabled. In the Developer Portal > Bot, enable SERVER MEMBERS INTENT"
                         " and MESSAGE CONTENT INTENT (or set MESSAGE_CONTENT_INTENT=false), then restart.")
            return 3
        except (aiohttp.ClientError, discord.GatewayNotFound, discord.ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
            log.error("Cannot reach Discord (%s). Retrying in %ds.", type(exc).__name__, delay)
        finally:
            await bot.flush(reason="disconnect")
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
        delay = min(delay * 2, 300)
    return 0


def main() -> int:
    sys.path.insert(0, str(ROOT))
    from coldd.bot import describe_start
    from coldd.collector import Collector
    from coldd.config import ConfigError, from_env, load_dotenv
    from coldd.storage import StorageError, Store

    load_dotenv(ROOT / ".env")
    try:
        cfg = from_env()
    except ConfigError as exc:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        logging.critical("Configuration problems (fix these in the hosting panel or .env, then restart):")
        for p in exc.problems:
            logging.critical("  - %s", p)
        return 1

    if not cfg.data_dir.is_absolute():
        cfg.data_dir = ROOT / cfg.data_dir
    if not cfg.log_dir.is_absolute():
        cfg.log_dir = ROOT / cfg.log_dir
    setup_logging(cfg.log_dir, cfg.log_max_kb, cfg.log_backups, cfg.secrets())
    log = logging.getLogger("coldd")
    log.info(describe_start(cfg))
    log.info("Settings: %s", cfg.safe_summary())

    store = Store(cfg.data_path, cfg.backup_count, cfg.backup_interval_hours * 3600, int(cfg.storage_warn_mb * 1048576))
    try:
        data = store.open()
    except StorageError as exc:
        log.critical("Storage error: %s", exc)
        return 4
    gid = data["meta"].get("guild_id")
    if gid and str(gid) != str(cfg.guild_id):
        log.critical("data.json belongs to server %s but GUILD_ID is %s. Use a separate DATA_FILE per server.", gid, cfg.guild_id)
        store.close()
        return 5

    collector = Collector(data, cfg)
    if store.recovered_from:
        log.warning("Running on data restored from backup %s; saving it as the new data.json.", store.recovered_from)
    if not store.save(force=True):
        log.critical("Cannot write %s. Check disk space and permissions.", store.path)
        store.close()
        return 4
    code = 0
    try:
        code = asyncio.run(run(cfg, store, collector))
    except KeyboardInterrupt:
        log.info("Interrupted.")
    finally:
        if collector.dirty or store.dirty:
            store.dirty = True
            store.save()
        store.close()
        log.info("Stopped. Data saved to %s.", store.path)
    return code


if __name__ == "__main__":
    sys.exit(main())
