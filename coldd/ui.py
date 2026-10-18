"""Presentation helpers: colours, bars, trend arrows, medals, the COLDD emoji and banner.

Pure formatting. Nothing here reads or writes stored data.
"""

from __future__ import annotations

import io
from functools import lru_cache
from pathlib import Path

import discord

ASSETS = Path(__file__).parent / "assets"
BANNER_NAME = "coldd-banner.webp"
LOGO_EMOJI_NAME = "colddlogo"

DRAGON_NAME = "coldd-dragon.png"

BRAND = 0xD61F26   # COLDD red, from the dragon emblem
GOOD = 0x8B0000   # deep red: "done" and verified
WARN = 0xFFB020   # amber: stays distinct from the brand red
MUTED = 0x2B2D31

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}


@lru_cache(maxsize=1)
def banner_bytes() -> bytes:
    """Read the banner once; it is ~1.2 MB and reused for every reply."""
    return (ASSETS / BANNER_NAME).read_bytes()


def banner_file() -> discord.File:
    """A fresh File object for each send (discord.py consumes the buffer)."""
    return discord.File(io.BytesIO(banner_bytes()), filename=BANNER_NAME)


def dragon_file() -> discord.File:
    """The COLDD dragon emblem, for thumbnails (a fresh File per send)."""
    return discord.File(str(ASSETS / DRAGON_NAME), filename=DRAGON_NAME)


def dragon_url() -> str:
    return f"attachment://{DRAGON_NAME}"


def banner_url() -> str:
    return f"attachment://{BANNER_NAME}"


def logo(guild: discord.Guild | None) -> str:
    """The server's :colddlogo: emoji, or a plain fallback if the server doesn't have it."""
    if guild is not None:
        for emoji in getattr(guild, "emojis", []) or []:
            if emoji.name == LOGO_EMOJI_NAME:
                return str(emoji)
    return "◆"


def card(title: str, description: str, guild: discord.Guild | None, color: int = BRAND,
         footer: str = "") -> discord.Embed:
    """A styled embed: logo in the title, banner-coloured accent, optional footer line."""
    embed = discord.Embed(title=f"{logo(guild)}  {title}"[:256], description=description[:4000], color=color)
    if footer:
        embed.set_footer(text=footer[:2000])
    return embed


def bar(value: float, top: float, width: int = 12) -> str:
    """A horizontal bar scaled to `top`, e.g. ████████░░░░."""
    if top <= 0:
        return "░" * width
    filled = max(0, min(width, round(width * value / top)))
    return "█" * filled + "░" * (width - filled)


def trend(cur: float, prev: float) -> str:
    """Arrow and percentage change against the previous period."""
    if not prev:
        return "— no prior data" if cur else "● 0%"
    pct = 100 * (cur - prev) / prev
    if abs(pct) < 0.05:
        return "● 0%"
    arrow = "▲" if pct > 0 else "▼"
    return f"{arrow} {pct:+.1f}%"


def medal(rank: int) -> str:
    return MEDALS.get(rank, f"`{rank:>2}`")


def pct(x: float) -> str:
    return f"{x:.0%}"


def color_for(coverage: float) -> int:
    """Warn colour when less than 90% of the period was observed."""
    return WARN if coverage < 0.9 else BRAND


def spark(values: list[int]) -> str:
    """Sparkline with the same glyph scale as analytics.sparkline."""
    glyphs = "▁▂▃▄▅▆▇█"
    top = max(values) if values else 0
    if not top:
        return "▁" * len(values)
    return "".join(glyphs[min(7, int(7 * v / top))] if v else "▁" for v in values)

