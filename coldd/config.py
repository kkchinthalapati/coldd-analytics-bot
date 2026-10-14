"""Configuration from environment variables (optionally loaded from a .env file).

Every setting has a safe default except DISCORD_TOKEN and GUILD_ID. Errors are
collected and reported together in plain language so the owner can fix them in
one pass from the hosting panel.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .timeutil import WEEKDAYS


class ConfigError(Exception):
    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("; ".join(problems))


def load_dotenv(path: Path) -> None:
    """Minimal .env loader: KEY=VALUE lines; real environment variables win."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key.startswith("export "):
            key = key[7:].strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        os.environ.setdefault(key, value)


@dataclass
class Weights:
    participation: float = 0.50
    consistency: float = 0.20
    helpfulness: float = 0.20
    extra: float = 0.10

    def as_dict(self) -> dict[str, float]:
        return {
            "participation": self.participation,
            "consistency": self.consistency,
            "helpfulness": self.helpfulness,
            "extra": self.extra,
        }


@dataclass
class Config:
    token: str
    guild_id: int
    owner_ids: set[int] = field(default_factory=set)
    staff_role_ids: set[int] = field(default_factory=set)
    allow_manage_guild: bool = True

    timezone_name: str = "UTC"
    week_start_day: int = 0  # Monday

    data_dir: Path = Path(".")
    data_file: str = "data.json"
    save_interval: int = 60
    backup_interval_hours: float = 6.0
    backup_count: int = 5
    storage_warn_mb: float = 25.0
    log_dir: Path = Path("logs")
    log_max_kb: int = 1024
    log_backups: int = 3

    member_retention_days: int = 90
    server_retention_days: int = 180
    award_retention_days: int = 1095
    inactive_member_retention_days: int = 120

    include_channel_ids: set[int] = field(default_factory=set)
    exclude_channel_ids: set[int] = field(default_factory=set)
    exclude_role_ids: set[int] = field(default_factory=set)
    count_bots: bool = False

    message_content_intent: bool = True
    duplicate_window_seconds: int = 600
    burst_window_seconds: int = 60
    burst_threshold: int = 12
    recent_message_cache: int = 20000
    recent_message_hours: int = 24

    voice_enabled: bool = True
    voice_min_members: int = 2
    reactions_enabled: bool = True

    award_amount: int = 100
    award_currency: str = "Robux"
    weights: Weights = field(default_factory=Weights)
    daily_message_cap: int = 60
    daily_reply_cap: int = 20
    daily_reaction_cap: int = 30
    min_weekly_messages: int = 20
    min_active_days: int = 3
    min_daily_messages_for_active: int = 1
    winner_cooldown_weeks: int = 4
    shortlist_size: int = 10
    spam_flag_ratio: float = 0.30
    nomination_weight: float = 5.0

    audit_max_days: int = 180
    export_max_rows: int = 5000
    history_import_enabled: bool = False
    history_import_max_messages: int = 20000

    ai_provider: str = ""
    ai_api_key: str = ""
    ai_model: str = ""
    ai_base_url: str = ""
    ai_timeout: int = 30
    ai_max_requests_per_day: int = 20
    ai_max_messages_per_request: int = 5

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    @property
    def data_path(self) -> Path:
        return self.data_dir / self.data_file

    @property
    def ai_enabled(self) -> bool:
        return bool(self.ai_provider and self.ai_api_key and self.ai_model)

    def with_ai_timeout(self, seconds: int) -> Config:
        """Copy of this config with a longer AI timeout (for slow, long-form calls)."""
        return replace(self, ai_timeout=seconds)

    def secrets(self) -> list[str]:
        return [s for s in (self.token, self.ai_api_key) if s]

    def safe_summary(self) -> dict[str, object]:
        """Non-secret settings for /config and the startup log."""
        return {
            "guild_id": self.guild_id,
            "timezone": self.timezone_name,
            "week_starts": WEEKDAYS[self.week_start_day],
            "data_file": str(self.data_path),
            "save_interval_s": self.save_interval,
            "backups": f"{self.backup_count} every {self.backup_interval_hours}h",
            "storage_warn_mb": self.storage_warn_mb,
            "retention_days": {
                "member_daily": self.member_retention_days,
                "server_daily": self.server_retention_days,
                "awards": self.award_retention_days,
            },
            "included_channels": sorted(self.include_channel_ids) or "all readable",
            "excluded_channels": sorted(self.exclude_channel_ids),
            "excluded_roles": sorted(self.exclude_role_ids),
            "count_bots": self.count_bots,
            "message_content_intent": self.message_content_intent,
            "voice_enabled": self.voice_enabled,
            "reactions_enabled": self.reactions_enabled,
            "award": f"{self.award_amount} {self.award_currency}",
            "weights": self.weights.as_dict(),
            "caps_per_day": {
                "messages": self.daily_message_cap,
                "replies": self.daily_reply_cap,
                "reactions": self.daily_reaction_cap,
            },
            "eligibility": {
                "min_weekly_messages": self.min_weekly_messages,
                "min_active_days": self.min_active_days,
                "winner_cooldown_weeks": self.winner_cooldown_weeks,
            },
            "shortlist_size": self.shortlist_size,
            "history_import_enabled": self.history_import_enabled,
            "ai": f"{self.ai_provider}:{self.ai_model}" if self.ai_enabled else "disabled",
            "owners": len(self.owner_ids),
            "staff_roles": len(self.staff_role_ids),
        }


def _ids(raw: str, name: str, problems: list[str]) -> set[int]:
    out: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        if not part.isdigit():
            problems.append(f"{name} must be a comma-separated list of numeric Discord IDs (bad value: {part!r}).")
            continue
        out.add(int(part))
    return out


def from_env(env: dict[str, str] | None = None) -> Config:
    env = dict(os.environ if env is None else env)
    problems: list[str] = []

    def get(name: str, default: str = "") -> str:
        return env.get(name, default).strip()

    def as_int(name: str, default: int, lo: int | None = None, hi: int | None = None) -> int:
        raw = get(name)
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError:
            problems.append(f"{name} must be a whole number (got {raw!r}).")
            return default
        if (lo is not None and value < lo) or (hi is not None and value > hi):
            problems.append(f"{name} must be between {lo} and {hi} (got {value}).")
            return default
        return value

    def as_float(name: str, default: float, lo: float = 0.0, hi: float = 1e9) -> float:
        raw = get(name)
        if not raw:
            return default
        try:
            value = float(raw)
        except ValueError:
            problems.append(f"{name} must be a number (got {raw!r}).")
            return default
        if not lo <= value <= hi:
            problems.append(f"{name} must be between {lo} and {hi} (got {value}).")
            return default
        return value

    def as_bool(name: str, default: bool) -> bool:
        raw = get(name).lower()
        if not raw:
            return default
        if raw in ("1", "true", "yes", "on"):
            return True
        if raw in ("0", "false", "no", "off"):
            return False
        problems.append(f"{name} must be true or false (got {raw!r}).")
        return default

    token = get("DISCORD_TOKEN")
    if not token:
        problems.append("DISCORD_TOKEN is missing. Copy the bot token from the Discord Developer Portal > Bot page.")
    elif token.count(".") < 2:
        problems.append("DISCORD_TOKEN does not look like a bot token (expected three dot-separated parts).")

    guild_raw = get("GUILD_ID")
    guild_id = 0
    if not guild_raw:
        problems.append("GUILD_ID is missing. Enable Developer Mode in Discord, right-click the server and Copy Server ID.")
    elif not guild_raw.isdigit():
        problems.append(f"GUILD_ID must be a numeric server ID (got {guild_raw!r}).")
    else:
        guild_id = int(guild_raw)

    tz_name = get("TIMEZONE", "UTC")
    try:
        ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError):
        problems.append(f"TIMEZONE {tz_name!r} is not a valid IANA timezone (e.g. UTC, Europe/London, America/New_York).")
        tz_name = "UTC"

    wsd_raw = get("WEEK_START_DAY", "monday").lower()
    if wsd_raw not in WEEKDAYS:
        problems.append(f"WEEK_START_DAY must be a weekday name such as monday or sunday (got {wsd_raw!r}).")
        wsd_raw = "monday"

    weights = Weights(
        participation=as_float("WEIGHT_PARTICIPATION", 0.50, 0, 1),
        consistency=as_float("WEIGHT_CONSISTENCY", 0.20, 0, 1),
        helpfulness=as_float("WEIGHT_HELPFULNESS", 0.20, 0, 1),
        extra=as_float("WEIGHT_EXTRA", 0.10, 0, 1),
    )
    if sum(weights.as_dict().values()) <= 0:
        problems.append("At least one WEIGHT_* setting must be greater than zero.")

    provider = get("AI_PROVIDER").lower()
    if provider and provider not in ("anthropic", "openai"):
        problems.append("AI_PROVIDER must be empty, 'anthropic' or 'openai' (any OpenAI-compatible API).")
        provider = ""
    if provider and not get("AI_MODEL"):
        problems.append("AI_MODEL must be set when AI_PROVIDER is set.")

    cfg = Config(
        token=token,
        guild_id=guild_id,
        owner_ids=_ids(get("OWNER_IDS") or get("OWNER_ID"), "OWNER_IDS", problems),
        staff_role_ids=_ids(get("STAFF_ROLE_IDS"), "STAFF_ROLE_IDS", problems),
        allow_manage_guild=as_bool("ALLOW_MANAGE_GUILD", True),
        timezone_name=tz_name,
        week_start_day=WEEKDAYS.index(wsd_raw),
        data_dir=Path(get("DATA_DIR", ".") or "."),
        data_file=get("DATA_FILE", "data.json") or "data.json",
        save_interval=as_int("SAVE_INTERVAL_SECONDS", 60, 10, 3600),
        backup_interval_hours=as_float("BACKUP_INTERVAL_HOURS", 6.0, 0.25, 168),
        backup_count=as_int("BACKUP_COUNT", 5, 1, 50),
        storage_warn_mb=as_float("STORAGE_WARN_MB", 25.0, 1, 10000),
        log_dir=Path(get("LOG_DIR", "logs") or "logs"),
        log_max_kb=as_int("LOG_MAX_KB", 1024, 64, 102400),
        log_backups=as_int("LOG_BACKUPS", 3, 0, 20),
        member_retention_days=as_int("MEMBER_RETENTION_DAYS", 90, 14, 730),
        server_retention_days=as_int("SERVER_RETENTION_DAYS", 180, 14, 1825),
        award_retention_days=as_int("AWARD_RETENTION_DAYS", 1095, 30, 3650),
        inactive_member_retention_days=as_int("INACTIVE_MEMBER_RETENTION_DAYS", 120, 14, 3650),
        include_channel_ids=_ids(get("INCLUDE_CHANNEL_IDS"), "INCLUDE_CHANNEL_IDS", problems),
        exclude_channel_ids=_ids(get("EXCLUDE_CHANNEL_IDS"), "EXCLUDE_CHANNEL_IDS", problems),
        exclude_role_ids=_ids(get("EXCLUDE_ROLE_IDS"), "EXCLUDE_ROLE_IDS", problems),
        count_bots=as_bool("COUNT_BOTS", False),
        message_content_intent=as_bool("MESSAGE_CONTENT_INTENT", True),
        duplicate_window_seconds=as_int("DUPLICATE_WINDOW_SECONDS", 600, 10, 86400),
        burst_window_seconds=as_int("BURST_WINDOW_SECONDS", 60, 10, 3600),
        burst_threshold=as_int("BURST_THRESHOLD", 12, 3, 1000),
        recent_message_cache=as_int("RECENT_MESSAGE_CACHE", 20000, 0, 200000),
        recent_message_hours=as_int("RECENT_MESSAGE_HOURS", 24, 1, 168),
        voice_enabled=as_bool("VOICE_ENABLED", True),
        voice_min_members=as_int("VOICE_MIN_MEMBERS", 2, 1, 100),
        reactions_enabled=as_bool("REACTIONS_ENABLED", True),
        award_amount=as_int("AWARD_AMOUNT", 100, 0, 1_000_000),
        award_currency=get("AWARD_CURRENCY", "Robux") or "Robux",
        weights=weights,
        daily_message_cap=as_int("DAILY_MESSAGE_CAP", 60, 1, 10000),
        daily_reply_cap=as_int("DAILY_REPLY_CAP", 20, 1, 10000),
        daily_reaction_cap=as_int("DAILY_REACTION_CAP", 30, 1, 10000),
        min_weekly_messages=as_int("MIN_WEEKLY_MESSAGES", 20, 0, 100000),
        min_active_days=as_int("MIN_ACTIVE_DAYS", 3, 0, 7),
        min_daily_messages_for_active=as_int("MIN_DAILY_MESSAGES_FOR_ACTIVE", 1, 1, 1000),
        winner_cooldown_weeks=as_int("WINNER_COOLDOWN_WEEKS", 4, 0, 520),
        shortlist_size=as_int("SHORTLIST_SIZE", 10, 3, 25),
        spam_flag_ratio=as_float("SPAM_FLAG_RATIO", 0.30, 0, 1),
        nomination_weight=as_float("NOMINATION_WEIGHT", 5.0, 0, 1000),
        audit_max_days=as_int("AUDIT_MAX_DAYS", 180, 7, 1825),
        export_max_rows=as_int("EXPORT_MAX_ROWS", 5000, 10, 100000),
        history_import_enabled=as_bool("HISTORY_IMPORT_ENABLED", False),
        history_import_max_messages=as_int("HISTORY_IMPORT_MAX_MESSAGES", 20000, 100, 1_000_000),
        ai_provider=provider,
        ai_api_key=get("AI_API_KEY"),
        ai_model=get("AI_MODEL"),
        ai_base_url=get("AI_BASE_URL"),
        ai_timeout=as_int("AI_TIMEOUT_SECONDS", 30, 5, 300),
        ai_max_requests_per_day=as_int("AI_MAX_REQUESTS_PER_DAY", 20, 0, 10000),
        ai_max_messages_per_request=as_int("AI_MAX_MESSAGES_PER_REQUEST", 5, 1, 20),
    )
    if cfg.include_channel_ids & cfg.exclude_channel_ids:
        problems.append("A channel cannot be in both INCLUDE_CHANNEL_IDS and EXCLUDE_CHANNEL_IDS.")
    if problems:
        raise ConfigError(problems)
    return cfg
