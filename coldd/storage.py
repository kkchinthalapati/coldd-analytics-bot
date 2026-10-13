"""Safe JSON persistence for data.json.

Guarantees:
* Writes go to ``data.json.tmp``, are flushed and fsynced, then atomically
  replace ``data.json`` (``os.replace``), so a crash mid-write leaves the
  previous good file in place.
* Rotating timestamped backups are copied from the last good file.
* A corrupt ``data.json`` is renamed (never deleted or overwritten with an empty
  dataset) and the newest valid backup is restored instead. If there is no
  valid backup the bot refuses to start, so nothing is lost silently.
* A lock file stops two bot processes writing the same data file.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

log = logging.getLogger("coldd.storage")

SCHEMA_VERSION = 1
SECTIONS = ("meta", "members", "channels", "days", "server", "awards", "nominations")


class StorageError(Exception):
    """Unrecoverable storage problem: the bot must not start or continue writing."""


class DataError(ValueError):
    """The JSON document is malformed or fails validation."""


def empty_dataset(now: int | None = None) -> dict[str, Any]:
    now = int(time.time()) if now is None else now
    return {
        "schema_version": SCHEMA_VERSION,
        "meta": {
            "created": now,
            "collection_start": None,
            "last_save": None,
            "last_event": None,
            "last_heartbeat": None,
            "guild_id": None,
            "guild_name": None,
            "timezone": None,
            "gaps": [],
            "migrations": [],
            "unreadable_channels": [],
            "imports": {},
            "ai_usage": {"day": None, "n": 0},
            "last_prune_day": None,
        },
        "members": {},
        "channels": {},
        "days": {},
        "server": {},
        "awards": {},
        "nominations": {},
    }


# --- migrations -------------------------------------------------------------

def _migrate_0_to_1(data: dict[str, Any]) -> dict[str, Any]:
    """Version 0 = files written before schema versioning: fill in missing sections/keys."""
    base = empty_dataset()
    for section in SECTIONS:
        if not isinstance(data.get(section), dict):
            data[section] = base[section]
    for key, value in base["meta"].items():
        data["meta"].setdefault(key, value)
    data["schema_version"] = 1
    return data


MIGRATIONS: dict[int, Callable[[dict[str, Any]], dict[str, Any]]] = {0: _migrate_0_to_1}


def migrate(data: dict[str, Any]) -> dict[str, Any]:
    version = data.get("schema_version", 0)
    if not isinstance(version, int):
        raise DataError(f"schema_version must be an integer, found {version!r}")
    if version > SCHEMA_VERSION:
        raise DataError(
            f"data file uses schema {version}, newer than this bot supports ({SCHEMA_VERSION}). "
            "Upgrade the bot instead of downgrading the data."
        )
    while version < SCHEMA_VERSION:
        step = MIGRATIONS.get(version)
        if step is None:
            raise DataError(f"no migration path from schema {version}")
        data = step(data)
        data["meta"].setdefault("migrations", []).append(
            {"from": version, "to": version + 1, "at": int(time.time())}
        )
        log.info("Migrated data schema %d -> %d", version, version + 1)
        version += 1
    return data


def validate(data: Any) -> None:
    if not isinstance(data, dict):
        raise DataError("top level must be a JSON object")
    for section in SECTIONS:
        if not isinstance(data.get(section), dict):
            raise DataError(f"section {section!r} is missing or not an object")
    meta = data["meta"]
    if not isinstance(meta.get("gaps", []), list):
        raise DataError("meta.gaps must be a list")
    for day, rec in data["days"].items():
        if not isinstance(rec, dict) or not isinstance(rec.get("u"), dict):
            raise DataError(f"days[{day!r}] must contain a 'u' object")
    for day, rec in data["server"].items():
        if not isinstance(rec, dict):
            raise DataError(f"server[{day!r}] must be an object")
    for uid, rec in data["members"].items():
        if not uid.isdigit() or not isinstance(rec, dict):
            raise DataError(f"members key {uid!r} must be a numeric ID mapping to an object")


def parse_document(text: str) -> dict[str, Any]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise DataError(f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise DataError("top level must be a JSON object")
    data = migrate(data)
    validate(data)
    _intern_user_keys(data)
    return data


def _intern_user_keys(data: dict[str, Any]) -> None:
    """Share one string object per user ID across all day records (saves RAM after load)."""
    for day in data["days"].values():
        day["u"] = {sys.intern(k): v for k, v in day["u"].items()}
    data["members"] = {sys.intern(k): v for k, v in data["members"].items()}


# --- file lock --------------------------------------------------------------

class FileLock:
    """Exclusive non-blocking OS lock, released automatically if the process dies."""

    def __init__(self, path: Path):
        self.path = path
        self._fh: Any = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+")
        try:
            if sys.platform == "win32":
                import msvcrt

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            fh.close()
            raise StorageError(
                f"another process is already using {self.path.name}. "
                "Stop the other bot instance first (only one instance may write data.json)."
            ) from exc
        self._fh = fh

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt

                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        self._fh.close()
        self._fh = None


# --- store ------------------------------------------------------------------

class Store:
    def __init__(
        self,
        path: Path,
        backup_count: int = 5,
        backup_interval_s: float = 6 * 3600,
        warn_bytes: int = 25 * 1024 * 1024,
    ):
        self.path = Path(path)
        self.tmp_path = self.path.with_name(self.path.name + ".tmp")
        self.backup_dir = self.path.parent / "backups"
        self.backup_count = backup_count
        self.backup_interval_s = backup_interval_s
        self.warn_bytes = warn_bytes
        self.lock = FileLock(self.path.with_name(self.path.name + ".lock"))
        self.data: dict[str, Any] = {}
        self.dirty = False
        self.last_size = 0
        self.last_save_duration = 0.0
        self.last_error: str | None = None
        self.consecutive_failures = 0
        self.recovered_from: str | None = None
        self._last_backup = 0.0
        self._last_size_warning = 0.0

    # startup ---------------------------------------------------------------

    def check_writable(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        probe = self.path.with_name(f".write-test-{os.getpid()}")
        try:
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
        except OSError as exc:
            raise StorageError(f"data directory {self.path.parent.resolve()} is not writable: {exc}") from exc

    def open(self) -> dict[str, Any]:
        """Lock, verify the directory is writable, and load (or recover) the dataset."""
        self.check_writable()
        self.lock.acquire()
        if self.tmp_path.exists():
            log.warning("Found leftover %s from an interrupted save; ignoring it.", self.tmp_path.name)
            try:
                self.tmp_path.unlink()
            except OSError:
                pass
        self.data = self._load_or_recover()
        return self.data

    def _read(self, path: Path) -> dict[str, Any]:
        return parse_document(path.read_text(encoding="utf-8"))

    def backups(self) -> list[Path]:
        if not self.backup_dir.is_dir():
            return []
        return sorted(self.backup_dir.glob(f"{self.path.stem}-*.json"), reverse=True)

    def _load_or_recover(self) -> dict[str, Any]:
        primary_existed = self.path.exists()
        if primary_existed:
            try:
                data = self._read(self.path)
                self.last_size = self.path.stat().st_size
                log.info("Loaded %s (%.1f KB)", self.path.name, self.last_size / 1024)
                self.make_backup(force=True)
                return data
            except (DataError, OSError, UnicodeDecodeError) as exc:
                quarantine = self.path.with_name(f"{self.path.name}.corrupt-{time.strftime('%Y%m%d-%H%M%S')}")
                log.error("%s is unreadable (%s). Moving it to %s and trying backups.", self.path.name, exc, quarantine.name)
                try:
                    os.replace(self.path, quarantine)
                except OSError as move_exc:
                    raise StorageError(f"could not quarantine corrupt {self.path.name}: {move_exc}") from move_exc

        for backup in self.backups():
            try:
                data = self._read(backup)
            except (DataError, OSError, UnicodeDecodeError) as exc:
                log.error("Backup %s is unusable: %s", backup.name, exc)
                continue
            log.warning("Restored analytics from backup %s", backup.name)
            self.recovered_from = backup.name
            self.dirty = True
            return data

        if primary_existed:
            raise StorageError(
                f"{self.path.name} was corrupt and no valid backup exists. The damaged file was kept as "
                f"{self.path.name}.corrupt-*. Repair it or restore a backup manually; refusing to start "
                "with an empty dataset."
            )
        log.info("No existing %s found; starting a new dataset.", self.path.name)
        self.dirty = True
        return empty_dataset()

    # saving ----------------------------------------------------------------

    def mark_dirty(self) -> None:
        self.dirty = True

    def serialize(self) -> bytes:
        """Snapshot the dataset (must run on the thread that mutates it)."""
        self.data["meta"]["last_save"] = int(time.time())
        return json.dumps(self.data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    def write(self, payload: bytes) -> None:
        """Atomically write a serialized payload. Safe to run in a worker thread."""
        start = time.perf_counter()
        with open(self.tmp_path, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(self.tmp_path, self.path)
        if os.name != "nt":
            try:
                dir_fd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        self.last_save_duration = time.perf_counter() - start
        self.last_size = len(payload)

    def save(self, force: bool = False) -> bool:
        """Synchronous save used at shutdown and in tests. Returns True on success."""
        if not (self.dirty or force):
            return True
        self.dirty = False
        try:
            payload = self.serialize()
            self.write(payload)
        except (OSError, TypeError, ValueError) as exc:
            self.on_save_failed(exc)
            return False
        self.on_save_ok()
        return True

    def on_save_ok(self) -> None:
        self.last_error = None
        self.consecutive_failures = 0
        self.recovered_from = None
        self._check_size()
        self.make_backup()

    def on_save_failed(self, exc: BaseException) -> None:
        self.dirty = True  # keep changes for the next attempt
        self.consecutive_failures += 1
        self.last_error = f"{type(exc).__name__}: {exc}"
        log.error("Saving %s failed (attempt %d): %s. Changes are kept in memory and will be retried.",
                  self.path.name, self.consecutive_failures, self.last_error)

    def _check_size(self) -> None:
        if self.last_size < 0.8 * self.warn_bytes:
            return
        now = time.time()
        if now - self._last_size_warning < 3600:
            return
        self._last_size_warning = now
        pct = 100 * self.last_size / self.warn_bytes
        log.warning(
            "%s is %.1f MB (%.0f%% of the STORAGE_WARN_MB threshold). Consider lowering MEMBER_RETENTION_DAYS "
            "or raising the threshold if your host has space. No data is deleted automatically.",
            self.path.name, self.last_size / 1048576, pct,
        )

    @property
    def size_warning(self) -> bool:
        return self.last_size >= 0.8 * self.warn_bytes

    def make_backup(self, force: bool = False) -> Path | None:
        now = time.time()
        if not force and now - self._last_backup < self.backup_interval_s:
            return None
        if not self.path.exists():
            return None
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            target = self.backup_dir / f"{self.path.stem}-{time.strftime('%Y%m%d-%H%M%S')}.json"
            shutil.copy2(self.path, target)
            self._last_backup = now
            for old in self.backups()[self.backup_count:]:
                old.unlink()
            return target
        except OSError as exc:
            log.error("Could not create backup: %s", exc)
            return None

    def close(self) -> None:
        self.lock.release()
