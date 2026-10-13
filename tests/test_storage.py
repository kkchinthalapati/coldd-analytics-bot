import json
import os

import pytest

from coldd.storage import SCHEMA_VERSION, DataError, StorageError, Store, empty_dataset, parse_document


def open_store(tmp_path, **kw):
    s = Store(tmp_path / "data.json", **kw)
    s.open()
    return s


def test_new_dataset_created_and_saved(tmp_path):
    s = open_store(tmp_path)
    assert s.data["schema_version"] == SCHEMA_VERSION
    assert s.save()
    loaded = json.loads((tmp_path / "data.json").read_text())
    assert loaded["meta"]["last_save"] is not None
    s.close()


def test_save_and_reload_roundtrip(tmp_path):
    s = open_store(tmp_path)
    s.data["members"]["42"] = {"n": "alice", "f": 1}
    s.mark_dirty()
    assert s.save()
    s.close()
    s2 = open_store(tmp_path)
    assert s2.data["members"]["42"]["n"] == "alice"
    s2.close()


def test_save_skipped_when_clean(tmp_path):
    s = open_store(tmp_path)
    s.save()
    mtime = (tmp_path / "data.json").stat().st_mtime_ns
    assert s.save()  # not dirty: no write
    assert (tmp_path / "data.json").stat().st_mtime_ns == mtime
    s.close()


def test_corrupt_file_recovers_from_backup_and_is_quarantined(tmp_path):
    s = open_store(tmp_path)
    s.data["members"]["7"] = {"n": "bob"}
    s.mark_dirty()
    s.save()
    assert s.make_backup(force=True)
    s.close()
    (tmp_path / "data.json").write_text('{"schema_version": 1, "meta": {', encoding="utf-8")
    s2 = open_store(tmp_path)
    assert s2.data["members"]["7"]["n"] == "bob"
    assert s2.recovered_from
    assert list(tmp_path.glob("data.json.corrupt-*")), "corrupt file must be kept, not deleted"
    s2.close()


def test_corrupt_without_backup_refuses_to_start(tmp_path):
    (tmp_path / "data.json").write_text("not json at all", encoding="utf-8")
    s = Store(tmp_path / "data.json")
    with pytest.raises(StorageError):
        s.open()
    s.close()
    assert not (tmp_path / "data.json").exists() or (tmp_path / "data.json").read_text() == "not json at all"
    assert list(tmp_path.glob("data.json.corrupt-*"))


def test_missing_primary_restores_latest_backup(tmp_path):
    s = open_store(tmp_path)
    s.data["members"]["9"] = {"n": "carol"}
    s.mark_dirty()
    s.save()
    s.make_backup(force=True)
    s.close()
    os.remove(tmp_path / "data.json")
    s2 = open_store(tmp_path)
    assert s2.data["members"]["9"]["n"] == "carol"
    s2.close()


def test_invalid_backup_skipped(tmp_path):
    (tmp_path / "backups").mkdir()
    (tmp_path / "backups" / "data-20990101-000000.json").write_text("{bad", encoding="utf-8")
    good = empty_dataset()
    good["members"]["5"] = {"n": "dave"}
    (tmp_path / "backups" / "data-20000101-000000.json").write_text(json.dumps(good), encoding="utf-8")
    s = open_store(tmp_path)
    assert s.data["members"]["5"]["n"] == "dave"
    s.close()


def test_failed_write_keeps_dirty_and_previous_file(tmp_path, monkeypatch):
    s = open_store(tmp_path)
    s.save(force=True)
    before = (tmp_path / "data.json").read_text()
    s.data["members"]["1"] = {"n": "x"}
    s.mark_dirty()

    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    assert not s.save()
    assert s.dirty and s.last_error and "disk full" in s.last_error
    assert (tmp_path / "data.json").read_text() == before
    monkeypatch.undo()
    assert s.save()
    assert s.last_error is None
    assert json.loads((tmp_path / "data.json").read_text())["members"]["1"]["n"] == "x"
    s.close()


def test_second_writer_is_blocked(tmp_path):
    s = open_store(tmp_path)
    s2 = Store(tmp_path / "data.json")
    with pytest.raises(StorageError):
        s2.open()
    s.close()
    s3 = open_store(tmp_path)  # lock released on close
    s3.close()


def test_leftover_tmp_is_ignored(tmp_path):
    s = open_store(tmp_path)
    s.save(force=True)
    s.close()
    (tmp_path / "data.json.tmp").write_text("{partial", encoding="utf-8")
    s2 = open_store(tmp_path)
    assert not (tmp_path / "data.json.tmp").exists()
    s2.close()


def test_backup_rotation(tmp_path):
    s = open_store(tmp_path, backup_count=2)
    s.save(force=True)
    for i in range(4):
        s.backup_dir.mkdir(exist_ok=True)
        (s.backup_dir / f"data-2020010{i}-000000.json").write_text(json.dumps(empty_dataset()))
    s.make_backup(force=True)
    assert len(s.backups()) == 2
    s.close()


def test_migration_from_unversioned():
    legacy = {"meta": {"created": 1}, "members": {"3": {"n": "old"}}}
    data = parse_document(json.dumps(legacy))
    assert data["schema_version"] == SCHEMA_VERSION
    assert data["members"]["3"]["n"] == "old"
    assert data["days"] == {} and data["awards"] == {}
    assert data["meta"]["migrations"][0]["from"] == 0


def test_newer_schema_rejected():
    doc = empty_dataset()
    doc["schema_version"] = SCHEMA_VERSION + 1
    with pytest.raises(DataError):
        parse_document(json.dumps(doc))


@pytest.mark.parametrize("bad", [
    "[]",
    json.dumps({**empty_dataset(), "days": {"2026-01-01": {"x": 1}}}),
    json.dumps({**empty_dataset(), "members": {"notanid": {}}}),
    json.dumps({**empty_dataset(), "server": []}),
])
def test_validation_rejects_malformed(bad):
    with pytest.raises(DataError):
        parse_document(bad)


def test_unwritable_directory_detected(tmp_path, monkeypatch):
    s = Store(tmp_path / "data.json")
    from pathlib import Path

    def deny(self, *a, **k):
        raise PermissionError("read-only")

    monkeypatch.setattr(Path, "write_text", deny)
    with pytest.raises(StorageError):
        s.check_writable()


def test_size_warning(tmp_path):
    s = open_store(tmp_path, warn_bytes=100)
    s.data["members"].update({str(i): {"n": "member" * 3} for i in range(50)})
    s.mark_dirty()
    s.save()
    assert s.size_warning
    s.close()
