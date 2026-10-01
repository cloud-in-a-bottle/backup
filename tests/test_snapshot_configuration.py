from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import stat
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import app as backup_app
import snapshot_configuration as snapshots
from configuration import ConfigurationError
from operations import OperationLock


RAW_KEY = "synthetic-source-key-not-in-metadata"
APP_SECRET = "ordinary-app-secret-in-encrypted-data"
OWNER_CREDENTIAL = "synthetic-bootstrap-credential-not-in-metadata"


def bundle(*, runtime=False):
    result = {
        "format_version": 1,
        "backup_app_name": "backup",
        "definitions": {
            "schema_version": 2, "mode": "private",
            "apps": [{"name": "demo", "source": {"kind": "remote", "repo_url": "https://example.com/demo", "ref": "main"}, "port_mappings": []}],
            "platform_api_tokens": [{"name": "same\nname", "token_hash": hashlib.sha256(RAW_KEY.encode()).hexdigest(), "expires_at": "2000-01-01T00:00:00Z"}],
        },
        "runtime": None,
    }
    if runtime:
        result["runtime"] = {"apps": {"demo": {"status": "running", "global_grants": [], "unresolved_provider_grants": []}}, "providers": []}
    return result


@pytest.fixture
def environment(tmp_path, monkeypatch):
    data = tmp_path / "app_data"
    temporary = tmp_path / "app_temp_data"
    archive = tmp_path / "app_archive"
    vm = tmp_path / "vm_data"
    own = data / "backup"
    own.mkdir(parents=True)
    (data / "demo").mkdir()
    (data / "demo" / "secret.txt").write_text(APP_SECRET)
    (temporary / "demo").mkdir(parents=True)
    (temporary / "demo" / "scratch.txt").write_text("scratch")
    (temporary / "backup").mkdir()
    (temporary / "backup" / "private-scratch").write_text(OWNER_CREDENTIAL)
    archive.mkdir()
    (archive / "archive.txt").write_text("archive must survive")
    roots = (data, temporary)
    values = {
        "ALL_APP_DATA": data, "APP_TEMP_DATA": temporary, "APP_ARCHIVE": archive,
        "VM_DATA_DIR": vm, "APP_DATA_DIR": own, "CONFIG_DIR": own,
        "CONFIG_FILE": own / "config.json", "DB_FILE": own / "backups.db",
        "RESTIC_REPO_DIR": own / "repository", "BACKUP_ROOTS": roots,
        "BACKUP_EXCLUDES": (own, temporary / "backup", archive, *(root / backup_app.RESTORE_WORK_NAME for root in roots)),
        "_ROOT_NAMES": {"app_data": data, "app_temp_data": temporary, "vm_data": vm},
        "APP_NAME": "backup", "APP_TOKEN": "synthetic-app-token",
        "ROUTER_API_TOKEN": "", "ZONE_DOMAIN": "source.example", "BACKUP_HOST": "source.example",
        "op_lock": OperationLock(), "_init_lock": asyncio.Lock(),
        "restore_last_snapshot": None, "restore_last_status": None,
        "restore_progress": None, "_restore_needs_attention": False, "_restore_session": None,
    }
    for name, value in values.items():
        monkeypatch.setattr(backup_app, name, value)
    monkeypatch.setattr(snapshots, "CONFIGURATION_FILE", tmp_path / "metadata" / "configuration.json")
    monkeypatch.setenv("RESTIC_CACHE_DIR", str(tmp_path / "restic-cache"))
    backup_app.init_db()
    conf = {**backup_app.DEFAULT_CONFIG, "repo": str(own / "repository"), "repo_password": "synthetic-restic-password", "router_api_token": OWNER_CREDENTIAL}
    backup_app.save_config(conf)
    capture = AsyncMock(return_value=bundle(runtime=True))
    monkeypatch.setattr(backup_app, "capture_configuration", capture)
    return data, temporary, archive, conf, capture


async def newest_snapshot():
    entries, ok = await backup_app.list_snapshots()
    assert ok and entries
    return entries[0]


async def test_real_restic_snapshot_contains_private_configuration_and_data(environment):
    data, temporary, archive, conf, capture = environment
    unusual = data / "demo" / "drafts [v1] & 'review' 📝"
    unusual.mkdir()
    (unusual / "notes.json").write_text("{}")
    assert await backup_app.run_backup(name="configuration recovery")
    capture.assert_awaited_once_with(backup_app.ROUTER_URL, "synthetic-app-token", OWNER_CREDENTIAL, "backup")
    saved = await newest_snapshot()
    assert saved["has_configuration"] and saved["has_runtime"]
    assert {str(data), str(temporary), str(snapshots.CONFIGURATION_FILE)} == set(saved["paths"])
    parsed = await snapshots.read_configuration(saved["id"], backup_app._restic_env(conf))
    assert parsed == bundle(runtime=True)
    assert not snapshots.CONFIGURATION_FILE.exists()
    assert stat.S_IMODE(snapshots.CONFIGURATION_FILE.parent.stat().st_mode) == 0o700
    encoded = json.dumps(parsed)
    assert RAW_KEY not in encoded and OWNER_CREDENTIAL not in encoded
    assert "synthetic-restic-password" not in encoded
    files, error = await backup_app.list_snapshot_files(saved["id"], str(snapshots.CONFIGURATION_FILE.parent).lstrip("/"))
    assert error is None
    assert any(entry["path"] == "configuration.json" and not entry["is_dir"] and entry["size"] > 0 for entry in files)
    files, error = await backup_app.list_snapshot_files(saved["id"], str(unusual).lstrip("/"))
    assert error is None and [entry["path"] for entry in files] == ["notes.json"]
    rc, content, _ = await backup_app._run_restic(["dump", saved["id"], str(data / "demo" / "secret.txt"), "--no-lock"], conf)
    assert rc == 0 and content.decode() == APP_SECRET
    for excluded in (backup_app.CONFIG_FILE, temporary / "backup" / "private-scratch", archive / "archive.txt"):
        rc, _, _ = await backup_app._run_restic(["dump", saved["id"], str(excluded), "--no-lock"], conf)
        assert rc != 0
    # The actual repository contains encrypted pack data, not these plaintexts.
    for path in Path(conf["repo"]).rglob("*"):
        if path.is_file():
            assert APP_SECRET.encode() not in path.read_bytes()
            assert RAW_KEY.encode() not in path.read_bytes()


async def test_export_failure_creates_no_snapshot_or_plaintext_artifact(environment, monkeypatch, caplog):
    *_, conf, capture = environment
    capture.side_effect = ConfigurationError("export_approval")
    assert not await backup_app.run_backup()
    entries, ok = await backup_app.list_snapshots()
    assert ok and entries == []
    assert not snapshots.CONFIGURATION_FILE.exists()
    assert not backup_app.op_lock.busy
    assert "Private" in backup_app.get_last_backup()["error_message"]
    assert RAW_KEY not in caplog.text and OWNER_CREDENTIAL not in caplog.text


async def test_configuration_capture_without_owner_key_remains_explicit(environment):
    *_, conf, capture = environment
    conf.pop("router_api_token")
    backup_app.save_config(conf)
    capture.return_value = bundle()
    assert await backup_app.run_backup()
    capture.assert_awaited_once_with(backup_app.ROUTER_URL, "synthetic-app-token", None, "backup")
    saved = await newest_snapshot()
    assert saved["has_configuration"] and not saved["has_runtime"]


async def test_capture_file_is_private_and_cleared_on_cancel(environment, monkeypatch):
    reached = asyncio.Event()
    original = backup_app._run_restic

    async def held(args, conf, timeout=None):
        if args[0] != "backup":
            return await original(args, conf, timeout)
        assert stat.S_IMODE(snapshots.CONFIGURATION_FILE.stat().st_mode) == 0o600
        assert RAW_KEY not in " ".join(args)
        reached.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(backup_app, "_run_restic", held)
    task = asyncio.create_task(backup_app.run_backup())
    await asyncio.wait_for(reached.wait(), 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not snapshots.CONFIGURATION_FILE.exists()
    assert not backup_app.op_lock.busy


async def test_legacy_snapshot_restores_files_with_explicit_scope(environment, monkeypatch):
    data, _, _, conf, _ = environment
    rc, _, _ = await backup_app._run_restic(["init"], conf)
    assert rc == 0
    rc, _, _ = await backup_app._run_restic(["backup", str(data), "--exclude", str(backup_app.APP_DATA_DIR), "--tag", "openhost", "--json"], conf)
    assert rc == 0
    saved = await newest_snapshot()
    assert not saved["has_configuration"]
    (data / "demo" / "secret.txt").write_text("changed")
    monkeypatch.setattr(backup_app, "RecoverySession", lambda *args: pytest.fail("legacy data-only restore must not import configuration"))
    assert await backup_app.run_restore(saved["id"])
    assert (data / "demo" / "secret.txt").read_text() == APP_SECRET
    assert backup_app.restore_progress["phase"] == "files_only"
    assert backup_app.restore_progress["warnings"]


async def test_new_snapshot_single_root_keeps_backup_repo_and_configuration(environment):
    data, temporary, _, conf, _ = environment
    assert await backup_app.run_backup()
    saved = await newest_snapshot()
    (data / "demo" / "secret.txt").write_text("changed")
    private_config = backup_app.CONFIG_FILE.read_bytes()
    # Sentinels written after the snapshot prove the protection is real: the
    # executor's own data and the excluded root must both survive untouched.
    executor_marker = backup_app.APP_DATA_DIR / "executor-marker.txt"
    executor_marker.write_text("backup app data")
    temporary_marker = temporary / "demo" / "after-snapshot.txt"
    temporary_marker.write_text("only on the destination")
    assert await backup_app.run_restore(saved["id"], root="app_data")
    assert (data / "demo" / "secret.txt").read_text() == APP_SECRET
    assert backup_app.CONFIG_FILE.read_bytes() == private_config
    assert (Path(conf["repo"]) / "config").is_file()
    assert executor_marker.read_text() == "backup app data"
    assert temporary_marker.read_text() == "only on the destination"
    assert (temporary / "demo" / "scratch.txt").read_text() == "scratch"
    assert not snapshots.CONFIGURATION_FILE.exists()


async def test_corrupt_configuration_cannot_touch_app_data(environment, monkeypatch, caplog):
    data, _, _, conf, _ = environment
    assert await backup_app.run_backup()
    saved = await newest_snapshot()
    (data / "demo" / "secret.txt").write_text("destination data")
    monkeypatch.setattr(snapshots, "read_configuration", AsyncMock(side_effect=ConfigurationError("invalid_configuration")))
    monkeypatch.setattr(backup_app, "RecoverySession", lambda *args: pytest.fail("invalid bundle reached recovery"))
    assert not await backup_app.run_restore(saved["id"], owner_token=OWNER_CREDENTIAL)
    assert (data / "demo" / "secret.txt").read_text() == "destination data"
    assert not backup_app.op_lock.busy
    assert RAW_KEY not in caplog.text and OWNER_CREDENTIAL not in caplog.text


class FakeRecovery:
    def __init__(self, router, token, configuration, name):
        assert token == OWNER_CREDENTIAL and configuration == bundle(runtime=True)
        self.restore_app_names = ("demo",)
        self.events = []
        self.done = False
        self.omitted = []

    @property
    def progress(self):
        return {"ok": self.done, "apps": [{"name": "demo"}], "warnings": [],
                "omitted_app_data": list(self.omitted)}

    @property
    def summary(self):
        return copy.deepcopy(self.progress)

    def note_omitted_data(self, names):
        self.omitted = sorted(set(self.omitted) | set(names))

    async def preflight(self):
        self.events.append("preflight")

    async def stop_apps(self):
        assert (backup_app.ALL_APP_DATA / "demo" / "secret.txt").read_text() == "destination data"
        self.events.append("stop")

    async def activate(self):
        assert (backup_app.ALL_APP_DATA / "demo" / "secret.txt").read_text() == APP_SECRET
        assert not (backup_app.ALL_APP_DATA / "demo" / "stale.db-wal").exists()
        self.events.append("activate")
        self.done = True

    async def restart_unaffected(self):
        self.events.append("cleanup")


async def test_real_staged_restore_replaces_trees_before_activation(environment, monkeypatch):
    data, temporary, archive, _, _ = environment
    assert await backup_app.run_backup()
    saved = await newest_snapshot()
    (data / "demo" / "secret.txt").write_text("destination data")
    (data / "demo" / "stale.db-wal").write_text("stale WAL must disappear")
    (data / "unrelated").mkdir()
    (data / "unrelated" / "keep").write_text("destination-only data")
    instances = []

    def make_session(*args):
        session = FakeRecovery(*args)
        instances.append(session)
        return session

    monkeypatch.setattr(backup_app, "RecoverySession", make_session)
    own_config = backup_app.CONFIG_FILE.read_bytes()
    assert await backup_app.run_restore(saved["id"], owner_token=OWNER_CREDENTIAL)
    assert instances[0].events == ["preflight", "stop", "activate", "cleanup"]
    assert (temporary / "demo" / "scratch.txt").read_text() == "scratch"
    assert (data / "unrelated" / "keep").read_text() == "destination-only data"
    assert (archive / "archive.txt").read_text() == "archive must survive"
    assert backup_app.CONFIG_FILE.read_bytes() == own_config
    assert backup_app.restore_last_status == "success"
    assert not backup_app._restore_needs_attention
    assert backup_app._restore_session is None
    assert not backup_app.op_lock.busy
    journal = json.loads(backup_app._restore_journal_path().read_text())
    assert journal["phase"] == "complete"
    assert RAW_KEY not in json.dumps(journal) and OWNER_CREDENTIAL not in json.dumps(journal)
    for root in (data, temporary):
        assert list((root / backup_app.RESTORE_WORK_NAME).iterdir()) == []


async def test_data_promotion_failure_never_activates_and_retry_notice_survives(environment, monkeypatch):
    data, _, _, _, _ = environment
    assert await backup_app.run_backup()
    saved = await newest_snapshot()
    (data / "demo" / "secret.txt").write_text("destination data")
    sessions = []

    def make_session(*args):
        result = FakeRecovery(*args)
        sessions.append(result)
        return result

    monkeypatch.setattr(backup_app, "RecoverySession", make_session)
    monkeypatch.setattr(backup_app.migration_data, "replace_app_trees", AsyncMock(side_effect=OSError("private-failure-marker")))
    assert not await backup_app.run_restore(saved["id"], owner_token=OWNER_CREDENTIAL)
    assert sessions[0].events == ["preflight", "stop", "cleanup"]
    assert backup_app._restore_needs_attention
    assert not await backup_app.run_backup()
    assert not backup_app.op_lock.busy
    assert backup_app._restore_session is None

    async def failed_preflight(self):
        raise ConfigurationError("router_auth")

    monkeypatch.setattr(FakeRecovery, "preflight", failed_preflight)
    assert not await backup_app.run_restore(saved["id"], owner_token=OWNER_CREDENTIAL)
    assert backup_app._restore_needs_attention
    backup_app._load_restore_journal()
    assert backup_app._restore_needs_attention
    assert backup_app.restore_progress["affected_apps"] == ["demo"]


@pytest.mark.parametrize("changed", [
    {"id": "b" * 64}, {"tags": []}, {"tags": ["bottle", "bottle-configuration-v99"]},
    {"tags": ["bottle", snapshots.CONFIGURATION_TAG]}, {"paths": ["relative"]},
    {"tags": ["bottle", snapshots.RUNTIME_TAG]},
    {"tags": ["bottle", "bottle-runtime-v2"]},
])
def test_snapshot_metadata_rejects_wrong_or_unsupported_records(changed):
    record = {"id": "a" * 64, "paths": ["/data/app_data"], "tags": ["bottle"]} | changed
    with pytest.raises(snapshots.SnapshotConfigurationError):
        snapshots.snapshot_metadata("a" * 8, json.dumps([record]).encode())


def test_snapshot_metadata_resolves_full_id_and_legacy_scope():
    record = {"id": "a" * 64, "paths": ["/data/app_data"], "tags": ["openhost"]}
    parsed = snapshots.snapshot_metadata("a" * 8, json.dumps([record]).encode())
    assert parsed.id == "a" * 64 and not parsed.has_configuration
    with pytest.raises(snapshots.SnapshotConfigurationError):
        snapshots.snapshot_metadata("a" * 8, json.dumps([record, record]).encode())


@pytest.mark.parametrize("tags", [
    ["bottle", snapshots.CONFIGURATION_TAG, "bottle-runtime-v2"],
    ["bottle", snapshots.CONFIGURATION_TAG, snapshots.RUNTIME_TAG, snapshots.RUNTIME_TAG],
])
def test_snapshot_metadata_rejects_unknown_or_duplicate_runtime_tags(tags):
    record = {"id": "a" * 64, "paths": [str(snapshots.CONFIGURATION_FILE)], "tags": tags}
    with pytest.raises(snapshots.SnapshotConfigurationError):
        snapshots.snapshot_metadata("a" * 8, json.dumps([record]).encode())


def test_interrupted_journal_retains_original_data_locations(environment):
    state = {
        "journal_version": 1, "snapshot": "a" * 64, "job_id": "b" * 32,
        "phase": "restoring_data", "needs_attention": True,
        "affected_apps": ["demo"], "affected_roots": ["app_data"],
        "retained_stages": [{"root": "app_data", "job_id": "c" * 32}],
    }
    original_mode = stat.S_IMODE(backup_app.APP_DATA_DIR.stat().st_mode)
    snapshots.save_journal(backup_app._restore_journal_path(), state)
    backup_app._load_restore_journal()
    assert backup_app._restore_needs_attention
    assert backup_app.restore_progress["phase"] == "interrupted"
    assert backup_app.restore_progress["affected_roots"] == ["app_data"]
    assert backup_app.restore_progress["retained_stages"] == state["retained_stages"]
    assert stat.S_IMODE(backup_app.APP_DATA_DIR.stat().st_mode) == original_mode
