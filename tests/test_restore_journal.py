import asyncio
import copy
import json
import os
import shutil
import stat
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import app as backup_app
import snapshot_configuration as snapshots
from configuration import ConfigurationError
from tests.test_snapshot_configuration import OWNER_CREDENTIAL, bundle, environment, newest_snapshot


class Session:
    restore_app_names = ("demo",)

    def __init__(self, *args):
        self.done = False
        self.omitted = []

    @property
    def progress(self):
        return {"ok": self.done, "apps": [], "warnings": [], "paused_apps": [],
                "omitted_app_data": list(self.omitted)}

    @property
    def summary(self):
        return self.progress

    def note_omitted_data(self, names):
        self.omitted = sorted(set(self.omitted) | set(names))

    async def preflight(self):
        pass

    async def stop_apps(self):
        pass

    async def activate(self):
        self.done = True

    async def restart_unaffected(self):
        pass


def state(**overrides):
    return {
        "journal_version": 1, "snapshot": "a" * 64, "job_id": "b" * 32,
        "phase": "incomplete", "needs_attention": True,
        "affected_apps": ["demo"], "affected_roots": ["app_data"],
        "retained_stages": [], "pending_restarts": [], "recovery": None,
        **overrides,
    }


async def test_retry_cannot_clear_unresolved_unaffected_restart(environment, monkeypatch):
    assert await backup_app.run_backup()
    snapshot = await newest_snapshot()
    backup_app.restore_progress = state(pending_restarts=[{"name": "other", "app_id": "222222222222"}])
    backup_app._restore_needs_attention = True
    monkeypatch.setattr(backup_app, "RecoverySession", Session)
    listing = [{"name": "other", "app_id": "222222222222", "status": "stopped"}]

    class Client:
        def __init__(self, *args):
            pass

        async def get(self, path):
            assert path == "/api/apps"
            return copy.deepcopy(listing)

    monkeypatch.setattr(backup_app, "RouterClient", Client)
    # A configuration snapshot recovers only for a caller the router confirmed
    # as owner, so the worker receives that verified token.
    assert not await backup_app.run_restore(snapshot["id"], owner_token=OWNER_CREDENTIAL)
    assert backup_app._restore_needs_attention
    assert backup_app.restore_progress["pending_restarts"] == [{"name": "other", "app_id": "222222222222"}]
    backup_app._load_restore_journal()
    assert backup_app.restore_progress["pending_restarts"]
    listing[0]["status"] = "running"
    assert await backup_app.run_restore(snapshot["id"], owner_token=OWNER_CREDENTIAL)
    assert not backup_app._restore_needs_attention
    assert backup_app.restore_progress["pending_restarts"] == []


async def test_successful_history_does_not_expand_new_failed_job(environment, monkeypatch):
    _, _, _, conf, _ = environment
    backup_app.restore_progress = state(phase="complete", needs_attention=False, affected_apps=["old-app"], affected_roots=["app_temp_data"])
    backup_app._restore_needs_attention = False
    monkeypatch.setattr(snapshots, "read_configuration", AsyncMock(return_value=bundle(runtime=True)))

    class Failing(Session):
        async def stop_apps(self):
            raise ConfigurationError("stop_failed")

    monkeypatch.setattr(backup_app, "RecoverySession", Failing)
    snapshot = snapshots.Snapshot("a" * 64, (str(snapshots.CONFIGURATION_FILE),), True, True)
    with pytest.raises(ConfigurationError):
        await backup_app._restore_configuration_snapshot(snapshot, conf, OWNER_CREDENTIAL)
    backup_app._load_restore_journal()
    assert backup_app.restore_progress["affected_apps"] == ["demo"]
    assert backup_app.restore_progress["affected_roots"] == []


async def test_failed_retry_keeps_original_coverage_and_roots(environment, monkeypatch):
    _, _, _, conf, _ = environment
    backup_app.restore_progress = state()
    backup_app._restore_needs_attention = True
    monkeypatch.setattr(snapshots, "read_configuration", AsyncMock(return_value=bundle(runtime=True)))
    monkeypatch.setattr(backup_app, "RecoverySession", Session)
    snapshot = snapshots.Snapshot("a" * 64, (str(backup_app.APP_TEMP_DATA), str(snapshots.CONFIGURATION_FILE)), True, True)
    before = copy.deepcopy(backup_app.restore_progress)
    with pytest.raises(snapshots.SnapshotConfigurationError):
        await backup_app._restore_configuration_snapshot(snapshot, conf, OWNER_CREDENTIAL)
    assert backup_app.restore_progress == before
    assert backup_app._restore_needs_attention


def _reject_directory_fsync(monkeypatch):
    real = snapshots.os.fsync

    def failing(descriptor):
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError("synthetic directory fsync failure")
        return real(descriptor)

    monkeypatch.setattr(snapshots.os, "fsync", failing)


def test_journal_rename_is_durable_or_fatal(environment, monkeypatch):
    path = backup_app._restore_journal_path()
    original = state(phase="incomplete", pending_restarts=[{"name": "other", "app_id": "222222222222"}])
    snapshots.save_journal(path, original)
    _reject_directory_fsync(monkeypatch)
    with pytest.raises(snapshots.SnapshotConfigurationError):
        snapshots.save_journal(path, state(phase="complete", needs_attention=False))
    # A failed directory sync is fatal, and the previous journal is restored, so
    # a later restart cannot read a cleared gate that the caller still reports
    # as a persistence failure.
    assert json.loads(path.read_text()) == original
    assert not list(path.parent.glob("journal-rollback-*"))


async def test_journal_persistence_failure_precedes_recovery_actions(environment, monkeypatch):
    _, _, _, conf, _ = environment
    called = []

    class Recorded(Session):
        async def preflight(self):
            called.append("preflight")

        async def stop_apps(self):
            called.append("stop")

    monkeypatch.setattr(snapshots, "read_configuration", AsyncMock(return_value=bundle(runtime=True)))
    monkeypatch.setattr(backup_app, "RecoverySession", Recorded)
    _reject_directory_fsync(monkeypatch)
    snapshot = snapshots.Snapshot("a" * 64, (str(backup_app.APP_TEMP_DATA), str(snapshots.CONFIGURATION_FILE)), True, True)
    with pytest.raises(snapshots.SnapshotConfigurationError):
        await backup_app._restore_configuration_snapshot(snapshot, conf, OWNER_CREDENTIAL)
    assert called == []
    assert not backup_app._restore_needs_attention
    assert not (backup_app.ALL_APP_DATA / backup_app.RESTORE_WORK_NAME).exists()


def test_restart_during_stopping_remembers_potentially_paused_unaffected_apps(environment):
    saved = state(phase="stopping", recovery={"destination_apps_before": [
        {"name": "demo", "app_id": "111111111111", "status": "running"},
        {"name": "other", "app_id": "222222222222", "status": "running"},
        {"name": "backup", "app_id": "333333333333", "status": "running"},
        {"name": "already-stopped", "app_id": "444444444444", "status": "stopped"},
    ], "paused_apps": []})
    snapshots.save_journal(backup_app._restore_journal_path(), saved)
    backup_app._load_restore_journal()
    assert backup_app.restore_progress["pending_restarts"] == [{"name": "other", "app_id": "222222222222"}]


async def test_acknowledgment_persistence_failure_keeps_gate(environment, monkeypatch):
    retained = [{"root": "app_data", "job_id": "c" * 32}]
    backup_app.restore_progress = state(retained_stages=retained)
    backup_app._restore_needs_attention = True
    snapshots.save_journal(backup_app._restore_journal_path(), backup_app.restore_progress)
    before = backup_app._restore_journal_path().read_bytes()
    save = snapshots.save_journal
    monkeypatch.setattr(snapshots, "save_journal", lambda *args: (_ for _ in ()).throw(OSError("synthetic")))
    monkeypatch.setattr(backup_app, "_caller_is_owner", AsyncMock(return_value=OWNER_CREDENTIAL))
    client = backup_app.app.test_client()
    response = await client.post("/api/restore/acknowledge")
    assert response.status_code == 500
    assert backup_app._restore_needs_attention
    assert backup_app._restore_journal_path().read_bytes() == before
    monkeypatch.setattr(snapshots, "save_journal", save)
    response = await client.post("/api/restore/acknowledge")
    assert response.status_code == 200
    assert not backup_app._restore_needs_attention
    saved = json.loads(backup_app._restore_journal_path().read_text())
    assert saved["phase"] == "acknowledged" and saved["retained_stages"] == retained


async def test_staging_references_exist_before_promotion(environment, monkeypatch):
    assert await backup_app.run_backup()
    snapshot = await newest_snapshot()
    monkeypatch.setattr(backup_app, "RecoverySession", Session)

    async def fail(staged, destination, names):  # noqa: ARG001 - promotion arguments
        saved = json.loads(backup_app._restore_journal_path().read_text())
        expected = {"root": "app_data", "job_id": saved["job_id"]}
        assert expected in saved["retained_stages"]
        (staged / ".migration-old-originals").mkdir()
        (staged / ".migration-old-originals" / "precious.txt").write_text("original data")
        raise OSError("promotion and rollback failed")

    monkeypatch.setattr(backup_app.migration_data, "replace_app_trees", fail)
    assert not await backup_app.run_restore(snapshot["id"], owner_token=OWNER_CREDENTIAL)
    saved = json.loads(backup_app._restore_journal_path().read_text())
    stage = backup_app.ALL_APP_DATA / backup_app.RESTORE_WORK_NAME / saved["job_id"]
    originals = stage / str(backup_app.ALL_APP_DATA).lstrip("/") / ".migration-old-originals" / "precious.txt"
    assert originals.read_text() == "original data"
    backup_app._load_restore_journal()
    assert saved["retained_stages"] == backup_app.restore_progress["retained_stages"]


async def test_repeated_cancel_keeps_real_capture_until_process_settles(environment, monkeypatch):
    started, settled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = asyncio.create_subprocess_exec
    processes = []

    async def spawn(*args, **kwargs):
        process = await original(*args, **kwargs)
        if args[:2] == ("restic", "backup"):
            communicate = process.communicate

            async def held():
                result = await communicate()
                settled.set()
                await release.wait()
                return result

            process.communicate = held
            processes.append(process)
            started.set()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(backup_app.run_backup())
    try:
        await asyncio.wait_for(started.wait(), 15)
        task.cancel()
        await asyncio.wait_for(settled.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert snapshots.CONFIGURATION_FILE.is_file()
        assert backup_app.op_lock.backup_running
        assert processes[0].returncode is not None
    finally:
        release.set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not snapshots.CONFIGURATION_FILE.exists()
    assert not backup_app.op_lock.busy


def _stage_for(backup_app_module, job_id):
    return backup_app_module.ALL_APP_DATA / backup_app_module.RESTORE_WORK_NAME / job_id


async def _restore(backup_app_module, snapshot):
    return await backup_app_module.run_restore(snapshot["id"], owner_token=OWNER_CREDENTIAL)


async def test_committed_recovery_barriers_then_discards_originals(environment, monkeypatch):
    assert await backup_app.run_backup()
    snapshot = await newest_snapshot()
    monkeypatch.setattr(backup_app, "RecoverySession", Session)
    order = []

    real_barrier = backup_app.migration_data.durability_barrier
    def barrier(*roots):
        phase = json.loads(backup_app._restore_journal_path().read_text())["phase"]
        if phase == "activating":
            order.append(("barrier", phase))
        real_barrier(*roots)

    async def discard(rollback):
        order.append(("discard", Path(rollback).is_dir()))
        await asyncio.to_thread(shutil.rmtree, rollback)

    monkeypatch.setattr(backup_app.migration_data, "durability_barrier", barrier)
    monkeypatch.setattr(backup_app.migration_data, "discard_app_trees", discard)
    assert await _restore(backup_app, snapshot)
    assert restore_last_snapshot_of(backup_app) == snapshot["id"]
    roots = {name for name, path in backup_app._ROOT_NAMES.items() if str(path) in snapshot["paths"]}
    # The barrier runs once, after activation and before the durable complete
    # write, then every promoted root is disposed.
    assert order[0] == ("barrier", "activating")
    assert order[1:] == [("discard", True)] * len(roots)
    assert not backup_app._restore_needs_attention
    assert backup_app.restore_progress["retained_stages"] == []
    for root in roots:
        assert not (_stage_for(backup_app, backup_app.restore_progress["job_id"]) /
                    str(backup_app._ROOT_NAMES[root]).lstrip("/")).exists()


def restore_last_snapshot_of(backup_app_module):
    return backup_app_module.restore_last_snapshot


async def test_failed_durability_barrier_keeps_originals_and_attention(environment, monkeypatch):
    assert await backup_app.run_backup()
    snapshot = await newest_snapshot()
    monkeypatch.setattr(backup_app, "RecoverySession", Session)
    disposed = []
    real_barrier = backup_app.migration_data.durability_barrier
    def fail_completion(*roots):
        if backup_app.restore_progress["phase"] == "activating":
            raise OSError("synthetic")
        real_barrier(*roots)
    monkeypatch.setattr(backup_app.migration_data, "durability_barrier", fail_completion)
    monkeypatch.setattr(backup_app.migration_data, "discard_app_trees", lambda rollback: disposed.append(rollback))
    assert not await _restore(backup_app, snapshot)
    assert not disposed
    assert backup_app._restore_needs_attention
    retained = backup_app.restore_progress["retained_stages"]
    roots = {name for name, path in backup_app._ROOT_NAMES.items() if str(path) in snapshot["paths"]}
    assert {entry["root"] for entry in retained} == roots
    for entry in retained:
        originals = list(_stage_for(backup_app, entry["job_id"]).rglob(".migration-old-*"))
        assert originals, "promoted originals must stay recoverable until a durable commit"
    saved = json.loads(backup_app._restore_journal_path().read_text())
    assert saved["needs_attention"] is True and saved["retained_stages"] == retained


async def test_incomplete_recovery_journals_the_final_per_app_results(environment, monkeypatch):
    """The last checkpoint predates activation, so the final results must persist."""
    assert await backup_app.run_backup()
    snapshot = await newest_snapshot()
    monkeypatch.setattr(snapshots, "read_configuration", AsyncMock(return_value=bundle(runtime=True)))

    class Incomplete(Session):
        @property
        def progress(self):
            return {"ok": False, "apps": [{"name": "demo", "outcome": "deployment_failed"}],
                    "warnings": ["A deployment failure."], "paused_apps": [],
                    "omitted_app_data": list(self.omitted)}

    session = Incomplete()
    monkeypatch.setattr(backup_app, "RecoverySession", lambda *args: session)
    assert not await _restore(backup_app, snapshot)
    saved = json.loads(backup_app._restore_journal_path().read_text())
    assert saved["phase"] == "incomplete" and saved["needs_attention"] is True
    # The activation outcome, not the stale pre-activation checkpoint.
    assert saved["recovery"]["ok"] is False
    assert saved["recovery"]["apps"][0]["outcome"] == "deployment_failed"


async def test_captured_data_without_a_definition_is_disclosed(environment, monkeypatch):
    data, _, _, _, _ = environment
    assert await backup_app.run_backup()
    # A directory captured by the snapshot with no exported definition.
    (data / "removed-app").mkdir()
    (data / "removed-app" / "keep.txt").write_text("orphan data")
    assert await backup_app.run_backup()
    snapshot = await newest_snapshot()
    monkeypatch.setattr(snapshots, "read_configuration", AsyncMock(return_value=bundle(runtime=True)))
    session = Session()
    monkeypatch.setattr(backup_app, "RecoverySession", lambda *args: session)
    assert await _restore(backup_app, snapshot)
    saved = json.loads(backup_app._restore_journal_path().read_text())
    assert session.omitted == ["removed-app"]
    assert saved["recovery"]["omitted_app_data"] == ["removed-app"]
    # The undisclosed directory is untouched, and the recovery still verified.
    assert (data / "removed-app" / "keep.txt").read_text() == "orphan data"


async def test_failed_complete_journal_keeps_originals(environment, monkeypatch):
    assert await backup_app.run_backup()
    snapshot = await newest_snapshot()
    monkeypatch.setattr(backup_app, "RecoverySession", Session)
    phases = []
    real_checkpoint = backup_app._checkpoint_restore

    def checkpoint(phase, *, needs_attention=False):
        phases.append(phase)
        if phase == "complete":
            raise OSError("synthetic complete-journal failure")
        return real_checkpoint(phase, needs_attention=needs_attention)

    disposed = []
    monkeypatch.setattr(backup_app, "_checkpoint_restore", checkpoint)
    monkeypatch.setattr(backup_app.migration_data, "discard_app_trees", lambda rollback: disposed.append(rollback))
    assert not await _restore(backup_app, snapshot)
    # The commit was attempted, then the final durable record demands attention.
    assert phases[-2:] == ["complete", "incomplete"]
    saved = json.loads(backup_app._restore_journal_path().read_text())
    assert saved["phase"] == "incomplete" and saved["needs_attention"] is True
    assert saved["recovery"]["ok"] is True
    # A recovery that was never durably committed must keep its originals.
    assert not disposed
    assert backup_app._restore_needs_attention
    retained = backup_app.restore_progress["retained_stages"]
    assert retained
    assert list(_stage_for(backup_app, retained[0]["job_id"]).rglob(".migration-old-*"))


async def test_post_commit_disposal_failure_keeps_retained_reference(environment, monkeypatch):
    assert await backup_app.run_backup()
    snapshot = await newest_snapshot()
    monkeypatch.setattr(backup_app, "RecoverySession", Session)

    async def fail_discard(rollback):
        raise OSError("synthetic post-commit disposal failure")

    monkeypatch.setattr(backup_app.migration_data, "discard_app_trees", fail_discard)
    # Disposal is garbage collection after a durable commit, so the recovery
    # stays successful and the stage stays for manual inspection.
    assert await _restore(backup_app, snapshot)
    assert restore_last_snapshot_of(backup_app) == snapshot["id"]
    assert not backup_app._restore_needs_attention
    retained = backup_app.restore_progress["retained_stages"]
    roots = {name for name, path in backup_app._ROOT_NAMES.items() if str(path) in snapshot["paths"]}
    assert {entry["root"] for entry in retained} == roots
    for entry in retained:
        assert _stage_for(backup_app, entry["job_id"]).is_dir()
    saved = json.loads(backup_app._restore_journal_path().read_text())
    assert saved["phase"] == "complete" and saved["needs_attention"] is False
    assert saved["retained_stages"] == retained
