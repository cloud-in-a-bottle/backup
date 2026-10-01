"""Promotion invariants independent of either restore entry point."""
import asyncio
import ctypes
import errno
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import migration_data as data
import migration as migration


def test_syncfs_surfaces_writeback_errors_without_opening_restrictive_files(tmp_path, monkeypatch):
    private = tmp_path / "private"
    private.write_text("private contents")
    private.chmod(0)
    try:
        data.durability_barrier(tmp_path)
        def fail(fd):
            ctypes.set_errno(errno.EIO)
            return -1
        monkeypatch.setattr(data, "_syncfs", fail)
        with pytest.raises(OSError) as error:
            data.durability_barrier(tmp_path)
        assert error.value.errno == errno.EIO
    finally:
        private.chmod(0o600)


@pytest.fixture
def trees(tmp_path):
    source, destination = tmp_path / "staged", tmp_path / "live"
    for root, value in ((source, "new"), (destination, "original")):
        for name in ("alpha", "beta"):
            (root / name).mkdir(parents=True)
            (root / name / "data").write_text(value)
    return source, destination


@pytest.mark.parametrize("fault", [None, "second-rename", "barrier"])
async def test_promote_retains_originals_until_explicit_disposal(trees, monkeypatch, fault):
    source, destination = trees
    if fault == "second-rename":
        rename = Path.rename
        def fail(path, target):
            if path == source / "beta":
                raise OSError("second promotion failed")
            return rename(path, target)
        monkeypatch.setattr(Path, "rename", fail)
    if fault == "barrier":
        monkeypatch.setattr(data, "durability_barrier", lambda *args: (_ for _ in ()).throw(OSError("writeback failed")))
    if fault:
        with pytest.raises(data.DataError):
            await data.replace_app_trees(source, destination, ["alpha", "beta"])
        for name in ("alpha", "beta"):
            assert (destination / name / "data").read_text() == "original"
    else:
        original = await data.replace_app_trees(source, destination, ["alpha", "beta"])
        for name in ("alpha", "beta"):
            assert (destination / name / "data").read_text() == "new"
            assert (original / name / "data").read_text() == "original"
        await data.discard_app_trees(original)
        assert not original.exists()


@pytest.mark.parametrize("which", ["source", "destination"])
async def test_symlink_roots_never_promote(trees, which):
    source, destination = trees
    path = (source if which == "source" else destination) / "alpha"
    moved = path.with_name("elsewhere")
    path.rename(moved)
    path.symlink_to(moved, target_is_directory=True)
    with pytest.raises(data.DataError):
        await data.replace_app_trees(source, destination, ["alpha"])
    assert (destination / "alpha" / "data").read_text() == "original"


def test_private_work_rejects_symlink_or_foreign_owner(tmp_path, monkeypatch):
    (tmp_path / "backup").mkdir()
    work = tmp_path / "backup" / ".migration"
    work.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(data.DataError):
        data.private_work_dir(tmp_path, work, "backup")
    work.unlink()
    monkeypatch.setattr(os, "geteuid", lambda: os.stat(tmp_path).st_uid + 1)
    with pytest.raises(data.DataError):
        data.private_work_dir(tmp_path, work, "backup")


@pytest.mark.parametrize("corrupt", [False, True])
def test_source_restart_retains_cutover_intent(tmp_path, corrupt):
    from operations import OperationLock
    (tmp_path / "backup").mkdir()
    kwargs = dict(lock=OperationLock(), all_app_data=tmp_path, work_dir=tmp_path / "backup" / ".migration", router_url="https://router.test")
    record = migration.SourceRecoveryRecord(**kwargs)
    before = [{"name": name, "app_id": identifier * 12, "status": "running"}
              for name, identifier in (("alpha", "A"), ("other", "B"))]
    record.begin("d" * 64, {"alpha"}, before, "backup")
    if corrupt:
        record._journal.write_text('{"needs_attention":false,"secret":"never echo"}')
    restarted = migration.SourceRecoveryRecord(**kwargs)
    assert restarted.needs_attention and not restarted.live
    assert "never echo" not in json.dumps(restarted.journal_status)
    if not corrupt:
        assert restarted.journal_status["restart_pending"] == ["other"]
        assert restarted.journal_status["selected_apps"] == ["alpha"]


def test_actual_process_death_at_first_stop_leaves_source_intent(tmp_path):
    from operations import OperationLock
    (tmp_path / "backup").mkdir()
    program = '''
import asyncio, os, sys
from pathlib import Path
import migration as m
from operations import OperationLock
from tests.test_recovery import make_bundle, inventory_entry
root = Path(sys.argv[1])
bundle = make_bundle("alpha", "other", "backup", runtime=True)
before = [inventory_entry(n, i * 12) for n, i in (("alpha", "A"), ("other", "B"), ("backup", "C"))]
async def capture(*args): return bundle
class Source:
    def __init__(self, *args): pass
    progress = {"destination_apps_before": before}
    async def preflight(self): pass
    async def stop_apps(self): os._exit(73)
class Peer:
    def __init__(self, *args): pass
    async def request(self, method, path, **kwargs):
        if path.endswith("capabilities"):
            return {"ok": True, "version": 5, "chunk_limit": m.CHUNK_LIMIT, "backup_app_name": "backup", "capture_complete": True}
        return {"ok": True, "version": 5, "session_id": "d" * 64, "accepted_apps": ["alpha"]}
m.capture_configuration, m.RecoverySession, m._Peer = capture, Source, Peer
asyncio.run(m.run_direct_push(target_url="https://destination.test", target_token="owner", selected_apps=["alpha"],
    lock=OperationLock(), all_app_data=root, work_dir=root / "backup" / ".migration",
    router_url="https://source.test", app_token="app", owner_token="owner"))
'''
    child = subprocess.run([sys.executable, "-c", program, str(tmp_path)], capture_output=True, timeout=15)
    assert child.returncode == 73, child.stderr.decode()
    restarted = migration.SourceRecoveryRecord(lock=OperationLock(), all_app_data=tmp_path,
        work_dir=tmp_path / "backup" / ".migration", router_url="https://source.test")
    assert restarted.needs_attention
    assert restarted.journal_status["restart_pending"] == ["other"]
