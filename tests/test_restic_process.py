import asyncio
import os
import sys
from pathlib import Path

import pytest

import app as backup_app
import restic_process
import snapshot_configuration as snapshots


@pytest.mark.parametrize("mode", ["oversized", "timeout", "cancel"])
async def test_dump_failure_reaps_and_drains_owned_subprocess(tmp_path, monkeypatch, mode):
    command = tmp_path / "restic"
    command.write_text(
        f"#!{sys.executable}\nimport os,time\n"
        + ("os.write(1, b'x' * 2000000)\n" if mode == "oversized" else "os.write(1, b'x')\n")
        + "time.sleep(60)\n"
    )
    command.chmod(0o755)
    monkeypatch.setattr(snapshots, "MAX_CONFIGURATION_BYTES", 128)
    original = asyncio.create_subprocess_exec
    processes = []

    async def spawn(*args, **kwargs):
        process = await original(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    env = {**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ.get("PATH", "")}
    task = asyncio.create_task(snapshots.read_configuration("a" * 64, env, timeout=0.1 if mode == "timeout" else 5))
    if mode == "cancel":
        while not processes:
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        expected = asyncio.CancelledError
    else:
        expected = snapshots.SnapshotConfigurationError
    with pytest.raises(expected):
        await asyncio.wait_for(task, timeout=3)
    assert processes and processes[0].returncode is not None
    assert processes[0].stdout.at_eof()


async def test_backup_communication_settles_after_repeated_cancellation(monkeypatch):
    started, release, killed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Process:
        returncode = None

        async def communicate(self):
            started.set()
            await release.wait()
            self.returncode = -9
            return b"", b""

    process = Process()

    async def spawn(*args, **kwargs):
        assert kwargs["start_new_session"] is True
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(restic_process, "kill_group", lambda proc: killed.set())
    task = asyncio.create_task(backup_app._run_restic(["backup", "/fixture"], {"repo": "/fixture-repo", "repo_password": "synthetic"}))
    await started.wait()
    task.cancel()
    await killed.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.returncode == -9
