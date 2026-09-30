"""Real filesystem archives and stateful v4 HTTP/cancellation regressions."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import gzip
import io
import json
import multiprocessing
import os
from pathlib import Path
import stat
import tarfile
import threading
import unittest.mock
from unittest.mock import AsyncMock

import httpx
import pytest

import migration as m
import migration_data as data
from operations import OpKind, OperationLock
from recovery import RecoverySession
from tests.test_configuration import OWNER_TOKEN, inventory_entry, mock_http
from tests.test_recovery import Router


def bundle(names=("alpha", "beta", "backup")):
    return {"format_version": 1, "backup_app_name": "backup", "definitions": {
        "schema_version": 2, "mode": "private",
        "apps": [{"name": n, "source": {"kind": "builtin", "identifier": n}, "port_mappings": []} for n in names],
        "platform_api_tokens": [{"name": "private-token-name", "token_hash": "b" * 64, "expires_at": None}]},
        "runtime": {"apps": {n: {"status": "running", "global_grants": [], "unresolved_provider_grants": []} for n in names}, "providers": []}}


def inventory(names):
    return [inventory_entry(n, str(i + 1) * 12) for i, n in enumerate(names)]


async def chunks(payload, size=97):
    for i in range(0, len(payload), size):
        yield payload[i:i + size]


def tar_bytes(entries):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as tar:
        for name, kind, value in entries:
            entry = tarfile.TarInfo(name)
            entry.uid, entry.gid, entry.mode = os.geteuid(), os.getegid(), 0o750
            entry.type = kind
            if kind == tarfile.REGTYPE:
                entry.size = len(value)
                tar.addfile(entry, io.BytesIO(value))
            else:
                if kind in {tarfile.SYMTYPE, tarfile.LNKTYPE}:
                    entry.linkname = value
                tar.addfile(entry)
    return stream.getvalue()


def make_root(path):
    path.mkdir()
    (path / "backup").mkdir()
    return path


@pytest.fixture
def environment(tmp_path, monkeypatch, mock_http):
    full = bundle()
    selected = m.subset_configuration(full, {"alpha"})
    router = Router(full, inventory(["alpha", "beta", "backup"]))
    root = make_root(tmp_path / "destination")
    for name in ("alpha", "beta"):
        (root / name).mkdir()
        (root / name / "old.db-wal").write_bytes(b"stale")
    lock = OperationLock()
    receiver = m.MigrationReceiver(lock=lock, all_app_data=root, work_dir=root / "backup" / "work", router_url="https://destination.test")
    original = RecoverySession
    monkeypatch.setattr(m, "RecoverySession", lambda *a, **kw: original(*a, **kw, poll_interval=.001, deployment_timeout=1))
    mock_http(router)
    return receiver, router, selected, root, lock


async def start(receiver, selected):
    return await receiver.start({"version": 4, "bundle": selected}, owner_token=OWNER_TOKEN)


async def test_start_failure_after_lock_acquisition_releases_it(environment, monkeypatch):
    receiver, router, selected, root, lock = environment
    real_create_task = asyncio.create_task

    def fail_for_monitor(coro, **kwargs):
        # The watchdog is the first task start() creates; a failure there used
        # to escape the cleanup that releases the migration lock.
        if coro.cr_code is receiver._watch.__code__:
            coro.close()
            raise RuntimeError("no running event loop")
        return real_create_task(coro, **kwargs)

    monkeypatch.setattr(asyncio, "create_task", fail_for_monitor)
    with pytest.raises(m.MigrationError):
        await start(receiver, selected)
    monkeypatch.setattr(asyncio, "create_task", real_create_task)
    assert not lock.busy
    assert receiver._busy is False
    assert receiver._monitor is None
    assert not (receiver.work / receiver._record["session_id"]).exists()
    # A second attempt is admitted, so the lock is not stranded.
    session = await start(receiver, selected)
    assert session["session_id"]


async def upload(receiver, session_id, payload=None, name="alpha", **overrides):
    if payload is None:
        payload = tar_bytes([(".", tarfile.DIRTYPE, None), ("database", tarfile.REGTYPE, b"new")])
    kwargs = {"index": 0, "final": True, "archive_bytes": len(payload), "archive_sha256": hashlib.sha256(payload).hexdigest(), "owner_token": OWNER_TOKEN}
    kwargs.update(overrides)
    return await receiver.upload(session_id, name, chunks(payload), **kwargs)


async def abort(receiver, session_id):
    await receiver.abort({"version": 4, "session_id": session_id}, owner_token=OWNER_TOKEN)


@pytest.mark.parametrize("name", ["myapp", "app.v2", "a_b", "2026-09-29T10:10:10", "a" * 200])
def test_utility_valid_names(name):
    assert m.validate_name(name)


@pytest.mark.parametrize("name", [None, [], "", "../bad", "bad/name", "bad\\name", ".hidden", "-bad", "x\n", "a" * 201])
def test_utility_invalid_names(name):
    assert not m.validate_name(name)


@pytest.mark.parametrize("source,target", [("example.test", "https://backup.example.test"), ("http://127.0.0.1:80", "http://127.0.0.1:80"), ("http://[::1]:80", "http://[::1]:80"), ("https://backup.example.test/", "https://backup.example.test")])
def test_target_url(source, target):
    assert m._target_backup_url(source) == target


async def test_peer_preserves_bounded_unicode_json_without_ascii_expansion(monkeypatch, mock_http):
    monkeypatch.setattr(m, "MAX_JSON_BYTES", 200)
    body = {"message": "雪" * 40}
    async def response(request):
        assert len(request.content) < 200 and json.loads(request.content) == body
        return httpx.Response(200, json={"ok": True, "version": 4})
    mock_http(response)
    result = await m._Peer("https://destination.test", OWNER_TOKEN).request("POST", "/test", body=body)
    assert result["ok"]


async def test_reject_legacy_before_auth_or_changes(environment, monkeypatch):
    receiver, router, selected, root, lock = environment
    auth = AsyncMock()
    monkeypatch.setattr(receiver, "_authenticate", auth)
    for body in ({"version": 3, "apps": []}, {"apps": []}, {"version": True, "bundle": selected}):
        with pytest.raises(m.MigrationError, match="protocol v4"):
            await receiver.start(body, owner_token=OWNER_TOKEN)
    auth.assert_not_called()
    assert not lock.busy and not router.events
    assert (root / "alpha" / "old.db-wal").exists()


async def test_owner_auth_preflight_start_no_mutation(environment):
    receiver, router, selected, root, lock = environment
    router.hooks["/api/app-definitions/parse"] = lambda *_: httpx.Response(403, json={"error": "secret"})
    with pytest.raises(m.MigrationError, match="authentication"):
        await start(receiver, selected)
    assert not lock.busy
    assert all(a["status"] == "running" for a in router.apps.values())
    assert (root / "alpha" / "old.db-wal").exists()


async def test_receiver_rejects_destination_executor_collision_before_acceptance(environment):
    receiver, router, _, root, lock = environment
    selected = bundle(("alpha", "backup"))
    selected["backup_app_name"] = "source-backup"
    with pytest.raises(m.MigrationError, match="conflicts"):
        await start(receiver, selected)
    assert receiver.journal_status is None and not lock.busy and not router.mutations()
    assert (root / "alpha" / "old.db-wal").exists()


async def test_complete_real_staging_tree_replacement_and_activation(environment):
    receiver, router, selected, root, lock = environment
    accepted = await start(receiver, selected)
    sid = accepted["session_id"]
    assert accepted["accepted_apps"] == ["alpha"] and len(sid) == 64
    assert lock.busy and router.apps["alpha"]["status"] == "running"
    receipt = await upload(receiver, sid)
    assert receipt["receipt"]["complete"] is True
    assert (root / "alpha" / "old.db-wal").exists()
    def before_import(*_):
        assert (root / "alpha" / "database").read_bytes() == b"new"
        assert not (root / "alpha" / "old.db-wal").exists()
        router.data_restored = True
    router.hooks["/api/app-definitions/import-private"] = before_import
    first = await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    assert first["phase"] == "finalizing"
    job = receiver._job
    await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    assert receiver._job is job
    await job
    result = await receiver.status(sid, owner_token=OWNER_TOKEN)
    assert result["phase"] == "complete" and result["result"]["ok"]
    assert not receiver.needs_attention
    assert not lock.busy
    assert all(a["status"] == "running" for a in router.apps.values())
    assert (root / "beta" / "old.db-wal").exists()
    assert router.tokens[-1] == selected["definitions"]["platform_api_tokens"][0]
    public = json.dumps(result) + receiver._journal.read_text()
    assert OWNER_TOKEN not in public and "b" * 64 not in public and "private-token-name" not in public
    assert receiver._recovery is None and not receiver._stage.exists()


@pytest.mark.parametrize("kind", ["missing", "extra", "wrong-session", "duplicate", "index", "size", "hash"])
async def test_reject_invalid_session_sequences(environment, kind):
    receiver, router, selected, root, lock = environment
    accepted = await start(receiver, selected)
    sid = accepted["session_id"]
    with pytest.raises(m.MigrationError):
        if kind == "missing":
            await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
        elif kind == "extra":
            await upload(receiver, sid, name="beta")
        elif kind == "wrong-session":
            await upload(receiver, "b" * 64)
        elif kind == "duplicate":
            await upload(receiver, sid)
            await upload(receiver, sid)
        else:
            await upload(receiver, sid, **{"index": {"index": 1}, "size": {"archive_bytes": True}, "hash": {"archive_sha256": "wrong"}}[kind])
    assert router.apps["alpha"]["status"] == "running"
    assert (root / "alpha" / "old.db-wal").exists()
    await abort(receiver, sid)


async def test_finalize_cannot_replace_bundle(environment):
    receiver, router, selected, root, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    await upload(receiver, sid)
    with pytest.raises(m.MigrationError):
        await receiver.finalize({"version": 4, "session_id": sid, "bundle": bundle()}, owner_token=OWNER_TOKEN)
    assert router.apps["alpha"]["status"] == "running"
    await abort(receiver, sid)


@pytest.mark.parametrize("fault", ["hash", "short", "long", "broken-gzip", "stream-error"])
async def test_failed_upload_poisoned_no_activation(environment, fault):
    receiver, router, selected, root, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    payload = tar_bytes([("file", tarfile.REGTYPE, b"hello")])
    settings = {}
    if fault == "hash":
        settings["archive_sha256"] = "b" * 64
    elif fault == "short":
        settings["archive_bytes"] = len(payload) + 1
    elif fault == "long":
        settings["archive_bytes"] = len(payload) - 1
    elif fault == "broken-gzip":
        payload = b"not-a-gzip"
    with pytest.raises(m.MigrationError, match="verification"):
        if fault == "stream-error":
            async def broken():
                yield payload[:10]
                raise OSError("private-token-name")
            await receiver.upload(sid, "alpha", broken(), index=0, final=True, archive_bytes=len(payload), archive_sha256=hashlib.sha256(payload).hexdigest(), owner_token=OWNER_TOKEN)
        else:
            await upload(receiver, sid, payload, **settings)
    assert not lock.busy
    assert (await receiver.status(sid, owner_token=OWNER_TOKEN))["phase"] == "failed"
    assert router.apps["alpha"]["status"] == "running"
    assert (root / "alpha" / "old.db-wal").exists()
    with pytest.raises(m.MigrationError):
        await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)


async def test_chunk_order_and_explicit_receipt(environment, monkeypatch):
    receiver, _, selected, _, _ = environment
    monkeypatch.setattr(m, "CHUNK_LIMIT", 50)
    sid = (await start(receiver, selected))["session_id"]
    payload = tar_bytes([("file", tarfile.REGTYPE, os.urandom(200))])
    digest = hashlib.sha256(payload).hexdigest()
    for index, offset in enumerate(range(0, len(payload), 50)):
        final = offset + 50 >= len(payload)
        response = await receiver.upload(sid, "alpha", chunks(payload[offset:offset+50]), index=index, final=final, archive_bytes=len(payload), archive_sha256=digest, owner_token=OWNER_TOKEN)
        assert bool(response["receipt"]) == final
        if index == 0:
            with pytest.raises(m.MigrationError):
                await receiver.upload(sid, "alpha", chunks(payload[:50]), index=0, final=False, archive_bytes=len(payload), archive_sha256=digest, owner_token=OWNER_TOKEN)
    await abort(receiver, sid)


async def test_upload_request_timeout_cleans_stage(environment):
    receiver, _, selected, _, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    receiver.request_timeout = .03
    async def stalled():
        yield b"a"
        await asyncio.sleep(10)
    with pytest.raises(m.MigrationError):
        await receiver.upload(sid, "alpha", stalled(), index=0, final=True, archive_bytes=2, archive_sha256="a"*64, owner_token=OWNER_TOKEN)
    assert not lock.busy and not receiver._stage.exists()


async def test_cancellation_drains_verification_and_preserves_receipt(environment, monkeypatch):
    receiver, _, selected, _, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    entered, release = threading.Event(), threading.Event()
    real = m.extract_archive
    def blocked(*args):
        entered.set()
        assert release.wait(5)
        real(*args)
    monkeypatch.setattr(m, "extract_archive", blocked)
    task = asyncio.create_task(upload(receiver, sid))
    await asyncio.to_thread(entered.wait, 3)
    task.cancel()
    task.cancel()
    await asyncio.sleep(.02)
    assert not task.done() and lock.busy and receiver._stage.exists()
    receiver._activity -= 10000
    await receiver.expire_stale()
    assert lock.busy
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert lock.busy and receiver._stage.exists()
    assert receiver.journal_status["receipts"]["alpha"]["complete"]
    assert receiver.journal_status["phase"] == "receiving"
    await abort(receiver, sid)


async def test_background_finalize_survives_request_cancellation(environment):
    receiver, router, selected, _, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    await upload(receiver, sid)
    entered, release = asyncio.Event(), asyncio.Event()
    async def pause(*_):
        entered.set()
        await release.wait()
        router.data_restored = True
    router.hooks["/api/app-definitions/import-private"] = pause
    await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    await entered.wait()
    assert lock.busy
    with pytest.raises(m.MigrationError, match="active"):
        await abort(receiver, sid)
    receiver._activity -= 10000
    await receiver.expire_stale()
    assert lock.busy
    release.set()
    await receiver._job
    assert receiver.journal_status["result"]["ok"] and not lock.busy


async def test_partial_promotion_never_activates_and_restarts_unaffected(environment, monkeypatch):
    receiver, router, selected, root, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    await upload(receiver, sid)
    monkeypatch.setattr(m, "replace_app_trees", AsyncMock(side_effect=OSError("secret repository")))
    await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    await receiver._job
    assert receiver.journal_status["phase"] == "incomplete"
    assert not receiver.journal_status["result"]["ok"]
    assert router.apps["alpha"]["status"] == "stopped"
    assert router.apps["beta"]["status"] == "running"
    assert not any(e[1] == "/api/app-definitions/import-private" for e in router.events)
    assert not lock.busy and receiver._stage.exists()
    assert "secret repository" not in json.dumps(receiver.journal_status)


async def test_stale_receiving_status_does_not_renew(environment):
    receiver, _, selected, _, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    receiver._activity -= 10000
    await receiver.status(sid, owner_token=OWNER_TOKEN)
    await receiver.expire_stale()
    assert not lock.busy and receiver.journal_status["phase"] == "aborted"


async def test_keepalive_renews_idle_receiving(environment):
    receiver, _, selected, _, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    receiver._activity -= 10000
    await receiver.keepalive({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    await receiver.expire_stale()
    assert lock.busy
    await abort(receiver, sid)


async def test_restart_journal_flags_interruption_without_activation(environment):
    receiver, _, selected, root, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    receiver._monitor.cancel()
    other = receiver.work / "not-owned"
    other.mkdir()
    replacement = m.MigrationReceiver(lock=OperationLock(), all_app_data=root, work_dir=root / "backup" / "work", router_url="https://destination.test")
    assert replacement.journal_status["phase"] == "interrupted"
    assert not replacement.journal_status["result"]["ok"]
    assert receiver._stage.exists() and not replacement.needs_attention
    await replacement.initialize()
    assert not receiver._stage.exists() and other.exists()
    lock.release(OpKind.MIGRATION)


def restarted(receiver, root):
    return m.MigrationReceiver(lock=OperationLock(), all_app_data=root,
                               work_dir=root / "backup" / "work", router_url=receiver.router_url)


async def test_failed_finalization_gates_restart_ack_preserves_only_originals(environment, monkeypatch):
    receiver, router, selected, root, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    await upload(receiver, sid)
    stage = receiver._stage
    rename = Path.rename
    def fail_promotion_and_rollback(path, target):
        if path == stage / "trees" / "alpha" or (path.name == "alpha" and path.parent.name.startswith(".migration-old-")):
            raise OSError("injected rename failure")
        return rename(path, target)
    monkeypatch.setattr(Path, "rename", fail_promotion_and_rollback)
    beta_id = router.apps["beta"]["app_id"]
    router.hooks[f"/reload_app/{beta_id}"] = lambda *_: httpx.Response(500, json={})
    await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    assert receiver.needs_attention
    durable = json.loads(receiver._journal.read_text())
    assert durable["needs_attention"] and sid in durable["retained_sessions"]
    await receiver._job
    assert not lock.busy and receiver.needs_attention
    assert router.apps["beta"]["status"] == "stopped"
    originals = list((stage / "trees").glob(".migration-old-*/alpha/old.db-wal"))
    assert len(originals) == 1 and originals[0].read_bytes() == b"stale"
    assert not (root / "alpha").exists()
    for candidate in (receiver, restarted(receiver, root)):
        before = candidate._journal.read_bytes()
        with pytest.raises(m.MigrationError, match="acknowledgment"):
            await start(candidate, selected)
        assert candidate._journal.read_bytes() == before
        assert candidate.needs_attention and sid in candidate.journal_status["retained_sessions"]
    receiver = restarted(receiver, root)
    before = copy.deepcopy(router.apps)
    result = await receiver.acknowledge(owner_token=OWNER_TOKEN)
    assert result["acknowledged"] and not receiver.needs_attention
    assert router.apps == before and originals[0].read_bytes() == b"stale"
    receiver = restarted(receiver, root)
    assert not receiver.needs_attention and receiver.journal_status["acknowledged"]
    monkeypatch.setattr(Path, "rename", rename)
    router.hooks.clear()
    router.data_restored = True
    retry = (await start(receiver, selected))["session_id"]
    assert retry != sid and sid in receiver.journal_status["retained_sessions"]
    await upload(receiver, retry)
    await receiver.finalize({"version": 4, "session_id": retry}, owner_token=OWNER_TOKEN)
    await receiver._job
    assert receiver.journal_status["result"]["ok"] and not receiver.needs_attention
    assert originals[0].read_bytes() == b"stale" and router.apps["beta"]["status"] == "stopped"
    assert restarted(receiver, root).journal_status["retained_sessions"] == [sid]


async def test_acknowledgment_auth_live_work_and_persistence_failure(environment, monkeypatch):
    receiver, router, selected, root, _ = environment
    sid = (await start(receiver, selected))["session_id"]
    with pytest.raises(m.MigrationError, match="active"):
        await receiver.acknowledge(owner_token=OWNER_TOKEN)
    await upload(receiver, sid)
    monkeypatch.setattr(m, "replace_app_trees", AsyncMock(side_effect=OSError()))
    await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    with pytest.raises(m.MigrationError, match="active"):
        await receiver.acknowledge(owner_token=OWNER_TOKEN)
    await receiver._job
    before = receiver._journal.read_bytes()
    router.hooks["/api/app-definitions/parse"] = lambda *_: httpx.Response(403, json={})
    with pytest.raises(m.MigrationError, match="authentication"):
        await receiver.acknowledge(owner_token=OWNER_TOKEN)
    router.hooks.clear()
    real = receiver._persist
    def fail(candidate):
        assert receiver.needs_attention and not candidate["needs_attention"]
        raise OSError("private token body")
    monkeypatch.setattr(receiver, "_persist", fail)
    with pytest.raises(m.MigrationError, match="Migration failed"):
        await receiver.acknowledge(owner_token=OWNER_TOKEN)
    assert receiver.needs_attention and not receiver.journal_status["acknowledged"]
    assert receiver._journal.read_bytes() == before and restarted(receiver, root).needs_attention
    monkeypatch.setattr(receiver, "_persist", real)
    await receiver.acknowledge(owner_token=OWNER_TOKEN)
    assert not receiver.needs_attention


@pytest.mark.parametrize("transition", ["acknowledgment", "completion"])
async def test_post_replace_fsync_failure_keeps_restart_gate(environment, monkeypatch, transition):
    receiver, router, selected, root, _ = environment
    sid = (await start(receiver, selected))["session_id"]
    await upload(receiver, sid)
    router.data_restored = True
    if transition == "acknowledgment":
        monkeypatch.setattr(m, "replace_app_trees", AsyncMock(side_effect=OSError()))
        await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
        await receiver._job
    real_fsync = os.fsync
    def fail_clear(fd):
        # Only fail directory fsync after the cleared candidate was renamed.
        if os.fstat(fd).st_ino == receiver.work.stat().st_ino:
            persisted = json.loads(receiver._journal.read_text())
            if persisted.get("needs_attention") is False:
                raise OSError("directory fsync failed after replacement")
        real_fsync(fd)
    monkeypatch.setattr(os, "fsync", fail_clear)
    if transition == "acknowledgment":
        with pytest.raises(m.MigrationError):
            await receiver.acknowledge(owner_token=OWNER_TOKEN)
    else:
        await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
        await receiver._job
        assert receiver.journal_status["phase"] == "incomplete"
    assert receiver.needs_attention and sid in receiver.journal_status["retained_sessions"]
    replacement = restarted(receiver, root)
    assert replacement.needs_attention and sid in replacement.journal_status["retained_sessions"]
    if transition == "completion":
        originals = list((receiver._stage / "trees").glob(".migration-old-*/alpha/old.db-wal"))
        assert len(originals) == 1 and originals[0].read_bytes() == b"stale"
    with pytest.raises(m.MigrationError, match="acknowledgment"):
        await start(replacement, selected)


@pytest.mark.parametrize("phase", ["receiving", "preflighting"])
async def test_start_drains_deferred_receiving_cleanup_across_cancellation(environment, monkeypatch, phase):
    receiver, _, selected, root, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    receiver._monitor.cancel()
    await m.heartbeat_cancelled(receiver._monitor)
    receiver._record["phase"] = phase
    receiver._save()
    lock.release(OpKind.MIGRATION)
    entered, release = threading.Event(), threading.Event()
    real = m.shutil.rmtree
    def blocked(path, *a, **kw):
        if path == receiver.work / sid:
            entered.set()
            assert release.wait(5)
        return real(path, *a, **kw)
    monkeypatch.setattr(m.shutil, "rmtree", blocked)
    replacement = restarted(receiver, root)
    assert not entered.is_set() and receiver._stage.exists()
    assert not replacement.needs_attention
    task = asyncio.create_task(start(replacement, selected))
    assert await asyncio.to_thread(entered.wait, 3)
    task.cancel()
    task.cancel()
    next_start = asyncio.create_task(start(replacement, selected))
    await asyncio.sleep(.02)
    assert not task.done() and not next_start.done()
    assert replacement.journal_status["session_id"] == sid
    assert not replacement.lock.busy
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    accepted = await next_start
    assert not receiver._stage.exists() and accepted["session_id"] != sid
    assert replacement._cleanup_job.done()
    await abort(replacement, accepted["session_id"])


@pytest.mark.parametrize("corruption", ["unknown", "nested-recovery", "nested-result", "invalid-phase", "invalid-flag", "invalid-references", "not-object", "invalid-json"])
async def test_restart_projects_private_or_malformed_journal_to_fixed_notice(environment, corruption):
    receiver, _, _, root, _ = environment
    sid, older = "a" * 64, "c" * 64
    secret = "PRIVATE-token-and-repository-body"
    record = {"version": 4, "session_id": sid, "ok": False, "phase": "finalizing",
              "needs_attention": True, "retained_sessions": [older], "result": None}
    if corruption == "unknown":
        record["bundle"] = {"token": secret}
    elif corruption == "nested-recovery":
        record["recovery"] = {"paused_apps": [{"name": secret, "restart": secret}], "tokens": {"token_hash": secret}}
    elif corruption == "nested-result":
        record["result"] = {"ok": True, "error": secret, "configuration": secret}
    elif corruption == "invalid-phase":
        record["phase"] = {"token": secret}
    elif corruption == "invalid-flag":
        record["needs_attention"] = secret
    elif corruption == "invalid-references":
        record["retained_sessions"] += ["../../" + secret, {"token": secret}]
    elif corruption == "not-object":
        record = [secret]
    receiver._journal.write_text(secret if corruption == "invalid-json" else json.dumps(record))
    retained = receiver.work / older
    retained.mkdir()
    (retained / "original").write_bytes(b"only-copy")
    replacement = restarted(receiver, root)
    public = replacement.journal_status
    assert public["result"] == {"ok": False, "error": "Migration journal requires manual inspection."}
    assert replacement.needs_attention and secret not in json.dumps(public)
    if corruption not in {"not-object", "invalid-json"}:
        assert set(public["retained_sessions"]) == {sid, older}
        assert secret not in json.dumps(await replacement.status(sid, owner_token=OWNER_TOKEN))
    await replacement.initialize()
    assert (retained / "original").read_bytes() == b"only-copy"


@pytest.mark.parametrize("phase", ["finalizing", "incomplete", "interrupted"])
async def test_legacy_finalization_journal_requires_attention(environment, phase):
    receiver, _, selected, root, _ = environment
    sid = "d" * 64
    receiver._journal.write_text(json.dumps({"version": 4, "session_id": sid, "ok": False, "phase": phase}))
    stage = receiver.work / sid
    stage.mkdir()
    (stage / "original").write_bytes(b"only-copy")
    replacement = restarted(receiver, root)
    with pytest.raises(m.MigrationError, match="acknowledgment"):
        await start(replacement, selected)
    await replacement.initialize()
    assert (stage / "original").read_bytes() == b"only-copy"
    assert replacement.needs_attention and replacement.journal_status["retained_sessions"] == [sid]


@pytest.mark.parametrize("duplicate", ['"phase":"finalizing","phase":"receiving"',
                                       '"phase":"receiving","needs_attention":true,"needs_attention":false',
                                       '"phase":"receiving","recovery":{"token":"private","token":"secret"}'])
async def test_duplicate_journal_keys_never_authorize_staging_cleanup(environment, duplicate):
    receiver, _, selected, root, _ = environment
    sid = "e" * 64
    receiver._journal.write_text('{"version":4,"ok":false,"session_id":"' + sid + '",' + duplicate + '}')
    stage = receiver.work / sid
    stage.mkdir()
    (stage / "only-original").write_bytes(b"original")
    replacement = restarted(receiver, root)
    assert replacement.needs_attention and replacement.journal_status["retained_sessions"] == [sid]
    await replacement.initialize()
    assert (stage / "only-original").read_bytes() == b"original"
    with pytest.raises(m.MigrationError, match="acknowledgment"):
        await start(replacement, selected)


@pytest.mark.parametrize("entries", [
    [("../escape", tarfile.REGTYPE, b"bad")],
    [("/absolute", tarfile.REGTYPE, b"bad")],
    [("sym", tarfile.SYMTYPE, "../../escape")],
    [("sym", tarfile.SYMTYPE, "/absolute")],
    [("hard", tarfile.LNKTYPE, "../../escape")],
    [("device", tarfile.CHRTYPE, None)],
    [("fifo", tarfile.FIFOTYPE, None)],
    [("sym", tarfile.SYMTYPE, "safe"), ("sym/file", tarfile.REGTYPE, b"bad")],
    [("sym/file", tarfile.REGTYPE, b"bad"), ("sym", tarfile.SYMTYPE, "safe")],
    [("same", tarfile.REGTYPE, b"one"), ("same", tarfile.REGTYPE, b"two")],
    [(".", tarfile.SYMTYPE, "safe")],
    [("a"*256, tarfile.REGTYPE, b"bad")],
    [("hard", tarfile.LNKTYPE, "sym"), ("sym", tarfile.SYMTYPE, "safe")],
    [("anchor", tarfile.SYMTYPE, "."), ("escape", tarfile.SYMTYPE, "anchor/../outside")],
    [("escape", tarfile.SYMTYPE, "anchor/../outside"), ("anchor", tarfile.SYMTYPE, ".")],
])
def test_archive_validation_rejects_unsafe_before_extraction(tmp_path, entries):
    archive = tmp_path / "archive.tar.gz"
    archive.write_bytes(tar_bytes([("valid", tarfile.REGTYPE, b"yes"), *entries]))
    destination = tmp_path / "staged"
    with pytest.raises((data.DataError, tarfile.TarError)):
        data.extract_archive(archive, destination)
    assert not destination.exists()


def test_real_archive_preserves_numeric_owners_modes_and_links(tmp_path):
    source = tmp_path / "source"
    source.mkdir(mode=0o750)
    (source / "data").write_bytes(b"content")
    (source / "data").chmod(0o640)
    (source / "link").symlink_to("data")
    os.link(source / "data", source / "hard")
    archive = tmp_path / "archive.tar.gz"
    size, digest = data.build_archive(source, archive)
    assert size == archive.stat().st_size and digest == hashlib.sha256(archive.read_bytes()).hexdigest()
    destination = tmp_path / "destination"
    data.extract_archive(archive, destination)
    assert (destination / "link").is_symlink()
    assert (destination / "link").read_bytes() == b"content"
    assert (destination / "data").stat().st_ino == (destination / "hard").stat().st_ino
    for relative in (".", "data", "link"):
        actual, expected = (destination / relative).lstat(), (source / relative).lstat()
        assert actual.st_uid == expected.st_uid and actual.st_gid == expected.st_gid
        assert actual.st_mode & 0o777 == expected.st_mode & 0o777


def test_unreadable_directory_during_a_flush_is_reported_not_skipped(tmp_path):
    """os.walk ignores scan errors by default, which would report as flushed."""
    root = tmp_path / "root"
    (root / "unreadable").mkdir(parents=True)
    (root / "unreadable" / "file").write_bytes(b"data")
    os.chmod(root / "unreadable", 0o000)
    try:
        # Reported, never counted as flushed: os.walk ignores scan errors by
        # default, which would let a whole subtree pass unreported.
        with pytest.raises((data.DataError, PermissionError)):
            data.flush_tree(root)
    finally:
        os.chmod(root / "unreadable", 0o700)
    # root, unreadable, file, and root again after the walk descends.
    assert data.flush_tree(root) == 4


def test_private_archived_modes_survive_extraction_without_being_reopened(tmp_path, monkeypatch):
    """Real app data is routinely 0000/0500; a barrier must not need to read it."""
    # Built by hand: archiving such a tree needs rights the receiving app does
    # not have, and the receive side must tolerate it either way.
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as tar:
        for name, kind, value, mode in [
            ("app", tarfile.DIRTYPE, None, 0o755),
            ("app/locked", tarfile.DIRTYPE, None, 0o000),
            ("app/locked/db", tarfile.REGTYPE, b"secret", 0o000),
            ("app/readonly", tarfile.DIRTYPE, None, 0o500),
            ("app/readonly/key", tarfile.REGTYPE, b"key", 0o400),
        ]:
            entry = tarfile.TarInfo(name)
            entry.type, entry.mode, entry.size = kind, mode, len(value or b"")
            entry.uid, entry.gid = os.geteuid(), os.getegid()
            entry.uname = entry.gname = ""
            tar.addfile(entry, io.BytesIO(value) if value is not None else None)
    archive = tmp_path / "archive.tar.gz"
    archive.write_bytes(stream.getvalue())
    destination = tmp_path / "destination"
    walks = []
    real_walk = os.walk

    def tracking_walk(root, **kwargs):
        walks.append(str(root))
        return real_walk(root, **kwargs)

    monkeypatch.setattr(data.os, "walk", tracking_walk)
    data.extract_archive(archive, destination)
    restored = destination / "app"
    assert (restored / "readonly" / "key").stat().st_mode & 0o777 == 0o400
    assert (restored / "readonly").stat().st_mode & 0o777 == 0o500
    # A 0000 tree is extracted and reported on without ever being reopened.
    assert (restored / "locked").stat().st_mode & 0o777 == 0
    # The tree is walked exactly once, while every entry is still reachable. The
    # barrier applied after the archived modes are restored covers only the
    # destination's own entries, so private data cannot fail closed on reopen.
    assert walks == [str(destination)]
    os.chmod(restored / "locked", 0o700)
    assert (restored / "locked" / "db").stat().st_mode & 0o777 == 0
    os.chmod(restored / "locked" / "db", 0o600)
    assert (restored / "locked" / "db").read_bytes() == b"secret"


def test_missing_source_has_explicit_empty_archive(tmp_path):
    archive = tmp_path / "empty.tar.gz"
    data.build_archive(tmp_path / "missing", archive)
    destination = tmp_path / "destination"
    data.extract_archive(archive, destination)
    assert destination.is_dir() and list(destination.iterdir()) == []


@pytest.mark.parametrize("bad_root", ["source-symlink", "target-symlink", "source-file", "target-file", "missing-source"])
async def test_replace_rejects_invalid_roots_without_changes(tmp_path, bad_root):
    stage, target = tmp_path / "stage", tmp_path / "target"
    stage.mkdir()
    target.mkdir()
    source, dest = stage / "alpha", target / "alpha"
    source.mkdir()
    dest.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    if bad_root.startswith("source") or bad_root == "missing-source":
        source.rmdir()
        if bad_root == "source-symlink":
            source.symlink_to(outside)
        elif bad_root == "source-file":
            source.write_text("bad")
    else:
        dest.rmdir()
        if bad_root == "target-symlink":
            dest.symlink_to(outside)
        else:
            dest.write_text("bad")
    with pytest.raises(data.DataError):
        await data.replace_app_trees(stage, target, ["alpha"])
    assert list(outside.iterdir()) == []


async def test_replace_rolls_back_promotion_failure(tmp_path, monkeypatch):
    stage, target = tmp_path / "stage", tmp_path / "target"
    for root in (stage, target):
        root.mkdir()
        for name in ("alpha", "beta"):
            (root / name).mkdir()
            (root / name / "file").write_text(root.name)
    real = Path.rename
    def rename(path, destination):
        if path == stage / "beta":
            raise OSError("injected atomic promotion failure")
        return real(path, destination)
    monkeypatch.setattr(Path, "rename", rename)
    with pytest.raises(data.DataError):
        await data.replace_app_trees(stage, target, ["alpha", "beta"])
    assert all((target / name / "file").read_text() == "target" for name in ("alpha", "beta"))
    assert all((stage / name / "file").read_text() == "stage" for name in ("alpha", "beta"))


async def test_multiroot_failure_retains_first_roots_originals(tmp_path, monkeypatch):
    roots = [tmp_path / name for name in ("stage", "target", "temp-stage", "temp-target")]
    for root in roots:
        (root / "alpha").mkdir(parents=True)
        (root / "alpha" / "file").write_text(root.name)
    stage, target, temp_stage, temp_target = roots
    rollback = await data.replace_app_trees(stage, target, ["alpha"])
    rename = Path.rename
    def fail(path, destination):
        if path == temp_stage / "alpha":
            raise OSError("second root failed")
        return rename(path, destination)
    monkeypatch.setattr(Path, "rename", fail)
    with pytest.raises(data.DataError):
        await data.replace_app_trees(temp_stage, temp_target, ["alpha"])
    assert rollback.parent == stage
    assert (rollback / "alpha" / "file").read_text() == "target"
    (target / "alpha").rename(stage / "alpha")
    (rollback / "alpha").rename(target / "alpha")
    assert (target / "alpha" / "file").read_text() == "target"


@pytest.mark.parametrize("success", [False, True])
async def test_receiver_originals_survive_activation_until_confirmed_cleanup(environment, monkeypatch, success):
    receiver, router, selected, root, _ = environment
    sid = (await start(receiver, selected))["session_id"]
    await upload(receiver, sid)
    originals = []
    def activate(*_):
        originals.extend((receiver._stage / "trees").glob(".migration-old-*/alpha/old.db-wal"))
        assert len(originals) == 1 and originals[0].read_bytes() == b"stale"
        if not success:
            return httpx.Response(403, json={})
        router.data_restored = True
    router.hooks["/api/app-definitions/import-private"] = activate
    await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    await receiver._job
    assert receiver.needs_attention is not success
    assert originals[0].exists() is not success
    if not success:
        assert restarted(receiver, root).needs_attention


async def test_replaced_data_is_flushed_with_an_error_reporting_barrier(tmp_path, monkeypatch):
    """os.sync() cannot report writeback errors, so data must be fsynced."""
    archive = tmp_path / "archive.gz"
    archive.write_bytes(tar_bytes([("file", tarfile.REGTYPE, b"new")]))
    flushed = []
    real_fsync = os.fsync

    def tracking(descriptor):
        link = f"/proc/self/fd/{descriptor}"
        flushed.append(os.readlink(link) if Path(link).is_symlink() else f"fd:{descriptor}")
        return real_fsync(descriptor)

    monkeypatch.setattr(data.os, "fsync", tracking)
    destination = tmp_path / "extracted"
    data.extract_archive(archive, destination)
    assert (destination / "file").read_bytes() == b"new"
    assert any(entry.endswith("/file") for entry in flushed), flushed

    # A writeback failure while flushing restored data must fail recovery
    # closed instead of passing silently like os.sync() would.
    monkeypatch.undo()
    monkeypatch.setattr(data.os, "fsync", lambda descriptor: (_ for _ in ()).throw(OSError("synthetic writeback")))
    with pytest.raises(OSError):
        data.extract_archive(archive, tmp_path / "unflushed")
    staged, target = tmp_path / "staged", tmp_path / "target"
    staged.mkdir()
    (staged / "file").write_bytes(b"new")
    target.mkdir()
    with pytest.raises(data.DataError):
        await data.replace_app_trees(staged, target, ["file"])
    assert not (target / "file").exists()
    assert (staged / "file").read_bytes() == b"new"


async def test_promotion_flushes_only_the_directories_whose_entries_changed(tmp_path, monkeypatch):
    """A rename is made durable by its parent, not by reopening archived trees."""
    flushed = []
    real_fsync = os.fsync

    def tracking(descriptor):
        link = f"/proc/self/fd/{descriptor}"
        flushed.append(os.readlink(link) if Path(link).is_symlink() else f"fd:{descriptor}")
        return real_fsync(descriptor)

    stage, target = tmp_path / "stage", tmp_path / "target"
    stage.mkdir()
    staged_app = stage / "alpha"
    staged_app.mkdir()
    (staged_app / "nested").mkdir()
    (staged_app / "nested" / "database").write_bytes(b"data")
    (staged_app / "locked").mkdir(mode=0o000)
    target.mkdir()
    monkeypatch.setattr(data.os, "fsync", tracking)
    rollback = await data.replace_app_trees(stage, target, ["alpha"])
    # Only the directories whose entries the renames changed are reopened. The
    # promoted tree keeps its archived modes, so walking it would rely on
    # os.walk's silent skip of unreadable directories.
    assert {entry for entry in flushed if entry.startswith("/")} == {str(stage), str(target), str(rollback)}
    assert (target / "alpha" / "nested" / "database").read_bytes() == b"data"
    monkeypatch.undo()
    monkeypatch.setattr(data.os, "fsync", lambda descriptor: (_ for _ in ()).throw(OSError("synthetic writeback")))
    (stage / "beta").mkdir()
    with pytest.raises(data.DataError):
        await data.replace_app_trees(stage, target, ["beta"])
    assert (target / "alpha" / "nested" / "database").read_bytes() == b"data"


async def test_extraction_and_promotion_have_durability_barriers(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(data.os, "sync", lambda: calls.append(True))
    archive, stage, target = tmp_path / "archive.gz", tmp_path / "stage", tmp_path / "target"
    archive.write_bytes(tar_bytes([("file", tarfile.REGTYPE, b"new")]))
    stage.mkdir()
    target.mkdir()
    data.extract_archive(archive, stage / "alpha")
    assert calls
    calls.clear()
    rollback = await data.replace_app_trees(stage, target, ["alpha"])
    assert len(calls) == 2 and rollback.exists()
    await data.discard_app_trees(rollback)
    assert not rollback.exists() and len(calls) == 3


async def test_postcommit_disposal_failure_keeps_original_reference_without_failing_recovery(environment, monkeypatch):
    receiver, router, selected, root, _ = environment
    sid = (await start(receiver, selected))["session_id"]
    await upload(receiver, sid)
    router.data_restored = True
    async def fail(rollback):
        committed = json.loads(receiver._journal.read_text())
        assert committed["phase"] == "complete" and not committed["needs_attention"]
        assert sid in committed["retained_sessions"]
        assert (rollback / "alpha" / "old.db-wal").read_bytes() == b"stale"
        raise OSError("garbage collection failed")
    monkeypatch.setattr(m, "discard_app_trees", fail)
    await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    await receiver._job
    assert receiver.journal_status["phase"] == "complete" and not receiver.needs_attention
    assert sid in restarted(receiver, root).journal_status["retained_sessions"]
    originals = list((receiver._stage / "trees").glob(".migration-old-*/alpha/old.db-wal"))
    assert len(originals) == 1 and originals[0].read_bytes() == b"stale"


@pytest.mark.parametrize("point", ["extraction", "promotion", "completion"])
async def test_durability_failure_prevents_receiver_completion(environment, monkeypatch, point):
    receiver, router, selected, root, _ = environment
    sid = (await start(receiver, selected))["session_id"]
    def fail():
        raise OSError("durability failed")
    if point == "extraction":
        monkeypatch.setattr(data.os, "sync", fail)
        with pytest.raises(m.MigrationError):
            await upload(receiver, sid)
    else:
        await upload(receiver, sid)
        if point == "promotion":
            monkeypatch.setattr(data.os, "sync", fail)
        else:
            router.data_restored = True
            monkeypatch.setattr(m, "durability_barrier", fail)
        await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
        await receiver._job
        assert receiver.needs_attention and restarted(receiver, root).needs_attention
    assert receiver.journal_status["phase"] != "complete"


def test_truncated_gzip_trailer_rejected_before_extract(tmp_path):
    archive = tmp_path / "bad.gz"
    archive.write_bytes(tar_bytes([("file", tarfile.REGTYPE, b"content")])[:-4])
    destination = tmp_path / "destination"
    with pytest.raises(EOFError):
        data.extract_archive(archive, destination)
    assert not destination.exists()


@pytest.mark.parametrize("kind", [tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK])
def test_oversized_archive_metadata_rejected_before_allocation(tmp_path, kind):
    header = tarfile.TarInfo("metadata")
    header.type, header.size = kind, 1024**3
    archive = tmp_path / "archive.gz"
    archive.write_bytes(gzip.compress(header.tobuf()))
    with pytest.raises(data.DataError):
        data.extract_archive(archive, tmp_path / "destination")
    assert not (tmp_path / "destination").exists()


def test_metadata_failures_are_not_silent(tmp_path, monkeypatch):
    archive = tmp_path / "archive.gz"
    archive.write_bytes(tar_bytes([("file", tarfile.REGTYPE, b"content")]))
    def denied(*_):
        raise tarfile.ExtractError("ownership denied")
    monkeypatch.setattr(tarfile.TarFile, "chown", denied)
    with pytest.raises(tarfile.ExtractError):
        data.extract_archive(archive, tmp_path / "destination")


def test_bounded_io_deadline_checked_inside_large_member(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "large").write_bytes(b"x" * 100000)
    ticks = iter([0, 0, 0, 901] + [901] * 100)
    monkeypatch.setattr(data.time, "monotonic", lambda: next(ticks))
    with pytest.raises(data.DataError):
        data.build_archive(source, tmp_path / "archive.gz")


async def test_finalize_journal_failure_is_retryable_before_mutation(environment, monkeypatch):
    receiver, router, selected, root, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    await upload(receiver, sid)
    real = receiver._save
    def fail():
        raise OSError("disk-full")
    monkeypatch.setattr(receiver, "_save", fail)
    with pytest.raises(m.MigrationError):
        await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    assert receiver._job is None and receiver.journal_status["phase"] == "receiving"
    assert router.apps["alpha"]["status"] == "running" and lock.busy
    monkeypatch.setattr(receiver, "_save", real)
    await abort(receiver, sid)
    assert not lock.busy


async def test_cleanup_failure_cannot_reopen_or_finalize_session(environment, monkeypatch):
    receiver, router, selected, root, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    real = m.shutil.rmtree
    def fail(*_):
        raise OSError("cleanup failed")
    monkeypatch.setattr(m.shutil, "rmtree", fail)
    await abort(receiver, sid)
    assert not lock.busy and receiver.journal_status["phase"] == "incomplete"
    assert receiver.journal_status["result"]["ok"] is False
    response = await receiver.finalize({"version": 4, "session_id": sid}, owner_token=OWNER_TOKEN)
    assert response["result"]["ok"] is False and receiver._job is None
    assert not receiver.needs_attention and receiver.journal_status["retained_sessions"] == [sid]
    assert not router.mutations()
    monkeypatch.setattr(m.shutil, "rmtree", real)
    replacement = restarted(receiver, root)
    assert not replacement.needs_attention and replacement.journal_status["retained_sessions"] == [sid]
    retry = await start(replacement, selected)
    assert retry["retained_sessions"] == [sid] and receiver._stage.exists()
    await abort(replacement, retry["session_id"])


async def test_cross_device_promotion_fails_before_moving_old(tmp_path, monkeypatch):
    stage, target = tmp_path / "stage", tmp_path / "target"
    stage.mkdir()
    target.mkdir()
    (stage / "alpha").mkdir()
    (target / "alpha").mkdir()
    (target / "alpha" / "old").write_bytes(b"old")
    real = Path.stat
    def stat(path, **kwargs):
        result = real(path, **kwargs)
        if path == stage / "alpha":
            values = list(result)
            values[2] += 1
            return os.stat_result(values)
        return result
    monkeypatch.setattr(Path, "stat", stat)
    with pytest.raises(data.DataError):
        await data.replace_app_trees(stage, target, ["alpha"])
    assert (target / "alpha" / "old").read_bytes() == b"old"


async def test_source_cancellation_drains_tar_before_restarting_and_releasing(tmp_path, monkeypatch):
    root = make_root(tmp_path / "source")
    full = bundle()
    entered, release = threading.Event(), threading.Event()
    cleaned = []
    class Source:
        progress = {"destination_apps_before": [{"name": n, "app_id": str(i + 1) * 12, "status": "running"} for i, n in enumerate(("alpha", "beta", "backup"))]}
        async def preflight(self):
            pass
        async def stop_apps(self):
            pass
        async def restart_unaffected(self):
            cleaned.append(True)
            return {"paused_apps": []}
    source = Source()
    monkeypatch.setattr(m, "RecoverySession", lambda *_: source)
    monkeypatch.setattr(m, "capture_configuration", AsyncMock(return_value=full))
    async def request(self, method, path, **kwargs):
        if path.endswith("capabilities"):
            return {"ok": True, "version": 4, "chunk_limit": m.CHUNK_LIMIT, "backup_app_name": "backup"}
        if path.endswith("start"):
            return {"ok": True, "version": 4, "session_id": "d"*64, "accepted_apps": ["alpha"]}
        return {"ok": True, "version": 4}
    monkeypatch.setattr(m._Peer, "request", request)
    def build(*_):
        entered.set()
        assert release.wait(5)
        raise OSError("cancelled tar")
    monkeypatch.setattr(m, "build_archive", build)
    lock = OperationLock()
    task = asyncio.create_task(m.run_direct_push(target_url="https://destination.test", target_token=OWNER_TOKEN, selected_apps=["alpha"], lock=lock, all_app_data=root, work_dir=root / "backup" / "work", router_url="https://source.test", app_token="source-app-token", owner_token=OWNER_TOKEN))
    assert await asyncio.to_thread(entered.wait, 3)
    task.cancel()
    await asyncio.sleep(.02)
    assert lock.busy and not cleaned and not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleaned and not lock.busy


@pytest.mark.parametrize("fault", [None, "differing-names", "executor-collision", "implicit-executor-collision", "ordinary-collision", "legacy", "float-version", "invalid-capability", "destination-auth", "missing-receipt", "invalid-receipt", "float-receipt-bytes", "invalid-index", "lost-finalize", "lost-upload", "pending-verification", "verification-timeout", "overall-verification-timeout", "activation-failure", "source-tar-failure", "source-cleanup-failure", "source-restart-failure", "no-persistent-data"])
async def test_source_stateful_http_protocol_cutover_and_failures(tmp_path, monkeypatch, mock_http, fault):
    different = fault in {"differing-names", "executor-collision", "implicit-executor-collision"}
    executor = "receiver-backup" if different else "backup"
    names = ["alpha", "beta", "backup"] + ([executor] if different else [])
    full = bundle(names)
    source_router = Router(full, [inventory_entry(n, "NPQR"[i] * 12) for i, n in enumerate(names)])
    if fault == "source-restart-failure":
        source_router.hooks[f"/reload_app/{source_router.apps['beta']['app_id']}"] = lambda *_: httpx.Response(403, json={})
    destination_router = Router(full, inventory(["beta", executor] + (["alpha"] if fault == "ordinary-collision" else [])))
    source_root = make_root(tmp_path / "source")
    destination_root = make_root(tmp_path / "destination")
    if different:
        (destination_root / executor).mkdir()
    (source_root / "alpha").mkdir()
    (source_root / "alpha" / "database").write_bytes(b"live data")
    if fault == "no-persistent-data":
        (source_root / "alpha" / "database").unlink()
        (source_root / "alpha").rmdir()
        (destination_root / "alpha").mkdir()
        (destination_root / "alpha" / "stale-wal").write_bytes(b"stale")
    receiver = m.MigrationReceiver(lock=OperationLock(), all_app_data=destination_root, work_dir=destination_root / executor / "work", router_url="https://destination.test", backup_app_name=executor)
    original = RecoverySession
    monkeypatch.setattr(m, "RecoverySession", lambda *a, **kw: original(*a, **kw, poll_interval=.001, deployment_timeout=.3))
    exported = []
    if fault == "activation-failure":
        destination_router.deploy_states["alpha"] = ["error"]
    if fault == "source-tar-failure":
        monkeypatch.setattr(m, "build_archive", lambda *_: (_ for _ in ()).throw(OSError("secret")))
    if fault == "source-cleanup-failure":
        real = m.shutil.rmtree
        def cleanup(path, *args, **kwargs):
            if Path(path).name.startswith("migration-source-"):
                raise OSError("private filesystem detail")
            return real(path, *args, **kwargs)
        monkeypatch.setattr(m.shutil, "rmtree", cleanup)
    if fault == "destination-auth":
        destination_router.hooks["/api/app-definitions/parse"] = lambda *_: httpx.Response(403, json={})
    def restored(*_):
        if fault == "no-persistent-data":
            assert list((destination_root / "alpha").iterdir()) == []
        else:
            assert (destination_root / "alpha" / "database").read_bytes() == b"live data"
        destination_router.data_restored = True
    destination_router.hooks["/api/app-definitions/import-private"] = restored
    events = []
    verification_until = None
    verification_polls = 0
    # overall-verification-timeout deliberately keeps the real verification
    # window, so the governing deadline is the enclosing whole-transfer one.
    if fault in {"pending-verification", "verification-timeout", "missing-receipt"}:
        monkeypatch.setattr(m, "VERIFICATION_TIMEOUT", .3)
    if fault == "pending-verification":
        monkeypatch.setattr(m, "PEER_REQUEST_TIMEOUT", .1)
        monkeypatch.setattr(m, "VERIFICATION_TIMEOUT", 1.0)
    async def http(request):
        nonlocal verification_until, verification_polls
        assert source_router.apps["backup"]["status"] == "running"
        assert destination_router.apps[executor]["status"] == "running"
        path = request.url.path
        events.append((str(request.url.host), path))
        if request.url.host == "source.test":
            if path.endswith("/definitions/export"):
                assert request.headers["Authorization"] == "Bearer source-app-token"
                assert all(a["status"] == "running" for a in source_router.apps.values())
                exported.append(True)
                return httpx.Response(200, json=full["definitions"])
            return await source_router(request)
        if request.url.host == "destination.test":
            return await destination_router(request)
        assert request.url.host == "backup.destination.test"
        assert request.headers["Authorization"] == f"Bearer {OWNER_TOKEN}"
        body = json.loads(request.content) if request.headers.get("content-type") == "application/json" else None
        try:
            if path.endswith("/capabilities"):
                if fault == "legacy":
                    return httpx.Response(200, json={"ok": True, "version": 3})
                result = await receiver.capabilities(owner_token=OWNER_TOKEN)
                if fault == "float-version":
                    result["version"] = 4.0
                if fault == "invalid-capability":
                    result["chunk_limit"] = float(result["chunk_limit"])
            elif path.endswith("/start"):
                assert source_router.apps["alpha"]["status"] == "running"
                result = await receiver.start(body, owner_token=OWNER_TOKEN)
            elif "/chunk/" in path:
                assert source_router.apps["alpha"]["status"] == "stopped"
                assert source_router.apps["beta"]["status"] == "stopped"
                if different:
                    assert source_router.apps[executor]["status"] == "stopped"
                sid, name = path.rsplit("/", 2)[1:]
                if fault in {"pending-verification", "verification-timeout", "overall-verification-timeout"}:
                    verification_until = asyncio.get_running_loop().time() + .4
                result = await receiver.upload(sid, name, chunks(request.content), index=int(request.headers["X-Chunk-Index"]), final=request.headers["X-Chunk-Final"] == "1", archive_bytes=int(request.headers["X-Archive-Bytes"]), archive_sha256=request.headers["X-Archive-SHA256"], owner_token=OWNER_TOKEN)
                if fault == "missing-receipt":
                    result["receipt"] = None
                if fault in {"pending-verification", "verification-timeout", "overall-verification-timeout"}:
                    if fault == "pending-verification":
                        # Lose the final response at the actual peer deadline;
                        # status continues to report accepted but pending.
                            await asyncio.sleep(.6)
                    else:
                        result["receipt"] = None
                if fault == "invalid-receipt":
                    result["receipt"]["complete"] = 1
                if fault == "float-receipt-bytes":
                    result["receipt"]["bytes"] = float(result["receipt"]["bytes"])
                if fault == "invalid-index":
                    result["next_index"] = True
                if fault == "lost-upload":
                    raise httpx.ReadError("receipt response lost")
            elif path.endswith("/finalize"):
                result = await receiver.finalize(body, owner_token=OWNER_TOKEN)
                if fault == "lost-finalize":
                    raise httpx.ReadError("response lost")
            elif "/status/" in path:
                result = await receiver.status(path.rsplit("/", 1)[-1], owner_token=OWNER_TOKEN)
                if fault in {"verification-timeout", "overall-verification-timeout", "missing-receipt"} or (
                        fault == "pending-verification" and asyncio.get_running_loop().time() < verification_until):
                    result["receipts"] = {}
                    verification_polls += 1
            elif path.endswith("/abort"):
                result = await receiver.abort(body, owner_token=OWNER_TOKEN)
            elif path.endswith("/keepalive"):
                result = await receiver.keepalive(body, owner_token=OWNER_TOKEN)
            else:
                pytest.fail(path)
            return httpx.Response(200, json=result)
        except m.MigrationError as error:
            return httpx.Response(error.status_code, json={"ok": False, "version": 4, "error": str(error)})
    mock_http(http)
    lock = OperationLock()
    selection = None if fault == "implicit-executor-collision" else (["alpha", executor] if fault == "executor-collision" else ["alpha"])
    began = asyncio.get_running_loop().time()
    result = await m.run_direct_push(target_url="https://destination.test", target_token=OWNER_TOKEN, selected_apps=selection, lock=lock, all_app_data=source_root, work_dir=source_root / "backup" / "work", router_url="https://source.test", app_token="source-app-token", owner_token=OWNER_TOKEN, poll_interval=.001, deadline=.5 if fault == "overall-verification-timeout" else 5)
    elapsed = asyncio.get_running_loop().time() - began
    assert result == (fault in {None, "differing-names", "ordinary-collision", "lost-finalize", "lost-upload", "pending-verification", "no-persistent-data"}), (m.status, events)
    if fault in {"pending-verification", "verification-timeout", "overall-verification-timeout"}:
        assert verification_polls > 1
        if fault == "pending-verification":
            assert elapsed > .4 > m.PEER_REQUEST_TIMEOUT
        else:
            assert elapsed < 1
            assert any(path.endswith("/abort") for _, path in events)
    assert not lock.busy
    assert source_router.apps["beta"]["status"] == ("stopped" if fault == "source-restart-failure" else "running")
    recovery_record = m.SourceRecoveryRecord(lock=OperationLock(), all_app_data=source_root,
                                            work_dir=source_root / "backup" / "work", router_url="https://source.test")
    assert recovery_record.needs_attention == m.source_recovery.needs_attention
    if result:
        assert not recovery_record.needs_attention and not recovery_record.journal_status["restart_pending"]
    elif source_router.apps["alpha"]["status"] == "stopped":
        assert recovery_record.needs_attention
    if fault == "source-restart-failure":
        assert recovery_record.journal_status["restart_pending"] == ["beta"]
    if fault in {"legacy", "float-version", "invalid-capability", "destination-auth"}:
        assert not exported
        assert source_router.apps["alpha"]["status"] == "running"
    elif fault in {"executor-collision", "implicit-executor-collision"}:
        assert not source_router.mutations() and not destination_router.mutations()
        assert "conflicts" in m.status["error"]
        assert not any(path.endswith("/start") for _, path in events)
    else:
        assert source_router.apps["alpha"]["status"] == "stopped"
    if fault in {"float-receipt-bytes", "invalid-receipt", "missing-receipt", "invalid-index"}:
        assert not any(path.endswith("/finalize") for _, path in events)
    if result:
        assert destination_router.apps["alpha"]["status"] == "running"
        assert len([e for e in destination_router.events if e[1] == "/api/add_app"]) == (0 if fault == "ordinary-collision" else 1)
        assert json.loads(destination_router.imported_content)["platform_api_tokens"] == full["definitions"]["platform_api_tokens"]
    if receiver._job:
        await receiver._job
    assert not receiver.lock.busy
    assert source_router.apps["backup"]["status"] == "running"
    if different:
        assert source_router.apps[executor]["status"] == "running"


def test_source_process_crash_after_first_stop_leaves_durable_intent(tmp_path, monkeypatch):
    root = make_root(tmp_path / "source")
    before = [{"name": n, "app_id": "NPQ"[i] * 12, "status": "running"}
              for i, n in enumerate(("alpha", "beta", "backup"))]
    class Source:
        progress = {"destination_apps_before": before}
        async def preflight(self):
            pass
        async def stop_apps(self):
            # First fake router stop takes effect, then the entire process dies:
            # no source cleanup/finally blocks are allowed to repair the journal.
            (root / "first-stop").write_text("alpha")
            os._exit(73)
        async def restart_unaffected(self):
            raise AssertionError("crash must bypass cleanup")
    async def peer(self, method, path, **kwargs):
        if path.endswith("capabilities"):
            return {"ok": True, "chunk_limit": m.CHUNK_LIMIT, "backup_app_name": "backup"}
        if path.endswith("start"):
            return {"ok": True, "session_id": "d" * 64, "accepted_apps": ["alpha"]}
        return {"ok": True}
    monkeypatch.setattr(m, "RecoverySession", lambda *_: Source())
    monkeypatch.setattr(m, "capture_configuration", AsyncMock(return_value=bundle()))
    monkeypatch.setattr(m._Peer, "request", peer)
    monkeypatch.setattr(m, "source_recovery", None)
    def child():
        asyncio.run(m.run_direct_push(target_url="https://destination.test", target_token=OWNER_TOKEN,
                    selected_apps=["alpha"], lock=OperationLock(), all_app_data=root,
                    work_dir=root / "backup" / "work", router_url="https://source.test",
                    app_token="app-token", owner_token=OWNER_TOKEN))
    process = multiprocessing.get_context("fork").Process(target=child)
    process.start()
    process.join(5)
    if process.is_alive():
        process.kill()
        process.join()
    assert process.exitcode == 73 and (root / "first-stop").read_text() == "alpha"
    record = m.initialize_source_recovery(lock=OperationLock(), all_app_data=root,
             work_dir=root / "backup" / "work", router_url="https://source.test")
    assert record.needs_attention and not record.journal_status["acknowledged"]
    assert record.journal_status["session_id"] == "d" * 64
    assert record.journal_status["apps_before"] == before
    assert record.journal_status["selected_apps"] == ["alpha"]
    assert record.journal_status["restart_pending"] == ["beta"]


@pytest.mark.parametrize("failure", ["write", "directory-fsync"])
async def test_source_acknowledgment_authority_live_gate_and_durable_publication(environment, monkeypatch, failure):
    _, router, _, root, lock = environment
    record = m.SourceRecoveryRecord(lock=lock, all_app_data=root, work_dir=root / "backup" / "work",
                                    router_url="https://destination.test")
    record.begin("e" * 64, {"alpha"}, [{"name": n, "app_id": "NPQ"[i] * 12, "status": "running"}
                                     for i, n in enumerate(router.apps)], "backup")
    before = copy.deepcopy(router.apps)
    router.hooks["/api/app-definitions/parse"] = lambda *_: httpx.Response(403, json={})
    with pytest.raises(m.MigrationError, match="authentication"):
        await record.acknowledge(owner_token=OWNER_TOKEN)
    router.hooks.clear()
    record.mark_live(True)
    with pytest.raises(m.MigrationError, match="active"):
        await record.acknowledge(owner_token=OWNER_TOKEN)
    record.mark_live(False)
    lock.try_acquire(OpKind.MIGRATION)
    with pytest.raises(m.MigrationError, match="active"):
        await record.acknowledge(owner_token=OWNER_TOKEN)
    lock.release(OpKind.MIGRATION)
    persist, fsync = record._persist, os.fsync
    def failed_write(*_):
        raise OSError("failed")
    def failed_sync(fd):
        if os.fstat(fd).st_ino == record.work.stat().st_ino and not json.loads(record._journal.read_text())["needs_attention"]:
            raise OSError("failed after publication")
        fsync(fd)
    if failure == "write":
        monkeypatch.setattr(record, "_persist", failed_write)
    else:
        monkeypatch.setattr(os, "fsync", failed_sync)
    with pytest.raises(m.MigrationError, match="Migration failed"):
        await record.acknowledge(owner_token=OWNER_TOKEN)
    assert record.needs_attention
    fresh = m.SourceRecoveryRecord(lock=lock, all_app_data=root, work_dir=root / "backup" / "work",
                                  router_url="https://destination.test")
    assert fresh.needs_attention and not fresh.journal_status["acknowledged"]
    monkeypatch.setattr(record, "_persist", persist)
    monkeypatch.setattr(os, "fsync", fsync)
    await record.acknowledge(owner_token=OWNER_TOKEN)
    fresh = m.SourceRecoveryRecord(lock=lock, all_app_data=root, work_dir=root / "backup" / "work",
                                  router_url="https://destination.test")
    assert not fresh.needs_attention and fresh.journal_status["acknowledged"]
    assert router.apps == before


def test_private_work_dir_rejects_foreign_and_shared_directories(tmp_path, monkeypatch):
    root = tmp_path / "app_data"
    executor = root / "backup"
    executor.mkdir(parents=True)
    work = executor / ".migration"

    # A pre-existing private directory we own is reused, and a permissive
    # directory is tightened because it holds private migration state.
    work.mkdir(mode=0o700)
    work.chmod(0o755)
    assert data.private_work_dir(root, work, "backup") == work
    assert stat.S_IMODE(work.stat().st_mode) == 0o700

    class Foreign:
        st_mode = stat.S_IFDIR | 0o700
        st_uid = os.geteuid() + 1

    monkeypatch.setattr(Path, "lstat", lambda self: Foreign())
    with pytest.raises(data.DataError):
        data.private_work_dir(root, work, "backup")
    monkeypatch.undo()
    # A missing executor directory is a data problem, not a raw OS error.
    with pytest.raises(data.DataError):
        data.private_work_dir(root / "absent", work, "backup")


def test_a_failed_journal_publish_leaves_no_temporary_file(tmp_path):
    # A publish that cannot replace its target rolls back to the previous inode;
    # the temporary it wrote must not survive to accumulate in the work
    # directory, and the caller must still see the failure.
    work = tmp_path / "work"
    work.mkdir(mode=0o700)
    journal = work / "migration-journal.json"
    journal.write_text('{"phase": "complete"}')

    def refuse_publish_only(source, destination):
        # Only the publish itself fails; the rollback must still be able to put
        # the previous inode back.
        if source.name == "migration-journal.tmp":
            raise OSError("rename refused")
        return real(source, destination)

    real = Path.replace
    with unittest.mock.patch.object(Path, "replace", refuse_publish_only):
        with pytest.raises(OSError):
            m._persist_journal(work, journal, {"phase": "interrupted"})
    assert json.loads(journal.read_text()) == {"phase": "complete"}
    assert not (work / "migration-journal.tmp").exists()


def test_a_failed_rollback_still_reports_the_publish_failure(tmp_path, caplog):
    # If even the rollback rename fails, the caller must still learn that the
    # publish failed rather than seeing only the rollback error, and the
    # surviving link must be visible in the log rather than silent.
    work = tmp_path / "work"
    work.mkdir(mode=0o700)
    journal = work / "migration-journal.json"
    journal.write_text('{"phase": "complete"}')
    real = Path.replace

    def refuse(source, destination):
        if str(destination) == str(journal):
            raise OSError("rename refused")
        return real(source, destination)

    with unittest.mock.patch.object(Path, "replace", refuse):
        with pytest.raises(OSError, match="rename refused"):
            m._persist_journal(work, journal, {"phase": "interrupted"})
    # The previous record is still the one in place and is intact.
    assert json.loads(journal.read_text()) == {"phase": "complete"}
    assert not (work / "migration-journal.tmp").exists()
    # The unreclaimed link holds the previous record and stays in the private
    # work directory, but only as something the log names.
    stale = [p for p in work.iterdir() if p.name.startswith("journal-rollback-")]
    assert len(stale) == 1
    assert json.loads(stale[0].read_text()) == {"phase": "complete"}
    assert "Journal rollback did not complete" in caplog.text


async def test_cancellation_while_draining_heartbeats_still_releases_the_lock(environment, monkeypatch):
    # A second cancellation arriving while the heartbeats are being drained must
    # not skip the release: an operation lock that is never handed back blocks
    # backups, restores and migrations until the process restarts.
    receiver, router, selected, root, lock = environment
    sid = (await start(receiver, selected))["session_id"]
    await upload(receiver, sid)
    router.data_restored = True
    assert lock.busy and lock.active == OpKind.MIGRATION

    async def cancel_while_draining(task):
        raise asyncio.CancelledError

    monkeypatch.setattr(m, "heartbeat_cancelled", cancel_while_draining)
    with pytest.raises(asyncio.CancelledError):
        await receiver._finish("complete", remove=True)
    assert not lock.busy
    # The operation is available again and the session is still recoverable.
    assert lock.try_acquire(OpKind.MIGRATION) is None
    lock.release(OpKind.MIGRATION)
