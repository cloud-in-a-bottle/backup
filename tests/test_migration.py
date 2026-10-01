"""Migration exercises the real restic transport and the ordinary restore.

Router APIs are stateful fakes; repositories, encryption, uploads, verification,
tree promotion, and journals are real. Recovery's router edge cases live in
test_recovery instead of being repeated for a second migration executor.
"""
import asyncio
import copy
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

import app as backup_app
import migration as m
import snapshot_configuration as snapshots
from operations import OpKind, OperationLock
from tests.test_snapshot_configuration import environment, APP_SECRET
from tests.test_recovery import Router, make_bundle, inventory_entry, mock_http, OWNER_TOKEN, ORIGIN, session_for


class Peer:
    """Source transport adapter through the real Quart routes."""
    def __init__(self, client, lost=None):
        self.client, self.lost = client, lost

    async def request(self, method, path, *, body=None, content=None, headers=None):
        payload = {"json": body} if body is not None else {"data": content}
        response = await self.client.open(path, method=method, **payload,
                                         headers={**(headers or {}), "Authorization": "Bearer " + OWNER_TOKEN})
        result = await response.get_json()
        if response.status_code != 200:
            raise m.MigrationError("transfer")
        if self.lost and self.lost in path:
            self.lost = None
            raise m.MigrationError("transfer")
        return result


@pytest.fixture
async def receiver(environment, monkeypatch, mock_http):
    root, *_ = environment
    bundle = make_bundle("demo", runtime=True)
    router = Router(bundle, [inventory_entry("demo", "D" * 12)])
    async def handle(request):
        if request.headers.get("Authorization") != "Bearer " + OWNER_TOKEN:
            return httpx.Response(403, json={"error": "no"})
        return await router(request)
    mock_http(handle)
    for module in (m, backup_app):
        monkeypatch.setattr(module, "RecoverySession", lambda url, token, bundle, name: session_for(bundle, timeout=2))
    monkeypatch.setattr(backup_app, "ROUTER_URL", ORIGIN)
    monkeypatch.setattr(backup_app, "_migration_receiver", None)
    monkeypatch.setattr(m, "source_recovery", None)
    monkeypatch.setattr(m, "status", None)
    original_restore = backup_app._restore_migration_snapshot
    async def restore(*args):
        router.data_restored = True
        return await original_restore(*args)
    monkeypatch.setattr(backup_app, "_restore_migration_snapshot", restore)
    receiver = backup_app._receiver()
    value = SimpleNamespace(receiver=receiver, root=root, router=router, bundle=bundle,
                            client=backup_app.app.test_client(), password="a" * 64)
    value.peer = Peer(value.client)
    yield value
    if receiver._job is not None and not receiver._job.done():
        await receiver._job
    if receiver._record and receiver._record["phase"] == "receiving":
        await receiver.abort({"version": 5, "session_id": receiver._record["session_id"]}, owner_token=OWNER_TOKEN)


async def start(env):
    result = await env.peer.request("POST", "/api/migration/receive/start", body={
        "version": 5, "bundle": env.bundle, "password": env.password, "capture_complete": True})
    return result["session_id"]


async def transfer(env, sid, tmp_path):
    repository = tmp_path / "source-repository"
    repository.mkdir()
    return await m._capture_and_transfer(env.peer, sid, repository, env.password, env.root, env.bundle)


async def finish(env, sid, snapshot):
    body = {"version": 5, "session_id": sid, "snapshot": snapshot}
    accepted = await env.peer.request("POST", "/api/migration/receive/finalize", body=body)
    assert accepted["phase"] == "finalizing"
    job = env.receiver._job
    await env.peer.request("POST", "/api/migration/receive/finalize", body=body)
    assert env.receiver._job is job, "retry must not start another recovery"
    await job
    return env.receiver.journal_status


@pytest.mark.parametrize("variant", ["normal", "lost-upload", "stopped", "empty", "large", "inherited-env"])
async def test_real_snapshot_transfer_and_shared_restore(receiver, tmp_path, monkeypatch, variant):
    env = receiver
    if variant == "inherited-env":
        monkeypatch.setenv("RESTIC_REPOSITORY_FILE", str(tmp_path / "unrelated-repository"))
    if variant == "stopped":
        env.bundle["runtime"]["apps"]["demo"]["status"] = "stopped"
    if variant == "empty":
        (env.root / "demo" / "secret.txt").unlink()
    if variant == "large":
        (env.root / "demo" / "large.bin").write_bytes(os.urandom(16 * 1024 * 1024))
    (env.root / "unselected").mkdir()
    (env.root / "unselected" / "secret").write_text("not selected")
    sid = await start(env)
    assert not env.router.mutations(), "start must only preflight"
    if variant == "lost-upload":
        env.peer.lost = "/object/"
    snapshot = await transfer(env, sid, tmp_path)
    for path in env.receiver._stage.rglob("*"):
        if path.is_file():
            assert APP_SECRET.encode() not in path.read_bytes()
    (env.root / "demo" / "stale-wal").write_text("stale destination data")
    (env.root / "demo" / "secret.txt").write_text("destination")
    backup_app.restore_last_status = "success"
    async def check_live_restore_status(request, body, response):
        progress = await (await env.client.get("/api/restore/status")).get_json()
        assert progress["running"] is True
        assert progress["last_status"] is None
    env.router.after["/api/app-definitions/import-private"] = check_live_restore_status
    result = await finish(env, sid, snapshot)
    assert result["phase"] == "complete", result
    assert result["result"]["ok"] and not result["needs_attention"]
    assert not (env.root / "demo" / "stale-wal").exists()
    if variant == "empty":
        assert list((env.root / "demo").iterdir()) == []
    else:
        assert (env.root / "demo" / "secret.txt").read_text() == APP_SECRET
    assert (env.root / "unselected" / "secret").read_text() == "not selected"
    assert env.router.apps["demo"]["status"] == ("stopped" if variant == "stopped" else "running")
    assert not backup_app.op_lock.busy
    assert backup_app.restore_progress["phase"] == "complete"
    public = json.dumps(result) + env.receiver._journal.read_text()
    assert OWNER_TOKEN not in public and env.password not in public


@pytest.mark.parametrize("fault", ["missing-object", "corrupt-object", "wrong-password", "different-bundle", "activation"])
async def test_invalid_or_incomplete_recovery_never_succeeds(receiver, tmp_path, fault):
    env = receiver
    sid = await start(env)
    snapshot = await transfer(env, sid, tmp_path)
    if fault in {"missing-object", "corrupt-object"}:
        path = next(p for p in (env.receiver._stage / "data").rglob("*") if p.is_file())
        if fault == "missing-object":
            path.unlink()
        else:
            path.write_bytes(b"broken encrypted pack")
    elif fault == "wrong-password":
        env.receiver._password = "wrong"
    elif fault == "different-bundle":
        env.receiver._bundle["runtime"]["apps"]["demo"]["status"] = "stopped"
    else:
        env.router.deploy_states["demo"] = ["error"]
    (env.root / "demo" / "original").write_text("retain me")
    result = await finish(env, sid, snapshot)
    assert result["phase"] == "incomplete" and not result["ok"]
    assert result["needs_attention"] and not backup_app.op_lock.busy
    if fault != "activation":
        assert not env.router.mutations()
        assert (env.root / "demo" / "original").read_text() == "retain me"
    else:
        assert backup_app._restore_needs_attention
        assert list(env.root.glob(".bottle-backup-restore/**/original"))
        response = await env.client.post("/api/migration/acknowledge", headers={"Authorization": "Bearer " + OWNER_TOKEN})
        assert response.status_code == 200
        assert not env.receiver.needs_attention and not backup_app._restore_needs_attention


@pytest.mark.parametrize("version", [None, 3, 4, 5.0, True])
async def test_old_or_invalid_protocol_fails_before_changes(receiver, version):
    response = await receiver.client.post("/api/migration/receive/start", json={
        "version": version, "bundle": receiver.bundle, "password": receiver.password, "capture_complete": True},
        headers={"Authorization": "Bearer " + OWNER_TOKEN})
    assert response.status_code == 400
    assert not receiver.router.mutations() and not backup_app.op_lock.busy


async def test_older_v5_source_is_rejected_before_receiving(receiver):
    response = await receiver.client.post("/api/migration/receive/start", json={
        "version": 5, "bundle": receiver.bundle, "password": receiver.password},
        headers={"Authorization": "Bearer " + OWNER_TOKEN})
    assert response.status_code == 400
    assert not receiver.router.mutations() and not backup_app.op_lock.busy


async def test_older_v5_receiver_is_rejected_before_source_capture(receiver, monkeypatch):
    peer = receiver.peer
    request = peer.request
    async def old_receiver(method, path, **kwargs):
        result = await request(method, path, **kwargs)
        if path.endswith("/capabilities"):
            result.pop("capture_complete")
        return result
    peer.request = old_receiver
    monkeypatch.setattr(m, "_Peer", lambda *args: peer)
    capture = AsyncMock(side_effect=AssertionError("Must negotiate before capturing or stopping apps"))
    monkeypatch.setattr(m, "capture_configuration", capture)
    lock = OperationLock()
    assert not await m.run_direct_push(target_url="https://destination.test", target_token=OWNER_TOKEN,
        selected_apps=["demo"], lock=lock, all_app_data=receiver.root, work_dir=receiver.root / "backup" / ".source",
        router_url=ORIGIN, app_token="synthetic", owner_token=OWNER_TOKEN)
    capture.assert_not_awaited()
    assert not lock.busy and not receiver.router.mutations()


@pytest.mark.parametrize("endpoint,method", [("capabilities", "GET"), ("start", "POST"),
    ("finalize", "POST"), ("abort", "POST"), ("keepalive", "POST"), ("status/" + "b" * 64, "GET"),
    ("object/" + "b" * 64 + "/config/config", "POST")])
async def test_receiver_requires_caller_authority(receiver, endpoint, method):
    response = await receiver.client.open("/api/migration/receive/" + endpoint, method=method)
    assert response.status_code in {401, 403}
    assert not backup_app.op_lock.busy and not receiver.router.mutations()


@pytest.mark.parametrize("kind,identifier,offset", [("other", "b" * 64, 0), ("data", "invalid", 0),
    ("data", "../escape", 0), ("config", "config", -1), ("config", "config", 1)])
async def test_object_bounds_and_paths(receiver, kind, identifier, offset):
    sid = await start(receiver)
    async def chunks():
        yield b"hello"
    with pytest.raises(m.MigrationError):
        await receiver.receiver.upload(sid, kind, identifier, chunks(), offset=offset, owner_token=OWNER_TOKEN)
    assert not receiver.router.mutations()


async def test_object_size_limit_replay_and_sealed_finalization(receiver, monkeypatch):
    env = receiver
    sid = await start(env)
    async def chunks(value):
        yield value
    for value in (b"first", b"first"):
        result = await env.receiver.upload(sid, "config", "config", chunks(value), offset=0, owner_token=OWNER_TOKEN)
        assert result["offset"] == 5
    await env.receiver.upload(sid, "config", "config", chunks(b"suffix"), offset=5, owner_token=OWNER_TOKEN)
    await env.receiver.upload(sid, "config", "config", chunks(b"first"), offset=0, owner_token=OWNER_TOKEN)
    assert (env.receiver._stage / "config").read_bytes() == b"firstsuffix"
    with pytest.raises(m.MigrationError):
        await env.receiver.upload(sid, "config", "config", chunks(b"wrong"), offset=0, owner_token=OWNER_TOKEN)
    monkeypatch.setattr(m, "CHUNK_LIMIT", 5)
    with pytest.raises(m.MigrationError):
        await env.receiver.upload(sid, "config", "config", chunks(b"123456"), offset=0, owner_token=OWNER_TOKEN)
    gate = asyncio.Event()
    monkeypatch.setattr(m, "_restic", AsyncMock(side_effect=lambda *a, **k: None))
    async def held(*args, **kwargs):
        await gate.wait()
        raise m.MigrationError("transfer")
    monkeypatch.setattr(m, "_restic", held)
    await env.receiver.finalize({"version": 5, "session_id": sid, "snapshot": "c" * 64}, owner_token=OWNER_TOKEN)
    try:
        with pytest.raises(m.MigrationError, match="Another operation"):
            await env.receiver.abort({"version": 5, "session_id": sid}, owner_token=OWNER_TOKEN)
        with pytest.raises(m.MigrationError):
            await env.receiver.upload(sid, "config", "config", chunks(b"late"), offset=0, owner_token=OWNER_TOKEN)
    finally:
        gate.set()
        await env.receiver._job


async def test_idle_expiry_and_restart_notice(receiver):
    env = receiver
    sid = await start(env)
    env.receiver._activity -= 1000
    await env.receiver.keepalive({"version": 5, "session_id": sid}, owner_token=OWNER_TOKEN)
    await env.receiver.expire_stale()
    assert backup_app.op_lock.busy
    env.receiver._activity -= 1000
    await env.receiver.expire_stale()
    assert not backup_app.op_lock.busy and env.receiver.journal_status["phase"] == "aborted"
    env.receiver._record.update(phase="finalizing", needs_attention=True)
    env.receiver._save()
    restarted = m.MigrationReceiver(lock=OperationLock(), all_app_data=env.root,
        work_dir=env.root / "backup" / ".migration", router_url=ORIGIN, restore=env.receiver.restore)
    assert restarted.needs_attention and restarted.journal_status["phase"] == "interrupted"
    with pytest.raises(m.MigrationError, match="inspection"):
        await restarted.start({"version": 5, "bundle": env.bundle, "password": env.password, "capture_complete": True}, owner_token=OWNER_TOKEN)
    await restarted.acknowledge(owner_token=OWNER_TOKEN)
    assert not restarted.needs_attention


async def test_latest_incoming_abort_supersedes_older_outgoing_success(receiver, monkeypatch):
    env = receiver
    monkeypatch.setattr(m, "status", {"phase": "done", "ok": True, "started_at": 1})
    sid = await start(env)
    await env.receiver.abort({"version": 5, "session_id": sid}, owner_token=OWNER_TOKEN)
    result = await (await env.client.get("/api/migration/status")).get_json()
    assert result["status"]["phase"] == "error" and not result["receive"]["needs_attention"]
    # A subsequent outgoing success then legitimately supersedes that history.
    m.status = {"phase": "done", "ok": True, "started_at": env.receiver.journal_status["started_at"] + 1}
    result = await (await env.client.get("/api/migration/status")).get_json()
    assert result["status"]["phase"] == "done"


@pytest.mark.parametrize("fault", ["preflight", "journal"])
async def test_start_failure_releases_lock_and_empty_repository(receiver, monkeypatch, fault):
    env = receiver
    if fault == "preflight":
        monkeypatch.setattr(m, "RecoverySession", lambda *args: SimpleNamespace(preflight=AsyncMock(side_effect=RuntimeError("failed preflight"))))
    else:
        monkeypatch.setattr(env.receiver, "_save", lambda: (_ for _ in ()).throw(OSError("failed publication")))
    with pytest.raises((OSError, RuntimeError)):
        await env.receiver.start({"version": 5, "bundle": env.bundle, "password": env.password, "capture_complete": True}, owner_token=OWNER_TOKEN)
    assert not backup_app.op_lock.busy and not env.router.mutations()
    assert not list(env.receiver.work.glob("[a-f0-9]" * 64))
    assert env.receiver.journal_status is None or env.receiver.journal_status["phase"] == "aborted"


async def test_failed_finalize_intent_is_retryable_without_mutation(receiver, monkeypatch):
    env = receiver
    sid = await start(env)
    persist = env.receiver._persist
    monkeypatch.setattr(env.receiver, "_persist", lambda record: (_ for _ in ()).throw(OSError("disk unavailable")))
    with pytest.raises(m.MigrationError):
        await env.receiver.finalize({"version": 5, "session_id": sid, "snapshot": "b" * 64}, owner_token=OWNER_TOKEN)
    assert env.receiver._job is None and env.receiver.journal_status["phase"] == "receiving"
    assert backup_app.op_lock.busy and not env.router.mutations()
    monkeypatch.setattr(env.receiver, "_persist", persist)
    await env.receiver.abort({"version": 5, "session_id": sid}, owner_token=OWNER_TOKEN)
    assert not backup_app.op_lock.busy


async def test_failed_shared_acknowledgment_leaves_both_notices(receiver, monkeypatch):
    env = receiver
    env.receiver._record = {"version": 5, "session_id": "b" * 64, "snapshot": "c" * 64,
                            "phase": "incomplete", "needs_attention": True, "ok": False}
    monkeypatch.setattr(backup_app, "restore_progress", {"snapshot": "c" * 64, "job_id": "d" * 32})
    monkeypatch.setattr(backup_app, "_restore_needs_attention", True)
    monkeypatch.setattr(snapshots, "save_journal", lambda *args: (_ for _ in ()).throw(OSError("disk unavailable")))
    response = await env.client.post("/api/migration/acknowledge", headers={"Authorization": "Bearer " + OWNER_TOKEN})
    assert response.status_code == 500
    assert env.receiver.needs_attention and backup_app._restore_needs_attention


@pytest.mark.parametrize("fault", ["json", "duplicate", "list", "phase", "flag", "symlink", "oversized"])
async def test_receiver_restart_sanitizes_corrupt_journals(receiver, fault):
    env = receiver
    path = env.receiver._journal
    record = {"version": 5, "session_id": "b" * 64, "phase": "complete", "needs_attention": False}
    if fault == "phase":
        record["phase"] = "private value"
    elif fault == "flag":
        record["needs_attention"] = "private value"
    value = {"json": "private value", "duplicate": '{"version":5,"version":5}', "list": "[]",
             "oversized": "x" * (m.MAX_JSON_BYTES + 1)}.get(fault, json.dumps(record))
    if fault == "symlink":
        target = env.receiver.work / "outside.json"
        target.write_text(value)
        path.symlink_to(target)
    else:
        path.write_text(value)
    restarted = m.MigrationReceiver(lock=OperationLock(), all_app_data=env.root,
        work_dir=env.root / "backup" / ".migration", router_url=ORIGIN, restore=env.receiver.restore)
    assert restarted.needs_attention
    assert "private value" not in json.dumps(restarted.journal_status)


@pytest.mark.parametrize("change", ["identity", "new-app", "source", "ports"])
async def test_transfer_cannot_adopt_changed_destination_configuration(receiver, tmp_path, change):
    env = receiver
    if change == "new-app":
        env.router.apps.pop("demo")
        env.router.definitions.pop("demo")
    sid = await start(env)
    snapshot = await transfer(env, sid, tmp_path)
    if change in {"identity", "new-app"}:
        env.router.apps["demo"] = inventory_entry("demo", "E" * 12)
        env.router.definitions["demo"] = copy.deepcopy(env.bundle["definitions"]["apps"][0])
    elif change == "source":
        env.router.definitions["demo"]["source"]["ref"] = "other-ref"
    else:
        env.router.definitions["demo"]["port_mappings"].append({"label": "other", "container_port": 8081, "host_port": 32000})
    (env.root / "demo" / "replacement-data").write_text("keep replacement")
    result = await finish(env, sid, snapshot)
    assert result["phase"] == "incomplete"
    assert not env.router.mutations()
    assert (env.root / "demo" / "replacement-data").read_text() == "keep replacement"


async def test_receiving_restart_reclaims_disposable_repository(receiver):
    env = receiver
    sid = await start(env)
    (env.receiver._stage / "config").write_bytes(b"partial upload")
    env.receiver._monitor.cancel()
    await asyncio.gather(env.receiver._monitor, return_exceptions=True)
    backup_app.op_lock.release(OpKind.MIGRATION)
    restarted = m.MigrationReceiver(lock=backup_app.op_lock, all_app_data=env.root,
        work_dir=env.root / "backup" / ".migration", router_url=ORIGIN, restore=env.receiver.restore)
    assert not restarted.needs_attention
    next_session = await restarted.start({"version": 5, "bundle": env.bundle, "password": env.password, "capture_complete": True}, owner_token=OWNER_TOKEN)
    assert not (restarted.work / sid).exists()
    await restarted.abort({"version": 5, "session_id": next_session["session_id"]}, owner_token=OWNER_TOKEN)
    env.receiver._record = None  # the old process is gone


async def test_corrupt_journal_acknowledgment_survives_restart(receiver):
    env = receiver
    env.receiver._journal.write_text("corrupt")
    kwargs = dict(lock=OperationLock(), all_app_data=env.root, work_dir=env.root / "backup" / ".migration",
                  router_url=ORIGIN, restore=env.receiver.restore)
    restarted = m.MigrationReceiver(**kwargs)
    assert restarted.needs_attention
    await restarted.acknowledge(owner_token=OWNER_TOKEN)
    assert not m.MigrationReceiver(**kwargs).needs_attention


@pytest.mark.parametrize("direction", ["source", "incoming"])
async def test_acknowledgment_response_distinguishes_success_from_failed_job(receiver, monkeypatch, direction):
    env = receiver
    if direction == "source":
        record = m.SourceRecoveryRecord(lock=backup_app.op_lock, all_app_data=env.root,
            work_dir=env.root / "backup" / ".source", router_url=ORIGIN)
        record.begin("b" * 64, {"demo"}, [inventory_entry("demo", "D" * 12)], "backup")
        monkeypatch.setattr(m, "source_recovery", record)
        path = "/api/migration/source-acknowledge"
    else:
        record = env.receiver
        record._record = {"version": 5, "session_id": "b" * 64, "phase": "incomplete", "ok": False, "needs_attention": True}
        path = "/api/migration/acknowledge"
    response = await env.client.post(path, headers={"Authorization": "Bearer " + OWNER_TOKEN})
    body = await response.get_json()
    assert response.status_code == 200 and body["ok"] is True
    assert not record.needs_attention and body["needs_attention"] is False
    assert body["journal_status"]["ok"] is False


@pytest.mark.parametrize("both_layouts", [False, True])
async def test_storage_layout_upgrade_preserves_unfinished_recovery(tmp_path, monkeypatch, both_layouts):
    base = tmp_path / "backup" / ".migration"
    legacy = base / "migration-v4"
    legacy.mkdir(parents=True)
    original = legacy / ("a" * 64) / "trees" / ".migration-old-original"
    original.mkdir(parents=True)
    (original / "valuable").write_text("original data")
    legacy_record = {"version": 4, "ok": False, "session_id": "a" * 64, "phase": "finalizing", "needs_attention": True,
                     "recovery": {"paused_apps": [{"name": "demo", "app_id": "D" * 12, "restart": "pending"}]},
                     "retained_sessions": ["a" * 64]}
    (legacy / "journal.json").write_text(json.dumps(legacy_record))
    if both_layouts:
        current = base / "migration-v5"
        current.mkdir()
        (current / "journal.json").write_text(json.dumps({"version": 5, "session_id": "b" * 64,
                                                         "phase": "complete", "needs_attention": False, "ok": True}))
    monkeypatch.setattr(m, "confirm_owner", AsyncMock(return_value=True))
    kwargs = dict(lock=OperationLock(), all_app_data=tmp_path, work_dir=base, router_url=ORIGIN, restore=AsyncMock())
    upgraded = m.MigrationReceiver(**kwargs)
    assert upgraded.work == base / "incoming" and upgraded.needs_attention
    assert not legacy.exists()
    retained = upgraded.work / ("a" * 64) / "trees" / ".migration-old-original" / "valuable"
    assert retained.read_text() == "original data"
    journals = list(upgraded.work.glob("journal-rollback-*.json"))
    assert any(json.loads(path.read_text()) == legacy_record for path in journals)
    await upgraded.acknowledge(owner_token=OWNER_TOKEN)
    assert not m.MigrationReceiver(**kwargs).needs_attention
    assert retained.read_text() == "original data"


async def test_peer_request_has_a_total_deadline(mock_http, monkeypatch):
    class Slow(httpx.AsyncByteStream):
        async def __aiter__(self):
            for _ in range(100):
                await asyncio.sleep(0.02)
                yield b" "
    mock_http(lambda request: httpx.Response(200, stream=Slow()))
    monkeypatch.setattr(m, "PEER_REQUEST_TIMEOUT", 0.05)
    with pytest.raises(m.MigrationError):
        await asyncio.wait_for(m._Peer("https://destination.test", OWNER_TOKEN).request("POST", "/api/migration/receive/abort"), 1)


@pytest.mark.parametrize("fault", [None, "lost-upload", "lost-finalize", "transfer", "resume", "resume-error", "final-state", "cancel", "receiver-restart"])
async def test_source_cutover_and_unaffected_cleanup(receiver, tmp_path, monkeypatch, fault):
    env = receiver
    captured = make_bundle("demo", "other", "backup", runtime=True)
    before = [inventory_entry(name, identifier * 12) for name, identifier in (("demo", "D"), ("other", "E"), ("backup", "F"))]
    events = []
    class Source:
        progress = {"destination_apps_before": before}
        async def preflight(self):
            events.append("preflight")
        async def stop_apps(self):
            assert m.source_recovery.needs_attention
            events.append("stopped")
        async def restart_unaffected(self):
            events.append("cleanup")
            if fault == "resume-error":
                raise RuntimeError("restart failed")
            self.progress = {"paused_apps": [{"name": "other", "selected": False, "previous_status": "running",
                                     "restart": "failed" if fault == "resume" else "confirmed"}]}
            return self.progress
        async def confirm_source_cutover(self):
            return fault != "final-state"
    factory = m.RecoverySession
    monkeypatch.setattr(m, "RecoverySession", lambda url, *args: Source() if url == "https://source.test" else factory(url, *args))
    monkeypatch.setattr(m, "capture_configuration", AsyncMock(return_value=captured))
    peer = Peer(env.client, "/object/" if fault == "lost-upload" else "/finalize" if fault == "lost-finalize" else None)
    finalizations = []
    if fault == "receiver-restart":
        request = peer.request
        async def restarted(method, path, **kwargs):
            if path.endswith("/finalize"):
                finalizations.append(path)
                if len(finalizations) > 1:
                    raise RuntimeError("Repeated finalize instead of observing interruption")
                await m._cancel_tasks(env.receiver._monitor)
                env.receiver._record.update(phase="interrupted", needs_attention=True)
                backup_app.op_lock.release(OpKind.MIGRATION)
                raise m.MigrationError("transfer")
            return await request(method, path, **kwargs)
        peer.request = restarted
    monkeypatch.setattr(m, "_Peer", lambda *args: peer)
    if fault == "transfer":
        monkeypatch.setattr(m, "_capture_and_transfer", AsyncMock(side_effect=m.MigrationError("transfer")))
    entered = asyncio.Event()
    if fault == "cancel":
        async def held(*args):
            entered.set()
            await asyncio.Event().wait()
        monkeypatch.setattr(m, "_capture_and_transfer", held)
    source_lock = OperationLock()
    operation = m.run_direct_push(target_url="https://destination.test", target_token=OWNER_TOKEN,
        selected_apps=["demo"], lock=source_lock, all_app_data=env.root, work_dir=env.root / "backup" / ".source",
        router_url="https://source.test", app_token="app-token", owner_token=OWNER_TOKEN, poll_interval=0.01)
    if fault == "cancel":
        task = asyncio.create_task(operation)
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        result = False
    else:
        result = await operation
    assert result == (fault not in {"transfer", "resume", "resume-error", "final-state", "cancel", "receiver-restart"}), m.status
    assert events == ["preflight", "stopped", "cleanup"]
    assert not source_lock.busy and not m.source_recovery.live
    assert m.source_recovery.needs_attention == (not result)
    assert m.source_recovery.journal_status["restart_pending"] == (["other"] if fault in {"resume", "resume-error"} else [])
    assert not list((env.root / "backup" / ".source").glob("outgoing-*"))
    if fault == "receiver-restart":
        assert len(finalizations) == 1


@pytest.mark.parametrize("failure", ["write", "fsync"])
def test_shared_journal_keeps_prior_intent(tmp_path, monkeypatch, failure):
    path = tmp_path / "journal.json"
    m.save_journal(path, {"needs_attention": True})
    real = os.fsync
    def fail(fd):
        if failure == "write" or os.fstat(fd).st_ino == tmp_path.stat().st_ino:
            raise OSError("unwritable")
        real(fd)
    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError):
        m.save_journal(path, {"needs_attention": False})
    assert json.loads(path.read_text()) == {"needs_attention": True}


async def test_owner_refusal_does_not_start_outgoing_migration(receiver, monkeypatch):
    monkeypatch.setattr(backup_app, "_caller_is_owner", AsyncMock(return_value=None))
    response = await receiver.client.post("/api/migration/push", json={"target_url": "https://destination.test", "target_token": "secret"})
    assert response.status_code in {401, 403}
    assert not backup_app.op_lock.busy and not receiver.router.mutations()
