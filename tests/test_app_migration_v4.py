"""HTTP contracts for v4; archive/recovery algorithms have dedicated core tests."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import tarfile
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from quart import request

import app as backup_app
import migration
import migration_data
from operations import OpKind
from tests.test_app_routes import client  # shared isolated Quart fixture
from tests.test_configuration import make_bundle, mock_http


OWNER = "owner-token-private-sentinel"
TARGET = "target-token-private-sentinel"
SESSION = "a" * 64
AUTH = {"Authorization": f"Bearer {OWNER}"}
RECEIVE = "/api/migration/receive/"
JSON_ACTIONS = ("start", "finalize", "abort", "keepalive")
ENDPOINTS = (
    ("POST", "start"),
    ("POST", f"chunk/{SESSION}/myapp"),
    ("POST", "finalize"),
    ("POST", "abort"),
    ("POST", "keepalive"),
    ("GET", "capabilities"),
    ("GET", f"status/{SESSION}"),
)


def payload(action):
    return {"version": 4, **({"bundle": make_bundle("myapp", runtime=True)}
                             if action == "start" else {"session_id": SESSION})}


def chunk_headers():
    return {**AUTH, "Content-Type": "application/octet-stream", "X-Chunk-Index": "0",
            "X-Chunk-Final": "1", "X-Archive-Bytes": "7", "X-Archive-SHA256": "b" * 64}


@pytest.fixture(autouse=True)
def caller_authority(monkeypatch):
    """These tests pin HTTP contracts; the real owner probe is tested separately.

    The stub mirrors the production rule: a Bearer header is the only thing
    that can establish caller authority, never the configured token.
    """
    async def stub():
        auth = request.headers.get("Authorization", "")
        return auth[7:] if auth.startswith("Bearer ") else None

    monkeypatch.setattr(backup_app, "_caller_is_owner", stub)


@pytest.fixture
def receiver(client, monkeypatch):
    mock = MagicMock(spec=migration.MigrationReceiver)
    # Parent gates read this flag; keep the default explicit rather than a truthy mock.
    mock.needs_attention = False
    monkeypatch.setattr(backup_app, "_migration_receiver", mock)
    return mock


async def assert_error(response, code):
    error = migration.MigrationError(code)
    assert response.status_code == error.status_code
    assert await response.get_json() == {"ok": False, "error": str(error)}


@pytest.mark.parametrize("method,path", ENDPOINTS)
@pytest.mark.parametrize("authorization", [None, "", "Bearer ", "Basic abc", OWNER])
async def test_explicit_bearer_required_before_receiver(client, monkeypatch, method, path, authorization):
    # Configured router/app credentials must never authorize an anonymous receiver call.
    monkeypatch.setattr(backup_app, "ROUTER_API_TOKEN", OWNER)
    factory = MagicMock(side_effect=AssertionError("receiver reached without caller token"))
    monkeypatch.setattr(backup_app, "_receiver", factory)
    headers = {} if authorization is None else {"Authorization": authorization}
    response = await client.open(RECEIVE + path, method=method, headers=headers)
    await assert_error(response, "auth")
    factory.assert_not_called()
    assert not backup_app.op_lock.busy


@pytest.mark.parametrize("path", ["data", "app/myapp", "chunk/myapp"])
async def test_legacy_routes_refuse_valid_archives_without_extraction(client, monkeypatch, path):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        entry = tarfile.TarInfo("myapp/database")
        entry.size = 3
        archive.addfile(entry, io.BytesIO(b"new"))
    app_data = backup_app.ALL_APP_DATA / "myapp"
    app_data.mkdir()
    sentinel = app_data / "database"
    sentinel.write_bytes(b"original")
    factory = MagicMock(side_effect=AssertionError("legacy route invoked receiver"))
    monkeypatch.setattr(backup_app, "_receiver", factory)
    response = await client.post(RECEIVE + path, data=stream.getvalue(), headers=AUTH)
    await assert_error(response, "protocol")
    factory.assert_not_called()
    assert sentinel.read_bytes() == b"original"
    assert list(app_data.iterdir()) == [sentinel]
    assert not backup_app.op_lock.busy


@pytest.mark.parametrize("action", JSON_ACTIONS)
async def test_json_actions_forward_exact_payload_and_caller_token(client, receiver, action):
    body = payload(action)
    result = {"ok": True, "version": 4, "session_id": SESSION, "phase": "receiving"}
    getattr(receiver, action).return_value = result
    response = await client.post(RECEIVE + action, json=body, headers=AUTH)
    assert response.status_code == 200
    assert await response.get_json() == result
    getattr(receiver, action).assert_awaited_once_with(body, owner_token=OWNER)
    assert OWNER not in await response.get_data(as_text=True)
    assert "token_hash" not in await response.get_data(as_text=True)
    assert not backup_app.op_lock.busy  # receiver, not handler, acquires the lock


@pytest.mark.parametrize("path,method,args,result", [
    ("capabilities", "capabilities", (), {"ok": True, "version": 4, "chunk_limit": migration.CHUNK_LIMIT, "backup_app_name": "backup"}),
    (f"status/{SESSION}", "status", (SESSION,), {"ok": True, "version": 4, "phase": "finalizing", "result": None}),
])
async def test_get_actions_forward_and_serialize(client, receiver, path, method, args, result):
    getattr(receiver, method).return_value = result
    response = await client.get(RECEIVE + path, headers=AUTH)
    assert response.status_code == 200
    assert await response.get_json() == result
    getattr(receiver, method).assert_awaited_once_with(*args, owner_token=OWNER)


@pytest.mark.parametrize("action", JSON_ACTIONS)
@pytest.mark.parametrize("raw,content_type", [
    (b"", "application/json"), (b"{", "application/json"),
    (b"[]", "application/json"), (b"null", "application/json"),
    (b'"private"', "application/json"), (b"true", "application/json"),
    (b"\xff", "application/json"), (b"{}", "text/plain"),
    (b'{"version":3,"version":4}', "application/json"),
    (b'{"version":4,"bundle":{"secret":1,"secret":2}}', "application/json"),
])
async def test_invalid_json_never_calls_receiver(client, receiver, action, raw, content_type):
    response = await client.post(RECEIVE + action, data=raw,
                                 headers={**AUTH, "Content-Type": content_type})
    await assert_error(response, "invalid")
    getattr(receiver, action).assert_not_called()
    assert not backup_app.op_lock.busy


@pytest.mark.parametrize("action", JSON_ACTIONS)
async def test_json_limit_enforced_without_content_length(client, receiver, monkeypatch, action):
    raw = json.dumps(payload(action)).encode()
    monkeypatch.setattr(migration, "MAX_JSON_BYTES", len(raw) - 1)
    async with client.request(RECEIVE + action, method="POST",
                              headers={**AUTH, "Content-Type": "application/json"}) as connection:
        await connection.send(raw[:10])
        await connection.send(raw[10:])
        await connection.send_complete()
    await assert_error(await connection.as_response(), "invalid")
    getattr(receiver, action).assert_not_called()


@pytest.mark.parametrize("action", JSON_ACTIONS)
async def test_json_exact_limit_accepted(client, receiver, monkeypatch, action):
    body = payload(action)
    raw = json.dumps(body).encode()
    monkeypatch.setattr(migration, "MAX_JSON_BYTES", len(raw))
    getattr(receiver, action).return_value = {"ok": True}
    response = await client.post(RECEIVE + action, data=raw,
                                 headers={**AUTH, "Content-Type": "application/json"})
    assert response.status_code == 200
    getattr(receiver, action).assert_awaited_once_with(body, owner_token=OWNER)


@pytest.mark.parametrize("action", JSON_ACTIONS)
@pytest.mark.parametrize("version", [None, 3, 5, "4", 4.0, True])
async def test_version_mismatch_rejected_by_real_receiver_before_effects(client, monkeypatch, action, version):
    auth = AsyncMock(side_effect=AssertionError("version check must precede auth/network"))
    monkeypatch.setattr(migration.MigrationReceiver, "_authenticate", auth)
    body = payload(action)
    body["version"] = version
    response = await client.post(RECEIVE + action, json=body, headers=AUTH)
    await assert_error(response, "protocol")
    auth.assert_not_called()
    assert not backup_app.op_lock.busy
    assert backup_app._migration_receiver.journal_status is None


@pytest.mark.parametrize("action", JSON_ACTIONS)
@pytest.mark.parametrize("change", ["missing", "extra"])
async def test_exact_payload_fields_validated_by_real_receiver(client, monkeypatch, action, change):
    auth = AsyncMock(side_effect=AssertionError("invalid payload reached authentication"))
    monkeypatch.setattr(migration.MigrationReceiver, "_authenticate", auth)
    body = payload(action)
    if change == "extra":
        body["private-token"] = OWNER
    else:
        del body["bundle" if action == "start" else "session_id"]
    response = await client.post(RECEIVE + action, json=body, headers=AUTH)
    await assert_error(response, "invalid")
    auth.assert_not_called()
    assert not backup_app.op_lock.busy


@pytest.mark.parametrize("header,value", [
    ("X-Chunk-Index", None), ("X-Chunk-Index", "-1"), ("X-Chunk-Index", "+1"),
    ("X-Chunk-Index", "1.0"), ("X-Chunk-Index", "9" * 13),
    ("X-Archive-Bytes", None), ("X-Archive-Bytes", "-1"),
    ("X-Archive-Bytes", "1e3"), ("X-Archive-Bytes", "9" * 19),
    ("X-Chunk-Final", None), ("X-Chunk-Final", "true"), ("X-Chunk-Final", "2"),
    ("X-Archive-SHA256", None), ("X-Archive-SHA256", "B" * 64),
    ("X-Archive-SHA256", "g" * 64), ("X-Archive-SHA256", "a" * 63),
])
async def test_malformed_chunk_headers_rejected_before_upload(client, receiver, header, value):
    headers = chunk_headers()
    if value is None:
        del headers[header]
    else:
        headers[header] = value
    response = await client.post(RECEIVE + f"chunk/{SESSION}/myapp", data=b"payload", headers=headers)
    await assert_error(response, "invalid")
    receiver.upload.assert_not_called()


@pytest.mark.parametrize("final", [False, True])
async def test_chunk_forwards_raw_request_stream_and_typed_headers(client, receiver, final):
    first_read = asyncio.Event()
    chunks = [b"\x00\xffbinary", b"\x80second"]
    raw = b"".join(chunks)
    digest = hashlib.sha256(raw).hexdigest()
    receipt = {"app_name": "myapp", "bytes": len(raw), "sha256": digest, "complete": True}
    result = {"ok": True, "version": 4, "next_index": 3, "receipt": receipt if final else None}
    seen = bytearray()

    async def upload(session, name, stream, **kwargs):
        assert (session, name) == (SESSION, "myapp")
        assert stream is request.body
        assert kwargs == {"index": 2, "final": final, "archive_bytes": len(raw),
                          "archive_sha256": digest, "owner_token": OWNER}
        assert type(kwargs["index"]) is int
        assert type(kwargs["archive_bytes"]) is int
        assert type(kwargs["final"]) is bool
        async for chunk in stream:
            seen.extend(chunk)
            first_read.set()
        return result

    receiver.upload.side_effect = upload
    headers = {**chunk_headers(), "X-Chunk-Index": "2", "X-Chunk-Final": str(int(final)),
               "X-Archive-Bytes": str(len(raw)), "X-Archive-SHA256": digest}
    async with client.request(RECEIVE + f"chunk/{SESSION}/myapp", method="POST", headers=headers) as connection:
        await connection.send(chunks[0])
        # Receiver sees bytes before the full request arrives: no HTTP buffering/decode.
        await asyncio.wait_for(first_read.wait(), 2)
        await connection.send(chunks[1])
        await connection.send_complete()
    response = await connection.as_response()
    assert response.status_code == 200
    assert await response.get_json() == result
    assert bytes(seen) == raw
    receiver.upload.assert_awaited_once()


@pytest.mark.parametrize("code", ["busy", "auth", "session", "sequence", "failed", "timeout"])
async def test_receiver_errors_translated_without_releasing_receiver_lock(client, receiver, code):
    backup_app.op_lock.try_acquire(OpKind.MIGRATION)
    receiver.finalize.side_effect = migration.MigrationError(code)
    response = await client.post(RECEIVE + "finalize", json=payload("finalize"), headers=AUTH)
    await assert_error(response, code)
    assert backup_app.op_lock.migration_running


async def test_finalize_response_retains_receiver_lock_and_pending_result(client, receiver):
    backup_app.op_lock.try_acquire(OpKind.MIGRATION)
    result = {"ok": True, "version": 4, "phase": "finalizing", "result": None, "session_id": SESSION}
    receiver.finalize.return_value = result
    for _ in range(2):
        response = await client.post(RECEIVE + "finalize", json=payload("finalize"), headers=AUTH)
        assert response.status_code == 200
        assert await response.get_json() == result
        assert backup_app.op_lock.migration_running
    assert receiver.finalize.await_count == 2


@pytest.mark.parametrize("failure", ["unauthorized", "connection"])
async def test_invalid_owner_rejected_by_real_receiver_without_private_echo(client, mock_http, caplog, failure):
    secret = "private-upstream-auth-detail"

    def upstream(req):
        assert req.headers["Authorization"] == f"Bearer {OWNER}"
        if failure == "connection":
            raise httpx.ConnectError(f"{secret} {OWNER}", request=req)
        return httpx.Response(403, json={"error": f"{secret} {OWNER}"})

    requests, _ = mock_http(upstream)
    response = await client.get(RECEIVE + "capabilities", headers=AUTH)
    await assert_error(response, "auth")
    assert [(req.method, req.url.path) for req in requests] == [("POST", "/api/app-definitions/parse")]
    assert not backup_app.op_lock.busy
    public = caplog.text + await response.get_data(as_text=True)
    assert OWNER not in public and secret not in public


async def test_preflight_exception_is_sanitized_and_lock_released(client, monkeypatch, caplog):
    monkeypatch.setattr(migration.MigrationReceiver, "_authenticate", AsyncMock())
    secret = "private-bundle-sentinel"
    body = payload("start")
    body["bundle"]["definitions"]["platform_api_tokens"][0]["name"] = secret
    recovery = MagicMock()
    recovery.progress = {"phase": "preflighting"}
    recovery.summary = {"ok": False}
    recovery.preflight = AsyncMock(side_effect=RuntimeError(f"{OWNER} {secret}"))
    monkeypatch.setattr(migration, "RecoverySession", MagicMock(return_value=recovery))
    response = await client.post(RECEIVE + "start", json=body, headers=AUTH)
    await assert_error(response, "failed")
    assert not backup_app.op_lock.busy
    recovery.preflight.assert_awaited_once()
    status = await client.get("/api/migration/status")
    public = await response.get_data(as_text=True) + await status.get_data(as_text=True) + caplog.text
    assert OWNER not in public and secret not in public and "token_hash" not in public


async def test_reclaim_delegates_to_receiver_instead_of_releasing_lock(client, receiver):
    backup_app.op_lock.try_acquire(OpKind.MIGRATION)
    await backup_app._reclaim_abandoned_migration()
    receiver.expire_stale.assert_awaited_once_with()
    assert backup_app.op_lock.migration_running


async def test_source_retains_one_job_reserves_lock_and_does_no_preflight(client, monkeypatch):
    started, finish = asyncio.Event(), asyncio.Event()
    seen = {}

    async def source(**kwargs):
        seen.update(kwargs)
        assert kwargs["lock_acquired"] is True
        assert kwargs["lock"].migration_running
        started.set()
        try:
            await finish.wait()
        finally:
            kwargs["lock"].release(OpKind.MIGRATION)

    job = AsyncMock(side_effect=source)
    monkeypatch.setattr(migration, "run_direct_push", job)
    # These running apps must not cause a handler-side rejection or preflight.
    apps = AsyncMock(return_value={"myapp": {"status": "running"}})
    capture = AsyncMock(side_effect=AssertionError("handler captured configuration"))
    network = MagicMock(side_effect=AssertionError("handler made a network request"))
    monkeypatch.setattr(backup_app, "_get_router_apps", apps)
    monkeypatch.setattr(backup_app, "capture_configuration", capture)
    monkeypatch.setattr("httpx.AsyncClient", network)
    body = {"target_url": "https://target.example/", "target_token": TARGET, "apps": ["myapp"]}
    task = None
    try:
        response = await client.post("/api/migration/push", json=body, headers=AUTH)
        assert response.status_code == 200
        assert (await response.get_json())["ok"] is True
        assert backup_app.op_lock.migration_running
        assert len(backup_app._background_tasks) == 1
        task = next(iter(backup_app._background_tasks))
        await asyncio.wait_for(started.wait(), 2)
        assert not task.done()
        assert seen == {
            "target_url": "https://target.example", "target_token": TARGET, "selected_apps": ["myapp"],
            "lock": backup_app.op_lock, "all_app_data": backup_app.ALL_APP_DATA,
            "work_dir": backup_app.APP_DATA_DIR / ".migration", "router_url": backup_app.ROUTER_URL,
            "app_token": backup_app.APP_TOKEN, "owner_token": OWNER,
            "backup_app_name": backup_app.APP_NAME, "lock_acquired": True,
        }
        second = await client.post("/api/migration/push", json=body, headers=AUTH)
        assert second.status_code == 409
        assert (await second.get_json())["ok"] is False
        job.assert_awaited_once()
        assert backup_app._background_tasks == {task}
        apps.assert_not_called()
        capture.assert_not_called()
        network.assert_not_called()
        public = await response.get_data(as_text=True) + await second.get_data(as_text=True)
        assert OWNER not in public and TARGET not in public
    finally:
        finish.set()
        if task is not None:
            await asyncio.wait_for(task, 2)
    await asyncio.sleep(0)  # retained-task completion callback
    assert not backup_app.op_lock.busy
    assert not backup_app._background_tasks


@pytest.mark.parametrize("body", [
    {}, {"target_url": "https://target.example"}, {"target_token": TARGET},
    {"target_url": [], "target_token": TARGET},
    {"target_url": "https://target.example", "target_token": 42},
    {"target_url": "https://target.example", "target_token": TARGET, "extra": OWNER},
    {"target_url": "https://target.example", "target_token": TARGET, "apps": []},
    {"target_url": "https://target.example", "target_token": TARGET, "apps": {}},
])
async def test_invalid_source_payload_does_not_schedule_or_acquire(client, monkeypatch, body):
    job = AsyncMock()
    monkeypatch.setattr(migration, "run_direct_push", job)
    response = await client.post("/api/migration/push", json=body, headers=AUTH)
    assert response.status_code == 400
    assert (await response.get_json())["ok"] is False
    assert not backup_app.op_lock.busy
    job.assert_not_called()
    assert TARGET not in await response.get_data(as_text=True)


@pytest.mark.parametrize("verified", [False, True])
async def test_source_requires_caller_owner_authority(client, monkeypatch, verified):
    # A configured token proves what this app may do unattended. It never
    # authorizes a caller, so neither an absent header nor one the router does
    # not confirm as owner may start a job that exports definitions and
    # API-key verifiers to a URL.
    monkeypatch.setattr(backup_app, "ROUTER_API_TOKEN", OWNER)
    monkeypatch.setattr(backup_app, "_caller_is_owner", AsyncMock(return_value=OWNER if verified else None))
    job = AsyncMock()
    monkeypatch.setattr(migration, "run_direct_push", job)
    response = await client.post(
        "/api/migration/push", json={"target_url": "https://target.example", "target_token": TARGET}, headers=AUTH)
    if verified:
        assert response.status_code == 200
        job.assert_awaited_once()
        # The retained job, not the handler, owns the reserved lock.
        assert backup_app.op_lock.migration_running
        backup_app.op_lock.release(OpKind.MIGRATION)
        return
    assert response.status_code == 401
    body = await response.get_json()
    assert body["ok"] is False
    assert "Owner authorization required" in body["error"]
    job.assert_not_called()
    assert not backup_app.op_lock.busy
    assert not backup_app._background_tasks
    assert OWNER not in await response.get_data(as_text=True)


# ---------------------------------------------------------------------------
# Incoming-migration acknowledgment and the gates it owns
# ---------------------------------------------------------------------------


async def test_acknowledge_never_falls_back_to_the_configured_owner_token(client, receiver, monkeypatch):
    monkeypatch.setattr(backup_app, "ROUTER_API_TOKEN", OWNER)
    response = await client.post("/api/migration/acknowledge", json={})
    assert response.status_code == 401
    assert (await response.get_json())["ok"] is False
    receiver.acknowledge.assert_not_called()


async def test_acknowledge_uses_the_callers_owner_token(client, receiver):
    result = {"ok": True, "phase": "incomplete", "needs_attention": False, "acknowledged": True,
              "retained_sessions": [SESSION]}
    receiver.acknowledge.return_value = result
    response = await client.post("/api/migration/acknowledge", json={}, headers=AUTH)
    assert response.status_code == 200
    assert await response.get_json() == result
    receiver.acknowledge.assert_awaited_once_with(owner_token=OWNER)


async def test_acknowledge_without_journal_reports_a_safe_success(client, receiver):
    receiver.acknowledge.return_value = None
    response = await client.post("/api/migration/acknowledge", json={}, headers=AUTH)
    assert response.status_code == 200
    assert await response.get_json() == {"ok": True, "needs_attention": False}


async def test_receiver_errors_translate_and_keep_attention(client, receiver):
    receiver.acknowledge.side_effect = migration.MigrationError("busy")
    response = await client.post("/api/migration/acknowledge", json={}, headers=AUTH)
    await assert_error(response, "busy")


async def test_incoming_attention_blocks_backups_and_pushes(client, receiver, monkeypatch):
    receiver.needs_attention = True
    job = AsyncMock()
    monkeypatch.setattr(migration, "run_direct_push", job)
    backup = await client.post("/api/backup", json={})
    assert backup.status_code == 409
    assert (await backup.get_json())["error"] == (
        "Inspect and acknowledge the interrupted incoming migration before running another backup."
    )
    push = await client.post("/api/migration/push", json={
        "target_url": "https://target.example", "target_token": TARGET, "apps": ["myapp"]}, headers=AUTH)
    assert push.status_code == 409
    assert (await push.get_json())["error"] == (
        "Inspect and acknowledge the interrupted incoming migration before migrating again."
    )
    assert not backup_app.op_lock.busy
    job.assert_not_called()
    assert not backup_app._background_tasks


async def _assert_reserved_backup_stops_before_capturing():
    # The reserved lock must be released, and the capture must never start: an
    # unconfigured repository would otherwise let this pass for the wrong
    # reason, so reading configuration at all is a failure.
    patch = pytest.MonkeyPatch()
    patch.setattr(
        backup_app, "load_config",
        lambda: pytest.fail("an attention gate must stop before reading configuration"))
    try:
        assert backup_app.op_lock.try_acquire(OpKind.BACKUP) is None
        assert not await backup_app.run_backup(lock_acquired=True)
        assert not backup_app.op_lock.busy
    finally:
        patch.undo()


async def test_reserved_backup_releases_its_lock_when_incoming_attention(client, receiver):
    receiver.needs_attention = True
    await _assert_reserved_backup_stops_before_capturing()


async def test_unreadable_attention_records_block_backups_instead_of_raising(monkeypatch):
    # The attention records live on disk; a filesystem failure must read as a
    # blocked backup, not escape through the scheduled path.
    def failing():
        raise OSError("synthetic filesystem failure")

    monkeypatch.setattr(backup_app, "_receiver", failing)
    assert await backup_app.run_backup() is False
    assert backup_app.op_lock.busy is False
    # A reserved operation lock must also be released when the records are unreadable.
    assert backup_app.op_lock.try_acquire(OpKind.BACKUP) is None
    assert await backup_app.run_backup(lock_acquired=True) is False
    assert backup_app.op_lock.busy is False


async def test_scheduler_survives_transient_failures(monkeypatch):
    attempts = []
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) > 3:
            raise asyncio.CancelledError

    async def failing_backup(*args, **kwargs):
        attempts.append("backup")
        if attempts.count("backup") == 1:
            raise RuntimeError("synthetic backup failure")
        raise asyncio.CancelledError

    def flaky_config():
        attempts.append("config")
        if len([a for a in attempts if a == "config"]) == 1:
            raise OSError("synthetic config failure")
        return {"interval_seconds": 1, "repo": "s3:https://example.invalid/repo"}

    monkeypatch.setattr(backup_app, "load_config", flaky_config)
    monkeypatch.setattr(backup_app, "get_last_backup", lambda: None)
    monkeypatch.setattr(backup_app, "run_backup", failing_backup)
    monkeypatch.setattr(backup_app.asyncio, "sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        await backup_app.scheduler_loop()
    # The failed read and the failed backup each returned to the loop, which then
    # reached a second scheduled backup attempt.
    assert 30 in sleeps
    assert attempts.count("backup") == 2
    assert attempts.count("config") >= 2


async def test_incomplete_local_restore_blocks_outgoing_push(client, monkeypatch):
    # Source preflight inspects router state, not local tree contents, so this
    # gate must exist before any capture of uncertain app data.
    monkeypatch.setattr(backup_app, "_restore_needs_attention", True)
    job = AsyncMock()
    monkeypatch.setattr(migration, "run_direct_push", job)
    response = await client.post("/api/migration/push", json={
        "target_url": "https://target.example", "target_token": TARGET, "apps": ["myapp"]}, headers=AUTH)
    assert response.status_code == 409
    assert (await response.get_json())["error"] == (
        "Retry or acknowledge the incomplete recovery before migrating this instance."
    )
    assert not backup_app.op_lock.busy
    assert not backup_app._background_tasks
    job.assert_not_called()


# ---------------------------------------------------------------------------
# Outgoing cutover intent: attention gate, status and acknowledgment
# ---------------------------------------------------------------------------


@pytest.fixture
def source(monkeypatch):
    mock = MagicMock(spec=migration.SourceRecoveryRecord)
    # Parent gates read this flag; keep the default explicit rather than a truthy mock.
    mock.needs_attention = False
    mock.journal_status = None
    monkeypatch.setattr(backup_app.migration, "source_recovery", mock)
    return mock


async def test_startup_names_an_unusable_private_migration_directory(client, tmp_path, monkeypatch, caplog):
    import snapshot_configuration as snapshots

    monkeypatch.setattr(snapshots, "CONFIGURATION_FILE", tmp_path / "metadata" / "configuration.json")
    monkeypatch.setattr(migration, "initialize_source_recovery",
                        MagicMock(side_effect=migration_data.DataError()))
    monkeypatch.setattr(backup_app, "_restic_unlock_if_stale", AsyncMock())
    with caplog.at_level("ERROR"):
        with pytest.raises(migration_data.DataError):
            await backup_app.startup()
    # A private-directory failure is named with the requirement, not left as a
    # bare data error from the staging helpers.
    assert any("Outgoing migration state unavailable" in record.message
               and "owned by this app" in record.message for record in caplog.records), caplog.text
    assert any(str(backup_app.APP_DATA_DIR / ".migration") in record.message
               for record in caplog.records), caplog.text


async def test_startup_publishes_source_recovery_before_the_scheduler(client, tmp_path, monkeypatch):
    import snapshot_configuration as snapshots

    calls = []
    record = MagicMock(spec=migration.SourceRecoveryRecord)
    record.needs_attention = False

    def initialize(**kwargs):
        calls.append(("source_recovery", kwargs))
        return record

    async def scheduler():
        calls.append(("scheduler", {}))

    monkeypatch.setattr(snapshots, "CONFIGURATION_FILE", tmp_path / "metadata" / "configuration.json")
    monkeypatch.setattr(migration, "initialize_source_recovery", initialize)
    monkeypatch.setattr(backup_app, "scheduler_loop", scheduler)
    monkeypatch.setattr(backup_app, "_restic_unlock_if_stale", AsyncMock())
    await backup_app.startup()
    # An interrupted outgoing migration must block capture from the first request.
    await asyncio.sleep(0)
    assert [name for name, _ in calls] == ["source_recovery", "scheduler"]
    _, kwargs = calls[0]
    assert kwargs["lock"] is backup_app.op_lock
    assert kwargs["all_app_data"] == backup_app.ALL_APP_DATA
    assert kwargs["work_dir"] == backup_app.APP_DATA_DIR / ".migration"
    assert kwargs["router_url"] == backup_app.ROUTER_URL
    assert kwargs["backup_app_name"] == backup_app.APP_NAME


async def test_outgoing_attention_blocks_backups_and_pushes(client, source, monkeypatch):
    source.needs_attention = True
    job = AsyncMock()
    monkeypatch.setattr(migration, "run_direct_push", job)
    backup = await client.post("/api/backup", json={})
    assert backup.status_code == 409
    assert (await backup.get_json())["error"] == (
        "Inspect and acknowledge the interrupted outgoing migration before running another backup."
    )
    push = await client.post("/api/migration/push", json={
        "target_url": "https://target.example", "target_token": TARGET, "apps": ["myapp"]}, headers=AUTH)
    assert push.status_code == 409
    assert (await push.get_json())["error"] == (
        "Inspect and acknowledge the interrupted outgoing migration before migrating again."
    )
    assert not backup_app.op_lock.busy
    job.assert_not_called()
    assert not backup_app._background_tasks


async def test_reserved_backup_releases_its_lock_for_outgoing_attention(client, source):
    source.needs_attention = True
    await _assert_reserved_backup_stops_before_capturing()


async def test_outgoing_status_is_exposed_without_credentials(client, source):
    source.journal_status = {
        "version": 1, "phase": "interrupted", "ok": False, "needs_attention": True,
        "acknowledged": False, "session_id": SESSION, "selected_apps": ["myapp"],
        "apps_before": [{"name": "myapp", "app_id": "app-one", "status": "running"}],
        "restart_pending": [{"name": "other", "app_id": "app-two"}],
    }
    response = await client.get("/api/migration/status")
    body = await response.get_json()
    assert body["source_recovery"] == source.journal_status
    serialized = await response.get_data(as_text=True)
    assert OWNER not in serialized and TARGET not in serialized


async def test_source_acknowledge_never_falls_back_to_the_configured_owner_token(client, source, monkeypatch):
    monkeypatch.setattr(backup_app, "ROUTER_API_TOKEN", OWNER)
    response = await client.post("/api/migration/source-acknowledge", json={})
    assert response.status_code == 401
    assert (await response.get_json())["ok"] is False
    source.acknowledge.assert_not_awaited()


async def test_source_acknowledge_uses_the_callers_owner_token(client, source):
    snapshot = {"phase": "interrupted", "needs_attention": False, "acknowledged": True}
    source.acknowledge.return_value = snapshot
    response = await client.post("/api/migration/source-acknowledge", json={}, headers=AUTH)
    assert response.status_code == 200
    assert await response.get_json() == snapshot
    source.acknowledge.assert_awaited_once_with(owner_token=OWNER)


async def test_source_acknowledge_without_a_record_reports_a_safe_success(client):
    assert backup_app.migration.source_recovery is None
    response = await client.post("/api/migration/source-acknowledge", json={}, headers=AUTH)
    assert response.status_code == 200
    assert await response.get_json() == {"ok": True, "needs_attention": False, "journal_status": None}


async def test_source_acknowledge_errors_translate_and_keep_attention(client, source):
    source.acknowledge.side_effect = migration.MigrationError("busy")
    response = await client.post("/api/migration/source-acknowledge", json={}, headers=AUTH)
    await assert_error(response, "busy")
    assert source.needs_attention is False
