"""Caller owner authority for privileged actions.

The configured ``router_api_token`` authorizes what this app may do unattended.
It is never proof of who is calling, because co-located containers can reach
this app directly and bypass the router's auth layer. Privileged actions
therefore require a caller token the router itself confirms as owner.
"""

from __future__ import annotations

import copy
import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import app as backup_app
import snapshot_configuration as snapshots
import configuration
from configuration import ConfigurationError
from tests.test_app_routes import client
from tests.test_configuration import mock_http
from tests.test_snapshot_configuration import OWNER_CREDENTIAL, environment, newest_snapshot

PROBE_PATH = "/api/app-definitions/parse"
PROBE_RESULT = {"schema_version": 2, "mode": "private", "apps": [], "platform_api_token_names": []}
SENTINEL = "owner-token-private-sentinel"
LEGACY_ID = "d" * 64
BEARER = {"Authorization": f"Bearer {SENTINEL}"}


def json_response(body, status=200):
    return httpx.Response(status, json=body)


async def test_no_caller_token_makes_no_request(client, mock_http, monkeypatch):
    requests, _ = mock_http(lambda request: json_response(PROBE_RESULT))
    async with backup_app.app.test_request_context("/api/restore"):
        assert await backup_app._caller_is_owner() is None
    assert requests == []


async def test_confirmed_owner_token_is_returned_verbatim(client, mock_http, monkeypatch):
    requests, options = mock_http(lambda request: json_response(PROBE_RESULT))
    async with backup_app.app.test_request_context("/api/restore", headers=BEARER):
        assert await backup_app._caller_is_owner() == SENTINEL
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST" and request.url.path == PROBE_PATH
    assert request.headers["Authorization"] == f"Bearer {SENTINEL}"
    assert json.loads(request.content) == configuration.OWNER_PROBE
    # The token travels in the header only, never in the URL or the body.
    assert SENTINEL not in str(request.url)
    assert SENTINEL.encode() not in request.content
    assert all(o.get("follow_redirects") is False for o in options)


@pytest.mark.parametrize("body", [
    {}, {"schema_version": 2, "mode": "private", "apps": []},
    {**PROBE_RESULT, "platform_api_token_names": ["router_api_token"]},
    {**PROBE_RESULT, "extra": True},
    {**PROBE_RESULT, "mode": "public"},
    [PROBE_RESULT],
])
async def test_unexpected_probe_answers_are_not_owner_authority(client, mock_http, monkeypatch, body):
    # An app token is rejected by the platform, and any answer that is not
    # exactly the owner-only private plan must fail closed rather than be
    # treated as partial success.
    mock_http(lambda request: json_response(body))
    async with backup_app.app.test_request_context("/api/restore", headers=BEARER):
        assert await backup_app._caller_is_owner() is None


@pytest.mark.parametrize("status", [401, 403, 500, 302])
async def test_failed_or_redirected_probe_is_not_owner_authority(client, mock_http, monkeypatch, status):
    mock_http(lambda request: json_response(PROBE_RESULT, status))
    async with backup_app.app.test_request_context("/api/restore", headers=BEARER):
        assert await backup_app._caller_is_owner() is None


@pytest.mark.parametrize("failure", [ConfigurationError("auth"), TimeoutError()])
async def test_transport_failures_fail_closed(client, mock_http, monkeypatch, failure):
    def dispatch(request):
        raise failure

    mock_http(dispatch)
    async with backup_app.app.test_request_context("/api/restore", headers=BEARER):
        assert await backup_app._caller_is_owner() is None


async def test_restore_route_rejects_an_unconfirmed_caller_before_reserving(client, mock_http, monkeypatch):
    monkeypatch.setattr(backup_app, "ROUTER_API_TOKEN", OWNER_CREDENTIAL)
    monkeypatch.setattr(backup_app, "_caller_is_owner", AsyncMock(return_value=None))
    started = AsyncMock(side_effect=AssertionError("restore started without owner authority"))
    monkeypatch.setattr(backup_app, "run_restore", started)
    response = await client.post("/api/restore", json={"snapshot": "a" * 64}, headers=BEARER)
    assert response.status_code == 401
    assert "Owner authorization required" in (await response.get_json())["error"]
    assert not backup_app.op_lock.busy
    started.assert_not_called()
    assert SENTINEL not in await response.get_data(as_text=True)


async def test_restore_route_never_falls_back_to_the_configured_token(client, mock_http, monkeypatch):
    monkeypatch.setattr(backup_app, "ROUTER_API_TOKEN", OWNER_CREDENTIAL)
    started = AsyncMock(side_effect=AssertionError("restore started from a configured token"))
    monkeypatch.setattr(backup_app, "run_restore", started)
    response = await client.post("/api/restore", json={"snapshot": "a" * 64})
    assert response.status_code == 200
    started.assert_awaited_once()
    # A file-only restore keeps its pre-existing behavior, but no owner token
    # is invented on the caller's behalf.
    assert started.await_args.kwargs["owner_token"] is None
    assert OWNER_CREDENTIAL not in await response.get_data(as_text=True)


async def test_restore_acknowledgment_requires_owner_authority(environment, monkeypatch):
    app = backup_app.app.test_client()
    backup_app.restore_progress = {
        "journal_version": 1, "job_id": "b" * 32, "phase": "incomplete", "needs_attention": True,
        "affected_apps": ["demo"], "affected_roots": [], "retained_stages": [], "pending_restarts": [],
    }
    backup_app._restore_needs_attention = True
    before = copy.deepcopy(backup_app.restore_progress)
    response = await app.post("/api/restore/acknowledge", json={})
    assert response.status_code == 401
    assert backup_app._restore_needs_attention
    assert backup_app.restore_progress == before

    async def owner():
        return OWNER_CREDENTIAL

    monkeypatch.setattr(backup_app, "_caller_is_owner", owner)
    granted = await app.post("/api/restore/acknowledge", json={})
    assert granted.status_code == 200
    assert not backup_app._restore_needs_attention


async def test_configuration_restore_refuses_without_caller_authority(environment, monkeypatch):
    # Worker level: even if a caller somehow bypasses the route, a
    # configuration snapshot must not be applied with the app's own configured
    # token standing in for the caller.
    data, temporary, archive, conf, capture = environment
    assert await backup_app.run_backup()
    latest = await newest_snapshot()
    snapshot = await backup_app._snapshot_for_restore(latest["id"], conf)
    assert snapshot.has_configuration
    monkeypatch.setattr(backup_app, "RecoverySession", MagicMock(
        side_effect=AssertionError("recovery started without owner authority")))
    monkeypatch.setattr(snapshots, "read_configuration", AsyncMock(
        side_effect=AssertionError("private bundle read without owner authority")))
    assert not await backup_app.run_restore(latest["id"])
    assert "owner authorization" in backup_app.restore_last_status
    assert not backup_app._restore_needs_attention


async def test_route_refuses_configuration_restore_without_caller_authority(environment, monkeypatch):
    # Route level: applying a configuration snapshot is owner-only, so a caller
    # with no token is refused before a job starts, rather than receiving
    # "Restore started" and a later failure.
    data, temporary, archive, conf, capture = environment
    assert await backup_app.run_backup()
    latest = await newest_snapshot()
    monkeypatch.setattr(backup_app, "_spawn_background", MagicMock(
        side_effect=AssertionError("restore job started without owner authority")))
    async with backup_app.app.test_client() as client:
        refused = await client.post("/api/restore", json={"snapshot": latest["id"]})
    assert refused.status_code == 401
    assert not backup_app.op_lock.busy
    # An unconfirmed owner token is refused the same way.
    async def denied():
        return None

    monkeypatch.setattr(backup_app, "_extract_bearer_token", lambda: SENTINEL)
    monkeypatch.setattr(backup_app, "_caller_is_owner", denied)
    async with backup_app.app.test_client() as unconfirmed_client:
        unconfirmed = await unconfirmed_client.post("/api/restore", json={"snapshot": latest["id"]})
    assert unconfirmed.status_code == 401
    assert not backup_app.op_lock.busy


async def test_route_still_starts_a_files_only_restore_without_a_token(environment, monkeypatch):
    # A legacy files-only snapshot keeps working for an unauthenticated caller:
    # no definitions or key verifiers are applied, so owner authority buys
    # nothing here and the request must not be blocked.
    seen = {}
    real_worker = backup_app.run_restore

    async def spy(snapshot_id, root=None, owner_token=None, *, lock_acquired=False):
        seen.update(snapshot_id=snapshot_id, root=root, owner_token=owner_token)
        return await real_worker(snapshot_id, root=root, owner_token=owner_token, lock_acquired=lock_acquired)

    legacy = snapshots.Snapshot(LEGACY_ID, ("/data/app_data",), False, False)
    monkeypatch.setattr(backup_app, "_snapshot_for_restore", AsyncMock(return_value=legacy))
    monkeypatch.setattr(backup_app, "run_restore", spy)
    monkeypatch.setattr(backup_app, "_spawn_background", lambda coro: seen.setdefault("coro", coro))
    monkeypatch.setattr(backup_app, "_reclaim_abandoned_migration", AsyncMock())
    async with backup_app.app.test_client() as client:
        accepted = await client.post("/api/restore", json={"snapshot": LEGACY_ID})
    assert accepted.status_code == 200
    coroutine = seen.pop("coro")
    # The route hands its reserved lock to the job, so the operation is owned
    # for as long as the restore runs, and the worker releases it either way.
    assert backup_app.op_lock.restore_running
    await coroutine
    assert not backup_app.op_lock.busy
    # No authority was needed or invented for a files-only restore.
    assert seen["owner_token"] is None and seen["root"] is None


async def test_an_unreadable_snapshot_never_widens_or_narrows_the_owner_check(client, monkeypatch):
    # The route cannot ask the repository what a snapshot holds, so it must
    # default to "not a configuration snapshot" and let run_restore refuse.
    # Only the worker's refusal is a security boundary, and it does not depend
    # on this lookup.
    async def unreadable(snapshot_id, conf):
        raise snapshots.SnapshotConfigurationError("Could not read the selected snapshot.")

    monkeypatch.setattr(backup_app, "_snapshot_for_restore", unreadable)
    assert await backup_app._snapshot_needs_owner("a" * 64) is False


async def test_both_owner_checks_use_the_one_shared_probe(client, mock_http):
    # The router probe is security relevant, so a second copy could drift into
    # a weaker check; both entry points must use configuration.confirm_owner.
    import migration

    assert migration._authenticate_owner.__module__ == migration.__name__
    assert configuration.confirm_owner.__module__ == "configuration"
    requests, _ = mock_http(lambda request: json_response(PROBE_RESULT))
    async with backup_app.app.test_request_context("/api/restore", headers=BEARER):
        assert await backup_app._caller_is_owner() == SENTINEL
    assert json.loads(requests[0].content) == configuration.OWNER_PROBE
    # The migration side accepts only the same answer.
    await migration._authenticate_owner(backup_app.ROUTER_URL, SENTINEL, 15.0)
    with pytest.raises(migration.MigrationError):
        mock_http(lambda request: json_response({**PROBE_RESULT, "extra": True}))
        await migration._authenticate_owner(backup_app.ROUTER_URL, SENTINEL, 15.0)
