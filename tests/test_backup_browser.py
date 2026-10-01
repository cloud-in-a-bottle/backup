"""Real Quart/Hypercorn pages in Chromium; expensive operations are route-mocked.

Missing Chromium or axe is an error, including in CI. No live platform, repository,
credentials or provisioned instances are used. Assertions inspect DOM, not images.
"""

from __future__ import annotations

import asyncio
import copy
import os
import socket
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from hypercorn.asyncio import serve
from hypercorn.config import Config
from playwright.async_api import async_playwright, expect

import app as backup_app
from operations import OperationLock


SNAPSHOT = "a" * 64
LIMITED = "b" * 64
LEGACY = "c" * 64
PRIVATE = "PRIVATE-CANARY-must-not-render"
OWNER = "owner-token-private-sentinel"
UNUSUAL_DIR = "drafts [v1] & 'review' 📝"

@pytest.fixture
async def ui_server(tmp_path, monkeypatch):
    own = tmp_path / "app_data" / "backup"
    own.mkdir(parents=True)
    temporary = tmp_path / "app_temp_data"
    temporary.mkdir()
    for name, value in {
        "APP_DATA_DIR": own, "ALL_APP_DATA": own.parent,
        "APP_TEMP_DATA": temporary, "APP_ARCHIVE": tmp_path / "archive",
        "CONFIG_DIR": own, "CONFIG_FILE": own / "config.json",
        "DB_FILE": own / "backups.db", "RESTIC_REPO_DIR": own / "repository",
        "BACKUP_ROOTS": (own.parent, temporary), "BACKUP_EXCLUDES": (own,),
        "op_lock": OperationLock(),
    }.items():
        monkeypatch.setattr(backup_app, name, value)
    # Start the actual application without scheduler/restic/router startup effects.
    monkeypatch.setattr(backup_app.app, "before_serving_funcs", [])
    monkeypatch.setattr(backup_app.app, "after_serving_funcs", [])
    backup_app.init_db()
    backup_app.save_config({**backup_app.DEFAULT_CONFIG, "repo": str(own / "repository")})
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    config = Config()
    config.bind = [f"127.0.0.1:{port}"]
    config.accesslog = None
    config.errorlog = None
    stop = asyncio.Event()
    task = asyncio.create_task(serve(backup_app.app, config, shutdown_trigger=stop.wait))
    url = f"http://127.0.0.1:{port}"
    try:
        async with httpx.AsyncClient() as client:
            for _ in range(100):
                if task.done():
                    task.result()
                try:
                    response = await client.get(url + "/health")
                    if response.status_code == 200:
                        break
                except httpx.ConnectError:
                    pass
                await asyncio.sleep(0.02)
            else:
                pytest.fail("Isolated browser server did not start")
        yield url
    finally:
        stop.set()
        await asyncio.wait_for(task, 10)


def recovery(*, ok=False, runtime=True):
    return {
        "ok": ok, "phase": "complete" if ok else "incomplete",
        "runtime_captured": runtime, "runtime_complete": runtime and ok,
        "tokens": {"expected": 3, "added": 2, "existing": 1, "confirmed": True,
                   "token_hash": PRIVATE, "name": PRIVATE},
        "providers": {"expected_defaults": 2, "restored_defaults": 2},
        "apps": [{"name": "notes", "plan_status": "existing", "status": "running",
                  "outcome": "restored" if ok else "provider_reauthorization_required",
                  "ok": ok, "warnings": []}],
        "warnings": [] if ok else ["Provider-scoped grants require manual reauthorization."],
        "definitions": {"token": PRIVATE}, "global_grants": [{"private": PRIVATE}],
    }


class MockAPI:
    def __init__(self):
        self.restore = {"running": False, "last_restore": None, "last_status": None,
                        "needs_attention": False, "progress": None}
        self.migration = {"running": False, "status": None, "log": [], "receive": None}
        self.busy = False
        self.posts = []
        self.gate = None
        self.ack_fails = False
        self.ack_gate = None
        self.restore_offline = False
        self.status_offline = False
        self.hold_status = None
        # Privileged actions need a caller token the router confirmed as owner.
        self.owner_required = False
        self.reject_owner = False
        self.auth = []

    OWNER_REQUIRED = "Owner authorization required: send a valid owner Router API token as a Bearer token."
    OWNER_ACTIONS = frozenset({
        "restore", "migration/push", "restore/acknowledge",
        "migration/acknowledge", "migration/source-acknowledge",
    })

    async def handle(self, route):
        request = route.request
        path = request.url.split("/api/", 1)[1].split("?", 1)[0]
        if request.method == "POST":
            self.posts.append((path, request.post_data_json))
            self.auth.append((path, request.headers.get("authorization")))
            if path in self.OWNER_ACTIONS and (
                    self.reject_owner or (self.owner_required and not self.auth[-1][1])):
                await route.fulfill(status=401, json={"ok": False, "error": self.OWNER_REQUIRED})
                return
            if path == "migration/acknowledge":
                if self.migration.get("receive"):
                    self.migration["receive"].update(needs_attention=False, acknowledged=True)
                await route.fulfill(json={"ok": True, "needs_attention": False})
                return
            if path == "migration/source-acknowledge":
                if self.migration.get("source_recovery"):
                    self.migration["source_recovery"].update(needs_attention=False, acknowledged=True)
                await route.fulfill(json={"ok": True, "needs_attention": False})
                return
            if path in {"restore", "migration/push"} and self.gate:
                await self.gate.wait()
            if path == "restore":
                self.restore.update(running=True, last_status=None, needs_attention=False,
                                    progress={"phase": "staging", "snapshot": SNAPSHOT})
            elif path == "migration/push":
                self.migration.update(running=True, status={"phase": "preflighting", "progress": 0})
            elif path == "restore/acknowledge":
                if self.ack_gate:
                    await self.ack_gate.wait()
                if self.ack_fails:
                    await route.fulfill(status=500, json={"ok": False})
                    return
                self.restore.update(needs_attention=False, progress={"phase": "acknowledged"})
            await route.fulfill(json={"ok": True})
            return
        if path == "events":
            await route.abort()
            return
        if path == "restore/status" and self.restore_offline:
            await route.abort()
            return
        if path == "status" and self.status_offline:
            await route.abort()
            return
        query = parse_qs(urlsplit(request.url).query)
        snapshot_path = query.get("path", [""])[0]
        tree = {
            "": [{"path": "data", "is_dir": True}] + (
                [{"path": "tmp", "is_dir": True}] if query.get("snapshot") != [LEGACY] else []),
            "data": [{"path": "app_data", "is_dir": True}, {"path": "app_temp_data", "is_dir": True}],
            "data/app_data": [{"path": UNUSUAL_DIR, "is_dir": True}],
            "data/app_data/" + UNUSUAL_DIR: [{"path": "notes.json", "is_dir": False, "size": 2}],
            "tmp": [{"path": "bottle-backup-configuration", "is_dir": True}],
            "tmp/bottle-backup-configuration": [{"path": "configuration.json", "is_dir": False, "size": 123}],
        }
        responses = {
            "status": {"busy": self.busy, "running": False},
            "restore/status": self.restore,
            "migration/status": self.migration,
            "snapshots": {"ok": True, "repo_ok": True, "snapshots": [
                {"id": ident, "short_id": ident[:8], "time": "2026-09-29T10:00:00Z",
                 "hostname": "source.example", "has_configuration": conf, "has_runtime": runtime}
                for ident, conf, runtime in [(SNAPSHOT, True, True), (LIMITED, True, False), (LEGACY, False, False)]
            ]},
            "history": {"ok": True, "history": []},
            "repo/stats": {"ok": True, "stats": {}},
            "apps-status": {"ok": True, "apps": {"notes": {"status": "running"}, "secrets": {"status": "stopped"}}},
            "snapshot/files": {"ok": True, "files": tree.get(snapshot_path, [])},
        }
        if path not in responses:
            await route.fulfill(status=404, json={"ok": False})
            return
        response = copy.deepcopy(responses[path])
        if self.hold_status and path == self.hold_status[0]:
            _, captured, release = self.hold_status
            self.hold_status = None
            captured.set()
            await release.wait()
        await route.fulfill(json=response)


@pytest.fixture
async def browser_ui(ui_server):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        context = await browser.new_context()
        page = await context.new_page()
        api = MockAPI()
        errors = []
        page.on("pageerror", lambda error: errors.append(f"{error.name}: {error.message}"))
        page.on("dialog", lambda dialog: dialog.accept())
        await page.route("**/api/**", api.handle)
        await page.goto(ui_server + "/backup/")
        await expect(page.get_by_role("button", name="Select snapshot aaaaaaaa")).to_be_visible()
        try:
            yield page, api
            assert not errors, "Browser JavaScript raised an exception"
        finally:
            if api.gate:
                api.gate.set()
            if api.ack_gate:
                api.ack_gate.set()
            await context.close()
            await browser.close()


async def select_snapshot(page, short_id="aaaaaaaa"):
    await page.get_by_role("button", name="Select snapshot " + short_id).click()


async def test_snapshot_contents_keyboard_selection_and_file_browser(browser_ui):
    page, _ = browser_ui
    await expect(page.get_by_role("columnheader", name="Recovery scope")).to_have_count(0)
    legacy = page.get_by_role("button", name="Select snapshot cccccccc")
    await legacy.focus()
    await page.keyboard.press("Enter")
    contents = page.locator("#selected-snapshot-contents")
    await expect(contents).to_have_text("Files only")
    await expect(page.locator("#selected-snapshot-scope")).to_be_hidden()
    await contents.focus()
    await page.keyboard.press("Enter")
    await expect(page.locator("#selected-snapshot-scope")).to_be_visible()
    await expect(page.locator("#selected-snapshot-scope")).to_contain_text("App definitions, API keys and app states are not included")
    await page.get_by_role("button", name="Browse", exact=True).click()
    await page.get_by_role("button", name="📁 data", exact=True).click()
    await expect(page.get_by_role("button", name="📁 app_data")).to_be_visible()
    await select_snapshot(page, "bbbbbbbb")
    await expect(contents).to_have_text("Files and app definitions")
    await expect(page.locator("#selected-snapshot-scope")).to_contain_text("were not captured")
    await select_snapshot(page)
    await expect(contents).to_have_text("Files and settings")
    await page.get_by_role("button", name="Browse", exact=True).click()
    await page.get_by_role("button", name="📁 tmp", exact=True).click()
    await page.get_by_role("button", name="📁 bottle-backup-configuration", exact=True).click()
    await expect(page.locator("#browse-body")).to_contain_text("configuration.json")
    await expect(page.locator("#browse-breadcrumb")).to_have_text("snapshot / tmp / bottle-backup-configuration")
    await page.locator("#browse-breadcrumb").get_by_role("button", name="tmp", exact=True).click()
    await expect(page.get_by_role("button", name="📁 bottle-backup-configuration", exact=True)).to_be_visible()
    await page.locator("#browse-breadcrumb").get_by_role("button", name="snapshot", exact=True).click()
    await expect(page.get_by_role("button", name="📁 data", exact=True)).to_be_visible()
    await page.get_by_role("button", name="📁 data", exact=True).click()
    await page.get_by_role("button", name="📁 app_data", exact=True).click()
    await page.get_by_role("button", name="📁 " + UNUSUAL_DIR, exact=True).click()
    await expect(page.locator("#browse-body")).to_contain_text("notes.json")


async def test_restore_acceptance_busy_and_eventual_verified_success(browser_ui):
    page, api = browser_ui
    api.gate = asyncio.Event()
    await remember_owner_token(page)
    await select_snapshot(page)
    button = page.locator("#btn-restore")
    await button.click()
    await button.dispatch_event("click")  # Even programmatic duplicate events are guarded.
    await expect(button).to_be_disabled()
    assert [path for path, _ in api.posts if path == "restore"] == ["restore"]
    api.gate.set()
    await expect(page.locator("#restore-state")).to_contain_text("Restore running")
    await expect(page.locator("#restore-state")).not_to_have_class("msg msg-ok")
    await expect(button).to_be_disabled()
    api.restore.update(running=False, last_status="success", last_restore=SNAPSHOT,
                       progress={"phase": "complete", "recovery": recovery(ok=True)})
    await expect(page.locator("#restore-state")).to_contain_text("Restore complete")
    await expect(page.locator("#restore-details")).to_be_hidden()
    await page.locator("#restore-report > summary").click()
    await expect(page.locator("#restore-details")).to_be_visible()
    await expect(page.locator("#restore-details")).to_contain_text("3 expected, 2 added, 1 already present. Import confirmed.")
    await expect(page.locator("#restore-details")).to_contain_text("notes: confirmed")
    await expect(button).to_be_enabled()


async def test_incomplete_restore_persists_safe_details_ack_failure_and_retry(browser_ui):
    page, api = browser_ui
    result = recovery()
    result["apps"][0]["name"] = '<img src=x onerror="window.injected=true">'
    result["warnings"].append({"secret": PRIVATE})
    api.restore.update(running=False, last_status="success", needs_attention=True,
                       progress={"phase": "incomplete", "snapshot": SNAPSHOT,
                                 "affected_apps": ["notes"], "affected_roots": ["app_data"],
                                 "pending_restarts": [{"name": "unaffected", "credential": PRIVATE}],
                                 "retained_stages": [{"root": "app_data", "job_id": "job123", "bundle": PRIVATE}],
                                 "recovery": result, "private": PRIVATE})
    await page.reload()
    await expect(page.locator("#restore-state")).to_contain_text("incomplete")
    await expect(page.locator("#restore-details")).to_contain_text("Pending restart: unaffected")
    await expect(page.locator("#restore-details")).to_contain_text("Retained original data: app_data (job job123)")
    await expect(page.locator("#restore-details")).to_contain_text("manual reauthorization")
    assert PRIVATE not in await page.content()
    assert await page.locator("#restore-details img").count() == 0
    assert await page.evaluate("window.injected === undefined")
    acknowledge = page.get_by_role("button", name="Acknowledge after inspection")
    api.ack_fails = True
    await acknowledge.click()
    await expect(page.locator("#manage-msg")).to_contain_text("could not be acknowledged")
    await expect(acknowledge).to_be_visible()
    api.ack_fails = False
    await acknowledge.click()
    await expect(acknowledge).to_be_hidden()
    await expect(page.locator("#restore-state")).to_contain_text("does not confirm recovery")
    # The owner token is page memory only, so a reload requires authorizing again.
    await remember_owner_token(page)
    await select_snapshot(page)
    await page.get_by_role("button", name="Restore", exact=True).click()
    await expect(page.locator("#restore-state")).to_contain_text("Restore running")
    assert [path for path, _ in api.posts if path.startswith("restore")] == [
        "restore/acknowledge", "restore/acknowledge", "restore"]


async def test_unavailable_status_never_reads_as_an_idle_system(browser_ui):
    page, api = browser_ui
    api.busy = True
    await page.reload()
    await expect(page.locator("#lock-banner")).to_be_visible()
    api.status_offline = True
    # A dropped status response must neither hide the banner nor re-enable
    # actions, and the badge must not claim "no backups yet".
    await page.evaluate("refreshLockStatusNow()")
    await expect(page.locator("#lock-banner")).to_be_visible()
    await page.evaluate("pollStatus()")
    await expect(page.locator("#status-state")).to_contain_text("status unavailable")
    await expect(page.get_by_role("button", name="Run backup now")).to_be_disabled()
    api.busy = False
    api.status_offline = False
    await page.evaluate("refreshLockStatusNow()")
    await expect(page.locator("#lock-banner")).to_be_hidden()
    await page.evaluate("pollStatus()")
    await expect(page.get_by_role("button", name="Run backup now")).to_be_enabled()


async def test_private_restore_status_is_never_rendered(browser_ui):
    page, api = browser_ui
    private = {"apps": [{"name": "notes", "api_keys": [PRIVATE]}], "platform_api_tokens": [PRIVATE]}
    api.restore.update(running=False, last_status="error: The snapshot metadata is missing, ambiguous, or unsupported.",
                       needs_attention=True, progress={"phase": "incomplete", "bundle": private,
                                                       "recovery": {"apps": [{"name": "notes", "bundle": private}]}})
    await expect(page.locator("#restore-state")).to_contain_text("Recovery incomplete")
    content = await page.content()
    assert PRIVATE not in content
    assert private["apps"][0]["api_keys"][0] not in content
    assert await page.locator("#restore-details pre, #restore-details script").count() == 0


async def test_limited_restore_and_status_loss_never_claim_full_recovery(browser_ui):
    page, api = browser_ui
    api.restore.update(last_status="success", progress={"phase": "complete", "recovery": recovery(ok=True, runtime=False)})
    await expect(page.locator("#restore-state")).to_contain_text("limited settings")
    await expect(page.locator("#restore-details")).to_contain_text("Configure these manually")
    api.restore_offline = True
    await expect(page.locator("#restore-state")).to_contain_text("Completion has not been confirmed")
    await expect(page.locator("#btn-acknowledge")).to_be_hidden()


async def test_acknowledgement_hidden_during_recovery_and_busy_blocks_actions(browser_ui):
    page, api = browser_ui
    api.restore.update(running=True, needs_attention=True, progress={"phase": "stopping"})
    api.busy = True
    await page.reload()
    await select_snapshot(page)
    await expect(page.locator("#btn-restore")).to_be_disabled()
    await expect(page.locator("#btn-acknowledge")).to_be_hidden()
    await page.get_by_role("button", name="Migrate", exact=True).click()
    await expect(page.locator("#btn-migrate")).to_be_disabled()


async def test_pending_acknowledgement_blocks_retry_until_it_finishes(browser_ui):
    page, api = browser_ui
    api.restore.update(last_status="incomplete", needs_attention=True,
                       progress={"phase": "incomplete", "recovery": recovery()})
    await select_snapshot(page)
    api.ack_gate = asyncio.Event()
    await page.get_by_role("button", name="Acknowledge after inspection").click()
    restore = page.locator("#btn-restore")
    await expect(restore).to_be_disabled()
    await restore.dispatch_event("click")
    assert [path for path, _ in api.posts] == ["restore/acknowledge"]
    api.ack_gate.set()
    await expect(page.locator("#restore-state")).to_contain_text("acknowledged")
    await expect(restore).to_be_enabled()


async def prepare_migration(page):
    await page.get_by_role("button", name="Migrate", exact=True).click()
    await page.get_by_label("Destination instance URL").fill("https://destination.example")
    await page.get_by_label("Destination API token", exact=True).fill("synthetic-destination-token")
    await expect(page.locator("#btn-migrate")).to_be_enabled()


async def test_migration_push_only_preserves_selection_and_busy_until_terminal(browser_ui):
    page, api = browser_ui
    await prepare_migration(page)
    await page.get_by_role("checkbox", name="secrets stopped").uncheck()
    api.gate = asyncio.Event()
    button = page.locator("#btn-migrate")
    await button.click()
    await button.dispatch_event("click")
    await expect(button).to_be_disabled()
    assert api.posts == [("migration/push", {"target_url": "https://destination.example", "target_token": "synthetic-destination-token", "apps": ["notes"]})]
    api.gate.set()
    await expect(page.locator("#mig-state")).to_contain_text("Migration running")
    await expect(page.get_by_label("Destination API token", exact=True)).to_have_value("")
    api.migration.update(running=True, status={"phase": "done", "progress": 100})
    await expect(page.locator("#mig-phase")).to_have_text("done")
    await expect(page.locator("#mig-state")).to_contain_text("not yet confirmed")
    await expect(button).to_be_disabled()
    api.migration.update(running=False, status={"phase": "failed", "progress": 100},
                         receive={"phase": "incomplete", "result": recovery()}, log=["Activation incomplete", {"token": PRIVATE}])
    await expect(page.locator("#mig-state")).to_contain_text("incomplete or failed")
    await expect(page.locator("#mig-details")).to_contain_text("3 expected, 2 added, 1 already present")
    assert PRIVATE not in await page.content()
    await expect(button).to_be_enabled()
    assert [path for path, _ in api.posts] == ["migration/push"]


async def test_migration_validation_and_confirmed_completion(browser_ui):
    page, api = browser_ui
    await prepare_migration(page)
    await page.get_by_label("Destination instance URL").fill("https://user:password@destination.example")
    await page.locator("#btn-migrate").click()
    await expect(page.locator("#mig-msg")).to_contain_text("without credentials")
    assert not api.posts
    await page.get_by_role("button", name="Deselect all").click()
    await expect(page.locator("#btn-migrate")).to_be_disabled()
    api.migration.update(running=False, status={"phase": "done", "progress": 100})
    await expect(page.locator("#mig-state")).to_contain_text("cleanup confirmed")
    await expect(page.locator("#mig-msg")).to_contain_text("without credentials")


async def test_live_receiver_details_and_incomplete_incoming_after_outgoing_success(browser_ui):
    page, api = browser_ui
    await page.get_by_role("button", name="Migrate", exact=True).click()
    api.migration.update(running=True, status={"phase": "finalizing"},
                         receive={"phase": "finalizing", "result": None, "recovery": recovery()})
    await expect(page.locator("#mig-details")).to_contain_text("3 expected, 2 added, 1 already present")
    api.migration.update(running=False, status={"phase": "done", "progress": 100},
                         receive={"phase": "incomplete", "result": {"ok": False, "recovery": recovery()}})
    await expect(page.locator("#mig-state")).to_contain_text("incomplete or failed")
    await expect(page.locator("#mig-state")).not_to_have_class("msg msg-ok")
    await expect(page.locator("#mig-details")).to_contain_text("manual reauthorization")


async def test_migration_cleanup_and_preflight_failure_guidance(browser_ui):
    page, api = browser_ui
    await prepare_migration(page)
    api.migration.update(running=False, status={"phase": "failed", "error": "Migration requires protocol v5. Upgrade both backup apps."})
    await expect(page.locator("#mig-details")).to_contain_text("Upgrade both backup apps")
    api.migration["status"] = {
        "phase": "failed", "error": PRIVATE,
        "source_recovery": {**recovery(), "tokens": {"expected": 3, "added": 0, "existing": 0, "confirmed": False},
                            "paused_apps": [
                                {"name": "mail", "selected": False, "previous_status": "running", "stop": "confirmed", "restart": "failed", "private": PRIVATE},
                                {"name": "notes", "selected": True, "previous_status": "running", "stop": "confirmed", "restart": "pending"},
                            ]},
    }
    await expect(page.locator("#mig-details")).to_contain_text("Paused app: mail / previous state: running / stop: confirmed / restart: failed")
    await expect(page.locator("#mig-details")).not_to_contain_text("Import not confirmed")
    await expect(page.locator("#mig-details")).not_to_contain_text("Runtime recovery is incomplete")
    await expect(page.locator("#mig-details")).not_to_contain_text("Paused app: notes")
    assert PRIVATE not in await page.content()


async def test_incoming_retained_data_survives_interruption_and_later_success(browser_ui):
    page, api = browser_ui
    await page.get_by_role("button", name="Migrate", exact=True).click()
    session_id = "d" * 64
    api.migration.update(status={"phase": "error"}, receive={
        "phase": "interrupted", "session_id": session_id, "retained_sessions": [session_id],
        "needs_attention": True, "acknowledged": False, "result": None, "bundle": PRIVATE,
    })
    await expect(page.locator("#mig-details")).to_contain_text("Receive session: " + session_id)
    await expect(page.locator("#mig-details")).to_contain_text("requires owner inspection")
    api.migration["receive"].update(needs_attention=False, acknowledged=True)
    await expect(page.locator("#mig-details")).to_contain_text("does not confirm recovery or remove retained data")
    api.migration.update(status={"phase": "done"}, receive={
        "phase": "complete", "session_id": "e" * 64, "retained_sessions": [session_id],
        "needs_attention": False, "result": recovery(ok=True),
    })
    await expect(page.locator("#mig-state")).to_contain_text("Migration complete")
    await expect(page.locator("#mig-details")).to_contain_text("Retained migration data session: " + session_id)
    assert PRIVATE not in await page.content()


@pytest.mark.parametrize("operation", ["restore", "migration"])
async def test_delayed_previous_success_cannot_overwrite_a_new_job(browser_ui, operation):
    page, api = browser_ui
    if operation == "restore":
        api.restore.update(last_status="success", progress={"phase": "complete", "recovery": recovery(ok=True)})
        button = page.locator("#btn-restore")
        state = page.locator("#restore-state")
        await remember_owner_token(page)
        await select_snapshot(page)
    else:
        api.migration.update(status={"phase": "done", "progress": 100})
        button = page.locator("#btn-migrate")
        state = page.locator("#mig-state")
        await prepare_migration(page)
    await expect(state).to_contain_text("complete")
    captured, release = asyncio.Event(), asyncio.Event()
    api.hold_status = (operation + "/status", captured, release)
    api.gate = asyncio.Event()
    try:
        await asyncio.wait_for(captured.wait(), 5)
        await button.click()
        await expect(state).to_contain_text("Submitting")
        await expect(state).not_to_have_class("msg msg-ok")
        await expect(button).to_be_disabled()
        api.gate.set()
        await expect(state).to_contain_text("running")
        # Deliver the old response after the new job is visibly running.
        async with page.expect_response(lambda r: "/api/" + operation + "/status" in r.url) as old:
            release.set()
        await (await old.value).finished()
        # Allow fetch/json continuations to paint before checking for regression.
        await page.wait_for_timeout(100)
        await expect(state).to_contain_text("running")
        await expect(button).to_be_disabled()
    finally:
        release.set()


async def test_restore_preflight_guidance_suppresses_unknown_private_errors(browser_ui):
    page, api = browser_ui
    api.restore.update(last_status="error: Configure a destination Router API Token before restoring apps and API keys.")
    await expect(page.locator("#restore-details")).to_contain_text("Configure a destination Router API Token")
    api.restore.update(last_status="error: " + PRIVATE)
    await expect(page.locator("#restore-details")).to_contain_text("Check the repository connection")
    assert PRIVATE not in await page.content()


async def test_mobile_keyboard_token_navigation_save_test_and_accessibility(browser_ui, tmp_path):
    page, api = browser_ui
    axe_path = Path(os.environ.get("AXE_CORE_PATH", "node_modules/axe-core/axe.min.js"))
    assert axe_path.is_file(), "Install axe-core@4.10.3 and set AXE_CORE_PATH (see README)"
    await page.set_viewport_size({"width": 390, "height": 844})
    await page.get_by_role("button", name="Migrate", exact=True).click()
    setup = page.get_by_role("button", name="Set up local Router API Token")
    await setup.focus()
    await page.keyboard.press("Enter")
    token = page.get_by_label("Router API Token", exact=True)
    await expect(token).to_be_focused()
    assert await page.locator("#router-token-input").count() == 1
    await token.fill("synthetic-local-owner-token")
    await page.get_by_role("button", name="Save token", exact=True).click()
    await expect(token).to_have_value("")
    await expect(token).to_have_attribute("type", "password")
    await page.get_by_role("button", name="Test connection", exact=True).click()
    await expect(page.locator("#router-token-msg")).to_contain_text("Connected")
    assert [path for path, _ in api.posts] == ["config", "router/test"]
    api.restore.update(last_status="incomplete", needs_attention=True,
                       progress={"phase": "incomplete", "recovery": recovery()})
    await expect(page.locator("#restore-state")).to_contain_text("incomplete")
    assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    assert await page.evaluate("new Set([...document.querySelectorAll('[id]')].map(e => e.id)).size === document.querySelectorAll('[id]').length")
    for tab in ["Backups", "Migrate"]:
        await page.get_by_role("button", name=tab, exact=True).click()
        if tab == "Backups":
            await page.get_by_role("button", name="Clear", exact=True).hover()
        await page.add_script_tag(path=str(axe_path))
        violations = await page.evaluate("""async () => (await axe.run(document, {
            runOnly: {type: 'tag', values: ['wcag2a', 'wcag2aa', 'wcag21aa']}
        })).violations.map(v => ({id: v.id, targets: v.nodes.map(n => n.target)}))""")
        assert not violations
        await page.screenshot(path=str(tmp_path / (tab.lower() + "-mobile.png")), full_page=True)



async def remember_owner_token(page):
    await page.get_by_role("button", name="Backups", exact=True).click()
    await page.get_by_label("Router API Token").fill(OWNER)
    await page.get_by_role("button", name="Save token").click()
    await expect(page.locator("#router-token-msg")).to_contain_text("saved successfully")


async def test_restore_refused_by_the_router_changes_nothing(browser_ui):
    page, api = browser_ui
    api.reject_owner = True
    # The router rejects the caller's token, so the backend refuses before any
    # job starts and the page explains how to authorize the caller.
    await remember_owner_token(page)
    await select_snapshot(page)
    await page.locator("#btn-restore").click()
    await expect(page.locator("#manage-msg")).to_contain_text("Owner authorization required")
    assert api.restore["running"] is False
    assert api.auth[-1] == ("restore", f"Bearer {OWNER}")
    await expect(page.locator("#restore-state")).not_to_contain_text("Restore running")


async def test_configuration_restore_without_a_token_never_reaches_the_server(browser_ui):
    page, api = browser_ui
    await select_snapshot(page)
    await page.locator("#btn-restore").click()
    # The page refuses before the request, so no job can be started on a guess.
    await expect(page.locator("#manage-msg")).to_contain_text("Owner authorization required")
    assert not [path for path, _ in api.posts if path == "restore"]


async def test_restore_sends_the_remembered_caller_owner_token(browser_ui):
    page, api = browser_ui
    api.owner_required = True
    await remember_owner_token(page)
    await select_snapshot(page)
    await page.locator("#btn-restore").click()
    await expect(page.locator("#restore-state")).to_contain_text("Restore running")
    assert api.auth[-1] == ("restore", f"Bearer {OWNER}")
    # The token lives in page memory only: it is never written to the document.
    assert OWNER not in await page.content()


async def test_incoming_migration_acknowledgment_sends_owner_authority(browser_ui):
    page, api = browser_ui
    api.owner_required = True
    session_id = "d" * 64
    await page.get_by_role("button", name="Migrate", exact=True).click()
    api.migration["receive"] = {
        "phase": "interrupted", "session_id": session_id, "retained_sessions": [session_id],
        "needs_attention": True, "acknowledged": False, "result": None,
    }
    await page.locator("#mig-report > summary").click()
    await expect(page.get_by_role("button", name="Acknowledge incoming migration")).to_be_visible()
    await page.get_by_role("button", name="Acknowledge incoming migration").click()
    await expect(page.locator("#manage-msg")).to_contain_text("Owner authorization required")
    assert api.auth[-1] == ("migration/acknowledge", None)
    assert api.migration["receive"]["needs_attention"] is True
    await remember_owner_token(page)
    await page.get_by_role("button", name="Migrate", exact=True).click()
    await page.get_by_role("button", name="Acknowledge incoming migration").click()
    await expect(page.locator("#mig-details")).to_contain_text("does not confirm recovery or remove retained data")
    assert api.auth[-1] == ("migration/acknowledge", f"Bearer {OWNER}")


async def test_outgoing_migration_attention_blocks_push_until_acknowledged(browser_ui):
    page, api = browser_ui
    api.owner_required = True
    await page.get_by_role("button", name="Migrate", exact=True).click()
    api.migration["source_recovery"] = {
        "version": 1, "phase": "interrupted", "ok": False, "needs_attention": True,
        "acknowledged": False, "session_id": "e" * 64, "selected_apps": ["notes"],
        "apps_before": [{"name": "notes", "app_id": "app-one", "status": "running"}],
        "restart_pending": ["other"],
    }
    details = page.locator("#mig-details")
    await expect(page.locator("#mig-state")).to_contain_text("See details")
    await expect(details).to_contain_text("Outgoing migration from this instance requires owner inspection")
    await expect(details).to_contain_text("Backups and new migrations stay blocked")
    await expect(details).to_contain_text("Affected apps: notes")
    await expect(details).to_contain_text("Pending restart: other")
    await expect(details).to_contain_text("Destination apps before this migration:")
    await expect(details).to_contain_text("notes / previous state: running")
    await page.locator("#mig-report > summary").click()
    await expect(page.locator("#btn-acknowledge-source-migration")).to_be_visible()
    await page.get_by_role("button", name="Acknowledge outgoing migration").click()
    await expect(page.locator("#mig-msg")).to_contain_text("Owner authorization required")
    assert api.auth[-1] == ("migration/source-acknowledge", None)
    assert api.migration["source_recovery"]["needs_attention"] is True
    await remember_owner_token(page)
    await page.get_by_role("button", name="Migrate", exact=True).click()
    await page.get_by_role("button", name="Acknowledge outgoing migration").click()
    await expect(details).to_contain_text("does not confirm the destination and does not start apps")
    assert api.auth[-1] == ("migration/source-acknowledge", f"Bearer {OWNER}")
    await expect(page.locator("#btn-acknowledge-source-migration")).to_have_count(0)
    assert OWNER not in await page.content()
