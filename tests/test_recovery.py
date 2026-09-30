"""Stateful owner-API recovery tests, including worker and cancellation races.

The fake router implements the inspected parse/import, grant, provider and app
API contracts. It keeps destination-only data/token identities, advances deploy
workers on inventory polls, and can lose a response *after* accepting a mutation.
No assertion equates an accepted install with a ready application.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging

import httpx
import pytest

from configuration import ConfigurationError, ROUTER_PROVIDER, subset_configuration
from recovery import RecoverySession
from tests.test_configuration import (
    APP_TOKEN,
    DEFINITIONS,
    OAUTH,
    ORIGIN,
    OWNER_TOKEN,
    SECRETS,
    VERIFIER,
    app_definition,
    inventory_entry,
    make_bundle,
    mock_http,
    provider_entry,
)

# A router-served service URL that this platform build does not register, as
# seen live on the default catalog app.
INSTALLER = "github.com/cloud-in-a-bottle/cloud-in-a-bottle/services/installer"


class Router:
    def __init__(self, bundle, apps=()):
        self.bundle = copy.deepcopy(bundle)
        self.apps = {a["name"]: copy.deepcopy(a) for a in apps}
        saved = {a["name"]: a for a in bundle["definitions"]["apps"]}
        self.definitions = {name: copy.deepcopy(saved.get(name, app_definition(name))) for name in self.apps}
        self.events = []
        self.hooks = {}
        self.after = {}
        self.workers = {}
        self.deploy_states = {}
        self.next_id = 0
        self.parsed_content = None
        self.imported_content = None
        self.data_restored = False
        self.permissions = []
        self.providers = []
        self.defaults = {}
        self.tokens = [
            {"name": "destination existing name", "token_hash": VERIFIER, "expires_at": "2035-01-01T00:00:00Z"},
            {"name": "destination only", "token_hash": "f" * 64, "expires_at": None},
        ]
        self.registrations = {}
        if bundle["runtime"] is not None:
            for p in bundle["runtime"]["providers"]:
                if p["app_name"] == ROUTER_PROVIDER or p["app_name"] in self.apps:
                    self.add_provider(p["service_url"], p["app_name"], default=p["is_default"])
                self.registrations.setdefault(p["app_name"], []).append(p["service_url"])

    def add_provider(self, service, name, *, default=True):
        if (service, name) not in self.providers:
            self.providers.append((service, name))
        if default:
            self.defaults[service] = name

    def plan(self, document):
        apps = []
        for definition in document["apps"]:
            name, source = definition["name"], definition["source"]
            app = {"name": name, "source_label": source["kind"]}
            if name in self.apps:
                app.update(status="existing", app_id=self.apps[name]["app_id"])
            elif source["kind"] in {"remote", "builtin"}:
                url = (source["repo_url"] + ("@" + source["ref"] if source["ref"] else "")) if source["kind"] == "remote" else "/opt/router-bundle/apps/" + source["identifier"]
                if source["kind"] == "builtin":
                    url = "file://" + url
                app.update(status="ready", install={"repo_url": url, "app_name": name,
                           "port_overrides": {p["label"]: p["host_port"] for p in definition["port_mappings"]}})
            else:
                app["status"] = "unavailable"
            apps.append(app)
        return {"schema_version": 2, "mode": "private", "apps": apps,
                "platform_api_token_names": [t["name"] for t in document["platform_api_tokens"]]}

    def listing(self):
        for name, states in list(self.workers.items()):
            if states:
                state = states.pop(0)
                self.apps[name]["status"] = state
                self.events.append(("state", name, state))
                if state == "running":
                    for service in self.registrations.get(name, []):
                        self.add_provider(service, name, default=service not in self.defaults)
                elif state == "error":
                    self.apps[name]["error_message"] = OWNER_TOKEN + VERIFIER
            if not states:
                del self.workers[name]
        return [copy.deepcopy(self.apps[name]) for name in sorted(self.apps)]

    def catalogue(self):
        return [provider_entry(service, "OpenHost Router" if name == ROUTER_PROVIDER else name,
                               ROUTER_PROVIDER if name == ROUTER_PROVIDER else self.apps[name]["app_id"],
                               status="running" if name == ROUTER_PROVIDER else self.apps[name]["status"],
                               default=self.defaults.get(service) == name)
                for service, name in self.providers if name == ROUTER_PROVIDER or name in self.apps]

    async def __call__(self, request):
        path = request.url.path
        body = json.loads(request.content) if request.content else None
        self.events.append((request.method, path, copy.deepcopy(body)))
        assert request.headers["Authorization"] == f"Bearer {OWNER_TOKEN}"
        if path in self.hooks:
            response = self.hooks[path](request, body)
            response = await response if hasattr(response, "__await__") else response
            if response is not None:
                return response
        response = self.handle(request.method, path, body)
        if path in self.after:
            replacement = self.after[path](request, body, response)
            replacement = await replacement if hasattr(replacement, "__await__") else replacement
            if replacement is not None:
                return replacement
        return response

    def handle(self, method, path, body):
        if method == "GET":
            if path == "/api/apps":
                return httpx.Response(200, json=self.listing())
            if path == "/api/services/v2":
                return httpx.Response(200, json=self.catalogue())
            if path == "/api/permissions/v2":
                return httpx.Response(200, json=copy.deepcopy(self.permissions))
            pytest.fail(f"Unsupported fake-router GET: {path}")
        if path == "/api/app-definitions/parse":
            self.parsed_content = body["content"]
            return httpx.Response(200, json=self.plan(json.loads(body["content"])))
        if path == "/api/app-definitions/export":
            assert body == {"mode": "sharing"}
            return httpx.Response(200, json={"schema_version": 2, "mode": "sharing",
                                            "apps": copy.deepcopy(list(self.definitions.values()))})
        if path == "/api/app-definitions/import-private":
            assert self.data_restored, "Private import must follow data restoration"
            self.imported_content = body["content"]
            tokens = json.loads(body["content"])["platform_api_tokens"]
            hashes = {t["token_hash"] for t in self.tokens}
            added = [t for t in tokens if t["token_hash"] not in hashes]
            self.tokens.extend(copy.deepcopy(added))
            return httpx.Response(200, json={"ok": True, "added_api_token_count": len(added), "existing_api_token_count": len(tokens) - len(added)})
        if path.startswith("/stop_app/"):
            app = self.by_id(path.rsplit("/", 1)[-1])
            assert app["name"] != "backup"
            assert app["status"] not in {"building", "starting", "removing"}, "Must not race deployment workers"
            app["status"] = "stopped"
            return httpx.Response(200, json={"ok": True})
        if path.startswith("/reload_app/"):
            app = self.by_id(path.rsplit("/", 1)[-1])
            assert body == {"update": False}, "Reload must not fetch different source or widen grants"
            assert app["name"] != "backup"
            app["status"] = "building"
            self.workers[app["name"]] = list(self.deploy_states.get(app["name"], ["building", "starting", "running"]))
            return httpx.Response(200, json={"ok": True})
        if path == "/api/add_app":
            assert self.data_restored, "add_app immediately starts a worker; restore data first"
            name = body["app_name"]
            assert name not in self.apps, "Duplicate install destroys the existing temporary repo"
            assert "grant_permissions_v2" not in body
            alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
            app_id = "R" * 11 + alphabet[self.next_id]
            self.next_id += 1
            self.apps[name] = inventory_entry(name, app_id, "building")
            for grant in body.get("permissions_v2_grants", []):
                self.permissions.append({"consumer_app_id": app_id, "service_url": grant["service_url"], "grant": copy.deepcopy(grant["grant"]), "scope": "global", "provider_app_id": None})
            self.workers[name] = list(self.deploy_states.get(name, ["building", "starting", "running"]))
            return httpx.Response(200, json={"ok": True, "app_id": app_id, "app_name": name, "status": "building"})
        if path == "/api/permissions/v2/grant_global_scoped":
            grant = {"consumer_app_id": body["app_id"], "service_url": body["service_url"], "grant": body["grant"], "scope": "global", "provider_app_id": None}
            if grant not in self.permissions:
                self.permissions.append(copy.deepcopy(grant))
            return httpx.Response(200, json={"ok": True})
        if path == "/api/permissions/v2/revoke":
            assert body["scope"] == "global"
            self.permissions = [g for g in self.permissions if not (g["consumer_app_id"] == body["app_id"] and g["service_url"] == body["service_url"] and g["grant"] == body["grant"] and g["scope"] == "global")]
            return httpx.Response(200, json={"ok": True})
        if path == "/api/services/v2/defaults":
            name = ROUTER_PROVIDER if body["app_id"] == ROUTER_PROVIDER else self.by_id(body["app_id"])["name"]
            assert (body["service_url"], name) in self.providers
            self.defaults[body["service_url"]] = name
            return httpx.Response(200, json={"ok": True})
        pytest.fail(f"Unsupported fake-router POST: {path}")

    def by_id(self, app_id):
        return next(app for app in self.apps.values() if app["app_id"] == app_id)

    def mutations(self, path=None):
        return [e for e in self.events if e[0] == "POST" and e[1] not in {"/api/app-definitions/parse", "/api/app-definitions/export"} and (path is None or e[1] == path)]

    def installs(self):
        return [e[2]["app_name"] for e in self.mutations("/api/add_app")]


def session_for(bundle, *, timeout=0.1):
    return RecoverySession(ORIGIN, OWNER_TOKEN, bundle, deployment_timeout=timeout, poll_interval=0.001, request_timeout=timeout)


async def prepare(router, session):
    await session.preflight()
    await session.stop_apps()
    router.events.append(("data", "restored", list(session.restore_app_names)))
    router.data_restored = True


def add_runtime_provider(bundle, service, name, *, default=True):
    bundle["runtime"]["providers"].append({"service_url": service, "app_name": name, "is_default": default})


def add_global(bundle, name, service, grant):
    bundle["runtime"]["apps"][name]["global_grants"].append({"service_url": service, "grant": grant})


async def test_full_recovery_sequences_files_tokens_providers_consumers_and_saved_states(mock_http):
    bundle = make_bundle("source-backup", "backup", "z-secrets", "y-oauth", "a-consumer", "b-stopped", "c-existing", runtime=True)
    bundle["backup_app_name"] = "source-backup"
    bundle["definitions"]["platform_api_tokens"].extend([
        {"name": "\n雪\r\n", "token_hash": "b" * 64, "expires_at": "2000-01-01T01:02:03.000001-07:00"},
        {"name": "", "token_hash": "c" * 64, "expires_at": None},
    ])
    bundle["definitions"]["apps"][4]["port_mappings"] = [{"label": "web", "host_port": 8123, "container_port": 8080}]
    add_runtime_provider(bundle, SECRETS, "z-secrets")
    add_runtime_provider(bundle, OAUTH, "y-oauth")
    add_runtime_provider(bundle, DEFINITIONS, ROUTER_PROVIDER)
    add_global(bundle, "y-oauth", SECRETS, {"key": "oauth/db"})
    for name in ("a-consumer", "b-stopped", "c-existing"):
        add_global(bundle, name, SECRETS, {"key": "DB_URL"})
    bundle["runtime"]["apps"]["b-stopped"]["status"] = "stopped"
    existing = [inventory_entry("backup", "B" * 12), inventory_entry("source-backup", "F" * 12),
                inventory_entry("c-existing", "C" * 12), inventory_entry("unaffected", "U" * 12),
                inventory_entry("already-stopped", "T" * 12, "stopped"), inventory_entry("old-error", "E" * 12, "error")]
    router = Router(bundle, existing)
    router.permissions = [
        {"consumer_app_id": "C" * 12, "service_url": SECRETS, "grant": "FULL_ACCESS", "scope": "global", "provider_app_id": None},
        {"consumer_app_id": "U" * 12, "service_url": SECRETS, "grant": "unaffected", "scope": "global", "provider_app_id": None},
        {"consumer_app_id": "C" * 12, "service_url": OAUTH, "grant": {"scopes": ["read"]}, "scope": "app", "provider_app_id": "Q" * 12},
    ]
    destination_tokens = copy.deepcopy(router.tokens)
    mock_http(router)
    session = session_for(bundle)
    preflight = await session.preflight()
    assert preflight["phase"] == "preflighted"
    assert session.restore_app_names == ("a-consumer", "b-stopped", "c-existing", "y-oauth", "z-secrets")
    assert not router.mutations()
    await session.stop_apps()
    assert router.apps["c-existing"]["status"] == "stopped"
    assert next(a for a in session.progress["apps"] if a["name"] == "c-existing")["status"] == "stopped"
    assert router.apps["unaffected"]["status"] == "stopped"
    assert router.apps["backup"]["status"] == "running"
    assert router.apps["source-backup"]["status"] == "stopped"
    assert not any("remove_app" in e[1] for e in router.mutations())
    router.events.append(("data", "restored", None))
    router.data_restored = True
    result = await session.activate()
    assert result["ok"] is True
    assert result["tokens"] == {"expected": 3, "added": 2, "existing": 1, "confirmed": True}
    assert router.tokens[:2] == destination_tokens
    assert router.tokens[2:] == bundle["definitions"]["platform_api_tokens"][1:]
    assert router.parsed_content == router.imported_content
    assert json.loads(router.imported_content) == bundle["definitions"]
    assert router.installs() == ["z-secrets", "y-oauth", "a-consumer", "b-stopped"]
    for name in router.installs():
        request = next(e[2] for e in router.mutations("/api/add_app") if e[2]["app_name"] == name)
        assert request["permissions_v2_grants"] == bundle["runtime"]["apps"][name]["global_grants"]
        assert request.keys() == {"repo_url", "app_name", "port_overrides", "permissions_v2_grants"}
        assert request["repo_url"].endswith("@release/v2")
    assert next(e[2] for e in router.mutations("/api/add_app") if e[2]["app_name"] == "a-consumer")["port_overrides"] == {"web": 8123}
    assert router.defaults[SECRETS] == "z-secrets" and router.defaults[OAUTH] == "y-oauth"
    assert router.apps["b-stopped"]["status"] == "running"
    assert router.apps["c-existing"]["app_id"] == "C" * 12
    assert router.permissions == [
        {"consumer_app_id": "U" * 12, "service_url": SECRETS, "grant": "unaffected", "scope": "global", "provider_app_id": None},
        {"consumer_app_id": "C" * 12, "service_url": OAUTH, "grant": {"scopes": ["read"]}, "scope": "app", "provider_app_id": "Q" * 12},
        *[{"consumer_app_id": router.apps[n]["app_id"], "service_url": SECRETS, "grant": {"key": "oauth/db" if n == "y-oauth" else "DB_URL"}, "scope": "global", "provider_app_id": None} for n in ["y-oauth", "a-consumer", "b-stopped", "c-existing"]],
    ]
    data_index = next(i for i, e in enumerate(router.events) if e[0] == "data")
    first_after_data = next(e for e in router.events[data_index + 1:] if e[0] == "POST")
    assert first_after_data[1] == "/api/app-definitions/import-private"
    for provider, consumer in [("z-secrets", "y-oauth"), ("y-oauth", "a-consumer")]:
        ready_index = router.events.index(("state", provider, "running"))
        consumer_index = next(i for i, e in enumerate(router.events) if e[0] == "POST" and e[1] == "/api/add_app" and e[2]["app_name"] == consumer)
        assert ready_index < consumer_index
    await session.restart_unaffected()
    # Recorded stopped states are reapplied only once paused apps are back.
    assert router.apps["b-stopped"]["status"] == "stopped"
    assert router.apps["unaffected"]["status"] == "running"
    assert router.apps["source-backup"]["status"] == "running"
    assert router.apps["old-error"]["status"] == router.apps["already-stopped"]["status"] == "stopped"
    mutations = router.mutations()
    await session.restart_unaffected()
    assert router.mutations() == mutations
    public = json.dumps(session.summary)
    for secret in (APP_TOKEN, OWNER_TOKEN, VERIFIER, "b" * 64, "c" * 64, "DB_URL", "oauth/db", "repo_url", "release/v2", "destination existing name"):
        assert secret not in public


async def test_captured_data_without_a_definition_is_disclosed_not_silently_dropped(mock_http):
    bundle = make_bundle("notes", "backup", runtime=True)
    router = Router(bundle, [inventory_entry("orphan", "O" * 12)])
    mock_http(router)
    session = session_for(bundle)
    assert session.note_omitted_data({"orphan", ""}) is None
    progress = session.progress
    assert progress["omitted_app_data"] == ["orphan"]
    assert any("no exported definition" in warning and "orphan" in warning
               for warning in progress["warnings"])
    assert "orphan" not in session.restore_app_names


async def test_cleanup_never_stops_an_app_the_recovery_did_not_activate(mock_http):
    bundle = make_bundle("notes", "backup", runtime=True)
    bundle["runtime"]["apps"]["notes"]["status"] = "stopped"
    router = Router(bundle, [inventory_entry("backup", "B" * 12), inventory_entry("notes", "N" * 12)])
    mock_http(router)
    session = session_for(bundle)
    await session.preflight()
    # Staging failed before any app was paused, so nothing here was activated.
    assert session.progress["paused_apps"] == []
    result = await session.restart_unaffected()
    # Recorded stopped states describe a destination this recovery never
    # changed; stopping them here would mutate unrelated state while
    # paused_apps stays empty and hides the difference from the parent.
    assert not [e for e in router.events if e[0] == "POST" and e[1].startswith("/stop_app/")]
    assert not [e for e in router.events if e[0] == "POST" and e[1].startswith("/reload_app/")]
    assert router.apps["notes"]["status"] == "running"
    assert result["paused_apps"] == []
    assert result["ok"] is False


async def test_recorded_stopped_states_apply_only_after_unaffected_apps_resume(mock_http):
    bundle = make_bundle("notes", "backup", "z-secrets", runtime=True)
    bundle["runtime"]["apps"]["z-secrets"]["status"] = "stopped"
    add_runtime_provider(bundle, SECRETS, "z-secrets")
    router = Router(bundle, [inventory_entry("backup", "B" * 12), inventory_entry("consumer", "U" * 12)])
    router.data_restored = True
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    # Activation starts the recorded-stopped provider, so a consumer can reach it.
    assert result["ok"] is True
    assert router.apps["z-secrets"]["status"] == "running"
    result = await session.restart_unaffected()
    stops = [e for e in router.events if e[0] == "POST" and e[1].startswith("/stop_app/")]
    reloads = [e for e in router.events if e[0] == "POST" and e[1].startswith("/reload_app/")]
    # The unaffected consumer resumed while its provider was still available.
    assert reloads and stops
    assert router.events.index(reloads[0]) < router.events.index(stops[-1])
    assert router.apps["consumer"]["status"] == "running"
    assert router.apps["z-secrets"]["status"] == "stopped"
    assert result["ok"] is True


async def test_a_failed_recorded_stop_is_reported_and_cleanup_continues(mock_http):
    """One app refusing its recorded state must not strand the others."""
    bundle = make_bundle("notes", "backup", "a-stopped", "b-stopped", runtime=True)
    bundle["runtime"]["apps"]["a-stopped"]["status"] = "stopped"
    bundle["runtime"]["apps"]["b-stopped"]["status"] = "stopped"
    router = Router(bundle, [inventory_entry("backup", "B" * 12), inventory_entry("consumer", "U" * 12)])
    router.data_restored = True
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    assert (await session.activate())["ok"] is True
    # Reverse order is applied first, so break that one and check the rest.
    router.hooks["/stop_app/" + router.apps["b-stopped"]["app_id"]] = lambda *_: httpx.Response(503, json={})
    result = await session.restart_unaffected()
    assert result["ok"] is False
    failed = next(app for app in result["apps"] if app["name"] == "b-stopped")
    assert failed["ok"] is False and failed["outcome"] == "failed"
    # The other recorded state is still applied, and unaffected apps are back.
    assert router.apps["a-stopped"]["status"] == "stopped"
    assert router.apps["consumer"]["status"] == "running"


async def test_an_unexpected_cleanup_error_is_logged_and_strands_nothing(mock_http, caplog):
    """The failure path must be reportable rather than raising out of cleanup."""
    bundle = make_bundle("notes", "backup", "a-stopped", "b-stopped", runtime=True)
    bundle["runtime"]["apps"]["a-stopped"]["status"] = "stopped"
    bundle["runtime"]["apps"]["b-stopped"]["status"] = "stopped"
    router = Router(bundle, [inventory_entry("backup", "B" * 12), inventory_entry("consumer", "U" * 12)])
    router.data_restored = True
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    assert (await session.activate())["ok"] is True
    real = session._restore_stopped_state

    async def flaky(name):
        if name == "b-stopped":
            raise RuntimeError("injected cleanup failure")
        await real(name)

    session._restore_stopped_state = flaky
    with caplog.at_level("WARNING", logger="recovery"):
        result = await session.restart_unaffected()
    assert any("b-stopped" in record.getMessage() for record in caplog.records), caplog.text
    assert router.apps["a-stopped"]["status"] == "stopped"
    assert result["apps"], result


async def test_unaffected_resumes_do_not_depend_on_paused_order(mock_http):
    bundle = make_bundle("notes", "backup", runtime=True)
    entries = [inventory_entry("backup", "Z" * 12)]
    entries += [inventory_entry(f"z-consumer-{index}", chr(ord("A") + index) * 12) for index in range(6)]
    entries += [inventory_entry("a-provider", "P" * 12)]
    router = Router(bundle, entries)
    router.data_restored = True
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    assert (await session.activate())["ok"] is True
    # Every paused, previously running app resumes in one pass, so a consumer is
    # never ordered behind a provider that happens to sort later.
    assert (await session.restart_unaffected())["ok"] is True
    assert all(router.apps[name]["status"] == "running"
               for name in router.apps if name != "backup")


async def test_final_boundary_rechecks_unaffected_apps_that_failed_mid_cleanup(mock_http):
    bundle = make_bundle("notes", "backup", runtime=True)
    router = Router(bundle, [inventory_entry("backup", "B" * 12), inventory_entry("unaffected", "U" * 12)])
    router.data_restored = True
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    assert (await session.activate())["ok"] is True
    # The app was confirmed running, then fell over before the final boundary.
    original = Router.handle

    def handle(self, method, path, body):
        response = original(self, method, path, body)
        if method == "GET" and path == "/api/apps" and self.apps["unaffected"]["status"] == "running":
            self.apps["unaffected"]["status"] = "error"
        return response

    monkey = pytest.MonkeyPatch()
    monkey.setattr(Router, "handle", handle)
    try:
        result = await session.restart_unaffected()
    finally:
        monkey.undo()
    assert result["ok"] is False
    assert any("unaffected app could not be restarted" in warning for warning in result["warnings"])
    assert next(app for app in result["paused_apps"] if app["name"] == "unaffected")["restart"] == "failed"


@pytest.mark.parametrize("kind", ["remote", "builtin", "local", "unknown"])
async def test_existing_configuration_matches_canonical_export_despite_record_order(mock_http, kind):
    bundle = make_bundle("notes", "other", runtime=True)
    saved = bundle["definitions"]["apps"][0]
    saved["source"] = app_definition("notes", kind=kind)["source"]
    saved["port_mappings"] = [{"label": "web", "container_port": 80, "host_port": 8080},
                              {"label": "disabled", "container_port": 443, "host_port": 0}]
    router = Router(bundle, [inventory_entry("other", "U" * 12), inventory_entry("notes", "N" * 12)])
    router.definitions["notes"]["port_mappings"].reverse()
    router.definitions["notes"]["source"] = dict(reversed(list(saved["source"].items())))
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    assert (await session.activate())["ok"] is True
    assert len(router.mutations("/reload_app/" + "N" * 12)) == 1
    assert not router.installs()


async def test_launch_error_is_retried_once_and_can_still_restore(mock_http):
    """A launch verdict is not a recovery verdict.

    The router judges one launch attempt with a fixed budget for the app's
    first HTTP response, so a resource-capped app on a loaded host comes back
    as an error while it is healthy. A single further reload starts it, which
    is exactly what an operator would do by hand.
    """
    bundle = make_bundle("notes", runtime=True)
    router = Router(bundle, [inventory_entry("notes", "N" * 12)])
    attempts = []

    def reload(request, body):
        attempts.append(body)
        # Fall through to the default handler, which re-seeds the worker states.
        router.deploy_states["notes"] = ["error"] if len(attempts) == 1 else ["running"]
        return None

    router.hooks["/reload_app/" + "N" * 12] = reload
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is True
    assert len(router.mutations("/reload_app/" + "N" * 12)) == 2
    assert all(body == {"update": False} for body in attempts), "A retry must not widen grants or fetch other source"
    restored = next(app for app in result["apps"] if app["name"] == "notes")
    assert (restored["ok"], restored["outcome"], restored["launch_attempts"]) == (True, "restored", 2)
    assert restored["warnings"] == []
    assert not result["warnings"]


async def test_persistent_launch_error_fails_after_exactly_one_retry(mock_http):
    """The retry is bounded: a genuinely broken app still fails, without looping."""
    bundle = make_bundle("notes", runtime=True)
    router = Router(bundle, [inventory_entry("notes", "N" * 12)])
    router.deploy_states["notes"] = ["error"]
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False
    assert result["phase"] == "incomplete"
    assert len(router.mutations("/reload_app/" + "N" * 12)) == 2
    failed = next(app for app in result["apps"] if app["name"] == "notes")
    assert (failed["ok"], failed["outcome"], failed["launch_attempts"]) == (False, "failed", 2)
    assert any("An app deployment failed" in warning for warning in failed["warnings"])


async def test_app_still_converging_at_the_ceiling_is_pending_not_failed(mock_http):
    """Running out of time while an app is still starting is not a failure."""
    bundle = make_bundle("notes", runtime=True)
    router = Router(bundle, [inventory_entry("notes", "N" * 12)])
    router.deploy_states["notes"] = ["starting"] * 200
    mock_http(router)
    session = session_for(bundle, timeout=0.2)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False
    assert result["phase"] == "incomplete"
    assert len(router.mutations("/reload_app/" + "N" * 12)) == 1, "A converging app is not a launch failure to retry"
    pending = next(app for app in result["apps"] if app["name"] == "notes")
    assert (pending["ok"], pending["outcome"], pending["status"]) == (False, "pending", "starting")
    assert any("still building or starting" in warning for warning in pending["warnings"])
    assert not any("An app deployment failed" in warning for warning in pending["warnings"])


@pytest.mark.parametrize("change", [
    lambda a: a["source"].update(repo_url="https://example.com/different/private-repo.git"),
    lambda a: a["source"].update(ref="different-private-ref"),
    lambda a: a["source"].update(ref=None),
    lambda a: a.update(source={"kind": "builtin", "identifier": "notes"}),
    lambda a: a["port_mappings"][0].update(host_port=9000),
    lambda a: a["port_mappings"][0].update(host_port=0),
    lambda a: a["port_mappings"][0].update(container_port=81),
    lambda a: a["port_mappings"][0].update(label="different-label"),
    lambda a: a.update(port_mappings=[]),
    lambda a: a["port_mappings"].append({"label": "extra", "container_port": 443, "host_port": 0}),
])
async def test_existing_configuration_conflict_fails_before_any_mutation(mock_http, change):
    bundle = make_bundle("notes", "missing", runtime=True)
    bundle["definitions"]["apps"][0]["port_mappings"] = [{"label": "web", "container_port": 80, "host_port": 8080}]
    router = Router(bundle, [inventory_entry("notes", "N" * 12), inventory_entry("unaffected", "U" * 12)])
    change(router.definitions["notes"])
    before = copy.deepcopy((router.apps, router.definitions, router.tokens, router.permissions, router.defaults))
    mock_http(router)
    session = session_for(bundle)
    with pytest.raises(ConfigurationError) as error:
        await prepare(router, session)
    assert error.value.code == "configuration_conflict"
    assert str(error.value) == "An existing selected app has a different source or published-port configuration. Resolve the conflicting app configuration before restoring."
    await session.restart_unaffected()
    assert session.summary["phase"] == "preflight_failed" and not session.summary["ok"]
    conflict, = [a for a in session.summary["apps"] if a["outcome"] == "configuration_conflict"]
    assert conflict["name"] == "notes" and conflict["app_id"] == "N" * 12 and not conflict["ok"]
    assert not router.mutations() and not router.data_restored
    assert before == (router.apps, router.definitions, router.tokens, router.permissions, router.defaults)
    assert session.summary["paused_apps"] == []
    for secret in (OWNER_TOKEN, VERIFIER, "private-repo", "private-ref", "repo_url", "release/v2", "port_mappings"):
        assert secret not in str(error.value) + json.dumps(session.summary)


@pytest.mark.parametrize("change,code", [
    (lambda d: d.update(mode="private"), "router_response"),
    (lambda d: d.update(schema_version=True), "router_response"),
    (lambda d: d.update(platform_api_tokens=[]), "router_response"),
    (lambda d: d["apps"].append(copy.deepcopy(d["apps"][0])), "router_response"),
    (lambda d: d["apps"][0].update(source={"kind": "remote", "repo_url": OWNER_TOKEN}), "router_response"),
    (lambda d: d["apps"][0].update(app_id="N" * 12), "router_response"),
    (lambda d: d["apps"][0].update(port_mappings=[{"label": "web", "container_port": True, "host_port": 80}]), "router_response"),
    (lambda d: d["apps"][0].update(port_mappings=[{"label": "web", "container_port": 80, "host_port": 80}] * 2), "router_response"),
    (lambda d: d.update(apps=[]), "destination_changed"),
    (lambda d: d["apps"].append(app_definition("extra")), "destination_changed"),
    (lambda d: d["apps"][0].update(name="different"), "destination_changed"),
])
async def test_destination_export_shape_and_full_inventory_must_match(mock_http, change, code):
    # Validate all destination identities even when none are selected.
    bundle = make_bundle("missing")
    router = Router(bundle, [inventory_entry("unaffected", "U" * 12)])

    def changed(request, body, response):
        document = response.json()
        change(document)
        return httpx.Response(200, json=document)

    router.after["/api/app-definitions/export"] = changed
    mock_http(router)
    session = session_for(bundle)
    with pytest.raises(ConfigurationError) as error:
        await prepare(router, session)
    assert error.value.code == code
    await session.restart_unaffected()
    assert not router.mutations() and not router.data_restored
    assert router.apps["unaffected"]["status"] == "running"
    assert OWNER_TOKEN not in str(error.value) + json.dumps(session.summary)


async def test_destination_identity_replacement_during_export_fails_before_stop(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle, [inventory_entry("notes", "N" * 12)])

    def replace(request, body, response):
        router.apps["notes"]["app_id"] = "Z" * 12

    router.after["/api/app-definitions/export"] = replace
    mock_http(router)
    with pytest.raises(ConfigurationError) as error:
        await prepare(router, session_for(bundle))
    assert error.value.code == "destination_changed"
    assert not router.mutations() and not router.data_restored


async def test_configuration_comparison_excludes_both_backup_names_and_unselected_apps(mock_http):
    bundle = make_bundle("source-backup", "backup", "notes")
    bundle["backup_app_name"] = "source-backup"
    router = Router(bundle, [inventory_entry(name, letter * 12) for name, letter in
                             [("source-backup", "S"), ("backup", "B"), ("notes", "N"), ("unaffected", "U")]])
    for name in ("source-backup", "backup", "unaffected"):
        router.definitions[name]["source"] = {"kind": "local"}
    mock_http(router)
    session = session_for(bundle)
    assert (await session.preflight())["phase"] == "preflighted"
    assert session.restore_app_names == ("notes",)
    assert not router.mutations()


@pytest.mark.parametrize("state", ["building", "starting", "removing"])
async def test_preflight_rejects_transient_destination_before_any_parse_or_mutation(mock_http, state):
    bundle = make_bundle("notes")
    router = Router(bundle, [inventory_entry("unaffected", "U" * 12, state)])
    mock_http(router)
    session = session_for(bundle)
    with pytest.raises(ConfigurationError) as error:
        await session.preflight()
    assert error.value.code == "destination_busy"
    assert router.parsed_content is None and not router.mutations()


@pytest.mark.parametrize("mutate", [
    lambda p: p.update(mode="sharing"),
    lambda p: p.update(schema_version=True),
    lambda p: p.update(platform_api_token_names=[OWNER_TOKEN]),
    lambda p: p.update(apps=[]),
    lambda p: p["apps"].append(copy.deepcopy(p["apps"][0])),
    lambda p: p["apps"][0].update(name="not-selected"),
    lambda p: p["apps"][0].update(status="unavailable", install=None),
    lambda p: p["apps"][0]["install"].update(app_name="backup"),
    lambda p: p["apps"][0]["install"].update(repo_url=f"https://{OWNER_TOKEN}@example.com/repo"),
    lambda p: p["apps"][0]["install"].update(grant_permissions_v2=True),
    lambda p: p["apps"][0]["install"].update(port_overrides={"web": True}),
    lambda p: p["apps"][0]["install"].update(port_overrides={"web": 9999}),
])
async def test_preflight_rejects_inconsistent_parse_plan_without_side_effects(mock_http, mutate):
    bundle = make_bundle("notes", runtime=True)
    bundle["definitions"]["apps"][0]["port_mappings"] = [{"label": "web", "host_port": 8080, "container_port": 80}]
    router = Router(bundle, [inventory_entry("unaffected", "U" * 12)])

    def changed(request, body, response):
        plan = response.json()
        mutate(plan)
        return httpx.Response(200, json=plan)

    router.after["/api/app-definitions/parse"] = changed
    mock_http(router)
    session = session_for(bundle)
    with pytest.raises(ConfigurationError) as error:
        await session.preflight()
    assert error.value.code == "invalid_plan"
    assert not router.mutations()
    assert router.apps["unaffected"]["status"] == "running"
    assert OWNER_TOKEN not in str(error.value) + json.dumps(session.progress)


async def test_existing_plan_identity_and_inventory_change_fail_before_stop(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle, [inventory_entry("notes", "N" * 12)])

    def changed(request, body, response):
        router.apps["notes"]["app_id"] = "Z" * 12
        return response

    router.after["/api/app-definitions/parse"] = changed
    mock_http(router)
    session = session_for(bundle)
    with pytest.raises(ConfigurationError) as error:
        await session.preflight()
    assert error.value.code == "destination_changed"
    assert not router.mutations()


@pytest.mark.parametrize("path", ["/api/apps", "/api/services/v2", "/api/permissions/v2"])
async def test_owner_catalogue_failures_are_validated_before_stopping(mock_http, path):
    bundle = make_bundle("notes", runtime=True)
    router = Router(bundle, [inventory_entry("notes", "N" * 12)])
    router.hooks[path] = lambda r, b: httpx.Response(200, json={"error": OWNER_TOKEN})
    mock_http(router)
    session = session_for(bundle)
    with pytest.raises(ConfigurationError):
        await session.preflight()
    assert not router.mutations()


async def test_constructor_freezes_input_and_public_snapshots_cannot_mutate_plan(mock_http):
    bundle = make_bundle("notes", runtime=True)
    original = copy.deepcopy(bundle)
    router = Router(bundle)
    mock_http(router)
    session = session_for(bundle)
    bundle["definitions"]["apps"][0]["source"]["ref"] = "unwanted-new-code"
    bundle["definitions"]["platform_api_tokens"][0]["name"] = OWNER_TOKEN
    bundle["runtime"]["apps"]["notes"]["global_grants"].append({"service_url": SECRETS, "grant": "FULL_ACCESS"})
    plan = await session.preflight()
    plan["apps"][0]["name"] = "backup"
    plan["warnings"].append(OWNER_TOKEN)
    assert session.restore_app_names == ("notes",)
    assert OWNER_TOKEN not in json.dumps(session.progress)
    await session.stop_apps()
    router.data_restored = True
    assert (await session.activate())["ok"] is True
    assert json.loads(router.imported_content) == original["definitions"]
    assert router.mutations("/api/add_app")[0][2]["permissions_v2_grants"] == []


async def test_journal_keeps_pre_stop_identities_and_states_even_for_already_stopped_apps(mock_http):
    bundle = make_bundle("notes", runtime=True)
    app = inventory_entry("notes", "N" * 12, "stopped")
    app["error_message"] = OWNER_TOKEN
    router = Router(bundle, [app])
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    assert session.progress["paused_apps"] == []
    before = session.progress["destination_apps_before"]
    assert before == [{"name": "notes", "app_id": "N" * 12, "status": "stopped"}]
    before[0]["status"] = "running"
    assert (await session.activate())["ok"] is True
    assert session.summary["destination_apps_before"][0]["status"] == "stopped"
    assert session.summary["apps"][0]["status"] == "running"
    assert OWNER_TOKEN not in json.dumps(session.summary)


async def test_recovery_methods_enforce_single_use_phase_order(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle)
    mock_http(router)
    session = session_for(bundle)
    for method in (session.activate, session.stop_apps):
        with pytest.raises(ConfigurationError) as error:
            await method()
        assert error.value.code == "invalid_sequence"
    await session.preflight()
    with pytest.raises(ConfigurationError):
        await session.preflight()
    with pytest.raises(ConfigurationError):
        await session.activate()
    await session.stop_apps()
    with pytest.raises(ConfigurationError):
        await session.stop_apps()
    router.data_restored = True
    await session.activate()
    with pytest.raises(ConfigurationError):
        await session.activate()
    assert len(router.mutations("/api/add_app")) == 1


async def test_file_restore_failure_only_restarts_paused_unselected_apps(mock_http):
    bundle = make_bundle("notes", "missing", runtime=True)
    router = Router(bundle, [inventory_entry("notes", "N" * 12), inventory_entry("other", "U" * 12), inventory_entry("stopped", "S" * 12, "stopped")])
    mock_http(router)
    session = session_for(bundle)
    await session.preflight()
    await session.stop_apps()
    # This is the parent's finally path after partially restoring files. It must
    # not import tokens, install missing selected apps, or reload selected apps.
    result = await session.restart_unaffected()
    assert result["ok"] is False
    assert router.apps["notes"]["status"] == "stopped"
    assert router.apps["other"]["status"] == "running"
    assert router.apps["stopped"]["status"] == "stopped"
    assert router.imported_content is None and not router.installs()
    assert [e[1] for e in router.mutations() if "reload_app" in e[1]] == ["/reload_app/" + "U" * 12]
    journal = result["paused_apps"]
    assert journal[0] == {"name": "notes", "app_id": "N" * 12, "previous_status": "running", "selected": True,
                          "stop": "confirmed", "restart": "pending", "restart_requested": False}


@pytest.mark.parametrize("source_state", ["running", "stopped", "error"])
async def test_source_backup_name_collision_is_quiesced_and_cleaned_up_as_unaffected(mock_http, source_state):
    bundle = make_bundle("source-backup", "backup", "notes", runtime=True)
    bundle["backup_app_name"] = "source-backup"
    router = Router(bundle, [inventory_entry("source-backup", "S" * 12, source_state),
                             inventory_entry("backup", "B" * 12), inventory_entry("notes", "N" * 12)])
    # A destination-local app with this name is still a cross-app data writer.
    router.definitions["source-backup"]["source"] = {"kind": "local"}
    mock_http(router)
    session = session_for(bundle)
    await session.preflight()
    await session.stop_apps()
    assert session.restore_app_names == ("notes",)
    assert router.apps["source-backup"]["status"] == "stopped"
    assert router.apps["backup"]["status"] == "running"
    paused = {p["name"]: p for p in session.progress["paused_apps"]}
    assert "backup" not in paused
    if source_state == "stopped":
        assert "source-backup" not in paused
    else:
        assert paused["source-backup"]["selected"] is False
        assert paused["source-backup"]["stop"] == "confirmed"
        assert paused["source-backup"]["previous_status"] == source_state
    # Cleanup after a failed/partial data restore must restart the unaffected
    # writer only if it was running before, while leaving selected notes stopped.
    await session.restart_unaffected()
    assert router.apps["source-backup"]["status"] == ("running" if source_state == "running" else "stopped")
    assert router.apps["notes"]["status"] == "stopped"
    assert len(router.mutations("/reload_app/" + "S" * 12)) == (source_state == "running")
    assert not router.mutations("/stop_app/" + "B" * 12)
    assert not router.mutations("/reload_app/" + "B" * 12)
    assert not router.installs() and router.imported_content is None
    mutations = router.mutations()
    await session.restart_unaffected()
    assert router.mutations() == mutations


async def test_restarted_source_backup_collision_invalidates_quiescence_before_activation(mock_http):
    bundle = make_bundle("source-backup", "notes")
    bundle["backup_app_name"] = "source-backup"
    router = Router(bundle, [inventory_entry("source-backup", "S" * 12), inventory_entry("backup", "B" * 12)])
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    assert router.apps["source-backup"]["status"] == "stopped"
    router.apps["source-backup"]["status"] = "running"
    result = await session.activate()
    assert not result["ok"]
    assert "Data restoration must not proceed" in " ".join(result["warnings"])
    assert router.imported_content is None and not router.installs()


@pytest.mark.parametrize("failure", ["false", "html", "http", "not-stopped", "worker"])
async def test_stop_must_have_valid_response_and_confirmed_quiescence(mock_http, failure):
    bundle = make_bundle("notes")
    router = Router(bundle, [inventory_entry("notes", "N" * 12)])
    path = "/stop_app/" + "N" * 12
    if failure == "false":
        router.hooks[path] = lambda r, b: httpx.Response(200, json={"ok": False, "error": OWNER_TOKEN})
    elif failure == "html":
        router.hooks[path] = lambda r, b: httpx.Response(200, text=OWNER_TOKEN)
    elif failure == "http":
        router.hooks[path] = lambda r, b: httpx.Response(500, json={"error": OWNER_TOKEN})
    else:
        def unconfirmed(r, b):
            router.apps["notes"]["status"] = "building" if failure == "worker" else "running"
            return httpx.Response(200, json={"ok": True})
        router.hooks[path] = unconfirmed
    mock_http(router)
    session = session_for(bundle, timeout=0.02)
    await session.preflight()
    with pytest.raises(ConfigurationError) as error:
        await session.stop_apps()
    assert error.value.code == "stop_failed"
    assert session.progress["phase"] == "stop_failed"
    assert session.progress["paused_apps"][0]["stop"] == "uncertain"
    with pytest.raises(ConfigurationError):
        await session.activate()
    await session.restart_unaffected()
    assert not router.installs() and router.imported_content is None
    assert OWNER_TOKEN not in str(error.value) + json.dumps(session.summary)


async def test_stale_preflight_and_unpaused_new_app_prevent_data_restore_and_activation(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle, [inventory_entry("other", "U" * 12)])
    mock_http(router)
    session = session_for(bundle)
    await session.preflight()
    router.apps["other"]["status"] = "stopped"
    with pytest.raises(ConfigurationError):
        await session.stop_apps()
    assert not router.mutations()
    router.apps["other"]["status"] = "running"
    session = session_for(bundle)
    await prepare(router, session)
    router.apps["new-writer"] = inventory_entry("new-writer", "W" * 12)
    result = await session.activate()
    assert result["ok"] is False and not router.installs() and router.imported_content is None


@pytest.mark.parametrize("response", [
    {"ok": False, "added_api_token_count": 1, "existing_api_token_count": 0},
    {"ok": True, "added_api_token_count": True, "existing_api_token_count": 0},
    {"ok": True, "added_api_token_count": -1, "existing_api_token_count": 2},
    {"ok": True, "added_api_token_count": 3, "existing_api_token_count": 0},
    {"ok": True, "added_api_token_count": 0},
    {"ok": True, "added_api_token_count": 0, "existing_api_token_count": 1, "error": OWNER_TOKEN},
])
async def test_import_counts_must_be_confirmed_before_any_app_activation(mock_http, response):
    bundle = make_bundle("notes", runtime=True)
    router = Router(bundle)
    router.hooks["/api/app-definitions/import-private"] = lambda r, b: httpx.Response(200, json=response)
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False and result["tokens"]["confirmed"] is False
    assert not router.installs()
    assert [e[1] for e in router.mutations()] == ["/api/app-definitions/import-private"]
    assert OWNER_TOKEN not in json.dumps(result)


async def test_lost_token_import_is_explicitly_unconfirmed_and_does_not_activate(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle)

    def lost(request, body, response):
        raise httpx.ReadError(OWNER_TOKEN + VERIFIER, request=request)

    router.after["/api/app-definitions/import-private"] = lost
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False and result["tokens"]["confirmed"] is False
    assert router.imported_content is not None and not router.installs()
    assert "could not be confirmed" in " ".join(result["warnings"])


@pytest.mark.parametrize("failure", ["network", "http-500", "malformed-id", "non-json", "ok-false"])
async def test_lost_or_unknown_install_response_reconciles_worker_without_duplicate(mock_http, failure, caplog):
    caplog.set_level(logging.DEBUG)
    bundle = make_bundle("notes", runtime=True)
    router = Router(bundle)

    def lost(request, body, response):
        if failure == "network":
            raise httpx.ReadError(OWNER_TOKEN + VERIFIER, request=request)
        if failure == "http-500":
            return httpx.Response(500, json={"error": OWNER_TOKEN + VERIFIER})
        if failure == "non-json":
            return httpx.Response(200, text=OWNER_TOKEN + VERIFIER)
        if failure == "ok-false":
            return httpx.Response(200, json={"ok": False, "error": OWNER_TOKEN})
        return httpx.Response(200, json={**response.json(), "app_id": OWNER_TOKEN})

    router.after["/api/add_app"] = lost
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is True
    assert router.installs() == ["notes"]
    assert result["apps"][0]["app_id"] == router.apps["notes"]["app_id"]
    assert router.events.count(("state", "notes", "running")) == 1
    for secret in (OWNER_TOKEN, VERIFIER):
        assert secret not in json.dumps(result) + caplog.text


async def test_lost_install_with_late_inventory_visibility_is_reconciled(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle)
    pending = None
    inventory_polls = 0

    def late(request, body):
        nonlocal pending
        pending = copy.deepcopy(body)
        raise httpx.ReadError(OWNER_TOKEN, request=request)

    def poll(request, body):
        nonlocal inventory_polls, pending
        if pending is not None:
            inventory_polls += 1
            if inventory_polls == 3:
                router.handle("POST", "/api/add_app", pending)
                pending = None

    router.hooks["/api/add_app"] = late
    router.hooks["/api/apps"] = poll
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is True and inventory_polls == 3
    assert router.installs() == ["notes"]


async def test_lost_install_with_no_app_is_unknown_and_never_retried(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle)

    def lost(request, body):
        raise httpx.ReadError(OWNER_TOKEN, request=request)

    router.hooks["/api/add_app"] = lost
    mock_http(router)
    session = session_for(bundle, timeout=0.02)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False
    assert result["apps"][0]["outcome"] == "unknown"
    assert result["apps"][0]["app_id"] is None
    assert router.installs() == ["notes"]
    await session.restart_unaffected()
    assert router.installs() == ["notes"]


@pytest.mark.parametrize("status", [400, 401, 403, 409])
async def test_rejected_install_never_adopts_concurrently_created_provider(mock_http, status):
    bundle = make_bundle("secrets", runtime=True)
    add_runtime_provider(bundle, SECRETS, "secrets")
    bundle["runtime"]["apps"]["secrets"]["status"] = "stopped"
    router = Router(bundle)

    def concurrent_install(request, body):
        router.apps["secrets"] = inventory_entry("secrets", "Z" * 12)
        router.add_provider(SECRETS, "secrets")
        return httpx.Response(status, json={"error": OWNER_TOKEN})

    router.hooks["/api/add_app"] = concurrent_install
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False and result["apps"][0]["outcome"] == "failed"
    assert result["apps"][0]["app_id"] is None
    assert router.installs() == ["secrets"]
    assert router.apps["secrets"]["status"] == "running"
    assert not router.mutations("/api/services/v2/defaults")
    assert not any("/stop_app/" in e[1] or "/reload_app/" in e[1] for e in router.mutations())
    assert OWNER_TOKEN not in json.dumps(result)


@pytest.mark.parametrize("states,outcome,fragment", [
    (["building", "error"], "failed", "An app deployment failed"),
    # Still building or starting at the ceiling has not been shown to be broken,
    # so it is pending rather than a failed deployment.
    (["building"], "pending", "still building or starting"),
    (["starting"], "pending", "still building or starting"),
    (["removing"], "failed", "Destination apps changed"),
])
async def test_accepted_install_does_not_count_as_complete_without_running(mock_http, states, outcome, fragment):
    bundle = make_bundle("notes")
    router = Router(bundle)
    router.deploy_states["notes"] = states
    mock_http(router)
    session = session_for(bundle, timeout=0.02)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False and result["apps"][0]["ok"] is False
    assert result["apps"][0]["outcome"] == outcome
    assert any(fragment in warning for warning in result["apps"][0]["warnings"])
    assert OWNER_TOKEN not in json.dumps(result) and VERIFIER not in json.dumps(result)


async def test_existing_reload_failure_never_becomes_reinstall(mock_http):
    bundle = make_bundle("notes", runtime=True)
    router = Router(bundle, [inventory_entry("notes", "N" * 12)])
    router.hooks["/reload_app/" + "N" * 12] = lambda r, b: httpx.Response(403, json={"error": OWNER_TOKEN})
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False and not router.installs()
    assert router.apps["notes"]["status"] == "stopped"
    await session.restart_unaffected()
    assert len(router.mutations("/reload_app/" + "N" * 12)) == 1


async def test_unavailable_sources_remain_explicit_incomplete_but_data_names_are_available(mock_http):
    bundle = make_bundle("local", "unknown", "portable", runtime=True)
    bundle["definitions"]["apps"][0]["source"] = {"kind": "local"}
    bundle["definitions"]["apps"][1]["source"] = {"kind": "unknown"}
    router = Router(bundle)
    mock_http(router)
    session = session_for(bundle)
    plan = await session.preflight()
    assert session.restore_app_names == ("local", "portable", "unknown")
    assert {a["name"]: a["plan_status"] for a in plan["apps"]} == {"local": "unavailable", "portable": "ready", "unknown": "unavailable"}
    await session.stop_apps()
    router.data_restored = True
    result = await session.activate()
    assert result["ok"] is False and router.installs() == ["portable"]
    assert sum(a["outcome"] == "unavailable" for a in result["apps"]) == 2


async def test_destination_parse_supplies_builtin_path_verbatim(mock_http):
    bundle = make_bundle("file-browser")
    bundle["definitions"]["apps"][0]["source"] = {"kind": "builtin", "identifier": "file_browser"}
    router = Router(bundle)
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    assert (await session.activate())["ok"] is True
    assert router.mutations("/api/add_app")[0][2]["repo_url"] == "file:///opt/router-bundle/apps/file_browser"


async def test_grant_on_a_service_with_no_captured_default_does_not_block_the_app(mock_http):
    """A grant with no default provider names no app to wait for.

    The router records a global grant without requiring the service to be
    registered, so an app holding a grant for a service nothing provides as
    default must still launch. This is the default-platform-app shape seen live:
    a grant on a service the destination router does not serve.
    """
    bundle = make_bundle("catalog", runtime=True)
    add_global(bundle, "catalog", INSTALLER, {"key": "INSTALL"})
    router = Router(bundle, [inventory_entry("catalog", "C" * 12)])
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is True, result
    assert result["warnings"] == []
    restored = next(a for a in result["apps"] if a["name"] == "catalog")
    assert (restored["ok"], restored["outcome"]) == (True, "restored")
    assert router.apps["catalog"]["status"] == "running"
    assert len(router.mutations("/reload_app/" + "C" * 12)) == 1, "The app must actually be launched"
    # The grant is still restored, even though no provider was ever mapped to it.
    assert [g for g in router.permissions if g["consumer_app_id"] == "C" * 12 and g["service_url"] == INSTALLER]


async def test_provider_failure_blocks_consumer_and_never_selects_other_data(mock_http):
    bundle = make_bundle("a-consumer", "z-secrets", runtime=True)
    add_global(bundle, "a-consumer", SECRETS, {"key": "DB_URL"})
    add_runtime_provider(bundle, SECRETS, "z-secrets")
    router = Router(bundle, [inventory_entry("other-secrets", "S" * 12)])
    router.add_provider(SECRETS, "other-secrets")
    router.deploy_states["z-secrets"] = ["building", "error"]
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False and router.installs() == ["z-secrets"]
    assert next(a for a in result["apps"] if a["name"] == "a-consumer")["outcome"] == "failed"
    assert router.defaults[SECRETS] == "other-secrets"
    assert not router.mutations("/api/services/v2/defaults")


@pytest.mark.parametrize("external_state", ["running", "stopped", "missing"])
async def test_subset_external_provider_restarts_only_if_paused_here_and_keeps_new_ids(mock_http, external_state):
    full = make_bundle("notes", "secrets", runtime=True)
    add_runtime_provider(full, SECRETS, "secrets")
    add_global(full, "notes", SECRETS, {"key": "DB_URL"})
    bundle = subset_configuration(full, {"notes"})
    existing = [] if external_state == "missing" else [inventory_entry("secrets", "S" * 12, external_state)]
    router = Router(bundle, existing)
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is (external_state == "running")
    if external_state == "running":
        assert router.installs() == ["notes"]
        assert router.mutations("/api/services/v2/defaults")[0][2] == {"service_url": SECRETS, "app_id": "S" * 12}
        await session.restart_unaffected()
        assert len(router.mutations("/reload_app/" + "S" * 12)) == 1
    else:
        assert not router.installs()
        assert not any("reload_app" in e[1] for e in router.mutations())


async def test_private_git_consumers_wait_for_oauth_even_without_app_grants(mock_http):
    bundle = make_bundle("a-private-git", "z-oauth", runtime=True)
    add_runtime_provider(bundle, OAUTH, "z-oauth")
    router = Router(bundle)
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is True and router.installs() == ["z-oauth", "a-private-git"]
    ready = router.events.index(("state", "z-oauth", "running"))
    selected = next(i for i, e in enumerate(router.events) if e[0] == "POST" and e[1] == "/api/services/v2/defaults")
    clone = next(i for i, e in enumerate(router.events) if e[0] == "POST" and e[1] == "/api/add_app" and e[2]["app_name"] == "a-private-git")
    assert ready < selected < clone


async def test_cycle_requires_bootstrap_instead_of_racing_dependent_installs(mock_http):
    bundle = make_bundle("first", "second", "independent", runtime=True)
    add_runtime_provider(bundle, "service-first", "first")
    add_runtime_provider(bundle, "service-second", "second")
    add_global(bundle, "first", "service-second", {})
    add_global(bundle, "second", "service-first", {})
    router = Router(bundle)
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False and router.installs() == ["independent"]
    assert {a["name"] for a in result["apps"] if a["outcome"] == "blocked"} == {"first", "second"}


@pytest.mark.parametrize("provider", ["oauth", None])
async def test_provider_scoped_grants_remain_manual_without_global_widening(mock_http, provider):
    bundle = make_bundle("notes", "oauth", runtime=True)
    add_runtime_provider(bundle, OAUTH, "oauth")
    bundle["runtime"]["apps"]["notes"]["unresolved_provider_grants"] = [{"service_url": OAUTH, "provider_name": provider, "grant": {"provider": "github", "scopes": ["repo"]}}]
    router = Router(bundle)
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    notes = next(a for a in result["apps"] if a["name"] == "notes")
    assert result["ok"] is False and notes["outcome"] == "manual_reauthorization"
    assert notes["status"] == "running" and notes["app_id"]
    assert "manual reauthorization" in " ".join(notes["warnings"])
    assert all(e[2]["permissions_v2_grants"] == [] for e in router.mutations("/api/add_app"))
    assert not any("grant_" in e[1] for e in router.mutations())
    assert router.permissions == []


async def test_saved_stopped_provider_is_kept_running_until_all_consumers_ready(mock_http):
    bundle = make_bundle("secrets", "notes", runtime=True)
    bundle["runtime"]["apps"]["secrets"]["status"] = "stopped"
    add_runtime_provider(bundle, SECRETS, "secrets")
    add_global(bundle, "notes", SECRETS, {"key": "DB_URL"})
    router = Router(bundle)
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is True
    assert router.apps["secrets"]["status"] == "running"
    result = await session.restart_unaffected()
    stop = next(i for i, e in enumerate(router.events) if e[0] == "POST" and e[1].startswith("/stop_app/"))
    assert stop > router.events.index(("state", "notes", "running"))
    assert router.apps["secrets"]["status"] == "stopped"
    assert any("starts restored apps" in w for w in result["warnings"])


@pytest.mark.parametrize("source_state", ["error", "building", "starting", "removing"])
async def test_unsupported_saved_desired_state_is_not_falsely_complete(mock_http, source_state):
    bundle = make_bundle("notes", runtime=True)
    bundle["runtime"]["apps"]["notes"]["status"] = source_state
    router = Router(bundle)
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False and result["apps"][0]["outcome"] == "desired_state_unavailable"
    assert router.apps["notes"]["status"] == "running"


async def test_absent_runtime_has_deterministic_limited_scope_and_normal_permissions(mock_http):
    bundle = make_bundle("zeta", "alpha", "middle")
    router = Router(bundle, [inventory_entry("middle", "M" * 12)])
    grant = {"consumer_app_id": "M" * 12, "service_url": SECRETS, "grant": {"key": "existing"}, "scope": "global", "provider_app_id": None}
    router.permissions = [grant]
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is True and result["runtime_captured"] is False and result["runtime_complete"] is False
    assert any("were not captured" in w for w in result["warnings"])
    starts = [(e[2]["app_name"] if e[1] == "/api/add_app" else "middle") for e in router.mutations() if e[1] == "/api/add_app" or e[1].startswith("/reload_app/")]
    assert starts == ["alpha", "middle", "zeta"]
    assert all("permissions_v2_grants" not in e[2] and "grant_permissions_v2" not in e[2] for e in router.mutations("/api/add_app"))
    assert not any("permissions/v2/" in e[1] or "/defaults" in e[1] for e in router.mutations())
    assert router.permissions == [grant]


async def test_changed_unaffected_identity_is_never_restarted(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle, [inventory_entry("other", "U" * 12)])
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    await session.activate()
    router.apps["other"]["app_id"] = "Z" * 12
    result = await session.restart_unaffected()
    assert result["ok"] is False
    assert result["paused_apps"][0]["restart"] == "failed"
    assert not any("reload_app" in e[1] for e in router.mutations())


@pytest.mark.parametrize("selected_provider", [True, False])
async def test_provider_replacement_under_same_name_blocks_consumers(mock_http, selected_provider):
    bundle = make_bundle("a-middle", "z-consumer", "secrets", runtime=True)
    add_runtime_provider(bundle, SECRETS, "secrets")
    add_global(bundle, "z-consumer", SECRETS, {"key": "DB_URL"})
    if not selected_provider:
        bundle = subset_configuration(bundle, {"a-middle", "z-consumer"})
    apps = [] if selected_provider else [inventory_entry("secrets", "S" * 12)]
    router = Router(bundle, apps)

    def replace_after_middle(request, body, response):
        if body["app_name"] == "a-middle":
            router.apps["secrets"] = inventory_entry("secrets", "Z" * 12)

    router.after["/api/add_app"] = replace_after_middle
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False
    assert "z-consumer" not in router.installs()
    assert not any(e[2]["app_id"] == "Z" * 12 for e in router.mutations("/api/services/v2/defaults"))


@pytest.mark.parametrize("change", ["default", "identity"])
async def test_late_provider_change_invalidates_previously_confirmed_default(mock_http, change):
    bundle = make_bundle("secrets", "z-last-app", runtime=True)
    add_runtime_provider(bundle, SECRETS, "secrets")
    router = Router(bundle)

    def changed(request, body, response):
        if body["app_name"] == "z-last-app":
            if change == "identity":
                router.apps["secrets"]["app_id"] = "Z" * 12
            else:
                router.defaults[SECRETS] = "no-longer-the-captured-default"

    router.after["/api/add_app"] = changed
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False
    assert result["providers"] == {"expected_defaults": 1, "restored_defaults": 0}
    assert any("selection could not be restored" in w for w in result["warnings"])


@pytest.mark.parametrize("change", ["replaced", "removed", "error", "stopped"])
async def test_completion_rechecks_previously_ready_selected_apps(mock_http, change):
    bundle = make_bundle("alpha", "zeta", runtime=True)
    router = Router(bundle)
    original_id = None

    def changed(request, body, response):
        nonlocal original_id
        if body["app_name"] == "zeta":
            original_id = router.apps["alpha"]["app_id"]
            if change == "replaced":
                router.apps["alpha"]["app_id"] = "Z" * 12
            elif change == "removed":
                del router.apps["alpha"]
            else:
                router.apps["alpha"]["status"] = change
                router.apps["alpha"]["error_message"] = OWNER_TOKEN

    router.after["/api/add_app"] = changed
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    alpha = next(a for a in result["apps"] if a["name"] == "alpha")
    assert result["ok"] is False and alpha["ok"] is False
    assert alpha["app_id"] == original_id
    assert alpha["outcome"] == "failed"
    assert OWNER_TOKEN not in json.dumps(result)


async def test_completion_rechecks_non_default_provider_registration(mock_http):
    bundle = make_bundle("a-main", "b-secondary", "z-last", runtime=True)
    add_runtime_provider(bundle, SECRETS, "a-main")
    add_runtime_provider(bundle, SECRETS, "b-secondary", default=False)
    router = Router(bundle)

    def changed(request, body, response):
        if body["app_name"] == "z-last":
            router.providers.remove((SECRETS, "b-secondary"))

    router.after["/api/add_app"] = changed
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    secondary = next(a for a in result["apps"] if a["name"] == "b-secondary")
    assert result["ok"] is False and secondary["ok"] is False
    assert result["providers"] == {"expected_defaults": 1, "restored_defaults": 1}


@pytest.mark.parametrize("during_cleanup", [False, True])
async def test_completion_rechecks_defaults_after_final_stops_and_cleanup(mock_http, during_cleanup):
    bundle = make_bundle("secrets", runtime=True)
    add_runtime_provider(bundle, SECRETS, "secrets")
    if not during_cleanup:
        bundle["runtime"]["apps"]["secrets"]["status"] = "stopped"
    router = Router(bundle, [inventory_entry("unaffected", "U" * 12)])

    def change_default(request, body, response):
        router.defaults[SECRETS] = "changed-after-final-default-pass"

    def after_add(request, body, response):
        router.after["/stop_app/" + response.json()["app_id"]] = change_default

    if during_cleanup:
        router.after["/reload_app/" + "U" * 12] = change_default
    else:
        router.after["/api/add_app"] = after_add
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    if during_cleanup:
        assert result["ok"] is True
    # Recorded stopped states are applied in cleanup, so the final boundary that
    # re-reads the router runs after it in both cases.
    result = await session.restart_unaffected()
    assert result["ok"] is False
    assert result["providers"]["restored_defaults"] == 0


async def test_completion_verifies_actual_global_grants_not_just_ok_responses(mock_http):
    bundle = make_bundle("notes", runtime=True)
    router = Router(bundle)

    def extra_grant(request, body, response):
        router.permissions.append({"consumer_app_id": response.json()["app_id"], "service_url": SECRETS,
                                   "grant": "FULL_ACCESS", "scope": "global", "provider_app_id": None})

    router.after["/api/add_app"] = extra_grant
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["ok"] is False and result["apps"][0]["ok"] is False
    assert "global permissions could not be confirmed" in " ".join(result["apps"][0]["warnings"])


async def test_unconfirmed_completion_is_incomplete_even_with_no_selected_apps(mock_http):
    bundle = make_bundle("backup")
    router = Router(bundle, [inventory_entry("backup", "B" * 12)])

    def after_import(request, body, response):
        router.hooks["/api/apps"] = lambda r, b: httpx.Response(503, json={"error": OWNER_TOKEN})

    router.after["/api/app-definitions/import-private"] = after_import
    mock_http(router)
    session = session_for(bundle)
    await prepare(router, session)
    result = await session.activate()
    assert result["tokens"]["confirmed"] is True
    assert result["ok"] is False and result["completion_confirmed"] is False
    del router.hooks["/api/apps"]
    result = await session.restart_unaffected()
    assert result["ok"] is True and result["completion_confirmed"] is True


async def test_successful_cleanup_retry_clears_only_outstanding_cleanup_failure(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle, [inventory_entry("other", "U" * 12)])
    path = "/reload_app/" + "U" * 12
    router.hooks[path] = lambda r, b: httpx.Response(503, json={"error": OWNER_TOKEN})
    mock_http(router)
    # The subject here is cleanup retry, so activation gets the ordinary budget
    # instead of a deadline this test never means to exercise.
    session = session_for(bundle)
    await prepare(router, session)
    assert (await session.activate())["ok"] is True
    first = await session.restart_unaffected()
    assert first["ok"] is False and first["phase"] == "incomplete"
    del router.hooks[path]
    second = await session.restart_unaffected()
    assert second["ok"] is True and second["phase"] == "complete"
    assert second["paused_apps"][0]["restart"] == "confirmed"
    assert not any("could not be restarted" in w for w in second["warnings"])
    assert second["journal_version"] == 1


async def test_cleanup_retry_reconciles_its_still_running_worker_without_duplicate_reload(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle, [inventory_entry("other", "U" * 12)])
    router.deploy_states["other"] = ["building"]
    mock_http(router)
    session = session_for(bundle)
    await session.preflight()
    await session.stop_apps()
    first = await session.restart_unaffected()
    assert first["paused_apps"][0]["restart"] == "failed"
    router.workers["other"] = ["starting", "running"]
    second = await session.restart_unaffected()
    assert second["paused_apps"][0]["restart"] == "confirmed"
    assert len(router.mutations("/reload_app/" + "U" * 12)) == 1


async def wait_started(event):
    await asyncio.wait_for(event.wait(), timeout=1)


async def test_cancellation_during_stop_drains_mutation_then_cleanup_excludes_selected(mock_http):
    bundle = make_bundle("a-selected", runtime=True)
    router = Router(bundle, [inventory_entry("a-selected", "A" * 12), inventory_entry("b-unaffected", "U" * 12)])
    started, release = asyncio.Event(), asyncio.Event()

    async def gate(request, body, response):
        started.set()
        await release.wait()
        return response

    router.after["/stop_app/" + "U" * 12] = gate
    mock_http(router)
    session = session_for(bundle, timeout=1)
    await session.preflight()
    task = asyncio.create_task(session.stop_apps())
    await wait_started(started)
    assert session.progress["paused_apps"][-1]["stop"] == "requested"
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert session.progress["phase"] == "interrupted"
    assert session.progress["paused_apps"][-1]["stop"] == "confirmed"
    await session.restart_unaffected()
    assert router.apps["a-selected"]["status"] == "stopped"
    assert router.apps["b-unaffected"]["status"] == "running"
    assert router.imported_content is None


async def test_cancellation_during_import_confirms_counts_but_never_starts_apps(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle)
    started, release = asyncio.Event(), asyncio.Event()

    async def gate(request, body, response):
        started.set()
        await release.wait()
        return response

    router.after["/api/app-definitions/import-private"] = gate
    mock_http(router)
    session = session_for(bundle, timeout=1)
    await prepare(router, session)
    task = asyncio.create_task(session.activate())
    await wait_started(started)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert session.progress["tokens"]["confirmed"] is True
    assert not router.installs()
    await session.restart_unaffected()
    assert not router.installs()


async def test_cancellation_during_install_drains_readiness_without_starting_next_app(mock_http):
    bundle = make_bundle("alpha", "zeta", runtime=True)
    router = Router(bundle)
    started, release = asyncio.Event(), asyncio.Event()

    async def gate(request, body, response):
        started.set()
        await release.wait()
        return response

    router.after["/api/add_app"] = gate
    mock_http(router)
    session = session_for(bundle, timeout=1)
    await prepare(router, session)
    task = asyncio.create_task(session.activate())
    await wait_started(started)
    assert session.progress["apps"][0]["outcome"] == "install_requested"
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert router.installs() == ["alpha"] and router.apps["alpha"]["status"] == "running"
    assert session.progress["apps"][0]["outcome"] == "restored"
    assert session.progress["ok"] is False and session.progress["phase"] == "interrupted"
    await session.restart_unaffected()
    assert router.installs() == ["alpha"]


async def test_finally_cleanup_drains_all_unaffected_apps_under_repeated_cancellation(mock_http):
    bundle = make_bundle("a-selected")
    router = Router(bundle, [inventory_entry("a-selected", "A" * 12), inventory_entry("first", "F" * 12), inventory_entry("second", "S" * 12)])
    started, release = asyncio.Event(), asyncio.Event()

    async def gate(request, body, response):
        started.set()
        await release.wait()
        return response

    router.after["/reload_app/" + "F" * 12] = gate
    mock_http(router)
    session = session_for(bundle, timeout=1)
    await session.preflight()
    await session.stop_apps()
    task = asyncio.create_task(session.restart_unaffected())
    await wait_started(started)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert router.apps["first"]["status"] == router.apps["second"]["status"] == "running"
    assert router.apps["a-selected"]["status"] == "stopped"
    assert all(p["restart"] == "confirmed" for p in session.progress["paused_apps"] if not p["selected"])
    assert not router.installs()


async def test_lost_restart_response_is_reconciled_without_another_reload(mock_http):
    bundle = make_bundle("notes")
    router = Router(bundle, [inventory_entry("other", "U" * 12)])

    def lost(request, body, response):
        raise httpx.ReadError(OWNER_TOKEN, request=request)

    router.after["/reload_app/" + "U" * 12] = lost
    mock_http(router)
    session = session_for(bundle)
    await session.preflight()
    await session.stop_apps()
    result = await session.restart_unaffected()
    assert result["paused_apps"][0]["restart"] == "confirmed"
    assert router.apps["other"]["status"] == "running"
    await session.restart_unaffected()
    assert len(router.mutations("/reload_app/" + "U" * 12)) == 1
