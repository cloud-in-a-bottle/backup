"""Configuration-aware recovery using supported owner APIs, never router files.

The caller owns the whole-operation lock and all file restoration::

    session = RecoverySession(router_url, owner_token, bundle)
    try:
        await session.preflight()
        # Staging (download and verification) runs while apps are still up; the
        # only writes that need quiescence are promoting staged data and the
        # reload that follows, so the stopped window stays as short as possible.
        await stage_data(session.restore_app_names)  # read-only for live apps
        await session.stop_apps()
        await promote_data(session.restore_app_names)  # must succeed completely
        await session.activate()
    finally:
        await session.restart_unaffected()
    result = session.summary  # includes cleanup failures, unlike an earlier snapshot

Keep the operation lock until the finally block finishes. Cancellation drains
an in-flight mutation/readiness check before propagating, and records its intent
before sending it. Cleanup never starts selected apps after a partial data
restore. ``progress``/``summary`` are independent, JSON-safe journal snapshots;
they contain names/IDs, states, counts and fixed messages, not configuration,
grant values, token names/verifiers, repository URLs, or router error details.

``ok`` means all *captured* configuration was recovered. Missing runtime is a
limited-scope restore with runtime_captured/runtime_complete false and a warning.
Unavailable sources, readiness failures, unconfirmed imports, provider grants
needing reauthorization, and unsupported saved desired states make ok false.
New apps receive new IDs; existing apps only reload local code (update=false).
The platform starts installations immediately, so saved stopped states can only
be reapplied after ordinary readiness and after dependent apps have activated.
"""

from __future__ import annotations

import asyncio
import logging
import copy
import math
from urllib.parse import urlsplit

from operations import drain
from configuration import (
    APP_STATUSES,
    MAX_DEFINITION_BYTES,
    ROUTER_PROVIDER,
    TRANSIENT_STATUSES,
    ConfigurationError,
    _ERRORS as _CONFIGURATION_ERRORS,
    RouterClient,
    _app_id,
    _app_name,
    _inventory,
    _json_bytes,
    _list,
    _object,
    _permissions,
    _providers,
    _record_key,
    _string,
    _validate_sharing_definitions,
    parse_configuration,
    serialize_configuration,
)

logger = logging.getLogger(__name__)

OAUTH_SERVICE = "github.com/imbue-openhost/openhost/services/oauth"
_RUNTIME_MISSING = "Permissions, service-provider selections, and desired app states were not captured; normal permission approval is still required."
_MANUAL_GRANTS = "Provider-scoped permissions require manual reauthorization at their providers; they were not replaced with global grants."
_STOPPED_START = "The platform starts restored apps before their recorded stopped states can be reapplied."
_UNKNOWN_STATE = "The saved app state was not running or stopped; its desired state needs manual review."
_UNAVAILABLE_SOURCE = "App data can be restored, but the app source is unavailable on this destination."
_DEPENDENCY_CYCLE = "Service-provider dependencies form a cycle; these apps require manual bootstrap."
_DEFAULT_FAILED = "A captured service-provider selection could not be restored."
_CLEANUP_FAILED = "An unaffected app could not be restarted; check the paused-app journal."
_FINAL_UNCONFIRMED = "The final recovery state could not be confirmed. Check the router before retrying."
_OMITTED_DATA = "Captured app data with no exported definition was not restored: "

# Router app states that mean a launch is still in progress rather than
# finished. The platform reports ``building`` while an image is being built and
# ``starting`` while the container comes up; neither is a verdict on the app.
_CONVERGING_STATUSES = frozenset({"building", "starting"})


def _failure_outcome(code: str) -> str:
    """Per-app outcome label for a recovery failure code.

    Kept in one place so the activation and validation passes cannot disagree
    about what a code means for the operator.
    """
    return {"install_unknown": "unknown", "deployment_pending": "pending"}.get(code, "failed")

def _require_ok(response: object) -> None:
    if type(response) is not dict or response.get("ok") is not True:
        raise ConfigurationError("router_response")


def journal_progress(value: object) -> dict | None:
    """Validate/project persisted public results without exposing private fields.

    Unknown fields are discarded. Warning text must come from this executor's
    fixed messages; the one filename-based warning is rebuilt from its names.
    Missing fields stay missing for journals written by earlier versions.
    """
    if value is None:
        return None
    messages = set(_CONFIGURATION_ERRORS.values()) | {
        _RUNTIME_MISSING, _MANUAL_GRANTS, _STOPPED_START, _UNKNOWN_STATE,
        _UNAVAILABLE_SOURCE, _DEPENDENCY_CYCLE, _DEFAULT_FAILED, _CLEANUP_FAILED, _FINAL_UNCONFIRMED,
    }

    def boolean(item):
        if type(item) is not bool:
            raise ValueError
        return item

    def count(item):
        if type(item) is not int or item < 0:
            raise ValueError
        return item

    def choice(*options):
        def validate(item):
            if item not in options or (item is not None and type(item) is not str):
                raise ValueError
            return item
        return validate

    def optional(check):
        return lambda item: None if item is None else check(item)

    def sequence(check):
        def validate(items):
            if type(items) is not list:
                raise ValueError
            return [check(item) for item in items]
        return validate

    def project(schema):
        def validate(record):
            if type(record) is not dict:
                raise ValueError
            return {key: check(record[key]) for key, check in schema.items() if key in record}
        return validate

    def warnings(items):
        if type(items) is not list:
            raise ValueError
        return [message for message in items if type(message) is str and message in messages]

    def filename(item):
        if type(item) is not str or not item or "/" in item or "\0" in item:
            raise ValueError
        return item

    app = project({
        "name": _app_name, "app_id": optional(_app_id), "plan_status": choice(None, "existing", "ready", "unavailable"),
        "action": choice(None, "reload", "install"), "status": choice(None, *APP_STATUSES),
        "outcome": choice("pending", "failed", "unknown", "unavailable", "blocked", "configuration_conflict",
                          "install_requested", "install_unconfirmed", "reload_requested", "restored",
                          "manual_reauthorization", "desired_state_unavailable"),
        "ok": boolean, "launch_attempts": count, "warnings": warnings,
    })
    paused = project({
        "name": _app_name, "app_id": _app_id, "previous_status": choice(*APP_STATUSES), "selected": boolean,
        "stop": choice("requested", "confirmed", "uncertain"), "restart": choice("pending", "requested", "confirmed", "failed"),
        "restart_requested": boolean,
    })
    result = project({
        "ok": boolean, "runtime_captured": boolean, "runtime_complete": boolean,
        "completion_confirmed": boolean, "source_cutover_confirmed": boolean,
        "phase": choice("new", "preflighting", "preflighted", "preflight_failed", "stopping", "stopped", "stop_failed",
                        "activating", "restarting_unaffected", "complete", "incomplete", "interrupted"),
        "current_app": optional(_app_name), "restore_app_names": sequence(_app_name), "apps": sequence(app),
        "tokens": project({"expected": count, "added": count, "existing": count, "confirmed": boolean}),
        "providers": project({"expected_defaults": count, "restored_defaults": count}),
        "destination_apps_before": lambda items: list(_inventory(items).values()),
        "omitted_app_data": sequence(filename), "paused_apps": sequence(paused), "warnings": warnings,
    })(value)
    if result.get("omitted_app_data"):
        result.setdefault("warnings", []).append(_omitted_warning(result["omitted_app_data"]))
    return result


def _omitted_warning(names) -> str:
    return _OMITTED_DATA + ", ".join(names) + ". Recover it from this snapshot's app_data root with a root-specific restore."


def _restart_prerequisites(graph: dict[str, set[str]]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Condense retained dependency cycles; their members bootstrap together.

    Iterative Kosaraju traversal avoids recursion limits on long app chains.
    Every member of a component waits for all external prerequisite components.
    """
    order, seen = [], set()
    reverse = {name: set() for name in graph}
    for name, dependencies in graph.items():
        for dependency in dependencies:
            reverse[dependency].add(name)
        stack = [(name, False)]
        while stack:
            current, finished = stack.pop()
            if finished:
                order.append(current)
            elif current not in seen:
                seen.add(current)
                stack.append((current, True))
                stack.extend((dependency, False) for dependency in graph[current] if dependency not in seen)
    groups, components = {}, []
    for name in reversed(order):
        if name in groups:
            continue
        component, pending = set(), [name]
        components.append(component)
        while pending:
            current = pending.pop()
            if current not in groups:
                component.add(current)
                groups[current] = component
                pending.extend(reverse[current])
    prerequisites = {}
    for component in components:
        required = {member for consumer in component for dependency in graph[consumer] - component for member in groups[dependency]}
        prerequisites.update({name: required for name in component})
    return prerequisites, groups


class RecoverySession:
    """A single-use plan; restore files only between stop_apps and activate.

    deployment_timeout bounds each readiness/reconciliation operation, including
    slow polling requests. request_timeout also bounds each HTTP operation.
    Neither method retries add_app. restart_unaffected is safe to call repeatedly.
    """

    def __init__(self, router_url: str, owner_token: str, bundle: dict, backup_app_name: str = "backup", *,
                 deployment_timeout: float = 900.0, poll_interval: float = 1.0, request_timeout: float = 60.0):
        for value in (deployment_timeout, poll_interval, request_timeout):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ConfigurationError("invalid_request")
        self._client = RouterClient(router_url, owner_token, timeout=min(request_timeout, deployment_timeout))
        self._bundle = parse_configuration(serialize_configuration(bundle))
        self._runtime = self._bundle["runtime"]
        self._definitions = self._bundle["definitions"]
        self._content = _json_bytes(self._definitions, limit=MAX_DEFINITION_BYTES).decode("utf-8")
        self._backup_app_name = _app_name(backup_app_name)
        restore_excluded = {self._bundle["backup_app_name"], self._backup_app_name}
        self._selected = tuple(sorted(app["name"] for app in self._definitions["apps"] if app["name"] not in restore_excluded))
        self._definition_apps = {app["name"]: app for app in self._definitions["apps"]}
        self._deployment_timeout = deployment_timeout
        self._poll_interval = poll_interval
        self._lock = asyncio.Lock()
        self._phase = "new"
        self._current_app = None
        self._baseline = {}
        self._plans = {}
        self._paused = {}
        self._requirements = {}
        self._order = []
        self._cycles = set()
        self._provider_specs = []
        self._defaults_done = set()
        self._ready = set()
        self._activated: set[str] = set()
        self._cleanup_failed = False
        self._omitted_data: list[str] = []
        self._activation_finished = False
        self._completion_confirmed = False
        self._source_cutover_confirmed = False
        self._resume_tasks: dict[str, asyncio.Task] = {}
        self._resume_generation: dict[str, int] = {}
        self._activation_generation = 0
        self._clone_oauth: str | None = None
        self._clone_bootstrap_remaining = min(request_timeout, deployment_timeout)
        self._external_dependencies: dict[str, set[str]] = {}
        self._external_services: dict[str, set[str]] = {}
        self._blocked_defaults: set[str] = set()
        self._warnings = []
        self._apps = {name: {"name": name, "app_id": None, "plan_status": None, "action": None,
                             "status": None, "outcome": "pending", "ok": False, "launch_attempts": 0, "warnings": []}
                      for name in self._selected}
        self._tokens = {"expected": len(self._definitions["platform_api_tokens"]), "added": 0, "existing": 0, "confirmed": False}
        if self._runtime is None:
            self._warn(_RUNTIME_MISSING)

    @property
    def restore_app_names(self) -> tuple[str, ...]:
        """The only app data names the caller may restore (includes unavailable)."""
        return self._selected

    @property
    def progress(self) -> dict:
        expected_defaults = sum(p["is_default"] for p in self._provider_specs) if self._runtime is not None else 0
        ok = (self._activation_finished and self._completion_confirmed and self._tokens["confirmed"] and not self._cleanup_failed
              and all(app["ok"] for app in self._apps.values()) and len(self._defaults_done) == expected_defaults)
        return copy.deepcopy({
            "journal_version": 1, "ok": ok, "phase": self._phase, "current_app": self._current_app,
            "restore_app_names": list(self._selected), "apps": list(self._apps.values()),
            "tokens": self._tokens, "warnings": self._warnings,
            "runtime_captured": self._runtime is not None,
            "runtime_complete": self._runtime is not None and ok,
            "completion_confirmed": self._completion_confirmed,
            "source_cutover_confirmed": self._source_cutover_confirmed,
            "providers": {"expected_defaults": expected_defaults, "restored_defaults": len(self._defaults_done)},
            "destination_apps_before": list(self._baseline.values()),
            "omitted_app_data": list(self._omitted_data),
            "paused_apps": list(self._paused.values()),
        })

    @property
    def summary(self) -> dict:
        return self.progress

    def note_omitted_data(self, names) -> None:
        """Disclose captured app data this recovery cannot restore.

        A snapshot can contain app-data directories for apps with no exported
        definition. Promotion only covers restore_app_names, so the gap is
        reported instead of silently reported as a complete recovery.
        """
        omitted = sorted({name for name in names if type(name) is str and name})
        if not omitted:
            return
        self._omitted_data = omitted
        self._warn(_omitted_warning(omitted))

    def _warn(self, message: str, name: str | None = None) -> None:
        warnings = self._warnings if name is None else self._apps[name]["warnings"]
        if message not in warnings:
            warnings.append(message)

    def _check_phase(self, expected: str) -> None:
        if self._phase != expected:
            raise ConfigurationError("invalid_sequence")

    def _check_stable(self, inventory: dict) -> None:
        if any(app["status"] in TRANSIENT_STATUSES for app in inventory.values()):
            raise ConfigurationError("destination_busy")

    def _check_identities(self, inventory: dict) -> None:
        if {n: a["app_id"] for n, a in inventory.items()} != {n: a["app_id"] for n, a in self._baseline.items()}:
            raise ConfigurationError("destination_changed")

    def _check_quiescent(self, inventory: dict) -> None:
        self._check_identities(inventory)
        if any(app["status"] != "stopped" for name, app in inventory.items() if name != self._backup_app_name):
            raise ConfigurationError("stop_failed")

    def _parse_plan(self, response: dict, inventory: dict) -> dict:
        """Validate *all* plan entries before exposing an actionable selection."""
        try:
            response = _object(response, {"schema_version", "mode", "apps", "platform_api_token_names"})
            if type(response["schema_version"]) is not int or response["schema_version"] != 2 or response["mode"] != "private":
                raise ConfigurationError()
            if response["platform_api_token_names"] != [t["name"] for t in self._definitions["platform_api_tokens"]]:
                raise ConfigurationError()
            plans = {}
            for app in _list(response["apps"]):
                app = _object(app, {"name", "source_label", "status"}, {"app_id", "install"})
                name = _app_name(app["name"])
                _string(app["source_label"], empty=True)  # deliberately absent from progress
                if name in plans or name not in self._definition_apps:
                    raise ConfigurationError()
                status = app["status"]
                if status == "existing":
                    if "install" in app or "app_id" not in app or name not in inventory:
                        raise ConfigurationError()
                    if _app_id(app["app_id"]) != inventory[name]["app_id"]:
                        raise ConfigurationError()
                elif status in {"ready", "unavailable"}:
                    if "app_id" in app or name in inventory:
                        raise ConfigurationError()
                    if status == "unavailable":
                        if "install" in app or self._definition_apps[name]["source"]["kind"] == "remote":
                            raise ConfigurationError()
                    else:
                        install = _object(app.get("install"), {"repo_url", "app_name", "port_overrides"})
                        if install["app_name"] != name:
                            raise ConfigurationError()
                        expected_ports = {p["label"]: p["host_port"] for p in self._definition_apps[name]["port_mappings"]}
                        if type(install["port_overrides"]) is not dict or any(type(p) is not int for p in install["port_overrides"].values()) or install["port_overrides"] != expected_ports:
                            raise ConfigurationError()
                        source = self._definition_apps[name]["source"]
                        url = _string(install["repo_url"])
                        if source["kind"] == "remote":
                            expected_url = source["repo_url"] + ("@" + source["ref"] if source["ref"] is not None else "")
                            if url != expected_url:
                                raise ConfigurationError()
                        elif source["kind"] == "builtin":
                            parsed = urlsplit(url)
                            if (parsed.scheme != "file" or parsed.netloc or not parsed.path.startswith("/")
                                or parsed.path.rsplit("/", 1)[-1] != source["identifier"]
                                or any(part in {".", ".."} for part in parsed.path.split("/"))
                                or any(c in url for c in "@?#\\")):
                                raise ConfigurationError()
                        else:
                            raise ConfigurationError()
                else:
                    raise ConfigurationError()
                plans[name] = copy.deepcopy(app)
            if plans.keys() != self._definition_apps.keys():
                raise ConfigurationError()
            return plans
        except (ValueError, TypeError, KeyError):
            raise ConfigurationError("invalid_plan") from None

    def _build_dependencies(self, destination_providers: list[dict], destination_permissions: list[dict]) -> None:
        self._provider_specs = self._runtime["providers"] if self._runtime is not None else []
        catalogue = self._provider_specs if self._runtime is not None else destination_providers
        defaults = {p["service_url"]: p["app_name"] for p in catalogue if p["is_default"]}
        self._clone_oauth = defaults.get(OAUTH_SERVICE, self._clone_oauth)
        provider_names = {p["app_name"] for p in catalogue}
        requirements = {name: set() for name in self._selected}
        if self._runtime is not None:
            for name in self._selected:
                app = self._runtime["apps"][name]
                for grant in app["global_grants"]:
                    requirements[name].add((grant["service_url"], defaults.get(grant["service_url"])))
                for grant in app["unresolved_provider_grants"]:
                    if grant["provider_name"] is not None:
                        requirements[name].add((grant["service_url"], grant["provider_name"]))
        dependencies = {name: {provider for _, provider in reqs if provider not in {name, None, ROUTER_PROVIDER}}
                        for name, reqs in requirements.items()}

        # Retained providers may depend on other destination apps. Resolve those
        # grants against the defaults recovery will establish, so early startup
        # cannot cache an old provider selection. Unrelated consumers stay paused.
        destination_defaults = {p["service_url"]: p["app_name"] for p in destination_providers if p["is_default"]}
        destination_defaults.update(defaults)
        names = {app["app_id"]: name for name, app in self._baseline.items()}
        names[ROUTER_PROVIDER] = ROUTER_PROVIDER
        external = {name: set() for name in self._baseline if name not in self._selected}
        self._external_services = {name: set() for name in external}
        for grant in destination_permissions:
            consumer = names.get(grant["consumer_app_id"])
            if consumer not in external:
                continue
            if grant["scope"] == "global":
                self._external_services[consumer].add(grant["service_url"])
            provider = (destination_defaults.get(grant["service_url"]) if grant["scope"] == "global"
                        else names.get(grant["provider_app_id"]))
            if provider not in {None, consumer, ROUTER_PROVIDER}:
                external[consumer].add(provider)
        self._external_dependencies = external
        for name, required in dependencies.items():
            pending, seen = list(required), set()
            while pending:
                provider = pending.pop()
                if provider in seen:
                    continue
                seen.add(provider)
                if provider in self._selected:
                    required.add(provider)
                else:
                    pending.extend(external.get(provider, ()))

        # Prefer OAuth and its prerequisites when they are part of this recovery,
        # so private clones can use them. This is an ordering preference, never
        # a requirement: the router tries anonymous cloning first, and a public
        # repository must not depend on an unrelated/unavailable OAuth provider.
        oauth = self._clone_oauth
        ancestors = set()
        pending = [oauth] if oauth is not None else []
        while pending:
            name = pending.pop()
            if name not in ancestors:
                ancestors.add(name)
                pending.extend((dependencies.get(name, set()) | external.get(name, set())) - ancestors)
        self._requirements = requirements
        remaining = set(self._selected)
        order = []
        while remaining:
            ready = [name for name in remaining if not dependencies[name] & remaining]
            if not ready:
                break
            name = min(ready, key=lambda n: (n not in ancestors, n not in provider_names, n))
            remaining.remove(name)
            order.append(name)
        self._order, self._cycles = order, remaining

    def _check_existing_configuration(self, document: dict, inventory: dict, plans: dict) -> None:
        destination = {app["name"]: app for app in document["apps"]}
        if destination.keys() != inventory.keys():
            raise ConfigurationError("destination_changed")

        def ports(app: dict) -> dict:
            return {p["label"]: (p["container_port"], p["host_port"]) for p in app["port_mappings"]}

        conflicts = False
        for name in self._selected:
            if plans[name]["status"] != "existing":
                continue
            saved, current = self._definition_apps[name], destination[name]
            # Both sources already use the canonical portable export contract:
            # compare its kind and fields, not raw repo URLs or local paths.
            if saved["source"] != current["source"] or ports(saved) != ports(current):
                self._apps[name].update(plan_status="existing", app_id=inventory[name]["app_id"],
                                        status=inventory[name]["status"], outcome="configuration_conflict")
                self._warn(str(ConfigurationError("configuration_conflict")), name)
                conflicts = True
        if conflicts:
            raise ConfigurationError("configuration_conflict")

    async def preflight(self) -> dict:
        """Authenticate and validate bundle, parse plan, catalogues and inventory."""
        async with self._lock:
            self._check_phase("new")
            self._phase = "preflighting"
            try:
                first = _inventory(await self._client.get("/api/apps"))
                self._check_stable(first)
                plan = await self._client.post("/api/app-definitions/parse", {"content": self._content})
                document = _validate_sharing_definitions(
                    await self._client.post("/api/app-definitions/export", {"mode": "sharing"}))
                inventory = _inventory(await self._client.get("/api/apps"))
                self._check_stable(inventory)
                if first != inventory:
                    raise ConfigurationError("destination_changed")
                plans = self._parse_plan(plan, inventory)
                self._check_existing_configuration(document, inventory, plans)
                providers = _providers(await self._client.get("/api/services/v2"), inventory)
                self._clone_oauth = next((p["app_name"] for p in providers if p["service_url"] == OAUTH_SERVICE and p["is_default"]), None)
                permissions = _permissions(await self._client.get("/api/permissions/v2"))
                self._baseline, self._plans = inventory, plans
                self._build_dependencies(providers, permissions)
                for name in self._selected:
                    app = plans[name]
                    self._apps[name].update(plan_status=app["status"], app_id=app.get("app_id"),
                                            status=inventory.get(name, {}).get("status"))
                    if app["status"] == "unavailable":
                        self._apps[name]["outcome"] = "unavailable"
                        self._warn(_UNAVAILABLE_SOURCE, name)
                    if name in self._cycles:
                        self._warn(_DEPENDENCY_CYCLE, name)
                    if self._runtime is not None:
                        saved = self._runtime["apps"][name]
                        if saved["unresolved_provider_grants"]:
                            self._warn(_MANUAL_GRANTS, name)
                        if saved["status"] == "stopped":
                            self._warn(_STOPPED_START)
                        elif saved["status"] != "running":
                            self._warn(_UNKNOWN_STATE, name)
                self._phase = "preflighted"
                return self.progress
            except asyncio.CancelledError:
                self._phase = "interrupted"
                raise
            except ConfigurationError:
                self._phase = "preflight_failed"
                raise
            except Exception:
                self._phase = "preflight_failed"
                raise ConfigurationError("recovery_failed") from None

    async def _get_before_deadline(self, path: str, deadline: float):
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise ConfigurationError("deployment_timeout")
        try:
            async with asyncio.timeout(remaining):
                return await self._client.get(path)
        except TimeoutError:
            raise ConfigurationError("deployment_timeout") from None

    async def _pause_poll(self, deadline: float) -> None:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise ConfigurationError("deployment_timeout")
        await asyncio.sleep(min(self._poll_interval, remaining))

    async def _wait_stopped(self, name: str, app_id: str) -> None:
        deadline = asyncio.get_running_loop().time() + min(self._deployment_timeout, 60.0)
        while True:
            inventory = _inventory(await self._get_before_deadline("/api/apps", deadline))
            app = inventory.get(name)
            if app is None or app["app_id"] != app_id:
                raise ConfigurationError("destination_changed")
            if app["status"] == "stopped":
                return
            if app["status"] != "running":
                raise ConfigurationError("stop_failed")
            await self._pause_poll(deadline)

    async def _stop_one(self, name: str, app_id: str) -> None:
        try:
            _require_ok(await self._client.post(f"/stop_app/{app_id}"))
            await self._wait_stopped(name, app_id)
            self._paused[name]["stop"] = "confirmed"
            if name in self._apps:
                self._apps[name]["status"] = "stopped"
        except ConfigurationError:
            self._paused[name]["stop"] = "uncertain"
            raise ConfigurationError("stop_failed") from None

    async def stop_apps(self) -> dict:
        """Pause every current non-executor that might still write shared data."""
        async with self._lock:
            self._check_phase("preflighted")
            self._phase = "stopping"
            try:
                inventory = _inventory(await self._client.get("/api/apps"))
                self._check_stable(inventory)
                if inventory != self._baseline:
                    raise ConfigurationError("destination_changed")
                document = _validate_sharing_definitions(
                    await self._client.post("/api/app-definitions/export", {"mode": "sharing"}))
                self._check_existing_configuration(document, inventory, self._plans)
                inventory = _inventory(await self._client.get("/api/apps"))
                if inventory != self._baseline:
                    raise ConfigurationError("destination_changed")
                for name, app in inventory.items():
                    # The source executor's name may identify a different writer
                    # here. Only this destination's active executor cannot stop.
                    if name == self._backup_app_name or app["status"] == "stopped":
                        continue
                    self._current_app = name
                    self._paused[name] = {"name": name, "app_id": app["app_id"], "previous_status": app["status"],
                                          "selected": name in self._selected, "stop": "requested", "restart": "pending",
                                          "restart_requested": False}
                    await drain(self._stop_one(name, app["app_id"]))
                self._check_quiescent(_inventory(await self._client.get("/api/apps")))
                self._phase = "stopped"
                return self.progress
            except asyncio.CancelledError:
                self._phase = "interrupted"
                raise
            except ConfigurationError:
                self._phase = "stop_failed"
                raise
            except Exception:
                self._phase = "stop_failed"
                raise ConfigurationError("stop_failed") from None
            finally:
                self._current_app = None

    async def _import_tokens(self) -> None:
        try:
            response = await self._client.post("/api/app-definitions/import-private", {"content": self._content})
            response = _object(response, {"ok", "added_api_token_count", "existing_api_token_count"})
            _require_ok(response)
            added, existing = response["added_api_token_count"], response["existing_api_token_count"]
            if any(type(count) is not int or count < 0 for count in (added, existing)) or added + existing != self._tokens["expected"]:
                raise ConfigurationError()
            self._tokens.update(added=added, existing=existing, confirmed=True)
        except ConfigurationError:
            raise ConfigurationError("import_failed") from None

    async def _wait_running(self, name: str, app_id: str | None, *, unknown_install: bool = False) -> str:
        deadline = asyncio.get_running_loop().time() + self._deployment_timeout
        found = False
        last_status = None
        try:
            while True:
                inventory = _inventory(await self._get_before_deadline("/api/apps", deadline))
                app = inventory.get(name)
                if app is not None:
                    found = True
                    if app_id is not None and app["app_id"] != app_id:
                        raise ConfigurationError("destination_changed")
                    app_id = app["app_id"]
                    if name in self._apps:
                        self._apps[name].update(app_id=app_id, status=app["status"])
                    last_status = app["status"]
                    if app["status"] == "running":
                        return app_id
                    if app["status"] == "error":
                        raise ConfigurationError("deployment_failed")
                    if app["status"] == "removing":
                        raise ConfigurationError("destination_changed")
                await self._pause_poll(deadline)
        except ConfigurationError as error:
            if unknown_install and not found and error.code == "deployment_timeout":
                raise ConfigurationError("install_unknown") from None
            # An app still building or starting when the ceiling expires has not
            # failed, it has not finished. Say so, instead of reporting a failed
            # deployment for a launch that may still succeed on its own.
            if error.code == "deployment_timeout" and last_status in _CONVERGING_STATUSES:
                raise ConfigurationError("deployment_pending") from None
            raise

    async def _restore_global_grants(self, name: str, app_id: str) -> None:
        if self._runtime is None:
            return
        saved = self._runtime["apps"][name]["global_grants"]
        desired = {_record_key(grant): grant for grant in saved}
        current = _permissions(await self._client.get("/api/permissions/v2"))
        actual = {_record_key({"service_url": grant["service_url"], "grant": grant["grant"]}): grant
                  for grant in current if grant["consumer_app_id"] == app_id and grant["scope"] == "global"}
        # Only selected apps' global grants are reconciled. Provider scopes stay
        # provider-scoped, including grants the destination already holds.
        for key, grant in actual.items():
            if key not in desired:
                _require_ok(await self._client.post("/api/permissions/v2/revoke", {
                    "app_id": app_id, "service_url": grant["service_url"], "grant": grant["grant"], "scope": "global"}))
        for key, grant in desired.items():
            if key not in actual:
                _require_ok(await self._client.post("/api/permissions/v2/grant_global_scoped", {"app_id": app_id, **grant}))

    def _count_launch(self, name: str) -> None:
        """Record one launch attempt for a selected app.

        Unaffected paused apps are resumed through the same path but are not
        part of the selected set, so they carry no per-app progress entry.
        """
        app = self._apps.get(name)
        if app is not None:
            app["launch_attempts"] += 1

    async def _request_reload(self, app_id: str) -> None:
        """POST a same-identity reload, refusing to widen grants or fetch other source.

        Only a 4xx refusal surfaces here. A lost or late response is left for
        the caller's wait-on-same-identity to reconcile against the existing
        app, so the identity is never replaced or retried as a new reload.
        """
        try:
            _require_ok(await self._client.post(f"/reload_app/{app_id}", {"update": False}))
        except ConfigurationError as error:
            if error.status_code is not None and 400 <= error.status_code < 500:
                raise

    async def _reload(self, name: str, app_id: str) -> str:
        self._count_launch(name)
        await self._request_reload(app_id)
        try:
            return await self._wait_running(name, app_id)
        except ConfigurationError as error:
            # The router judges one launch attempt with a fixed budget for the
            # app's first HTTP response, so a resource-capped app on a loaded
            # host can come back as an error while it is perfectly healthy. The
            # platform treats that verdict as retryable, since an operator who
            # reloads the app again gets a running container. Do the same here,
            # exactly once, before recording a failure. Identity is still
            # checked on every poll, so this cannot mask a replaced app.
            if error.code != "deployment_failed":
                raise
        self._count_launch(name)
        await self._request_reload(app_id)
        return await self._wait_running(name, app_id)

    async def _install(self, name: str) -> str:
        before = _inventory(await self._client.get("/api/apps"))
        if name in before:
            raise ConfigurationError("destination_changed")
        payload = copy.deepcopy(self._plans[name]["install"])
        if self._runtime is not None:
            payload["permissions_v2_grants"] = copy.deepcopy(self._runtime["apps"][name]["global_grants"])
        self._apps[name]["outcome"] = "install_requested"
        app_id = None
        unknown = False
        try:
            response = await self._client.post("/api/add_app", payload)
            response = _object(response, {"ok", "app_id", "app_name", "status"})
            _require_ok(response)
            if response["app_name"] != name or type(response["status"]) is not str or response["status"] not in APP_STATUSES:
                raise ConfigurationError("router_response")
            app_id = _app_id(response["app_id"])
            self._apps[name]["app_id"] = app_id
        except ConfigurationError as error:
            unknown = True
            self._apps[name]["outcome"] = "install_unconfirmed"
            if error.status_code is not None and 400 <= error.status_code < 500:
                # A definitive rejection cannot establish ownership of an app
                # created concurrently under this name. Inspect the inventory
                # for a useful conflict outcome, but never adopt that app.
                inventory = _inventory(await self._client.get("/api/apps"))
                if name in inventory:
                    raise ConfigurationError("destination_changed") from None
                raise
        return await self._wait_running(name, app_id, unknown_install=unknown)

    async def _resume_unaffected(self, name: str) -> None:
        paused = self._paused[name]
        if paused["selected"] or paused["previous_status"] != "running" or paused["restart"] == "confirmed":
            return
        inventory = _inventory(await self._client.get("/api/apps"))
        app = inventory.get(name)
        if app is None or app["app_id"] != paused["app_id"]:
            raise ConfigurationError("destination_changed")
        if app["status"] == "running":
            paused["restart"] = "confirmed"
            return
        if app["status"] == "removing" or (app["status"] in TRANSIENT_STATUSES and not paused["restart_requested"]):
            raise ConfigurationError("destination_busy")
        if app["status"] in TRANSIENT_STATUSES:
            await self._wait_running(name, paused["app_id"])
        else:
            paused["restart"] = "requested"
            paused["restart_requested"] = True
            await self._reload(name, paused["app_id"])
        paused["restart"] = "confirmed"

    async def _provider_ready(self, service: str, name: str | None) -> tuple[str, dict]:
        if name is None:
            raise ConfigurationError("provider_unavailable")
        if name != ROUTER_PROVIDER:
            if name in self._selected:
                if name not in self._ready:
                    raise ConfigurationError("provider_unavailable")
            elif name in self._paused:
                # Start this provider's retained prerequisite group together,
                # keeping unrelated consumers paused until defaults are restored.
                self._start_unaffected(name)
                task = self._resume_tasks.get(name)
                while task is not None:
                    await asyncio.shield(task)
                    # An early attempt may have failed before a selected
                    # prerequisite activated. Retry at most once per changed
                    # activation generation, before failing this consumer too.
                    self._start_unaffected(name)
                    current = self._resume_tasks.get(name)
                    if current is task:
                        break
                    task = current
        deadline = asyncio.get_running_loop().time() + self._deployment_timeout
        while True:
            inventory = _inventory(await self._get_before_deadline("/api/apps", deadline))
            if name != ROUTER_PROVIDER and (name not in inventory or inventory[name]["status"] != "running"):
                raise ConfigurationError("provider_unavailable")
            if name != ROUTER_PROVIDER:
                expected = self._apps[name]["app_id"] if name in self._selected else self._baseline.get(name, {}).get("app_id")
                if expected is None or inventory[name]["app_id"] != expected:
                    raise ConfigurationError("destination_changed")
            providers = _providers(await self._get_before_deadline("/api/services/v2", deadline), inventory)
            match = next((p for p in providers if p["app_name"] == name and p["service_url"] == service and p["status"] == "running"), None)
            if match is not None:
                return match["app_id"], match
            await self._pause_poll(deadline)

    async def _set_default(self, service: str, name: str) -> None:
        if (service, name) in self._defaults_done:
            try:
                _, provider = await self._provider_ready(service, name)
                if not provider["is_default"]:
                    raise ConfigurationError("provider_unavailable")
            except ConfigurationError:
                self._defaults_done.discard((service, name))
                raise
            return
        app_id, _ = await self._provider_ready(service, name)
        _require_ok(await self._client.post("/api/services/v2/defaults", {"service_url": service, "app_id": app_id}))
        _, provider = await self._provider_ready(service, name)
        if not provider["is_default"]:
            raise ConfigurationError("provider_unavailable")
        self._defaults_done.add((service, name))
        self._blocked_defaults.discard(service)

    async def _prime_retained_defaults(self) -> None:
        """Select registered retained providers while their consumers are stopped.

        The router permits selecting a stopped registered provider. Readiness is
        still required later, before the selection counts as recovered.
        """
        saved = [p for p in self._provider_specs if p["is_default"] and p["app_name"] not in self._selected]
        if not saved:
            return
        self._blocked_defaults.update(p["service_url"] for p in saved)
        inventory = _inventory(await self._client.get("/api/apps"))
        self._check_quiescent(inventory)
        try:
            providers = _providers(await self._client.get("/api/services/v2"), inventory)
        except ConfigurationError:
            self._warn(_DEFAULT_FAILED)
            return  # unknown selections stay blocked; independent apps can recover
        expected = {}
        for provider in saved:
            name, service = provider["app_name"], provider["service_url"]
            app_id = ROUTER_PROVIDER if name == ROUTER_PROVIDER else self._baseline.get(name, {}).get("app_id")
            current = next((p for p in providers if p["app_name"] == name and p["service_url"] == service and p["app_id"] == app_id), None)
            if current is None:
                continue
            expected[service] = app_id
            if not current["is_default"]:
                try:
                    _require_ok(await self._client.post("/api/services/v2/defaults", {"service_url": service, "app_id": app_id}))
                except ConfigurationError:
                    pass  # readback reconciles lost replies; refusal blocks consumers
        inventory = _inventory(await self._client.get("/api/apps"))
        self._check_quiescent(inventory)
        try:
            actual = {p["service_url"]: p["app_id"] for p in _providers(await self._client.get("/api/services/v2"), inventory) if p["is_default"]}
        except ConfigurationError:
            self._warn(_DEFAULT_FAILED)
            return
        for service, app_id in expected.items():
            if actual.get(service) == app_id:
                self._blocked_defaults.discard(service)
        if self._blocked_defaults:
            self._warn(_DEFAULT_FAILED)

    async def _ensure_requirements(self, name: str) -> None:
        for service, provider in sorted(self._requirements[name], key=lambda pair: (pair[0], pair[1] or "")):
            if provider == name:
                continue  # a provider can consume its own service after startup
            if provider is None:
                # No app was the captured default for this service, so there is
                # no provider to wait for. The router records a global grant
                # without requiring the service to be registered, so gating this
                # app's launch on one would refuse to start a healthy app over a
                # permission the destination can still hold. The grant itself is
                # restored with the app's other global grants.
                continue
            await self._provider_ready(service, provider)
            if self._runtime is not None and any(p["is_default"] and p["service_url"] == service and p["app_name"] == provider for p in self._provider_specs):
                await self._set_default(service, provider)

    async def _activate_app(self, name: str) -> None:
        result = self._apps[name]
        plan = self._plans[name]
        if plan["status"] == "unavailable":
            return
        try:
            await self._ensure_requirements(name)
            if plan["status"] == "existing":
                app_id = plan["app_id"]
                inventory = _inventory(await self._client.get("/api/apps"))
                if name not in inventory or inventory[name]["app_id"] != app_id or inventory[name]["status"] != "stopped":
                    raise ConfigurationError("destination_changed")
                result.update(action="reload", outcome="reload_requested")
                await self._restore_global_grants(name, app_id)
                await self._reload(name, app_id)
            else:
                result["action"] = "install"
                source = self._definition_apps[name]["source"]
                oauth = self._clone_oauth
                if (source["kind"] == "remote" and urlsplit(source["repo_url"]).hostname == "github.com"
                        and oauth in self._paused and oauth not in self._selected
                        and self._paused[oauth]["previous_status"] == "running" and self._clone_bootstrap_remaining > 0):
                    # Best-effort bootstrap of the destination's own GitHub
                    # authentication, with one shared request-sized budget.
                    # Failure must still allow anonymous cloning; the owned
                    # restart task is settled later in final cleanup.
                    started = asyncio.get_running_loop().time()
                    try:
                        async with asyncio.timeout(self._clone_bootstrap_remaining):
                            await self._provider_ready(OAUTH_SERVICE, oauth)
                    except (ConfigurationError, TimeoutError):
                        pass
                    finally:
                        self._clone_bootstrap_remaining = max(0, self._clone_bootstrap_remaining - (asyncio.get_running_loop().time() - started))
                self._count_launch(name)
                app_id = await self._install(name)
            result.update(app_id=app_id, status="running", outcome="restored", ok=True)
            self._ready.add(name)
            # Only an app this recovery actually started may have its recorded
            # stopped state reapplied during cleanup.
            self._activated.add(name)
            self._activation_generation += 1
            # Registration is part of readiness for a captured provider, even
            # when no other selected app currently consumes its service.
            for provider in self._provider_specs:
                if provider["app_name"] == name:
                    await self._provider_ready(provider["service_url"], name)
                    if provider["is_default"]:
                        await self._set_default(provider["service_url"], name)
            if self._runtime is not None:
                saved = self._runtime["apps"][name]
                if saved["unresolved_provider_grants"]:
                    result.update(ok=False, outcome="manual_reauthorization")
                elif saved["status"] not in {"running", "stopped"}:
                    result.update(ok=False, outcome="desired_state_unavailable")
        except ConfigurationError as error:
            self._ready.discard(name)
            # A launch that ran out of time while still converging is reported as
            # pending, not failed: nothing about it has been shown to be broken,
            # and it may still reach running on its own.
            result.update(ok=False, outcome=_failure_outcome(error.code))
            self._warn(str(error), name)

    async def _apply_saved_stopped_states(self) -> None:
        """Reapply recorded stopped states once paused apps are back.

        An unaffected destination-only consumer can require an app that was
        recorded stopped, so it must stay available while paused apps resume.

        Only apps this recovery activated are eligible. If activation was never
        reached the recorded states describe a destination this recovery never
        changed, so stopping them here would mutate unrelated state and hide the
        difference from ``paused_apps``.
        """
        if self._runtime is None or not self._activated:
            return
        for name in reversed(self._order):
            if name not in self._activated:
                continue
            if self._runtime["apps"][name]["status"] == "stopped" and self._apps[name]["status"] == "running":
                self._current_app = name
                try:
                    await drain(self._restore_stopped_state(name))
                except Exception:
                    # One app's stop must not strand the remaining recorded states.
                    logger.warning("Could not reapply the recorded stopped state for %s", name)
        self._current_app = None

    async def _restore_stopped_state(self, name: str) -> None:
        result = self._apps[name]
        try:
            _require_ok(await self._client.post(f"/stop_app/{result['app_id']}"))
            await self._wait_stopped(name, result["app_id"])
            result["status"] = "stopped"
        except ConfigurationError:
            result.update(ok=False, outcome="failed")
            self._warn(str(ConfigurationError("stop_failed")), name)

    async def _validate_completion(self, *, states_final: bool = True) -> None:
        """Recheck the final boundary, including intentionally stopped providers.

        Earlier readiness is not proof that an app survived later deployments,
        final stops, or cleanup. This is a read-only confirmation, never an
        attempt to adopt a replacement identity or silently repair an app.
        """
        self._completion_confirmed = False
        try:
            inventory = _inventory(await self._client.get("/api/apps"))
            providers = _providers(await self._client.get("/api/services/v2"), inventory) if self._runtime is not None else []
            permissions = _permissions(await self._client.get("/api/permissions/v2")) if self._runtime is not None else []
        except ConfigurationError:
            self._warn(_FINAL_UNCONFIRMED)
            return

        def fail(name: str, code: str) -> None:
            self._apps[name].update(ok=False, outcome=_failure_outcome(code))
            self._ready.discard(name)
            self._warn(str(ConfigurationError(code)), name)

        for name, result in self._apps.items():
            if result["app_id"] is None:
                continue  # unavailable or unconfirmed installations cannot be adopted
            app = inventory.get(name)
            if app is None or app["app_id"] != result["app_id"]:
                result["status"] = None
                fail(name, "destination_changed")
                continue
            result["status"] = app["status"]
            recorded_stopped = (self._runtime is not None and self._runtime["apps"][name]["status"] == "stopped")
            expected_state = "stopped" if recorded_stopped and states_final else "running"
            if expected_state == "running" and app["status"] in _CONVERGING_STATUSES:
                # Still building or starting: the launch is in progress, so this
                # is a pending outcome, not a failed deployment. It stays the
                # dominant fact for this app: the permission comparison is left
                # undone until the launch actually settles, so a grant that has
                # not been confirmed yet cannot be reported as the failure.
                fail(name, "deployment_pending")
                continue
            if app["status"] != expected_state:
                fail(name, "deployment_failed")
            if self._runtime is not None:
                desired = {_record_key(g) for g in self._runtime["apps"][name]["global_grants"]}
                actual = {_record_key({"service_url": g["service_url"], "grant": g["grant"]})
                          for g in permissions if g["consumer_app_id"] == result["app_id"] and g["scope"] == "global"}
                if actual != desired:
                    fail(name, "permissions_incomplete")

        for paused in self._paused.values() if states_final else ():
            # A confirmation from earlier in cleanup is not proof the app
            # survived, and a name alone never authorizes a replacement.
            if paused["selected"] or paused["previous_status"] != "running":
                continue
            app = inventory.get(paused["name"])
            if app is None or app["app_id"] != paused["app_id"] or app["status"] != "running":
                paused["restart"] = "failed"
                self._cleanup_failed = True
                self._warn(_CLEANUP_FAILED)
            elif paused["restart"] != "confirmed":
                paused["restart"] = "confirmed"

        catalogue = {(p["service_url"], p["app_name"]): p for p in providers}
        for saved in self._provider_specs:
            name, service = saved["app_name"], saved["service_url"]
            provider = catalogue.get((service, name))
            if name == ROUTER_PROVIDER:
                expected_id, expected_state = ROUTER_PROVIDER, "running"
            else:
                expected_id = self._apps[name]["app_id"] if name in self._apps else self._baseline.get(name, {}).get("app_id")
                expected_state = inventory.get(name, {}).get("status")
            valid = provider is not None and expected_id is not None and provider["app_id"] == expected_id and provider["status"] == expected_state
            if not valid and name in self._apps and self._apps[name]["app_id"] is not None:
                fail(name, "provider_unavailable")
            if saved["is_default"] and (not valid or not provider["is_default"]):
                self._defaults_done.discard((service, name))
                self._warn(_DEFAULT_FAILED)
        self._completion_confirmed = True
        self._warnings = [warning for warning in self._warnings if warning != _FINAL_UNCONFIRMED]
        if len(self._defaults_done) == sum(p["is_default"] for p in self._provider_specs):
            self._warnings = [warning for warning in self._warnings if warning != _DEFAULT_FAILED]
        if not self._cleanup_failed:
            self._warnings = [warning for warning in self._warnings if warning != _CLEANUP_FAILED]

    async def activate(self) -> dict:
        """Call only after all selected app data was successfully restored."""
        async with self._lock:
            self._check_phase("stopped")
            self._phase = "activating"
            try:
                self._check_quiescent(_inventory(await self._client.get("/api/apps")))
                await drain(self._import_tokens())  # first write; exact same canonical content
                await drain(self._prime_retained_defaults())
                for name in self._cycles:
                    self._apps[name].update(outcome="blocked", ok=False)
                for name in self._order:
                    self._current_app = name
                    await drain(self._activate_app(name))
                # Include selected defaults with no consumer, router builtins,
                # and external dependencies retained by a migration subset.
                for provider in self._provider_specs:
                    if provider["is_default"]:
                        try:
                            await drain(self._set_default(provider["service_url"], provider["app_name"]))
                        except ConfigurationError:
                            self._warn(_DEFAULT_FAILED)
                await drain(self._validate_completion(states_final=False))
                self._activation_finished = True
                self._phase = "complete" if self.progress["ok"] else "incomplete"
            except asyncio.CancelledError:
                self._phase = "interrupted"
                raise
            except ConfigurationError as error:
                self._phase = "incomplete"
                self._warn(str(error))
            except Exception:
                self._phase = "incomplete"
                self._warn(str(ConfigurationError("recovery_failed")))
            finally:
                self._current_app = None
            return self.progress

    def _start_unaffected(self, provider: str | None = None) -> None:
        """Own each restart task through final cleanup, including cancellation."""
        async def resume(name: str, paused: dict) -> None:
            try:
                if any(self._external_services.get(member, set()) & self._blocked_defaults for member in components[name]):
                    raise ConfigurationError("provider_unavailable")
                for dependency in prerequisites[name]:
                    task = self._resume_tasks.get(dependency)
                    if task is not None:
                        await asyncio.shield(task)
                        if self._paused[dependency]["restart"] != "confirmed":
                            raise ConfigurationError("provider_unavailable")
                await self._resume_unaffected(name)
            except ConfigurationError:
                paused["restart"] = "failed"
                self._warn(_CLEANUP_FAILED)

        group = set(self._paused) if provider is None else set()
        pending = [] if provider is None else [provider]
        while pending:
            name = pending.pop()
            if name in group:
                continue
            group.add(name)
            if name in self._selected:
                if name not in self._ready:
                    raise ConfigurationError("provider_unavailable")
            else:
                pending.extend(self._external_dependencies.get(name, ()))
        prerequisites, components = _restart_prerequisites({name: self._external_dependencies.get(name, set()) & group for name in group})
        for name, paused in self._paused.items():
            if name not in group:
                continue
            if not paused["selected"] and paused["previous_status"] == "running" and paused["restart"] != "confirmed":
                task = self._resume_tasks.get(name)
                changed = (task is not None and task.done() and not task.cancelled() and task.exception() is None
                           and self._resume_generation[name] < self._activation_generation)
                if task is None or changed:
                    self._resume_generation[name] = self._activation_generation
                    self._resume_tasks[name] = asyncio.create_task(resume(name, paused))

    async def _restart_all(self) -> None:
        async def settle() -> list:
            return await asyncio.gather(*self._resume_tasks.values(), return_exceptions=True)

        self._current_app = None
        # Earlier activation may have resumed providers and their prerequisites.
        # Settle those tasks, then retry failures now that selected providers are
        # available. Never issue a duplicate reload while a worker is in flight.
        results = await drain(settle()) if self._resume_tasks else []
        self._resume_tasks.clear()
        self._start_unaffected()
        results.extend(await drain(settle()))
        self._resume_tasks.clear()
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, ConfigurationError):
                raise result
        await drain(self._apply_saved_stopped_states())
        self._cleanup_failed = any(not p["selected"] and p["previous_status"] == "running" and p["restart"] != "confirmed"
                                   for p in self._paused.values())
        if not self._cleanup_failed:
            self._warnings = [warning for warning in self._warnings if warning != _CLEANUP_FAILED]
        if self._activation_finished:
            await self._validate_completion()

    async def confirm_source_cutover(self) -> bool:
        """Verify source identities/states after cleanup, without activation.

        Source sessions only pause apps. Destination activation's completion
        checks therefore cannot establish that the source is safe to cut over.
        """
        async with self._lock:
            self._source_cutover_confirmed = False
            try:
                inventory = _inventory(await self._client.get("/api/apps"))
                self._check_identities(inventory)
            except ConfigurationError:
                self._warn(_FINAL_UNCONFIRMED)
                return False
            valid = True
            for name, before in self._baseline.items():
                if name == self._backup_app_name:
                    continue
                expected = "running" if name not in self._selected and before["status"] == "running" else "stopped"
                matches = inventory[name]["status"] == expected
                valid = valid and matches
                if name in self._paused and not self._paused[name]["selected"] and before["status"] == "running":
                    self._paused[name]["restart"] = "confirmed" if matches else "failed"
                if not matches:
                    self._warn(_FINAL_UNCONFIRMED)
            self._source_cutover_confirmed = valid
            return valid

    async def restart_unaffected(self) -> dict:
        """Finally-block cleanup: resume only our paused, unselected running apps.

        The entire cleanup is drained on cancellation, including other paused
        apps after the current one. IDs must still match; a name alone never
        authorizes restarting a replacement app. Selected apps are never resumed
        here, even if activation was never reached or file restoration failed.
        """
        async with self._lock:
            phase = self._phase
            self._phase = "restarting_unaffected"
            try:
                await drain(self._restart_all())
            finally:
                self._current_app = None
                if self._activation_finished:
                    self._phase = "complete" if self.progress["ok"] else "incomplete"
                else:
                    self._phase = phase
            return self.progress
