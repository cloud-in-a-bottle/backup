"""Private configuration bundles and the bounded, authenticated router transport.

The canonical ``definitions`` object is the platform's schema-v2 Private export.
JSON is also accepted by its YAML loader; no YAML conversion or extra fields are
needed. ``runtime`` supplements that document with portable, name-keyed state::

    {"apps": {name: {"status": str, "global_grants": [{"service_url": str,
       "grant": JSON}], "unresolved_provider_grants": [{"service_url": str,
       "provider_name": str | None, "grant": JSON}]}},
     "providers": [{"service_url": str, "app_name": str, "is_default": bool}]}

``_openhost_router`` identifies a builtin provider. A null provider_name means
the source provider no longer exists. Provider-scoped grants are evidence for
manual reauthorization, never instructions to issue global grants. Effective
defaults are captured; the catalogue does not distinguish explicit selections
from automatically chosen defaults. No source app IDs or authentication tokens
are stored in runtime. All public errors are fixed text.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
from datetime import datetime
from urllib.parse import unquote, urlsplit

import httpx

MAX_DEFINITION_BYTES = 1024 * 1024
MAX_CONFIGURATION_BYTES = 4 * MAX_DEFINITION_BYTES
ROUTER_PROVIDER = "_openhost_router"
APP_STATUSES = frozenset({"running", "stopped", "error", "building", "starting", "removing"})
TRANSIENT_STATUSES = frozenset({"building", "starting", "removing"})
_APP_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")
_APP_ID = re.compile(r"[1-9A-HJ-NP-Za-km-z]{12}")
_EXPORT_PATH = "/api/services/v2/call/definitions/export"

_ERRORS = {
    "invalid_configuration": "Invalid private configuration bundle.",
    "configuration_too_large": "Configuration exceeds the supported size limit.",
    "invalid_definitions": "Expected a valid schema-v2 Private app definition document.",
    "invalid_runtime": "Invalid supplemental runtime configuration.",
    "invalid_router": "A valid router origin and authentication token are required.",
    "invalid_request": "Unsupported router request.",
    "router_auth": "Router owner authentication failed. Check the configured owner API token.",
    "export_approval": (
        "Private configuration export requires approval. Approve the backup app's "
        "global Private definitions grant in the router, then retry."
    ),
    "export_auth": "Private configuration export authentication failed. Check the backup app token.",
    "router_request": "The router request failed.",
    "router_connection": "The router could not be reached or the request timed out.",
    "router_response": "The router returned an invalid or unsupported response.",
    "capture_changed": "Source apps changed during configuration capture. Retry the backup.",
    "destination_changed": "Destination apps changed during recovery. Retry after app operations finish.",
    "configuration_conflict": "An existing selected app has a different source or published-port configuration. Resolve the conflicting app configuration before restoring.",
    "destination_busy": "Destination apps are building, starting, or being removed. Wait before restoring.",
    "invalid_plan": "The router returned an inconsistent app recovery plan.",
    "invalid_sequence": "Recovery steps must run once in preflight, stop, restore-data, activate order.",
    "stop_failed": "An app could not be confirmed stopped. Data restoration must not proceed.",
    "deployment_failed": "An app deployment failed. Check the app in the router.",
    "deployment_timeout": "An app did not become ready before the recovery deadline.",
    "install_unknown": "An install response was lost and the app could not be identified. Check the router before retrying.",
    "provider_unavailable": "A required service provider is unavailable or not ready.",
    "permissions_incomplete": "Saved global permissions could not be confirmed. Review app permissions in the router.",
    "import_failed": "Private API-token import could not be confirmed. App activation was not started.",
    "recovery_failed": "Recovery could not be completed. Check the router before retrying.",
}


class ConfigurationError(ValueError):
    """A safe error code/message; never accepts arbitrary exception details."""

    def __init__(self, code: str = "invalid_configuration", *, status_code: int | None = None):
        self.code = code if type(code) is str and code in _ERRORS else "invalid_configuration"
        self.status_code = status_code if type(status_code) is int and 100 <= status_code <= 599 else None
        super().__init__(_ERRORS[self.code])


def _object(value: object, fields: set[str], optional: set[str] | frozenset[str] = frozenset()) -> dict:
    if type(value) is not dict or not fields <= value.keys() or value.keys() - fields - optional:
        raise ConfigurationError()
    return value


def _string(value: object, *, empty: bool = False) -> str:
    if type(value) is not str or (not empty and not value):
        raise ConfigurationError()
    return value


def _list(value: object) -> list:
    if type(value) is not list:
        raise ConfigurationError()
    return value


def _app_name(value: object) -> str:
    if type(value) is not str or not _APP_NAME.fullmatch(value):
        raise ConfigurationError()
    return value


def _app_id(value: object) -> str:
    if type(value) is not str or not _APP_ID.fullmatch(value):
        raise ConfigurationError("router_response")
    return value


def _provider_name(value: object) -> str:
    return ROUTER_PROVIDER if value == ROUTER_PROVIDER else _app_name(value)


def _check_json(value: object, *, max_nodes: int = 100000) -> None:
    """Bound depth, nodes, scalar conversion work, and reject lossy Python types."""
    nodes = 0
    string_bytes = 0

    def visit(item: object, depth: int) -> None:
        nonlocal nodes, string_bytes
        nodes += 1
        if depth > 32 or nodes > max_nodes:
            raise ConfigurationError("configuration_too_large")
        if type(item) is str:
            string_bytes += len(item.encode("utf-8"))
            if string_bytes > MAX_CONFIGURATION_BYTES:
                raise ConfigurationError("configuration_too_large")
        elif type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise ConfigurationError()
                visit(key, depth + 1)
                visit(child, depth + 1)
        elif type(item) is list:
            for child in item:
                visit(child, depth + 1)
        elif type(item) is int:
            if item.bit_length() > 213 or len(str(item)) > 64:
                raise ConfigurationError()
        elif type(item) is float:
            if not math.isfinite(item):
                raise ConfigurationError()
        elif item is not None and type(item) is not bool:
            raise ConfigurationError()

    try:
        visit(value, 0)
    except (UnicodeError, RecursionError):
        raise ConfigurationError() from None


def _json_bytes(value: object, *, limit: int = MAX_CONFIGURATION_BYTES, max_nodes: int = 100000) -> bytes:
    _check_json(value, max_nodes=max_nodes)
    try:
        data = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ConfigurationError() from None
    if len(data) > limit:
        raise ConfigurationError("configuration_too_large")
    return data


def _decode_json(data: bytes) -> object:
    def pairs(items: list[tuple[str, object]]) -> dict:
        result = {}
        for key, value in items:
            if key in result:
                raise ConfigurationError()
            result[key] = value
        return result

    def constant(value: str) -> None:
        raise ConfigurationError()

    try:
        result = json.loads(data.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
        _check_json(result)
        return result
    except ConfigurationError:
        raise
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ConfigurationError() from None


def _validate_source(value: object) -> None:
    if type(value) is not dict:
        raise ConfigurationError()
    kind = value.get("kind")
    if kind in ("local", "unknown"):
        _object(value, {"kind"})
    elif kind == "builtin":
        source = _object(value, {"kind", "identifier"})
        if not re.fullmatch(r"[A-Za-z0-9_-]+", _string(source["identifier"])):
            raise ConfigurationError()
    elif kind == "remote":
        source = _object(value, {"kind", "repo_url", "ref"})
        url = _string(source["repo_url"])
        try:
            parsed = urlsplit(url)
            path = unquote(parsed.path)
            if (
                parsed.scheme not in {"http", "https", "git"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.port == 0
                or any(c in url for c in "\\?#;@")
                or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in url)
                or any(c in path for c in "@\\?#;%")
                or any((c.isspace() and c != " ") or ord(c) < 32 or ord(c) == 127 for c in path)
                or path.count("/") != parsed.path.count("/")
                or not path.strip("/")
                or "//" in path
                or any(part in {".", ".."} for part in path.split("/"))
            ):
                raise ConfigurationError()
            ref = source["ref"]
            if ref is not None:
                ref = _string(ref)
                if (
                    ref.startswith(("-", "/")) or ref.endswith("/") or ".." in ref or "//" in ref
                    or any(c in ref for c in "@\\?#;:^~[*")
                    or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in ref)
                ):
                    raise ConfigurationError()
        except (ValueError, UnicodeError):
            raise ConfigurationError() from None
    else:
        raise ConfigurationError()


def _validate_definitions(value: object) -> dict:
    try:
        document = _object(value, {"schema_version", "mode", "apps", "platform_api_tokens"})
        if type(document["schema_version"]) is not int or document["schema_version"] != 2 or document["mode"] != "private":
            raise ConfigurationError()
        names = set()
        for app in _list(document["apps"]):
            app = _object(app, {"name", "source", "port_mappings"})
            name = _app_name(app["name"])
            if name in names:
                raise ConfigurationError()
            names.add(name)
            _validate_source(app["source"])
            labels = set()
            for port in _list(app["port_mappings"]):
                port = _object(port, {"label", "container_port", "host_port"})
                label = _string(port["label"])
                if label in labels:
                    raise ConfigurationError()
                labels.add(label)
                container, host = port["container_port"], port["host_port"]
                if type(container) is not int or not 1 <= container <= 65535:
                    raise ConfigurationError()
                if type(host) is not int or not (host == 0 or 25 <= host <= 65535):
                    raise ConfigurationError()
        hashes = set()
        for token in _list(document["platform_api_tokens"]):
            token = _object(token, {"name", "token_hash", "expires_at"})
            _string(token["name"], empty=True)
            verifier = _string(token["token_hash"])
            if not re.fullmatch(r"[0-9a-f]{64}", verifier) or verifier in hashes:
                raise ConfigurationError()
            hashes.add(verifier)
            if token["expires_at"] is not None:
                expiry = datetime.fromisoformat(_string(token["expires_at"]))
                if expiry.utcoffset() is None:
                    raise ConfigurationError()
        _json_bytes(document, limit=MAX_DEFINITION_BYTES, max_nodes=20000)
        return document
    except (ValueError, TypeError, UnicodeError, OverflowError):
        raise ConfigurationError("invalid_definitions") from None


def _validate_sharing_definitions(value: object) -> dict:
    """Owner Sharing exports have the same app records, but no token field."""
    try:
        document = _object(value, {"schema_version", "mode", "apps"})
        if document["mode"] != "sharing":
            raise ConfigurationError()
        _validate_definitions({**document, "mode": "private", "platform_api_tokens": []})
        return document
    except ConfigurationError:
        raise ConfigurationError("router_response") from None


def _grant(value: object) -> None:
    if type(value) not in (str, dict, list):
        raise ConfigurationError()


def _record_key(value: object) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _validate_runtime(value: object, names: set[str]) -> dict | None:
    if value is None:
        return None
    try:
        runtime = _object(value, {"apps", "providers"})
        if type(runtime["apps"]) is not dict or runtime["apps"].keys() != names:
            raise ConfigurationError()
        for name, app in runtime["apps"].items():
            _app_name(name)
            app = _object(app, {"status", "global_grants", "unresolved_provider_grants"})
            if _string(app["status"]) not in APP_STATUSES:
                raise ConfigurationError()
            for field in ("global_grants", "unresolved_provider_grants"):
                seen = set()
                for entry in _list(app[field]):
                    entry = _object(entry, {"service_url", "grant"} | ({"provider_name"} if field == "unresolved_provider_grants" else set()))
                    _string(entry["service_url"])
                    _grant(entry["grant"])
                    if field == "unresolved_provider_grants" and entry["provider_name"] is not None:
                        _provider_name(entry["provider_name"])
                    key = _record_key(entry)
                    # Two removed providers may have issued the same scoped
                    # grant. Their names are unknowable, but both unresolved
                    # records must survive capture/subsetting losslessly.
                    orphan = field == "unresolved_provider_grants" and entry["provider_name"] is None
                    if key in seen and not orphan:
                        raise ConfigurationError()
                    seen.add(key)
        seen_providers = set()
        defaults = set()
        for provider in _list(runtime["providers"]):
            provider = _object(provider, {"service_url", "app_name", "is_default"})
            service = _string(provider["service_url"])
            name = _provider_name(provider["app_name"])
            if type(provider["is_default"]) is not bool or (service, name) in seen_providers:
                raise ConfigurationError()
            seen_providers.add((service, name))
            if provider["is_default"]:
                if service in defaults:
                    raise ConfigurationError()
                defaults.add(service)
        return runtime
    except (ValueError, TypeError):
        raise ConfigurationError("invalid_runtime") from None


def _validate_bundle(bundle: object) -> dict:
    bundle = _object(bundle, {"format_version", "backup_app_name", "definitions", "runtime"})
    if type(bundle["format_version"]) is not int or bundle["format_version"] != 1:
        raise ConfigurationError()
    _app_name(bundle["backup_app_name"])
    document = _validate_definitions(bundle["definitions"])
    _validate_runtime(bundle["runtime"], {app["name"] for app in document["apps"]})
    return bundle


def serialize_configuration(bundle: dict) -> bytes:
    """Validate and encode a bounded bundle, without normalizing any records."""
    data = _json_bytes(bundle)
    _validate_bundle(bundle)
    return data


def parse_configuration(data: bytes) -> dict:
    """Parse strict UTF-8 JSON, rejecting duplicate keys and unsupported shapes."""
    if type(data) is not bytes:
        raise ConfigurationError()
    if len(data) > MAX_CONFIGURATION_BYTES:
        raise ConfigurationError("configuration_too_large")
    return _validate_bundle(_decode_json(data))


def subset_configuration(bundle: dict, names: set[str]) -> dict:
    """Copy selected apps; keep all platform token records and relevant providers.

    Providers for services consumed/provided by the selected apps are retained,
    including an external selected default, so recovery can report missing
    dependencies rather than silently switch a consumer to different data.
    """
    result = parse_configuration(serialize_configuration(bundle))
    if type(names) is not set or any(type(name) is not str for name in names):
        raise ConfigurationError()
    known = {app["name"] for app in result["definitions"]["apps"]}
    if not names <= known:
        raise ConfigurationError()
    result["definitions"]["apps"] = [app for app in result["definitions"]["apps"] if app["name"] in names]
    runtime = result["runtime"]
    if runtime is not None:
        runtime["apps"] = {name: app for name, app in runtime["apps"].items() if name in names}
        services = {
            entry["service_url"]
            for app in runtime["apps"].values()
            for field in ("global_grants", "unresolved_provider_grants")
            for entry in app[field]
        }
        services.update(p["service_url"] for p in runtime["providers"] if p["app_name"] in names)
        # Private Git clones are router OAuth consumers, not app grant holders.
        if any(app["source"]["kind"] == "remote" for app in result["definitions"]["apps"]):
            services.add("github.com/imbue-openhost/openhost/services/oauth")
        runtime["providers"] = [p for p in runtime["providers"] if p["service_url"] in services]
    return result


# The owner probe is one empty private bundle parsed by the router. Parsing it
# is the only supported way to confirm authority, so both the request and the
# expected answer live here; a second copy could silently diverge into a
# weaker or stricter check.
OWNER_PROBE = {"content": '{"schema_version":2,"mode":"private","apps":[],"platform_api_tokens":[]}'}
OWNER_PROBE_RESULT = {
    "schema_version": 2, "mode": "private", "apps": [], "platform_api_token_names": []
}


async def confirm_owner(router_url: str, token: str, timeout: float = 15.0) -> bool:
    """Whether the router accepts this token as the platform owner.

    False covers every failure, including a router that cannot be reached: no
    caller may be treated as the owner on a guess or on an error response.
    """
    if not token:
        return False
    try:
        result = await RouterClient(router_url, token, timeout=timeout).post(
            "/api/app-definitions/parse", dict(OWNER_PROBE)
        )
    except (ConfigurationError, TimeoutError, httpx.HTTPError):
        return False
    return result == OWNER_PROBE_RESULT


class RouterClient:
    """Finite, no-redirect JSON GET/POST requests to an allowlist of router APIs.

    Each request owns its HTTP client, so cancellation cannot leak a connection
    pool. Authentication is sent only in Authorization, never a URL or body.
    ``get`` returns a list for catalogue endpoints and a dict for app_status;
    ``post`` always returns a dict. HTTP failures never read/render their body.
    """

    def __init__(self, router_url: str, token: str, *, timeout: float = 60.0):
        try:
            if type(router_url) is not str or type(token) is not str:
                raise ValueError
            parsed = urlsplit(router_url)
            if (
                parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
                or any(c in router_url for c in "\\?#")
                or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in router_url)
                or parsed.port == 0
                or not token or len(token) > 8192 or any(not 33 <= ord(c) <= 126 for c in token)
                or type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0
            ):
                raise ValueError
        except (ValueError, TypeError, UnicodeError):
            raise ConfigurationError("invalid_router") from None
        self._origin = router_url.rstrip("/")
        self._token = token
        self._timeout = timeout

    async def get(self, path: str) -> dict | list:
        if type(path) is not str:
            raise ConfigurationError("invalid_request")
        if path in {"/api/apps", "/api/permissions/v2", "/api/services/v2"}:
            expected = list
        elif type(path) is str and re.fullmatch(r"/api/app_status/[1-9A-HJ-NP-Za-km-z]{12}", path):
            expected = dict
        else:
            raise ConfigurationError("invalid_request")
        return await self._request("GET", path, None, expected)

    async def post(self, path: str, data: dict | None = None) -> dict:
        if type(path) is not str or not (
            path in {"/api/app-definitions/parse", "/api/app-definitions/import-private", "/api/app-definitions/export", "/api/add_app",
                     "/api/permissions/v2/grant_global_scoped", "/api/permissions/v2/revoke", "/api/services/v2/defaults"}
            or re.fullmatch(r"/(?:stop_app|reload_app)/[1-9A-HJ-NP-Za-km-z]{12}", path)
        ):
            raise ConfigurationError("invalid_request")
        if data is not None and type(data) is not dict:
            raise ConfigurationError("invalid_request")
        return await self._request("POST", path, data, dict)

    async def export_private(self) -> dict:
        return await self._request("POST", _EXPORT_PATH, {"mode": "private"}, dict, private_export=True)

    async def _request(self, method: str, path: str, data: dict | None, expected: type, *, private_export: bool = False):
        limit = MAX_DEFINITION_BYTES if private_export else MAX_CONFIGURATION_BYTES
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json", "Accept-Encoding": "identity"}
        if private_export:
            headers["X-OpenHost-Provider"] = ROUTER_PROVIDER
        content = None
        if data is not None:
            content = _json_bytes(data, limit=6 * MAX_DEFINITION_BYTES + 1024)
            headers["Content-Type"] = "application/json"
        try:
            async with asyncio.timeout(self._timeout):
                async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=False, trust_env=False) as client:
                    async with client.stream(method, self._origin + path, headers=headers, content=content) as response:
                        if response.status_code != 200:
                            code = "router_request"
                            if private_export and response.status_code == 403:
                                code = "export_approval"
                            elif response.status_code in {401, 403}:
                                code = "export_auth" if private_export else "router_auth"
                            raise ConfigurationError(code, status_code=response.status_code)
                        if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
                            raise ConfigurationError("router_response")
                        if response.headers.get("content-encoding", "identity").lower() != "identity":
                            raise ConfigurationError("router_response")
                        length = response.headers.get("content-length")
                        if length is not None and (not length.isdecimal() or int(length) > limit):
                            raise ConfigurationError("router_response")
                        body = bytearray()
                        async for chunk in response.aiter_bytes():
                            if len(body) + len(chunk) > limit:
                                raise ConfigurationError("router_response")
                            body.extend(chunk)
                        try:
                            result = _decode_json(bytes(body))
                        except ConfigurationError:
                            raise ConfigurationError("router_response") from None
                        if type(result) is not expected:
                            raise ConfigurationError("router_response")
                        return result
        except ConfigurationError:
            raise
        except (httpx.HTTPError, OSError, TimeoutError, ValueError, TypeError, UnicodeError):
            raise ConfigurationError("router_connection") from None


def _inventory(value: object) -> dict[str, dict]:
    """Validate an /api/apps catalogue and discard untrusted error details."""
    try:
        apps, ids = {}, set()
        for app in _list(value):
            app = _object(app, {"app_id", "name", "status"}, {"error_message"})
            name, app_id = _app_name(app["name"]), _app_id(app["app_id"])
            status = _string(app["status"])
            if name in apps or app_id in ids or status not in APP_STATUSES:
                raise ConfigurationError()
            if app.get("error_message") is not None:
                _string(app["error_message"], empty=True)
            apps[name] = {"name": name, "app_id": app_id, "status": status}
            ids.add(app_id)
        return apps
    except ConfigurationError:
        raise ConfigurationError("router_response") from None


def _permissions(value: object) -> list[dict]:
    try:
        result, seen = [], set()
        for entry in _list(value):
            entry = _object(entry, {"consumer_app_id", "service_url", "grant", "scope", "provider_app_id"})
            _app_id(entry["consumer_app_id"])
            _string(entry["service_url"])
            _grant(entry["grant"])
            if entry["scope"] == "global":
                if entry["provider_app_id"] is not None:
                    raise ConfigurationError()
            elif entry["scope"] == "app":
                if entry["provider_app_id"] != ROUTER_PROVIDER:
                    _app_id(entry["provider_app_id"])
            else:
                raise ConfigurationError()
            key = _record_key(entry)
            if key in seen:
                raise ConfigurationError()
            seen.add(key)
            result.append(entry)
        return result
    except ConfigurationError:
        raise ConfigurationError("router_response") from None


def _providers(value: object, inventory: dict[str, dict]) -> list[dict]:
    try:
        result, seen, defaults = [], set(), set()
        for entry in _list(value):
            entry = _object(entry, {"service_url", "app_id", "app_name", "service_version", "endpoint", "status", "is_default"})
            service = _string(entry["service_url"])
            for field in ("app_name", "service_version", "endpoint", "status"):
                _string(entry[field])
            if entry["app_id"] == ROUTER_PROVIDER:
                if entry["status"] != "running":
                    raise ConfigurationError()
                name = ROUTER_PROVIDER
            else:
                name = _app_name(entry["app_name"])
                if name not in inventory or inventory[name]["app_id"] != _app_id(entry["app_id"]):
                    raise ConfigurationError()
            if entry["status"] not in APP_STATUSES or type(entry["is_default"]) is not bool:
                raise ConfigurationError()
            if (service, name) in seen or (entry["is_default"] and service in defaults):
                raise ConfigurationError()
            seen.add((service, name))
            if entry["is_default"]:
                defaults.add(service)
            result.append({**entry, "app_name": name})
        return result
    except ConfigurationError:
        raise ConfigurationError("router_response") from None


async def capture_configuration(router_url: str, app_token: str, owner_token: str | None = None, backup_app_name: str = "backup") -> dict:
    """Capture Private definitions; a configured owner token must also succeed."""
    _app_name(backup_app_name)
    document = _validate_definitions(await RouterClient(router_url, app_token).export_private())
    runtime = None
    if owner_token is not None:
        client = RouterClient(router_url, owner_token)
        inventory = _inventory(await client.get("/api/apps"))
        if inventory.keys() != {app["name"] for app in document["apps"]}:
            raise ConfigurationError("capture_changed")
        permissions = _permissions(await client.get("/api/permissions/v2"))
        providers = _providers(await client.get("/api/services/v2"), inventory)
        if _inventory(await client.get("/api/apps")) != inventory:
            raise ConfigurationError("capture_changed")
        names = {app["app_id"]: name for name, app in inventory.items()}
        names[ROUTER_PROVIDER] = ROUTER_PROVIDER
        runtime = {"apps": {name: {"status": app["status"], "global_grants": [], "unresolved_provider_grants": []}
                            for name, app in inventory.items()},
                   "providers": [{key: p[key] for key in ("service_url", "app_name", "is_default")} for p in providers]}
        for permission in permissions:
            consumer = names.get(permission["consumer_app_id"])
            if consumer not in runtime["apps"]:
                raise ConfigurationError("capture_changed")
            grant = {"service_url": permission["service_url"], "grant": permission["grant"]}
            if permission["scope"] == "global":
                runtime["apps"][consumer]["global_grants"].append(grant)
            else:
                grant["provider_name"] = names.get(permission["provider_app_id"])
                runtime["apps"][consumer]["unresolved_provider_grants"].append(grant)
        # The allowlisted APIs contain no credential fields. Reject an accidental
        # credential echo in even an opaque grant rather than persist our tokens.
        def echoed(value: object) -> bool:
            if type(value) is str:
                return any(token in value for token in (app_token, owner_token))
            if type(value) is dict:
                return any(echoed(key) or echoed(child) for key, child in value.items())
            return type(value) is list and any(echoed(child) for child in value)

        if echoed(runtime):
            raise ConfigurationError("router_response")
    bundle = {"format_version": 1, "backup_app_name": backup_app_name, "definitions": document, "runtime": runtime}
    return parse_configuration(serialize_configuration(bundle))
