"""Configuration wire contracts, losslessness, and failure/privacy boundaries."""

from __future__ import annotations

import asyncio
import copy
import json
import logging

import httpx
import pytest

import configuration
from configuration import (
    ConfigurationError,
    MAX_CONFIGURATION_BYTES,
    MAX_DEFINITION_BYTES,
    ROUTER_PROVIDER,
    RouterClient,
    capture_configuration,
    parse_configuration,
    serialize_configuration,
    subset_configuration,
)

ORIGIN = "https://router.example"
APP_TOKEN = "app-auth-sentinel-not-for-output"
OWNER_TOKEN = "owner-auth-sentinel-not-for-output"
VERIFIER = "a" * 64
SECRETS = "github.com/imbue-openhost/openhost/services/secrets"
OAUTH = "github.com/imbue-openhost/openhost/services/oauth"
DEFINITIONS = "github.com/cloud-in-a-bottle/cloud-in-a-bottle/services/app-definitions"
REAL_CLIENT = httpx.AsyncClient


def app_definition(name: str, *, kind: str = "remote") -> dict:
    source = {"kind": kind}
    if kind == "remote":
        source.update(repo_url=f"https://example.com/team/{name}.git", ref="release/v2")
    elif kind == "builtin":
        source["identifier"] = name.replace("-", "_")
    return {"name": name, "source": source, "port_mappings": []}


def make_bundle(*names: str, runtime: bool = False) -> dict:
    return {
        "format_version": 1,
        "backup_app_name": "backup",
        "definitions": {
            "schema_version": 2,
            "mode": "private",
            "apps": [app_definition(name) for name in names],
            "platform_api_tokens": [{"name": "key", "token_hash": VERIFIER, "expires_at": None}],
        },
        "runtime": {
            "apps": {name: {"status": "running", "global_grants": [], "unresolved_provider_grants": []} for name in names},
            "providers": [],
        } if runtime else None,
    }


def inventory_entry(name: str, app_id: str, status: str = "running") -> dict:
    return {"name": name, "app_id": app_id, "status": status, "error_message": None}


def provider_entry(service: str, name: str, app_id: str, *, default: bool = True, status: str = "running") -> dict:
    return {"service_url": service, "app_name": name, "app_id": app_id, "is_default": default,
            "status": status, "service_version": "1.0.0", "endpoint": "/v2"}


@pytest.fixture
def mock_http(monkeypatch):
    def install(handler):
        requests = []
        options = []

        async def dispatch(request):
            requests.append(request)
            response = handler(request)
            return await response if hasattr(response, "__await__") else response

        def client(**kwargs):
            options.append(kwargs)
            return REAL_CLIENT(transport=httpx.MockTransport(dispatch), **kwargs)

        monkeypatch.setattr(configuration.httpx, "AsyncClient", client)
        return requests, options

    return install


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks, *, delay=0):
        self.chunks = chunks
        self.delay = delay
        self.read = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            self.read += 1
            if self.delay:
                await asyncio.sleep(self.delay)
            yield chunk

    async def aclose(self):
        self.closed = True


def test_exact_private_records_round_trip_and_copy():
    bundle = make_bundle("notes", runtime=True)
    tokens = bundle["definitions"]["platform_api_tokens"] = [
        {"name": "", "token_hash": "1" * 64, "expires_at": None},
        {"name": "line 1\n雪\r\n\tline 2\x00", "token_hash": "2" * 64, "expires_at": "2000-01-01T01:02:03.000001-07:30"},
        {"name": "duplicate", "token_hash": "3" * 64, "expires_at": "2032-12-31T00:00:00+00:00"},
        {"name": "duplicate", "token_hash": "4" * 64, "expires_at": "2032-12-31T00:00:00Z"},
    ]
    bundle["definitions"]["apps"][0]["port_mappings"] = [
        {"label": "日本語\nport", "container_port": 8080, "host_port": 0},
        {"label": "https", "container_port": 8443, "host_port": 443},
    ]
    bundle["runtime"]["apps"]["notes"]["global_grants"] = [
        {"service_url": SECRETS, "grant": {"key": "雪", "values": [True, 1, 1.0, -0.0, None, ""]}}
    ]
    data = serialize_configuration(bundle)
    assert b"\xe9\x9b\xaa" in data
    parsed = parse_configuration(data)
    assert parsed == bundle
    assert parsed["definitions"]["platform_api_tokens"] == tokens
    assert serialize_configuration(parsed) == data
    parsed["definitions"]["platform_api_tokens"][0]["name"] = "changed"
    parsed["runtime"]["apps"]["notes"]["global_grants"][0]["grant"]["values"].append("changed")
    assert tokens[0]["name"] == ""
    assert len(bundle["runtime"]["apps"]["notes"]["global_grants"][0]["grant"]["values"]) == 6


@pytest.mark.parametrize("data", [
    b'{"format_version":1,"format_version":1}',
    b'{"runtime":{"apps":{},"apps":{}}}',
    b'{"secret":"\\ud800"}', b'{"\\udfff":"secret"}', b'"\xff"',
    b'{"secret":NaN}', b'{"secret":Infinity}', b'{"secret":-Infinity}', b'{"secret":1e999}',
    b'{"secret":1} trailing', b'\xef\xbb\xbf{}', b'[]', b'null',
])
def test_strict_json_rejects_ambiguous_or_invalid_documents(data):
    with pytest.raises(ConfigurationError) as error:
        parse_configuration(data)
    assert "secret" not in str(error.value)


@pytest.mark.parametrize("change", [
    lambda b: b.update(format_version=True),
    lambda b: b.update(format_version=2),
    lambda b: b.update(owner_token=OWNER_TOKEN),
    lambda b: b.update(backup_app_name="../backup"),
    lambda b: b["definitions"].update(mode="sharing"),
    lambda b: b["definitions"].update(schema_version=True),
    lambda b: b["definitions"].update(schema_version=1),
    lambda b: b["definitions"].update(environment={"secret": OWNER_TOKEN}),
    lambda b: b["definitions"].pop("platform_api_tokens"),
    lambda b: b["definitions"]["apps"].append(copy.deepcopy(b["definitions"]["apps"][0])),
    lambda b: b["definitions"]["apps"][0].update(name="notes\n"),
    lambda b: b["definitions"]["apps"][0]["source"].update(ref="--upload-pack=secret"),
    lambda b: b["definitions"]["apps"][0]["source"].update(repo_url=f"https://{OWNER_TOKEN}@example.com/repo"),
    lambda b: b["definitions"]["apps"][0]["source"].update(repo_url=f"https://example.com/repo?key={OWNER_TOKEN}"),
    lambda b: b["definitions"]["apps"][0].update(port_mappings=[{"label": "http", "container_port": True, "host_port": 80}]),
    lambda b: b["definitions"]["apps"][0].update(port_mappings=[{"label": "http", "container_port": 80, "host_port": 24}]),
    lambda b: b["definitions"]["platform_api_tokens"].append(copy.deepcopy(b["definitions"]["platform_api_tokens"][0])),
    lambda b: b["definitions"]["platform_api_tokens"][0].update(expires_at="2030-01-01T00:00:00"),
    lambda b: b["definitions"]["platform_api_tokens"][0].update(token_hash="A" * 64),
    lambda b: b["definitions"]["platform_api_tokens"][0].update(token_hash=OWNER_TOKEN),
    lambda b: b["runtime"]["apps"].clear(),
    lambda b: b["runtime"]["apps"]["notes"].update(status=OWNER_TOKEN),
    lambda b: b["runtime"]["apps"]["notes"].update(global_grants=[{"service_url": SECRETS, "grant": True}]),
    lambda b: b["runtime"]["apps"]["notes"].update(unresolved_provider_grants=[{"service_url": OAUTH, "grant": {}, "provider_name": "../other"}]),
    lambda b: b["runtime"].update(providers=[{"service_url": SECRETS, "app_name": "notes", "is_default": 1}]),
    lambda b: b["runtime"].update(providers=[{"service_url": SECRETS, "app_name": name, "is_default": True} for name in ["notes", "secrets"]]),
])
def test_bundle_validation_fails_closed_without_private_details(change):
    bundle = make_bundle("notes", runtime=True)
    change(bundle)
    for operation in (lambda: serialize_configuration(bundle), lambda: parse_configuration(json.dumps(bundle).encode())):
        with pytest.raises(ConfigurationError) as error:
            operation()
        assert OWNER_TOKEN not in str(error.value)
        assert VERIFIER not in str(error.value)
        assert error.value.__cause__ is None


@pytest.mark.parametrize("value", [float("nan"), float("inf"), (1, 2), {1: "bad"}, "\ud800", 10 ** 70])
def test_serializer_rejects_non_json_or_unbounded_python_values(value):
    bundle = make_bundle("notes", runtime=True)
    bundle["runtime"]["apps"]["notes"]["global_grants"] = [{"service_url": SECRETS, "grant": {"value": value}}]
    with pytest.raises(ConfigurationError):
        serialize_configuration(bundle)


def test_independent_bundle_and_canonical_limits():
    with pytest.raises(ConfigurationError):
        parse_configuration(b" " * (MAX_CONFIGURATION_BYTES + 1))
    bundle = make_bundle("notes")
    bundle["definitions"]["platform_api_tokens"][0]["name"] = "x" * MAX_DEFINITION_BYTES
    with pytest.raises(ConfigurationError):
        serialize_configuration(bundle)
    bundle = make_bundle("notes", runtime=True)
    nested = []
    for _ in range(34):
        nested = [nested]
    bundle["runtime"]["apps"]["notes"]["global_grants"] = [{"service_url": SECRETS, "grant": nested}]
    with pytest.raises(ConfigurationError):
        serialize_configuration(bundle)


def test_subset_keeps_whole_instance_tokens_and_external_provider_requirements():
    bundle = make_bundle("notes", "secrets", "oauth", "unrelated", runtime=True)
    bundle["runtime"]["apps"]["notes"]["global_grants"] = [{"service_url": SECRETS, "grant": {"key": ""}}]
    bundle["runtime"]["apps"]["notes"]["unresolved_provider_grants"] = [{"service_url": OAUTH, "provider_name": "oauth", "grant": {"scopes": ["repo"]}}]
    bundle["runtime"]["providers"] = [
        {"service_url": SECRETS, "app_name": "secrets", "is_default": True},
        {"service_url": SECRETS, "app_name": "unrelated", "is_default": False},
        {"service_url": OAUTH, "app_name": "oauth", "is_default": True},
        {"service_url": "unrelated-service", "app_name": "unrelated", "is_default": True},
        {"service_url": DEFINITIONS, "app_name": ROUTER_PROVIDER, "is_default": True},
    ]
    before = copy.deepcopy(bundle)
    subset = subset_configuration(bundle, {"notes"})
    assert [a["name"] for a in subset["definitions"]["apps"]] == ["notes"]
    assert subset["definitions"]["platform_api_tokens"] == bundle["definitions"]["platform_api_tokens"]
    assert list(subset["runtime"]["apps"]) == ["notes"]
    assert subset["runtime"]["providers"] == bundle["runtime"]["providers"][:3]
    assert parse_configuration(serialize_configuration(subset)) == subset
    subset["runtime"]["providers"][0]["is_default"] = False
    assert bundle == before
    empty = subset_configuration(bundle, set())
    assert empty["definitions"]["apps"] == []
    assert empty["runtime"] == {"apps": {}, "providers": []}
    assert empty["definitions"]["platform_api_tokens"] == before["definitions"]["platform_api_tokens"]
    with pytest.raises(ConfigurationError):
        subset_configuration(bundle, {"not-in-backup"})


async def test_capture_uses_app_token_private_builtin_and_no_owner_when_absent(mock_http):
    document = make_bundle("backup", "notes")["definitions"]
    requests, options = mock_http(lambda request: httpx.Response(200, json=document))
    result = await capture_configuration(ORIGIN, APP_TOKEN)
    assert result == {"format_version": 1, "backup_app_name": "backup", "definitions": document, "runtime": None}
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert request.url == ORIGIN + "/api/services/v2/call/definitions/export"
    assert json.loads(request.content) == {"mode": "private"}
    assert request.headers["Accept"] == "application/json"
    assert request.headers["X-OpenHost-Provider"] == ROUTER_PROVIDER
    assert request.headers["Authorization"] == f"Bearer {APP_TOKEN}"
    assert APP_TOKEN not in request.url.query.decode()
    assert APP_TOKEN.encode() not in request.content
    assert all(o["timeout"] > 0 and o["follow_redirects"] is False and o["trust_env"] is False for o in options)


async def test_owner_sharing_export_uses_owner_endpoint_and_json_without_service_routing(mock_http):
    document = {"schema_version": 2, "mode": "sharing", "apps": [app_definition("notes")]}
    requests, _ = mock_http(lambda r: httpx.Response(200, json=document))
    assert await RouterClient(ORIGIN, OWNER_TOKEN).post("/api/app-definitions/export", {"mode": "sharing"}) == document
    request, = requests
    assert request.method == "POST" and request.url == ORIGIN + "/api/app-definitions/export"
    assert json.loads(request.content) == {"mode": "sharing"}
    assert request.headers["Authorization"] == f"Bearer {OWNER_TOKEN}"
    assert request.headers["Accept"] == "application/json"
    assert "X-OpenHost-Provider" not in request.headers


@pytest.mark.parametrize("status", [401, 403])
async def test_owner_sharing_export_auth_errors_do_not_request_app_grant(mock_http, status):
    stream = Chunks([OWNER_TOKEN.encode()])
    mock_http(lambda r: httpx.Response(status, stream=stream))
    with pytest.raises(ConfigurationError) as error:
        await RouterClient(ORIGIN, OWNER_TOKEN).post("/api/app-definitions/export", {"mode": "sharing"})
    assert error.value.code == "router_auth"
    assert stream.read == 0 and stream.closed
    assert OWNER_TOKEN not in str(error.value)


async def test_capture_exact_runtime_resolves_ids_and_preserves_orphan_grants(mock_http):
    document = make_bundle("backup", "notes", "secrets", "oauth")["definitions"]
    ids = {"backup": "B" * 12, "notes": "N" * 12, "secrets": "S" * 12, "oauth": "Q" * 12}
    global_grant = {"service_url": SECRETS, "grant": {"key": "", "where": ["雪", 1.5, True, None]}}
    responses = {
        "/api/services/v2/call/definitions/export": document,
        "/api/apps": [inventory_entry(n, i, "stopped" if n == "notes" else "running") for n, i in ids.items()],
        "/api/permissions/v2": [
            {**global_grant, "consumer_app_id": ids["notes"], "scope": "global", "provider_app_id": None},
            {"consumer_app_id": ids["notes"], "service_url": OAUTH, "grant": {"provider": "github", "scopes": ["repo"]}, "scope": "app", "provider_app_id": ids["oauth"]},
            {"consumer_app_id": ids["notes"], "service_url": "orphan", "grant": "EXACT", "scope": "app", "provider_app_id": "Z" * 12},
        ],
        "/api/services/v2": [provider_entry(SECRETS, "secrets", ids["secrets"]), provider_entry(OAUTH, "oauth", ids["oauth"]), provider_entry(DEFINITIONS, "OpenHost Router", ROUTER_PROVIDER)],
    }
    requests, _ = mock_http(lambda r: httpx.Response(200, json=responses[r.url.path]))
    result = await capture_configuration(ORIGIN, APP_TOKEN, OWNER_TOKEN)
    assert result["definitions"] == document
    assert result["runtime"]["apps"]["notes"] == {
        "status": "stopped", "global_grants": [global_grant], "unresolved_provider_grants": [
            {"service_url": OAUTH, "grant": {"provider": "github", "scopes": ["repo"]}, "provider_name": "oauth"},
            {"service_url": "orphan", "grant": "EXACT", "provider_name": None},
        ],
    }
    assert result["runtime"]["providers"][-1] == {"service_url": DEFINITIONS, "app_name": ROUTER_PROVIDER, "is_default": True}
    runtime_bytes = json.dumps(result["runtime"])
    assert all(value not in runtime_bytes for value in [*ids.values(), APP_TOKEN, OWNER_TOKEN])
    assert [r.headers["Authorization"] for r in requests] == [f"Bearer {APP_TOKEN}"] + [f"Bearer {OWNER_TOKEN}"] * 4
    assert all("X-OpenHost-Provider" not in r.headers for r in requests[1:])


@pytest.mark.parametrize("status", [301, 302, 307, 308, 401, 403, 404, 500])
async def test_failed_export_does_not_read_private_error_or_redirect(mock_http, caplog, status):
    caplog.set_level(logging.DEBUG)
    stream = Chunks([(APP_TOKEN + OWNER_TOKEN + VERIFIER).encode()])
    requests, _ = mock_http(lambda r: httpx.Response(status, headers={"Content-Type": "application/json", "Location": "https://other.example/leak"}, stream=stream))
    with pytest.raises(ConfigurationError) as error:
        await capture_configuration(ORIGIN, APP_TOKEN, OWNER_TOKEN)
    assert len(requests) == 1
    assert stream.read == 0 and stream.closed
    if status == 403:
        assert error.value.code == "export_approval"
        assert "Approve" in str(error.value) and "Private" in str(error.value)
    for sentinel in (APP_TOKEN, OWNER_TOKEN, VERIFIER):
        assert sentinel not in str(error.value) + repr(error.value) + caplog.text
    assert error.value.__cause__ is None


@pytest.mark.parametrize("response", [
    lambda d: httpx.Response(200, text=f"<html>{OWNER_TOKEN}</html>"),
    lambda d: httpx.Response(200, json={**d, "mode": "sharing"}),
    lambda d: httpx.Response(200, json={**d, "schema_version": 1}),
    lambda d: httpx.Response(200, json={"error": OWNER_TOKEN}),
    lambda d: httpx.Response(200, json=[]),
    lambda d: httpx.Response(200, content=b'{"schema_version":2,"schema_version":1}', headers={"Content-Type": "application/json"}),
    lambda d: httpx.Response(200, content=b'{"token_hash":"\\ud800"}', headers={"Content-Type": "application/json"}),
])
async def test_malformed_private_success_is_never_a_data_only_success(mock_http, response):
    requests, _ = mock_http(lambda r: response(make_bundle("notes")["definitions"]))
    with pytest.raises(ConfigurationError) as error:
        await capture_configuration(ORIGIN, APP_TOKEN, OWNER_TOKEN)
    assert len(requests) == 1
    assert OWNER_TOKEN not in str(error.value)


@pytest.mark.parametrize("failure_path", ["/api/apps", "/api/permissions/v2", "/api/services/v2"])
async def test_configured_owner_failure_fails_capture_instead_of_omitting_runtime(mock_http, failure_path):
    responses = {"/api/services/v2/call/definitions/export": make_bundle("notes")["definitions"],
                 "/api/apps": [inventory_entry("notes", "N" * 12)], "/api/permissions/v2": [], "/api/services/v2": []}
    requests, _ = mock_http(lambda r: httpx.Response(403, json={"error": OWNER_TOKEN}) if r.url.path == failure_path else httpx.Response(200, json=responses[r.url.path]))
    with pytest.raises(ConfigurationError) as error:
        await capture_configuration(ORIGIN, APP_TOKEN, OWNER_TOKEN)
    assert error.value.code == "router_auth"
    assert requests[-1].url.path == failure_path


async def test_empty_configured_owner_token_is_not_treated_as_absent(mock_http):
    requests, _ = mock_http(lambda r: httpx.Response(200, json=make_bundle("notes")["definitions"]))
    with pytest.raises(ConfigurationError):
        await capture_configuration(ORIGIN, APP_TOKEN, "")
    assert len(requests) == 1


async def test_capture_detects_changed_inventory_and_credential_echo(mock_http):
    document = make_bundle("notes")["definitions"]
    responses = {"/api/services/v2/call/definitions/export": document, "/api/apps": [], "/api/services/v2": [], "/api/permissions/v2": []}
    mock_http(lambda r: httpx.Response(200, json=responses[r.url.path]))
    with pytest.raises(ConfigurationError) as error:
        await capture_configuration(ORIGIN, APP_TOKEN, OWNER_TOKEN)
    assert error.value.code == "capture_changed"
    responses["/api/apps"] = [inventory_entry("notes", "N" * 12)]
    responses["/api/permissions/v2"] = [{"consumer_app_id": "N" * 12, "service_url": SECRETS, "scope": "global", "provider_app_id": None, "grant": {"key": OWNER_TOKEN}}]
    with pytest.raises(ConfigurationError) as error:
        await capture_configuration(ORIGIN, APP_TOKEN, OWNER_TOKEN)
    assert OWNER_TOKEN not in str(error.value)


async def test_capture_preserves_distinct_orphan_scoped_records(mock_http):
    document = make_bundle("notes")["definitions"]
    responses = {
        "/api/services/v2/call/definitions/export": document,
        "/api/apps": [inventory_entry("notes", "N" * 12)],
        "/api/services/v2": [],
        "/api/permissions/v2": [
            {"consumer_app_id": "N" * 12, "service_url": OAUTH, "scope": "app", "provider_app_id": provider * 12,
             "grant": {"provider": "github", "scopes": ["repo"]}} for provider in ("Q", "R")
        ],
    }
    mock_http(lambda r: httpx.Response(200, json=responses[r.url.path]))
    result = await capture_configuration(ORIGIN, APP_TOKEN, OWNER_TOKEN)
    grants = result["runtime"]["apps"]["notes"]["unresolved_provider_grants"]
    assert len(grants) == 2 and grants[0] == grants[1]
    assert grants[0]["provider_name"] is None
    assert parse_configuration(serialize_configuration(result)) == result


async def test_capture_rechecks_source_inventory_after_supplemental_reads(mock_http):
    polls = 0

    def response(request):
        nonlocal polls
        if request.url.path.endswith("/export"):
            return httpx.Response(200, json=make_bundle("notes")["definitions"])
        if request.url.path == "/api/apps":
            polls += 1
            return httpx.Response(200, json=[inventory_entry("notes", "N" * 12, "running" if polls == 1 else "stopped")])
        return httpx.Response(200, json=[])

    mock_http(response)
    with pytest.raises(ConfigurationError) as error:
        await capture_configuration(ORIGIN, APP_TOKEN, OWNER_TOKEN)
    assert error.value.code == "capture_changed"


async def test_credential_echo_detection_precedes_json_escaping(mock_http):
    token = 'owner-quote-"-backslash-\\-sentinel'
    responses = {
        "/api/services/v2/call/definitions/export": make_bundle("notes")["definitions"],
        "/api/apps": [inventory_entry("notes", "N" * 12)],
        "/api/services/v2": [],
        "/api/permissions/v2": [{"consumer_app_id": "N" * 12, "service_url": SECRETS, "scope": "global", "provider_app_id": None, "grant": {"key": token}}],
    }
    mock_http(lambda r: httpx.Response(200, json=responses[r.url.path]))
    with pytest.raises(ConfigurationError) as error:
        await capture_configuration(ORIGIN, APP_TOKEN, token)
    assert token not in str(error.value)


@pytest.mark.parametrize("path", [None, [], {}, 1])
async def test_wrong_path_types_and_error_codes_have_fixed_errors(path):
    client = RouterClient(ORIGIN, OWNER_TOKEN)
    with pytest.raises(ConfigurationError) as error:
        await client.get(path)
    assert error.value.code == "invalid_request"
    assert str(ConfigurationError(path)) == "Invalid private configuration bundle."


async def test_streaming_size_limit_without_content_length_and_closes_early(mock_http):
    stream = Chunks([b" " * (MAX_DEFINITION_BYTES // 2)] * 4)
    mock_http(lambda r: httpx.Response(200, headers={"Content-Type": "application/json"}, stream=stream))
    with pytest.raises(ConfigurationError):
        await capture_configuration(ORIGIN, APP_TOKEN)
    assert stream.read == 3
    assert stream.closed


async def test_content_length_limit_precedes_reading(mock_http):
    stream = Chunks([b"not read"])
    mock_http(lambda r: httpx.Response(200, headers={"Content-Type": "application/json", "Content-Length": str(MAX_DEFINITION_BYTES + 1)}, stream=stream))
    with pytest.raises(ConfigurationError):
        await capture_configuration(ORIGIN, APP_TOKEN)
    assert stream.read == 0 and stream.closed


async def test_total_deadline_bounds_slow_stream_and_cancellation_closes_connection(mock_http):
    stream = Chunks([b"[", b"]"], delay=0.03)
    mock_http(lambda r: httpx.Response(200, headers={"Content-Type": "application/json"}, stream=stream))
    with pytest.raises(ConfigurationError) as error:
        await RouterClient(ORIGIN, OWNER_TOKEN, timeout=0.01).get("/api/apps")
    assert error.value.code == "router_connection" and stream.closed
    stream = Chunks([b"[", b"]"], delay=30)
    task = asyncio.create_task(RouterClient(ORIGIN, OWNER_TOKEN).get("/api/apps"))
    while not stream.read:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stream.closed


@pytest.mark.parametrize("origin,token", [
    ("https://user:secret@router.example", OWNER_TOKEN), (ORIGIN + "/api", OWNER_TOKEN),
    (ORIGIN + "?token=secret", OWNER_TOKEN), (ORIGIN + "#secret", OWNER_TOKEN),
    ("file:///tmp/router", OWNER_TOKEN), ("https://router.example:bad", OWNER_TOKEN),
    (ORIGIN, "secret\r\nX-Leak: value"), (ORIGIN, ""),
])
def test_origin_and_header_validation_do_not_leak_input(origin, token):
    with pytest.raises(ConfigurationError) as error:
        RouterClient(origin, token)
    assert "secret" not in str(error.value)


@pytest.mark.parametrize("path", ["https://other.example/api/apps", "//other.example/api/apps", "/api/apps?token=secret", "/api/app_status/../other", "/remove_app/" + "N" * 12])
async def test_router_client_rejects_non_allowlisted_paths_without_network(mock_http, path):
    requests, _ = mock_http(lambda r: pytest.fail("Unexpected network request"))
    client = RouterClient(ORIGIN, OWNER_TOKEN)
    with pytest.raises(ConfigurationError):
        await client.get(path)
    with pytest.raises(ConfigurationError):
        await client.post(path, {})
    assert not requests


async def test_router_connection_errors_are_fixed_and_not_chained(mock_http, caplog):
    def fail(request):
        raise httpx.ConnectError(OWNER_TOKEN + VERIFIER, request=request)

    mock_http(fail)
    with pytest.raises(ConfigurationError) as error:
        await RouterClient(ORIGIN, OWNER_TOKEN).get("/api/apps")
    assert error.value.code == "router_connection"
    assert error.value.__cause__ is None
    assert OWNER_TOKEN not in str(error.value) + caplog.text
    assert VERIFIER not in str(error.value) + caplog.text
