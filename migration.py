"""Direct migration is an encrypted restic snapshot plus the ordinary restore.

The source pauses writers, captures a temporary repository, and uploads its
objects. The destination verifies that repository before calling the same
recovery path as a backup restore. No shared storage account is required.
"""
from __future__ import annotations

import asyncio
import copy
import ipaddress
import json
import logging
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import tempfile
import time
from urllib.parse import urlsplit

import httpx

from configuration import (ConfigurationError, RouterClient, _app_id, _decode_json,
                           capture_configuration, confirm_owner, parse_configuration,
                           serialize_configuration, subset_configuration)
from journal import save_journal
from migration_data import app_name, directory, durability_barrier, private_work_dir
from operations import OpKind, OperationLock, drain
from recovery import RecoverySession
import restic_process
import snapshot_configuration as snapshots

MIGRATION_PROTOCOL_VERSION = 5
CHUNK_LIMIT = 14 * 1024 * 1024
MAX_JSON_BYTES = 5 * 1024 * 1024
PEER_REQUEST_TIMEOUT = 120.0
_SESSION = re.compile(r"[0-9a-f]{64}")
_OBJECT_KINDS = {"data", "index", "keys", "snapshots"}
logger = logging.getLogger(__name__)
status: dict | None = None
log: list[str] = []

_ERRORS = {
    "protocol": (400, "Migration requires protocol v5. Upgrade both backup apps."),
    "invalid": (400, "Invalid migration request."),
    "auth": (403, "Destination owner authentication failed."),
    "busy": (409, "Another operation is active."),
    "collision": (409, "A selected app conflicts with the destination backup executor. Change the selection or executor name."),
    "attention": (409, "Migration requires owner inspection and acknowledgment before retrying."),
    "session": (404, "Unknown migration session."),
    "sequence": (409, "Invalid or incomplete migration transfer sequence."),
    "transfer": (400, "Snapshot transfer or verification failed."),
    "failed": (500, "Migration failed. Inspect safe recovery status before retrying."),
}


class MigrationError(ValueError):
    def __init__(self, code="invalid"):
        self.code = code if code in _ERRORS else "invalid"
        self.status_code, message = _ERRORS[self.code]
        super().__init__(message)


def validate_name(name):
    return type(name) is str and 0 < len(name) <= 200 and ".." not in name and bool(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._:T-]*", name))


def _normalize_app_listing(listing):
    if isinstance(listing, dict):
        return [{"name": name, **(info if isinstance(info, dict) else {})} for name, info in listing.items()]
    return [a for a in listing if isinstance(a, dict)] if isinstance(listing, list) else []


def _is_ip_or_localhost(host):
    if host.lower() in {"localhost", "host.docker.internal"} or host.lower().endswith(".local"):
        return True
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def _target_backup_url(url):
    parsed = urlsplit(url if "://" in url else "https://" + url)
    host = parsed.hostname or ""
    if not _is_ip_or_localhost(host) and not host.startswith("backup."):
        host = "backup." + host
    host = f"[{host}]" if ":" in host else host
    return f"{parsed.scheme}://{host}" + (f":{parsed.port}" if parsed.port else "")


def _positive(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise MigrationError()
    return value


def _session_id(value):
    if type(value) is not str or not _SESSION.fullmatch(value):
        raise MigrationError("session")
    return value


def _body(body, fields):
    if type(body) is not dict or type(body.get("version")) is not int or body.get("version") != MIGRATION_PROTOCOL_VERSION:
        raise MigrationError("protocol")
    if body.keys() != fields | {"version"}:
        raise MigrationError()


async def _authenticate_owner(router_url, token, timeout=120):
    if not await confirm_owner(router_url, token, timeout):
        raise MigrationError("auth")


def _restic_env(repository, password):
    # Local temporary repositories must not inherit configured remote-backend
    # selectors, password commands, or credentials from the app's environment.
    env = {k: v for k, v in os.environ.items() if not k.startswith("RESTIC_")}
    return {**env, "RESTIC_REPOSITORY": str(repository), "RESTIC_PASSWORD": password}


async def _restic(repository, password, *args, timeout=3600):
    try:
        return await restic_process.read(["--no-cache", *args], _restic_env(repository, password),
                                         limit=MAX_JSON_BYTES, timeout=timeout)
    except restic_process.ReadError:
        raise MigrationError("transfer") from None


def _repository_object(root, kind, identifier):
    if kind == "config" and identifier == "config":
        return root / "config"
    if kind not in _OBJECT_KINDS or type(identifier) is not str or not _SESSION.fullmatch(identifier):
        raise MigrationError()
    return root / kind / identifier[:2] / identifier if kind == "data" else root / kind / identifier


async def _heartbeat(lock):
    while True:
        lock.touch()
        await asyncio.sleep(1)


async def _cancel_tasks(*tasks):
    pending = [t for t in tasks if t is not None and t is not asyncio.current_task()]
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


def _interrupted_record():
    return {"ok": False, "version": 5, "phase": "interrupted", "needs_attention": True,
            "result": {"ok": False, "error": "Migration journal requires manual inspection."}}


def _receiver_directory(root, work_dir, backup_name):
    """Converge version-named storage once; never abandon an earlier journal."""
    base = private_work_dir(root, work_dir, backup_name)
    work = base / "incoming"
    for name in ("migration-v4", "migration-v5"):
        previous = base / name
        if not previous.exists() and not previous.is_symlink():
            continue
        directory(previous)
        if work.exists():
            directory(work)
            # Two layouts are ambiguous. Persist the gate before moving either
            # journal out of its authoritative location; retain every old tree.
            if any(previous.iterdir()):
                save_journal(work / "journal.json", _interrupted_record())
            previous.rename(work / name)
        else:
            previous.rename(work)
        durability_barrier(base, work)
    return private_work_dir(root, work, backup_name)


class MigrationReceiver:
    """Only transport state lives here; the restore callback owns data recovery."""

    def __init__(self, *, lock, all_app_data, work_dir, router_url, restore,
                 backup_app_name="backup", idle_timeout=300, request_timeout=120):
        self.lock, self.root, self.restore = lock, all_app_data, restore
        self.work = _receiver_directory(all_app_data, work_dir, backup_app_name)
        self.router_url, self.backup_app_name = router_url, app_name(backup_app_name)
        self.idle_timeout, self.request_timeout = _positive(idle_timeout), _positive(request_timeout)
        self._mutex = asyncio.Lock()
        self._record = self._stage = self._job = self._monitor = None
        self._bundle = self._password = self._recovery = self._disposable = None
        self._activity = time.monotonic()
        self._journal = self.work / "journal.json"
        if self._journal.exists() or self._journal.is_symlink():
            self._record = _interrupted_record()
            try:
                if self._journal.is_symlink() or self._journal.stat().st_size > MAX_JSON_BYTES:
                    raise ValueError
                saved = _decode_json(self._journal.read_bytes())
                if type(saved) is not dict or type(saved.get("version")) is not int or saved["version"] not in {4, 5}:
                    raise ValueError
                sid = saved.get("session_id")
                if sid is not None:
                    _session_id(sid)
                phase = saved["phase"]
                if phase not in {"preflighting", "receiving", "finalizing", "complete", "incomplete", "failed", "aborted", "interrupted"}:
                    raise ValueError
                acknowledged = saved.get("acknowledged", False)
                if type(saved["needs_attention"]) is not bool or type(acknowledged) is not bool:
                    raise ValueError
                if sid is None and (phase != "interrupted" or (not saved["needs_attention"] and not acknowledged)):
                    raise ValueError
                if phase == "complete" and saved.get("ok") is not True:
                    raise ValueError
                attention = saved["needs_attention"] or phase == "finalizing" or (phase == "incomplete" and not acknowledged)
                self._record.update(needs_attention=attention, acknowledged=acknowledged)
                if saved["version"] == 5 and sid is not None:
                    self._record["session_id"] = sid
                if "snapshot" in saved:
                    self._record["snapshot"] = _session_id(saved["snapshot"])
                if phase == "complete" and saved.get("ok") is True and not attention:
                    self._record.update(ok=True, phase="complete", result={"ok": True})
                if saved["version"] == 4:
                    # Legacy trees can hold original data, unlike v5 repositories.
                    # Keep them, but publish only the current safe journal shape.
                    self._record.update(phase="interrupted")
                    if not attention:
                        self._record["acknowledged"] = True
                    self._persist(self._record)
                elif not attention and sid is not None:
                    self._disposable = self.work / sid
            except (ValueError, OSError, KeyError, TypeError):
                self._record = _interrupted_record()
                self._disposable = None

    @property
    def journal_status(self):
        return copy.deepcopy(self._record)

    @property
    def needs_attention(self):
        return bool(self._record and self._record["needs_attention"])

    def _persist(self, record):
        save_journal(self._journal, record)

    def _save(self):
        self._persist(self._record)

    async def capabilities(self, *, owner_token):
        await _authenticate_owner(self.router_url, owner_token)
        return {"ok": True, "version": 5, "chunk_limit": CHUNK_LIMIT, "backup_app_name": self.backup_app_name}

    def _lookup(self, sid):
        _session_id(sid)
        if not self._record or self._record.get("session_id") != sid:
            raise MigrationError("session")

    async def start(self, body, *, owner_token):
        _body(body, {"bundle", "password"})
        try:
            bundle = parse_configuration(serialize_configuration(body["bundle"]))
        except (ValueError, TypeError):
            raise MigrationError() from None
        password = body["password"]
        if type(password) is not str or not _SESSION.fullmatch(password):
            raise MigrationError()
        await _authenticate_owner(self.router_url, owner_token)
        if bundle["backup_app_name"] != self.backup_app_name and any(a["name"] == self.backup_app_name for a in bundle["definitions"]["apps"]):
            raise MigrationError("collision")
        async with self._mutex:
            if self.needs_attention:
                raise MigrationError("attention")
            if self.lock.try_acquire(OpKind.MIGRATION):
                raise MigrationError("busy")
            stage = None
            try:
                if self._disposable is not None:
                    if self._disposable.exists() or self._disposable.is_symlink():
                        directory(self._disposable)
                        await drain(asyncio.to_thread(shutil.rmtree, self._disposable))
                    self._disposable = None
                session = RecoverySession(self.router_url, owner_token, bundle, self.backup_app_name)
                await session.preflight()
                if not session.restore_app_names:
                    raise MigrationError()
                sid = secrets.token_hex(32)
                stage = self._stage = self.work / sid
                self._stage.mkdir(mode=0o700)
                for kind in _OBJECT_KINDS | {"locks"}:
                    (self._stage / kind).mkdir(mode=0o700)
                self._bundle, self._password = bundle, password
                self._recovery = session
                self._record = {"ok": True, "version": 5, "session_id": sid, "phase": "receiving",
                                "accepted_apps": list(session.restore_app_names), "result": None, "needs_attention": False}
                self._save()
                self._activity = time.monotonic()
                self._monitor = asyncio.create_task(self._watch())
                return self.journal_status
            except BaseException:
                try:
                    if stage is not None and self._record and self._record.get("session_id") == stage.name:
                        self._record.update(phase="aborted", ok=False)
                    if stage is not None and stage.exists():
                        await drain(asyncio.to_thread(shutil.rmtree, stage))
                finally:
                    self._bundle = self._password = self._recovery = None
                    self.lock.release(OpKind.MIGRATION)
                raise

    async def _watch(self):
        try:
            while self._record["phase"] in {"receiving", "finalizing"}:
                if self._record["phase"] == "finalizing":
                    self.lock.touch()
                await self.expire_stale()
                await asyncio.sleep(min(1, self.idle_timeout / 2))
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Migration receiver monitor failed")

    async def expire_stale(self):
        if self._mutex.locked():
            return
        async with self._mutex:
            if self._record and self._record["phase"] == "receiving" and time.monotonic() - self._activity > self.idle_timeout:
                await self._finish("aborted")

    async def upload(self, sid, kind, identifier, chunks, *, offset, owner_token):
        await _authenticate_owner(self.router_url, owner_token)
        async with self._mutex:
            self._lookup(sid)
            if self._record["phase"] != "receiving":
                raise MigrationError("sequence")
            if type(offset) is not int or not 0 <= offset <= 1024 ** 4:
                raise MigrationError()
            path = _repository_object(self._stage, kind, identifier)
            body = bytearray()
            async with asyncio.timeout(self.request_timeout):
                async for chunk in chunks:
                    if len(body) + len(chunk) > CHUNK_LIMIT:
                        raise MigrationError("transfer")
                    body.extend(chunk)
            if not body or offset > (path.stat().st_size if path.exists() else 0):
                raise MigrationError("sequence")
            def write():
                path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
                # A delayed retry may follow a newer range. Validate overlap and
                # preserve its suffix instead of truncating acknowledged bytes.
                with path.open("r+b" if path.exists() else "wb") as stream:
                    stream.seek(offset)
                    existing = stream.read(len(body)) if stream.readable() else b""
                    if existing != body[:len(existing)]:
                        raise MigrationError("transfer")
                    stream.write(body[len(existing):])
            await drain(asyncio.to_thread(write))
            self._activity = time.monotonic()
            self.lock.touch()
            return {"ok": True, "version": 5, "offset": offset + len(body)}

    async def finalize(self, body, *, owner_token):
        _body(body, {"session_id", "snapshot"})
        await _authenticate_owner(self.router_url, owner_token)
        _session_id(body["snapshot"])
        async with self._mutex:
            self._lookup(body["session_id"])
            if self._record["phase"] in {"finalizing", "complete", "incomplete"}:
                return self.journal_status
            if self._record["phase"] != "receiving":
                raise MigrationError("sequence")
            candidate = {**self._record, "phase": "finalizing", "needs_attention": True, "snapshot": body["snapshot"]}
            try:
                self._persist(candidate)
            except OSError:
                # The prior receiving record remains authoritative; no restore
                # job is created until its finalizing intent is durable.
                raise MigrationError("failed") from None
            self._record = candidate
            self._job = asyncio.create_task(self._finalize_job(body["snapshot"], owner_token))
            return self.journal_status

    async def _finalize_job(self, snapshot_id, owner_token):
        phase = "incomplete"
        try:
            await _restic(self._stage, self._password, "check", "--read-data")
            env = _restic_env(self._stage, self._password)
            captured = await snapshots.read_configuration(snapshot_id, env)
            if captured != self._bundle:
                raise MigrationError("transfer")
            metadata = snapshots.snapshot_metadata(snapshot_id, await _restic(
                self._stage, self._password, "snapshots", "--json", snapshot_id))
            result = await self.restore(metadata, self._stage, self._password, owner_token, self._recovery)
            self._record["result"] = result
            if result.get("ok") is True:
                phase = "complete"
        except BaseException:
            logger.exception("Incoming migration did not complete")
            self._record["result"] = {"ok": False, "error": str(MigrationError("failed"))}
        finally:
            try:
                await drain(self._finish(phase))
            except Exception:
                logger.exception("Could not persist incoming migration completion")

    async def _finish(self, phase):
        candidate = {**self._record, "phase": phase, "ok": phase == "complete", "needs_attention": phase == "incomplete"}
        try:
            try:
                self._persist(candidate)
            except OSError:
                self._record.update(phase="incomplete", ok=False, needs_attention=True)
                raise
            self._record = candidate
            if phase in {"complete", "aborted"} and self._stage is not None:
                try:
                    await drain(asyncio.to_thread(shutil.rmtree, self._stage))
                except OSError:
                    logger.warning("Retaining encrypted migration repository after completion", exc_info=True)
        finally:
            self._bundle = self._password = self._recovery = None
            try:
                monitor = None if self._monitor is asyncio.current_task() else self._monitor
                await drain(_cancel_tasks(monitor))
            finally:
                self.lock.release(OpKind.MIGRATION)

    async def status(self, session_id, *, owner_token):
        await _authenticate_owner(self.router_url, owner_token)
        self._lookup(session_id)
        return self.journal_status

    async def keepalive(self, body, *, owner_token):
        _body(body, {"session_id"})
        await _authenticate_owner(self.router_url, owner_token)
        self._lookup(body["session_id"])
        if self._record["phase"] not in {"receiving", "finalizing"}:
            raise MigrationError("sequence")
        self._activity = time.monotonic()
        self.lock.touch()
        return {"ok": True, "version": 5}

    async def abort(self, body, *, owner_token):
        _body(body, {"session_id"})
        await _authenticate_owner(self.router_url, owner_token)
        async with self._mutex:
            self._lookup(body["session_id"])
            if self._record["phase"] == "finalizing":
                raise MigrationError("busy")
            if self._record["phase"] == "receiving":
                await self._finish("aborted")
            return self.journal_status

    async def acknowledge(self, *, owner_token):
        await _authenticate_owner(self.router_url, owner_token)
        if self.lock.busy or self._mutex.locked():
            raise MigrationError("busy")
        if self.needs_attention:
            candidate = {**self._record, "phase": "interrupted", "needs_attention": False, "acknowledged": True}
            try:
                self._persist(candidate)
            except OSError:
                raise MigrationError("failed") from None
            self._record = candidate
            if self._record.get("session_id"):
                self._disposable = self.work / self._record["session_id"]
        return self.journal_status


class SourceRecoveryRecord:
    """Durable cutover intent; acknowledgment accepts manual responsibility."""
    def __init__(self, *, lock, all_app_data, work_dir, router_url, backup_app_name="backup"):
        self.lock, self.router_url = lock, router_url
        self.work = private_work_dir(all_app_data, work_dir, backup_app_name)
        self._journal = self.work / "migration-source-journal.json"
        self._record, self._live = None, False
        if self._journal.exists() or self._journal.is_symlink():
            self._record = {"version": 1, "phase": "interrupted", "needs_attention": True,
                            "acknowledged": False, "session_id": None, "selected_apps": [],
                            "apps_before": [], "restart_pending": [], "ok": False}
            try:
                if self._journal.is_symlink() or self._journal.stat().st_size > MAX_JSON_BYTES:
                    raise ValueError
                saved = _decode_json(self._journal.read_bytes())
                if type(saved) is not dict or saved.keys() != self._record.keys() or type(saved["version"]) is not int or saved["version"] != 1:
                    raise ValueError
                if saved["session_id"] is not None:
                    _session_id(saved["session_id"])
                for field in ("needs_attention", "acknowledged", "ok"):
                    if type(saved[field]) is not bool:
                        raise ValueError
                if saved["phase"] not in {"stopping", "incomplete", "complete", "interrupted"}:
                    raise ValueError
                for field in ("selected_apps", "restart_pending"):
                    if type(saved[field]) is not list or len(set(saved[field])) != len(saved[field]):
                        raise ValueError
                    for name in saved[field]:
                        app_name(name)
                names = set()
                for entry in saved["apps_before"]:
                    if type(entry) is not dict or entry.keys() != {"name", "app_id", "status"}:
                        raise ValueError
                    app_name(entry["name"])
                    _app_id(entry["app_id"])
                    if entry["status"] not in {"running", "stopped", "error"} or entry["name"] in names:
                        raise ValueError
                    names.add(entry["name"])
                if not set(saved["selected_apps"]) <= names or not set(saved["restart_pending"]) <= names:
                    raise ValueError
                self._record.update(saved, phase="interrupted", ok=False)
                if saved["phase"] == "stopping" or (saved["needs_attention"] and saved["acknowledged"]) or (not saved["acknowledged"] and (saved["phase"] != "complete" or not saved["ok"] or saved["restart_pending"])):
                    self._record.update(needs_attention=True, acknowledged=False)
            except (ValueError, OSError, KeyError, TypeError):
                pass

    @property
    def live(self):
        return self._live

    def mark_live(self, live):
        self._live = live

    @property
    def needs_attention(self):
        return bool(self._record and self._record["needs_attention"])

    @property
    def journal_status(self):
        return copy.deepcopy(self._record)

    def _persist(self, record):
        save_journal(self._journal, record)

    def _publish(self, record):
        self._persist(record)
        self._record = record

    def begin(self, session_id, selected, before, backup_app_name):
        self._publish({"version": 1, "phase": "stopping", "ok": False,
                       "needs_attention": True, "acknowledged": False, "session_id": session_id,
                       "selected_apps": sorted(selected),
                       "apps_before": [{key: app[key] for key in ("name", "app_id", "status")} for app in before],
                       "restart_pending": sorted(a["name"] for a in before if a["name"] not in selected
                                                 and a["name"] != backup_app_name and a["status"] == "running")})

    def finish(self, outcome, progress):
        candidate = self.journal_status
        confirmed = {a["name"] for a in progress.get("paused_apps", []) if a["restart"] == "confirmed"}
        candidate["restart_pending"] = [n for n in candidate["restart_pending"] if n not in confirmed]
        success = outcome and not candidate["restart_pending"]
        candidate.update(phase="complete" if success else "incomplete", ok=success, needs_attention=not success)
        self._publish(candidate)
        return success

    async def acknowledge(self, *, owner_token):
        await _authenticate_owner(self.router_url, owner_token)
        if self.lock.busy or self._live:
            raise MigrationError("busy")
        if self.needs_attention:
            try:
                self._publish({**self._record, "needs_attention": False, "acknowledged": True, "phase": "interrupted"})
            except OSError:
                raise MigrationError("failed") from None
        return self.journal_status


source_recovery: SourceRecoveryRecord | None = None


def initialize_source_recovery(**kwargs):
    global source_recovery
    if source_recovery is not None and source_recovery.work.absolute() == kwargs["work_dir"].absolute():
        if source_recovery.lock is not kwargs["lock"] or source_recovery.router_url != kwargs["router_url"]:
            raise MigrationError("busy")
        return source_recovery
    if source_recovery is not None and source_recovery.live:
        raise MigrationError("busy")
    source_recovery = SourceRecoveryRecord(**kwargs)
    return source_recovery


class _Peer:
    def __init__(self, origin, token):
        RouterClient(origin, token)
        self.origin, self.token = origin, token

    async def request(self, method, path, *, body=None, content=None, headers=None):
        try:
            async with asyncio.timeout(PEER_REQUEST_TIMEOUT), httpx.AsyncClient(timeout=PEER_REQUEST_TIMEOUT, follow_redirects=False, trust_env=False) as client:
                async with client.stream(method, self.origin + path, json=body, content=content,
                                         headers={**(headers or {}), "Authorization": "Bearer " + self.token}) as response:
                    if response.status_code != 200:
                        code = {401: "auth", 403: "auth", 404: "session", 409: "sequence"}.get(response.status_code, "transfer")
                        raise MigrationError(code)
                    data = bytearray()
                    async for part in response.aiter_bytes():
                        if len(data) + len(part) > MAX_JSON_BYTES:
                            raise MigrationError("transfer")
                        data.extend(part)
                    result = _decode_json(bytes(data))
                    if type(result) is not dict or type(result.get("version")) is not int or result["version"] != 5:
                        raise MigrationError("protocol")
                    return result
        except (httpx.HTTPError, ConfigurationError, TimeoutError):
            raise MigrationError("transfer") from None


async def _capture_and_transfer(peer, sid, temporary, password, root, bundle):
    await _restic(temporary, password, "init")
    selected = {a["name"] for a in bundle["definitions"]["apps"]}
    args = ["backup", "--quiet", "--json", "--pack-size", "8", "--tag", "bottle",
            "--tag", snapshots.CONFIGURATION_TAG, "--tag", snapshots.RUNTIME_TAG]
    for child in root.iterdir():
        if child.name not in selected:
            pattern = re.sub(r"([\\*?\[\]])", r"\\\1", str(child))
            args.extend(["--exclude", pattern])
        elif child.is_symlink() or not child.is_dir():
            raise MigrationError("transfer")
    with snapshots.configuration_file(bundle) as configuration_file:
        output = await _restic(temporary, password, *args, str(root), str(configuration_file))
    summary = json.loads(output.splitlines()[-1])
    snapshot_id = _session_id(summary["snapshot_id"])
    objects = [("config", "config", temporary / "config")]
    for kind in sorted(_OBJECT_KINDS):
        objects.extend((kind, p.name, p) for p in (temporary / kind).rglob("*") if p.is_file())
    for kind, identifier, path in objects:
        with path.open("rb") as stream:
            offset = 0
            while chunk := await drain(asyncio.to_thread(stream.read, CHUNK_LIMIT)):
                route = f"/api/migration/receive/object/{sid}/{kind}/{identifier}"
                for attempt in range(2):
                    try:
                        answer = await peer.request("POST", route, content=chunk, headers={"X-Object-Offset": str(offset)})
                        if answer.get("ok") is not True or type(answer.get("offset")) is not int or answer["offset"] != offset + len(chunk):
                            raise MigrationError("transfer")
                        break
                    except MigrationError as error:
                        if attempt or error.code != "transfer":
                            raise
                offset += len(chunk)
    return snapshot_id


async def run_direct_push(*, target_url, target_token, selected_apps, lock, all_app_data,
                          work_dir, router_url, app_token, owner_token, backup_app_name="backup",
                          deadline=3600, poll_interval=1, lock_acquired=False):
    global status
    if lock_acquired:
        if lock.active != OpKind.MIGRATION:
            raise MigrationError("busy")
    elif lock.try_acquire(OpKind.MIGRATION):
        raise MigrationError("busy")
    source = record = peer = sid = temporary = ping = None
    outcome = intent = False
    heartbeat = asyncio.create_task(_heartbeat(lock))
    log.clear()
    status = {"phase": "preflighting", "ok": False}
    try:
        work = private_work_dir(all_app_data, work_dir, backup_app_name)
        record = initialize_source_recovery(lock=lock, all_app_data=all_app_data, work_dir=work_dir,
                                            router_url=router_url, backup_app_name=backup_app_name)
        if record.needs_attention:
            raise MigrationError("attention")
        record.mark_live(True)
        RouterClient(target_url if "://" in target_url else "https://" + target_url, target_token)
        peer = _Peer(_target_backup_url(target_url), target_token)
        _positive(poll_interval)
        async with asyncio.timeout(_positive(deadline)):
            capability = await peer.request("GET", "/api/migration/receive/capabilities")
            if capability.get("ok") is not True or capability.get("chunk_limit") != CHUNK_LIMIT:
                raise MigrationError("protocol")
            executor = app_name(capability.get("backup_app_name"))
            captured = await capture_configuration(router_url, app_token, owner_token, backup_app_name)
            known = {a["name"] for a in captured["definitions"]["apps"]}
            if selected_apps is not None and (type(selected_apps) is not list or any(type(n) is not str for n in selected_apps)
                                               or len(set(selected_apps)) != len(selected_apps) or backup_app_name in selected_apps):
                raise MigrationError()
            selected = (known if selected_apps is None else set(selected_apps)) - {backup_app_name}
            if not selected or not selected <= known:
                raise MigrationError()
            if executor != backup_app_name and executor in selected:
                raise MigrationError("collision")
            bundle = subset_configuration(captured, selected)
            source = RecoverySession(router_url, owner_token, bundle, backup_app_name)
            await source.preflight()
            before = source.progress["destination_apps_before"]
            if {a["name"]: a["status"] for a in before} != {n: a["status"] for n, a in captured["runtime"]["apps"].items()}:
                raise MigrationError("sequence")
            password = secrets.token_hex(32)
            accepted = await peer.request("POST", "/api/migration/receive/start", body={"version": 5, "bundle": bundle, "password": password})
            sid = _session_id(accepted.get("session_id"))
            if accepted.get("ok") is not True or accepted.get("accepted_apps") != sorted(selected):
                raise MigrationError("sequence")
            async def keepalive():
                while True:
                    try:
                        await peer.request("POST", "/api/migration/receive/keepalive", body={"version": 5, "session_id": sid})
                    except MigrationError:
                        pass
                    await asyncio.sleep(10)
            ping = asyncio.create_task(keepalive())
            status.update(phase="stopping", session_id=sid)
            record.begin(sid, selected, before, backup_app_name)
            intent = True
            await drain(asyncio.to_thread(durability_barrier, work))
            await source.stop_apps()
            temporary = Path(tempfile.mkdtemp(prefix="outgoing-", dir=work))
            status.update(phase="transferring")
            snapshot_id = await _capture_and_transfer(peer, sid, temporary, password, all_app_data, bundle)
            status.update(phase="finalizing")
            body = {"version": 5, "session_id": sid, "snapshot": snapshot_id}
            try:
                await peer.request("POST", "/api/migration/receive/finalize", body=body)
            except MigrationError as error:
                if error.code != "transfer":
                    raise
            while True:
                try:
                    remote = await peer.request("GET", f"/api/migration/receive/status/{sid}")
                    if remote.get("session_id") != sid:
                        raise MigrationError("session")
                    if remote.get("phase") == "complete":
                        outcome = remote.get("result", {}).get("ok") is True
                        break
                    if remote.get("phase") == "receiving":
                        await peer.request("POST", "/api/migration/receive/finalize", body=body)
                    elif remote.get("phase") != "finalizing":
                        raise MigrationError("failed")
                except MigrationError as error:
                    if error.code != "transfer":
                        raise
                await asyncio.sleep(poll_interval)
    except asyncio.CancelledError:
        status.update(phase="interrupted", ok=False)
        raise
    except Exception:
        logger.exception("Outgoing migration did not complete")
        status.update(phase="failed", error=str(MigrationError("failed")))
    finally:
        async def cleanup():
            nonlocal outcome
            if source is not None:
                try:
                    result = await source.restart_unaffected()
                    status["source_recovery"] = result
                    if any(not a["selected"] and a["previous_status"] == "running" and a["restart"] != "confirmed" for a in result["paused_apps"]):
                        outcome = False
                except BaseException:
                    logger.exception("Could not resume unaffected source apps")
                    outcome = False
            if not outcome and peer is not None and sid is not None:
                try:
                    await peer.request("POST", "/api/migration/receive/abort", body={"version": 5, "session_id": sid})
                except MigrationError:
                    pass  # the destination's retained finalizer owns recovery
            if temporary is not None:
                await drain(asyncio.to_thread(shutil.rmtree, temporary))
        try:
            await drain(cleanup())
        except Exception:
            logger.exception("Outgoing migration cleanup failed")
            outcome = False
        finally:
            try:
                if record is not None:
                    try:
                        if intent:
                            try:
                                outcome = record.finish(outcome, status.get("source_recovery", {}))
                            except OSError:
                                outcome = False
                    finally:
                        record.mark_live(False)
            finally:
                try:
                    await drain(_cancel_tasks(ping, heartbeat))
                finally:
                    lock.release(OpKind.MIGRATION)
    status.update(phase="done" if outcome else "failed", ok=outcome)
    log.append("Migration complete." if outcome else "Migration incomplete; inspect recovery status.")
    return outcome
