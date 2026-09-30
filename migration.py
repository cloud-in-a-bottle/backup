"""Configuration-aware direct migration, explicitly versioned protocol v4.

Receiver methods are route-independent. All calls require an owner token; data
is staged inside the excluded backup executor until every receipt is verified.
The receiver owns OperationLock from start through background finalization.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
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
import urllib.parse

import httpx

from configuration import (ConfigurationError, RouterClient, _app_id, _inventory,
    confirm_owner,
                           capture_configuration, parse_configuration,
                           serialize_configuration, subset_configuration)
from migration_data import (FILE_WORK_TIMEOUT, MAX_ARCHIVE_BYTES, app_name, build_archive, directory,
                            discard_app_trees, drain, durability_barrier, extract_archive,
                            private_work_dir, replace_app_trees)
from operations import OpKind, OperationLock
from recovery import RecoverySession

MIGRATION_PROTOCOL_VERSION = 4
CHUNK_LIMIT = 14 * 1024 * 1024
MAX_JSON_BYTES = 5 * 1024 * 1024
logger = logging.getLogger(__name__)

PEER_REQUEST_TIMEOUT = 120.0
VERIFICATION_TIMEOUT = FILE_WORK_TIMEOUT + PEER_REQUEST_TIMEOUT
_SESSION = re.compile(r"[0-9a-f]{64}")
_HASH = re.compile(r"[0-9a-f]{64}")
status: dict | None = None
log: list[str] = []

_ERRORS = {
    "protocol": (400, "Migration requires protocol v4. Upgrade both backup apps."),
    "invalid": (400, "Invalid migration request."),
    "auth": (403, "Destination owner authentication failed."),
    "busy": (409, "Another operation is active."),
    "collision": (409, "A selected app conflicts with the destination backup executor. Change the selection or executor name."),
    "attention": (409, "Migration requires owner inspection and acknowledgment before retrying."),
    "session": (404, "Unknown migration session."),
    "sequence": (409, "Invalid or incomplete migration transfer sequence."),
    "transfer": (400, "App data transfer or archive verification failed."),
    "failed": (500, "Migration failed. Inspect safe recovery status before retrying."),
    "timeout": (408, "Migration deadline exceeded."),
}


class MigrationError(ValueError):
    def __init__(self, code="invalid"):
        self.code = code if code in _ERRORS else "invalid"
        self.status_code, message = _ERRORS[self.code]
        super().__init__(message)


def validate_name(name: str) -> bool:
    return (type(name) is str and 0 < len(name) <= 200 and ".." not in name
            and bool(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._:T-]*", name)))


def _normalize_app_listing(listing: dict | list) -> list[dict]:
    if isinstance(listing, dict):
        return [{"name": name, **(info if isinstance(info, dict) else {})} for name, info in listing.items()]
    return [a for a in listing if isinstance(a, dict)] if isinstance(listing, list) else []


def _is_ip_or_localhost(host: str) -> bool:
    if host.lower() in {"localhost", "host.docker.internal"} or host.lower().endswith(".local"):
        return True
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def _target_backup_url(target_url: str) -> str:
    if "://" not in target_url:
        target_url = "https://" + target_url
    parsed = urllib.parse.urlparse(target_url)
    host = parsed.hostname or ""
    if not _is_ip_or_localhost(host) and not host.startswith("backup."):
        host = "backup." + host
    host = f"[{host}]" if ":" in host else host
    return f"{parsed.scheme}://{host}" + (f":{parsed.port}" if parsed.port else "")


def _is_local_url(url: str) -> bool:
    try:
        host = (urllib.parse.urlparse(url).hostname or "").removeprefix("backup.")
        return _is_ip_or_localhost(host)
    except ValueError:
        return False


def _positive(value):
    if type(value) not in (float, int) or not math.isfinite(value) or value <= 0:
        raise MigrationError()
    return value


def _body(body, fields):
    if type(body) is not dict or type(body.get("version")) is not int or body.get("version") != 4:
        raise MigrationError("protocol")
    if body.keys() != fields | {"version"}:
        raise MigrationError()


def _session_id(value):
    if type(value) is not str or not _SESSION.fullmatch(value):
        raise MigrationError("session")
    return value


async def _heartbeat(lock):
    while True:
        lock.touch()
        await asyncio.sleep(1)


class MigrationReceiver:
    def __init__(self, *, lock: OperationLock, all_app_data: Path, work_dir: Path,
                 router_url: str, backup_app_name="backup", idle_timeout=300.0, request_timeout=120.0):
        self.lock, self.root = lock, all_app_data
        self.work = private_work_dir(all_app_data, work_dir, backup_app_name) / "migration-v4"
        if self.work.is_symlink():
            raise MigrationError()
        self.work.mkdir(mode=0o700, exist_ok=True)
        directory(self.work)
        self.router_url, self.backup_app_name = router_url, app_name(backup_app_name)
        self.idle_timeout, self.request_timeout = _positive(idle_timeout), _positive(request_timeout)
        self._mutex = asyncio.Lock()
        self._record = None
        self._recovery = None
        self._stage = None
        self._uploads = {}
        self._job = None
        self._monitor = None
        self._busy = False
        self._cleanup_pending = []
        self._cleanup_job = None
        self._activity = time.monotonic()
        self._journal = self.work / "journal.json"
        self._load_journal()

    def _load_journal(self):
        if not self._journal.exists() and not self._journal.is_symlink():
            return
        # Restart is a fixed projection, never a replay of persisted progress or
        # error strings. Leave the original journal on disk for owner inspection.
        summary = {"ok": False, "version": 4, "phase": "interrupted",
                   "result": {"ok": False, "error": "Migration journal requires manual inspection."},
                   "needs_attention": True, "acknowledged": False, "retained_sessions": []}
        try:
            if self._journal.is_symlink() or self._journal.stat().st_size > MAX_JSON_BYTES:
                raise ValueError
            ambiguous = False
            def unique_object(pairs):
                nonlocal ambiguous
                result = {}
                for key, value in pairs:
                    if key in result:
                        ambiguous = True
                    result[key] = value
                return result
            record = json.loads(self._journal.read_bytes(), object_pairs_hook=unique_object)
            if type(record) is not dict:
                raise ValueError
            # Salvage only validated, task-owned directory identifiers even when
            # another field is malformed. Never use persisted paths for deletion.
            sid = record.get("session_id")
            if type(sid) is str and _SESSION.fullmatch(sid):
                summary["session_id"] = sid
                summary["retained_sessions"].append(sid)
            retained = record.get("retained_sessions", [])
            if type(retained) is list:
                summary["retained_sessions"] = sorted(set(summary["retained_sessions"]) | {
                    s for s in retained if type(s) is str and _SESSION.fullmatch(s)})
            allowed = {"ok", "version", "session_id", "phase", "accepted_apps", "receipts", "result",
                       "recovery", "needs_attention", "acknowledged", "retained_sessions"}
            if (ambiguous or record.keys() - allowed or type(record.get("version")) is not int or record["version"] != 4
                    or type(record.get("ok")) is not bool
                    or type(record.get("phase")) is not str
                    or record["phase"] not in {"preflighting", "receiving", "finalizing", "complete", "incomplete", "failed", "aborted", "interrupted"}
                    or type(retained) is not list or any(type(s) is not str or not _SESSION.fullmatch(s) for s in retained)
                    or len(set(retained)) != len(retained)):
                raise ValueError
            if sid is not None:
                _session_id(sid)
            elif record["phase"] != "interrupted":
                raise ValueError
            for key in ("needs_attention", "acknowledged"):
                if key in record and type(record[key]) is not bool:
                    raise ValueError
            phase = record["phase"]
            attention = record.get("needs_attention", phase in {"finalizing", "incomplete", "failed", "interrupted"})
            acknowledged = record.get("acknowledged", False)
            if phase == "finalizing" or (attention and acknowledged):
                attention, acknowledged = True, False
            summary.update(needs_attention=attention, acknowledged=acknowledged, retained_sessions=sorted(set(retained)))
            if attention and sid is not None:
                summary["retained_sessions"] = sorted(set(retained) | {sid})
            elif phase in {"preflighting", "receiving"} and sid is not None:
                if sid in retained:
                    raise ValueError  # a retained original must never be cleaned
                self._cleanup_pending = [sid]
        except (OSError, ValueError, KeyError, TypeError, RecursionError):
            summary.update(needs_attention=True, acknowledged=False)
        self._record = summary

    @property
    def journal_status(self):
        return self._snapshot()

    @property
    def needs_attention(self):
        return bool(self._record and self._record.get("needs_attention", False))

    def _save(self):
        self._persist(self._record)

    def _persist(self, record):
        _persist_journal(self.work, self._journal, record)

    async def initialize(self):
        """Optional async startup cleanup; start also awaits this automatically.

        Constructor never recursively removes trees. Cancellation drains the
        independent worker before releasing the mutex to another start.
        """
        async with self._mutex:
            if not self._cleanup_pending:
                return
            def clean():
                for sid in self._cleanup_pending:
                    stage = self.work / sid
                    if stage.is_symlink():
                        raise OSError("Invalid staging directory")
                    if stage.exists():
                        directory(stage)
                        shutil.rmtree(stage)
            self._cleanup_job = asyncio.create_task(asyncio.to_thread(clean))
            async def wait():
                await self._cleanup_job
            try:
                await drain(wait())
            except (OSError, ValueError):
                raise MigrationError("failed") from None
            finally:
                if self._cleanup_job.done() and not self._cleanup_job.cancelled() and self._cleanup_job.exception() is None:
                    self._cleanup_pending = []

    async def acknowledge(self, *, owner_token):
        """Owner accepts manual recovery responsibility; no data/app mutations."""
        await self._authenticate(owner_token)
        # Refuse promptly instead of waiting for an upload/preflight to finish.
        if self._mutex.locked() or self.lock.busy or self._busy or (self._job and not self._job.done()):
            raise MigrationError("busy")
        async with self._mutex:
            if not self.needs_attention:
                return self._snapshot()
            candidate = copy.deepcopy(self._record)
            candidate.update(needs_attention=False, acknowledged=True)
            try:
                self._persist(candidate)
            except OSError:
                raise MigrationError("failed") from None
            self._record = candidate  # publish only after durable acknowledgment
            return self._snapshot()

    async def _authenticate(self, token):
        await _authenticate_owner(self.router_url, token, self.request_timeout)

    async def capabilities(self, *, owner_token):
        await self._authenticate(owner_token)
        return {"ok": True, "version": 4, "chunk_limit": CHUNK_LIMIT, "backup_app_name": self.backup_app_name}

    def _lookup(self, session_id):
        _session_id(session_id)
        if not self._record or self._record.get("session_id") != session_id:
            raise MigrationError("session")

    def _snapshot(self):
        result = copy.deepcopy(self._record)
        if result is not None and result.get("phase") in {"preflighting", "receiving", "finalizing"}:
            result["result"] = None
        if self._recovery is not None:
            result["recovery"] = self._recovery.progress
        return result

    async def _watch(self):
        try:
            while self._record and self._record["phase"] in {"preflighting", "receiving", "finalizing"}:
                if self._busy or self._record["phase"] == "finalizing":
                    self.lock.touch()
                    self._record["recovery"] = self._recovery.progress if self._recovery else None
                    try:
                        self._save()
                    except OSError:
                        # Durable phase transitions still fail closed; a
                        # heartbeat journal failure must not drop the lock.
                        pass
                await self.expire_stale()
                await asyncio.sleep(min(1.0, self.idle_timeout / 2))
        except asyncio.CancelledError:
            pass

    async def start(self, body, *, owner_token):
        _body(body, {"bundle"})
        try:
            bundle = parse_configuration(serialize_configuration(body["bundle"]))
            for entry in bundle["definitions"]["apps"]:
                app_name(entry["name"])
        except (ValueError, TypeError):
            raise MigrationError() from None
        await self._authenticate(owner_token)
        if (bundle["backup_app_name"] != self.backup_app_name
                and any(entry["name"] == self.backup_app_name for entry in bundle["definitions"]["apps"])):
            raise MigrationError("collision")
        if self.needs_attention:
            raise MigrationError("attention")
        await self.initialize()
        async with self._mutex:
            if self.needs_attention:
                raise MigrationError("attention")
            # Build the session identity before taking the lock: everything here
            # is pure, and every mutation that can fail belongs inside the try
            # that releases the lock, cancels the monitor and cleans the stage.
            session_id = secrets.token_hex(32)
            record = {"ok": True, "version": 4, "session_id": session_id,
                      "phase": "preflighting", "accepted_apps": [], "receipts": {}, "result": None,
                      "needs_attention": False, "acknowledged": False,
                      "retained_sessions": list(self._record.get("retained_sessions", [])) if self._record else []}
            stage = self.work / session_id
            if self.lock.try_acquire(OpKind.MIGRATION):
                raise MigrationError("busy")
            try:
                self._busy = True
                self._record = record
                self._uploads, self._job = {}, None
                self._stage = stage
                self._monitor = asyncio.create_task(self._watch())
                self._save()
                self._recovery = RecoverySession(self.router_url, owner_token, bundle, self.backup_app_name)
                await self._recovery.preflight()
                names = self._recovery.restore_app_names
                if not names:
                    raise MigrationError()
                self._stage.mkdir(mode=0o700)
                (self._stage / "trees").mkdir(mode=0o700)
                self._record.update(phase="receiving", accepted_apps=list(names))
                self._activity = time.monotonic()
                self._save()
                return self._snapshot()
            except BaseException as error:
                await drain(self._finish("failed", remove=True))
                if isinstance(error, asyncio.CancelledError):
                    raise
                raise MigrationError("failed") from None
            finally:
                self._busy = False

    async def _finish(self, phase, *, remove, rollbacks=()):
        self._busy = True
        heartbeat = asyncio.create_task(_heartbeat(self.lock))
        cancelled = False
        try:
            if phase == "complete":
                # Clean disposable staging while the only originals remain in
                # place. Rollback disposal is post-commit garbage collection,
                # never part of a recovery that can subsequently become failed.
                def clean_disposable():
                    trees = self._stage / "trees"
                    for child in self._stage.iterdir():
                        if child == trees:
                            for tree in trees.iterdir():
                                if tree not in rollbacks:
                                    if tree.is_dir() and not tree.is_symlink():
                                        shutil.rmtree(tree)
                                    else:
                                        tree.unlink()
                        elif child.is_dir() and not child.is_symlink():
                            shutil.rmtree(child)
                        else:
                            child.unlink()
                await drain(asyncio.to_thread(clean_disposable))
                await drain(asyncio.to_thread(durability_barrier))
            elif remove and self._stage is not None and self._stage.exists():
                await drain(asyncio.to_thread(shutil.rmtree, self._stage))
            if self._recovery:
                self._record["recovery"] = self._recovery.summary
        except OSError:
            phase = "incomplete"
            self._record["result"] = {"ok": False, "error": str(MigrationError("failed"))}
        except asyncio.CancelledError:
            phase, cancelled = "interrupted", True
            self._record["result"] = {"ok": False, "error": str(MigrationError("failed"))}
        finally:
            self._record.update(phase=phase, ok=phase == "complete")
            if remove and phase in {"incomplete", "interrupted"}:
                # Failed disposable cleanup still needs a durable directory
                # reference, but does not imply that live app data was touched.
                self._record["retained_sessions"] = sorted(set(self._record["retained_sessions"]) | {self._record["session_id"]})
            candidate = copy.deepcopy(self._record)
            if phase == "complete":
                candidate["needs_attention"] = False
                # Commit with the reference intact: a crash or failed disposal
                # leaves reachable originals. Completion fsync failure restores
                # the prior intent and originals have not yet been touched.
            try:
                self._persist(candidate)
                self._record = candidate
            except OSError:
                self._record.update(phase="incomplete", ok=False, result={"ok": False, "error": str(MigrationError("failed"))})
                # Old persisted intent still flags an interrupted restart.
            if self._record["phase"] == "complete":
                try:
                    for rollback in rollbacks:
                        await discard_app_trees(rollback)
                    await drain(asyncio.to_thread(shutil.rmtree, self._stage))
                    cleaned = copy.deepcopy(self._record)
                    cleaned["retained_sessions"] = [s for s in cleaned["retained_sessions"] if s != cleaned["session_id"]]
                    self._persist(cleaned)
                    self._record = cleaned
                except (OSError, ValueError, asyncio.CancelledError):
                    # Recovery is already durably successful. A stale GC
                    # reference is harmless, and must not recast it as failed,
                    # but it is retained on disk until an operator removes it.
                    logger.warning("Retained migration staging was not reclaimed", exc_info=True)
            self._recovery = None  # discard credentials and private configuration
            self._uploads = {}
            self._busy = False
            if self._monitor is not None and self._monitor is not asyncio.current_task():
                self._monitor.cancel()
            heartbeat.cancel()
            try:
                await heartbeat_cancelled(heartbeat)
            finally:
                # A second cancellation at this await must not skip the release:
                # an operation lock that is never handed back blocks every other
                # operation until the process restarts.
                self.lock.release(OpKind.MIGRATION)
        if cancelled:
            raise asyncio.CancelledError

    async def expire_stale(self):
        if (self._record and self._record["phase"] == "receiving" and not self._busy
                and time.monotonic() - self._activity > self.idle_timeout):
            async with self._mutex:
                if self._record["phase"] == "receiving" and not self._busy:
                    await self._finish("aborted", remove=True)

    async def upload(self, session_id, name, chunks, *, index, final, archive_bytes, archive_sha256, owner_token):
        await self._authenticate(owner_token)
        async with self._mutex:
            self._lookup(session_id)
            if self._record["phase"] != "receiving" or name not in self._record["accepted_apps"]:
                raise MigrationError("sequence")
            if (type(index) is not int or index < 0 or type(final) is not bool
                    or type(archive_bytes) is not int or not 0 < archive_bytes <= MAX_ARCHIVE_BYTES
                    or type(archive_sha256) is not str or not _HASH.fullmatch(archive_sha256)):
                raise MigrationError()
            expected_chunks = (archive_bytes + CHUNK_LIMIT - 1) // CHUNK_LIMIT
            previous = self._uploads.get(name)
            if (index >= expected_chunks or final != (index == expected_chunks - 1)
                    or name in self._record["receipts"] or index != (previous["index"] if previous else 0)
                    or (previous and (archive_bytes, archive_sha256) != (previous["size"], previous["sha256"]))):
                raise MigrationError("sequence")
            self._busy = True
            self.lock.touch()
            verification = None
            try:
                state = previous or {"index": 0, "size": archive_bytes, "sha256": archive_sha256, "hash": hashlib.sha256(), "bytes": 0}
                self._uploads[name] = state
                path = self._stage / (name + ".tar.gz")
                expected = min(CHUNK_LIMIT, archive_bytes - state["bytes"])
                received = 0
                # Network is streamed, with a total wall deadline and an actual
                # byte bound independent of Content-Length or transfer encoding.
                async with asyncio.timeout(self.request_timeout):
                    with path.open("ab") as stream:
                        async for chunk in chunks:
                            if not isinstance(chunk, bytes) or received + len(chunk) > expected:
                                raise MigrationError("transfer")
                            await drain(asyncio.to_thread(stream.write, chunk))
                            state["hash"].update(chunk)
                            received += len(chunk)
                        if received != expected:
                            raise MigrationError("transfer")
                state["bytes"] += received
                state["index"] += 1
                receipt = None
                if final:
                    if state["bytes"] != archive_bytes or state["hash"].hexdigest() != archive_sha256:
                        raise MigrationError("transfer")
                    async def verify():
                        # The whole verification/receipt publication is owned by
                        # an independent task. Disconnect drains it under the
                        # mutex but cannot poison already-received valid bytes.
                        await asyncio.to_thread(extract_archive, path, self._stage / "trees" / name)
                        receipt = {"app_name": name, "bytes": archive_bytes, "sha256": archive_sha256, "complete": True}
                        self._record["receipts"][name] = receipt
                        path.unlink()
                        self._activity = time.monotonic()
                        self.lock.touch()
                        self._save()
                        return receipt
                    verification = asyncio.create_task(verify())
                    async def wait():
                        return await verification
                    receipt = await drain(wait())
                self._activity = time.monotonic()
                self.lock.touch()
                self._save()
                return {"ok": True, "version": 4, "session_id": session_id, "next_index": state["index"], "receipt": receipt}
            except BaseException as error:
                if (isinstance(error, asyncio.CancelledError) and verification is not None
                        and verification.done() and not verification.cancelled() and verification.exception() is None):
                    raise  # receipt was durably published despite disconnect
                await drain(self._finish("failed", remove=True))
                if isinstance(error, asyncio.CancelledError):
                    raise
                raise MigrationError("transfer") from None
            finally:
                self._busy = False

    async def finalize(self, body, *, owner_token):
        _body(body, {"session_id"})
        await self._authenticate(owner_token)
        async with self._mutex:
            self._lookup(body["session_id"])
            if self._record["phase"] in {"finalizing", "complete", "incomplete"}:
                return self._snapshot()
            if (self._record["phase"] != "receiving"
                    or set(self._record["receipts"]) != set(self._record["accepted_apps"])):
                raise MigrationError("sequence")
            self._record["phase"] = "finalizing"
            self._record["needs_attention"] = True
            self._record["retained_sessions"] = sorted(set(self._record["retained_sessions"]) | {body["session_id"]})
            self.lock.touch()
            try:
                self._save()  # durable intent precedes any remote stop/install
            except OSError:
                self._record["phase"] = "receiving"
                # Keep attention conservative: replace may have succeeded before
                # directory fsync failed. No job is launched until intent saves.
                raise MigrationError("failed") from None
            self._job = asyncio.create_task(self._finalize_job())
            return self._snapshot()

    async def _finalize_job(self):
        # This job absorbs cancellation on purpose. It runs after the destination
        # has already been told to stop and promote, so abandoning it would leave
        # that destination paused with no record here. Instead the session fails
        # closed: the journal keeps the retained originals and needs attention,
        # and every exception is logged with its cause.
        phase = "incomplete"
        rollbacks = []
        try:
            await self._recovery.stop_apps()
            self._record["recovery"] = self._recovery.progress
            self._save()
            rollbacks.append(await replace_app_trees(self._stage / "trees", self.root, self._recovery.restore_app_names))
            await self._recovery.activate()
        except BaseException:
            # The session fails closed either way, but the cause of a failure
            # inside the stop/promote/activate window is otherwise unrecoverable.
            logger.exception("Recovery did not complete; retained originals keep this session recoverable")
            self._record["result"] = {"ok": False, "error": str(MigrationError("failed"))}
        finally:
            try:
                await drain(self._recovery.restart_unaffected())
                summary = self._recovery.summary
                if self._record["result"] is None:
                    self._record["result"] = summary
                else:
                    self._record["result"]["recovery"] = summary
                if self._record["result"].get("ok") is True:
                    phase = "complete"
            except BaseException:
                logger.exception("Unaffected-app cleanup did not confirm; the paused-app journal is authoritative")
                self._record["result"] = {"ok": False, "error": str(MigrationError("failed"))}
            await drain(self._finish(phase, remove=phase == "complete", rollbacks=rollbacks))

    async def status(self, session_id, *, owner_token):
        await self._authenticate(owner_token)
        self._lookup(session_id)
        return self._snapshot()

    async def keepalive(self, body, *, owner_token):
        """Source-owned activity while compression has no upload requests."""
        _body(body, {"session_id"})
        await self._authenticate(owner_token)
        self._lookup(body["session_id"])
        if self._record["phase"] not in {"receiving", "finalizing"}:
            raise MigrationError("sequence")
        self._activity = time.monotonic()
        self.lock.touch()
        return {"ok": True, "version": 4, "session_id": body["session_id"]}

    async def abort(self, body, *, owner_token):
        _body(body, {"session_id"})
        await self._authenticate(owner_token)
        async with self._mutex:
            self._lookup(body["session_id"])
            if self._record["phase"] == "finalizing":
                raise MigrationError("busy")
            if self._record["phase"] == "receiving":
                await self._finish("aborted", remove=True)
            return self._snapshot()


def _persist_journal(work, journal, record):
    """Atomic durable publication; restore the previous inode on fsync failure."""
    temporary = work / (journal.stem + ".tmp")
    with open(temporary, "w", opener=lambda p, f: os.open(p, f | os.O_NOFOLLOW, 0o600)) as stream:
        json.dump(record, stream, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(work, os.O_RDONLY | os.O_DIRECTORY)
    rollback = work / ("journal-rollback-" + secrets.token_hex(16) + ".json")
    previous = False
    attempted = False
    try:
        if journal.exists() or journal.is_symlink():
            os.link(journal, rollback, follow_symlinks=False)
            previous = True
        attempted = True
        temporary.replace(journal)
        os.fsync(directory_fd)
    except OSError as error:
        if attempted:
            # The target is either the previous inode (the rename did not
            # happen) or the new one; nothing can leave it half written, so
            # this only makes the intended state explicit. A rollback that
            # cannot be completed is not the caller's error and must not
            # replace the publish failure they need to see.
            try:
                if previous:
                    rollback.replace(journal)
                else:
                    journal.unlink(missing_ok=True)
            except OSError:
                logger.error(
                    "Journal rollback did not complete; %s still holds the previous record", journal,
                    exc_info=True,
                )
            try:
                os.fsync(directory_fd)
            except OSError:
                pass
        raise error
    finally:
        os.close(directory_fd)
        # A failed publish keeps the previous inode and leaves this file
        # behind; a successful one has already renamed it away, so this is a
        # no-op there. Never raise or return from here: the publish error must
        # reach the caller.
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            logger.warning("Could not remove an unpublished journal temporary", exc_info=True)
    if previous:
        try:
            rollback.unlink()
        except OSError:
            # A harmless stale link: the new journal is already durably
            # committed, and this file is its own predecessor.
            logger.warning("Could not remove a superseded journal rollback link", exc_info=True)


class SourceRecoveryRecord:
    """Restart-visible outgoing cutover intent, containing no credentials/config.

    Construct once at app startup, using the same OperationLock and private work
    directory as run_direct_push. Reading needs_attention/journal_status does no
    filesystem or network I/O. Acknowledgment only accepts manual responsibility.
    """
    def __init__(self, *, lock: OperationLock, all_app_data: Path, work_dir: Path,
                 router_url: str, backup_app_name="backup"):
        self.lock = lock
        self.work = private_work_dir(all_app_data, work_dir, backup_app_name)
        self._journal = self.work / "migration-source-journal.json"
        self.router_url = router_url
        self._record = None
        self._live = False
        self._load()

    @property
    def live(self) -> bool:
        """Whether an outgoing cutover is running in this process."""
        return self._live

    def mark_live(self, live: bool) -> None:
        """Record that this process is (or is no longer) cutting over."""
        self._live = live

    @property
    def needs_attention(self) -> bool:
        return bool(self._record and self._record["needs_attention"])

    @property
    def journal_status(self) -> dict | None:
        return copy.deepcopy(self._record)

    def _load(self):
        if not self._journal.exists() and not self._journal.is_symlink():
            return
        summary = {"version": 1, "phase": "interrupted", "needs_attention": True,
                   "acknowledged": False, "session_id": None, "selected_apps": [],
                   "apps_before": [], "restart_pending": [], "ok": False}
        try:
            if self._journal.is_symlink() or self._journal.stat().st_size > MAX_JSON_BYTES:
                raise ValueError
            def unique(pairs):
                result = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError
                    result[key] = value
                return result
            record = json.loads(self._journal.read_bytes(), object_pairs_hook=unique)
            if type(record) is not dict or record.keys() != summary.keys():
                raise ValueError
            if type(record["version"]) is not int or record["version"] != 1:
                raise ValueError
            if record["session_id"] is not None:
                _session_id(record["session_id"])
            for field in ("needs_attention", "acknowledged", "ok"):
                if type(record[field]) is not bool:
                    raise ValueError
            if record["phase"] not in {"stopping", "incomplete", "complete", "interrupted"}:
                raise ValueError
            for field in ("selected_apps", "restart_pending"):
                if type(record[field]) is not list:
                    raise ValueError
                for name in record[field]:
                    app_name(name)
                if len(set(record[field])) != len(record[field]):
                    raise ValueError
            if type(record["apps_before"]) is not list:
                raise ValueError
            names = set()
            for app in record["apps_before"]:
                if type(app) is not dict or app.keys() != {"name", "app_id", "status"}:
                    raise ValueError
                app_name(app["name"])
                _app_id(app["app_id"])
                if (app["name"] in names
                        or app["status"] not in {"running", "stopped", "error"}):
                    raise ValueError
                names.add(app["name"])
            if not set(record["selected_apps"]) <= names or not set(record["restart_pending"]) <= names:
                raise ValueError
            summary.update(record)
            summary.update(phase="interrupted", ok=False)
            if (record["phase"] == "stopping" or (record["needs_attention"] and record["acknowledged"])
                    or (not record["needs_attention"] and not record["acknowledged"]
                        and (record["phase"] != "complete" or not record["ok"] or record["restart_pending"]))):
                summary.update(needs_attention=True, acknowledged=False)
        except (OSError, ValueError, TypeError, KeyError, RecursionError):
            pass  # fixed fail-closed notice; never echo persisted arbitrary strings
        self._record = summary

    def _persist(self, record):
        _persist_journal(self.work, self._journal, record)

    def _publish(self, record):
        self._persist(record)
        self._record = record

    def begin(self, session_id, selected, before, backup_app_name):
        record = {"version": 1, "phase": "stopping", "ok": False,
                  "needs_attention": True, "acknowledged": False, "session_id": session_id,
                  "selected_apps": sorted(selected),
                  "apps_before": [{key: app[key] for key in ("name", "app_id", "status")} for app in before],
                  "restart_pending": sorted(app["name"] for app in before
                                            if app["name"] not in selected and app["name"] != backup_app_name
                                            and app["status"] == "running")}
        self._publish(record)  # must finish before the first stop request

    def finish(self, outcome, progress):
        candidate = self.journal_status
        confirmed = {app["name"] for app in progress.get("paused_apps", []) if app["restart"] == "confirmed"}
        candidate["restart_pending"] = [name for name in candidate["restart_pending"] if name not in confirmed]
        success = outcome and not candidate["restart_pending"]
        candidate.update(phase="complete" if success else "incomplete", ok=success, needs_attention=not success)
        self._publish(candidate)
        return success

    async def acknowledge(self, *, owner_token: str) -> dict | None:
        await _authenticate_owner(self.router_url, owner_token, 120.0)
        if self.lock.busy or self._live:
            raise MigrationError("busy")
        if self.needs_attention:
            candidate = self.journal_status
            candidate.update(needs_attention=False, acknowledged=True, phase="interrupted")
            try:
                self._publish(candidate)
            except OSError:
                raise MigrationError("failed") from None
        return self.journal_status


source_recovery: SourceRecoveryRecord | None = None


def initialize_source_recovery(*, lock: OperationLock, all_app_data: Path, work_dir: Path,
                               router_url: str, backup_app_name="backup") -> SourceRecoveryRecord:
    """Call once at startup before enabling captures; run_direct_push also calls it."""
    global source_recovery
    if source_recovery is not None and source_recovery.work.absolute() == work_dir.absolute():
        if source_recovery.lock is not lock or source_recovery.router_url != router_url:
            raise MigrationError("busy")
        return source_recovery
    if source_recovery is not None and source_recovery.live:
        raise MigrationError("busy")
    source_recovery = SourceRecoveryRecord(lock=lock, all_app_data=all_app_data, work_dir=work_dir,
                                         router_url=router_url, backup_app_name=backup_app_name)
    return source_recovery


async def _authenticate_owner(router_url, token, timeout):
    # The shared probe in configuration.py is the only supported owner check.
    try:
        async with asyncio.timeout(timeout):
            if not await confirm_owner(router_url, token, timeout):
                raise MigrationError("auth")
    except (ConfigurationError, TimeoutError, asyncio.TimeoutError):
        raise MigrationError("auth") from None


class _Peer:
    def __init__(self, origin, token):
        RouterClient(origin, token)  # shared strict origin/token validation
        self.origin, self.token = origin, token

    async def request(self, method, path, *, body=None, content=None, headers=None):
        extra = {"Authorization": f"Bearer {self.token}", "Accept": "application/json", "Accept-Encoding": "identity"}
        extra.update(headers or {})
        if body is not None:
            content = json.dumps(body, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
            if len(content) > MAX_JSON_BYTES:
                raise MigrationError()
            extra["Content-Type"] = "application/json"
        try:
            async with asyncio.timeout(PEER_REQUEST_TIMEOUT):
                async with httpx.AsyncClient(timeout=PEER_REQUEST_TIMEOUT, follow_redirects=False, trust_env=False) as client:
                    async with client.stream(method, self.origin + path, headers=extra, content=content) as response:
                        if response.status_code != 200 or response.headers.get("content-type", "").split(";")[0] != "application/json":
                            raise MigrationError("transfer")
                        if response.headers.get("content-encoding", "identity") != "identity":
                            raise MigrationError("transfer")
                        data = bytearray()
                        async for chunk in response.aiter_bytes():
                            if len(data) + len(chunk) > MAX_JSON_BYTES:
                                raise MigrationError("transfer")
                            data.extend(chunk)
                        result = json.loads(data)
                        if type(result) is not dict or type(result.get("version")) is not int or result.get("version") != 4:
                            raise MigrationError("protocol")
                        return result
        except (httpx.HTTPError, TimeoutError, ValueError) as error:
            if isinstance(error, MigrationError):
                raise
            raise MigrationError("transfer") from None


async def _wait_for_receipt(peer, session_id, name, poll_interval):
    # A peer request may time out while the independently owned verification is
    # still valid for FILE_WORK_TIMEOUT. Both this nested window and the enclosing
    # whole-transfer deadline bound polling, including stalled status requests.
    async with asyncio.timeout(VERIFICATION_TIMEOUT):
        while True:
            try:
                remote = await peer.request("GET", f"/api/migration/receive/status/{session_id}")
            except MigrationError:
                await asyncio.sleep(poll_interval)
                continue
            if remote.get("session_id") != session_id or remote.get("phase") != "receiving":
                raise MigrationError("transfer")
            receipts = remote.get("receipts")
            if type(receipts) is not dict:
                raise MigrationError("transfer")
            receipt = receipts.get(name)
            if receipt is not None:
                return receipt
            await asyncio.sleep(poll_interval)


async def run_direct_push(*, target_url: str, target_token: str, selected_apps: list[str] | None,
                          lock: OperationLock, all_app_data: Path, work_dir: Path,
                          router_url: str, app_token: str, owner_token: str,
                          backup_app_name="backup", deadline=3600.0, poll_interval=1.0,
                          lock_acquired=False) -> bool:
    """Own capture, quiescence, transfer and cleanup; return only confirmed success."""
    global status
    if lock_acquired:
        if lock.active != OpKind.MIGRATION:
            raise MigrationError("busy")
    elif lock.try_acquire(OpKind.MIGRATION):
        raise MigrationError("busy")
    heartbeat = asyncio.create_task(_heartbeat(lock))
    source = None
    record = None
    source_intent = False
    peer = None
    session_id = None
    temporary = None
    ping = None
    outcome = False
    log.clear()
    status = {"phase": "preflighting", "ok": False}
    try:
        _positive(deadline)
        _positive(poll_interval)
        work = private_work_dir(all_app_data, work_dir, backup_app_name)
        record = initialize_source_recovery(lock=lock, all_app_data=all_app_data, work_dir=work_dir,
                                            router_url=router_url, backup_app_name=backup_app_name)
        if record.needs_attention:
            raise MigrationError("attention")
        record.mark_live(True)
        # Validate raw origin before URL rewriting can discard credentials.
        raw_url = target_url if "://" in target_url else "https://" + target_url
        RouterClient(raw_url, target_token)
        peer = _Peer(_target_backup_url(target_url), target_token)
        async with asyncio.timeout(deadline):
            capability = await peer.request("GET", "/api/migration/receive/capabilities")
            if (capability.get("ok") is not True or type(capability.get("chunk_limit")) is not int
                    or capability.get("chunk_limit") != CHUNK_LIMIT):
                raise MigrationError("protocol")
            destination_executor = app_name(capability.get("backup_app_name"))
            captured = await capture_configuration(router_url, app_token, owner_token, backup_app_name)
            known = {a["name"] for a in captured["definitions"]["apps"]}
            if selected_apps is not None and (type(selected_apps) is not list or any(type(n) is not str for n in selected_apps) or len(set(selected_apps)) != len(selected_apps)):
                raise MigrationError()
            selected = known if selected_apps is None else set(selected_apps)
            if not selected <= known:
                raise MigrationError()
            if destination_executor != backup_app_name and destination_executor in selected:
                raise MigrationError("collision")
            if selected_apps is not None and backup_app_name in selected:
                raise MigrationError()
            selected = selected - {backup_app_name}
            if not selected:
                raise MigrationError()
            bundle = subset_configuration(captured, selected)
            source = RecoverySession(router_url, owner_token, bundle, backup_app_name)
            await source.preflight()
            # Detect a desired-state change between capture and source preflight.
            before = {a["name"]: a["status"] for a in source.progress["destination_apps_before"]}
            if before != {n: a["status"] for n, a in captured["runtime"]["apps"].items()}:
                raise MigrationError("sequence")
            accepted = await peer.request("POST", "/api/migration/receive/start", body={"version": 4, "bundle": bundle})
            session_id = _session_id(accepted.get("session_id"))
            if accepted.get("ok") is not True or accepted.get("accepted_apps") != sorted(selected):
                raise MigrationError("sequence")
            async def keep_destination_alive():
                while True:
                    try:
                        await peer.request("POST", "/api/migration/receive/keepalive", body={"version": 4, "session_id": session_id})
                    except MigrationError:
                        pass  # the next upload/finalizer status must still succeed
                    await asyncio.sleep(10)
            ping = asyncio.create_task(keep_destination_alive())
            status = {"phase": "stopping", "ok": False, "session_id": session_id}
            record.begin(session_id, selected, source.progress["destination_apps_before"], backup_app_name)
            source_intent = True
            # Include newly created private-work ancestors, not just the journal
            # inode and its immediate directory, before any source stop.
            await drain(asyncio.to_thread(durability_barrier))
            await source.stop_apps()
            temporary = Path(tempfile.mkdtemp(prefix="migration-source-", dir=work))
            for name in sorted(selected):
                status.update(phase="transferring", current_app=name)
                archive = temporary / "app.tar.gz"
                size, digest = await drain(asyncio.to_thread(build_archive, all_app_data / name, archive))
                with archive.open("rb") as stream:
                    count = (size + CHUNK_LIMIT - 1) // CHUNK_LIMIT
                    for index in range(count):
                        chunk = await drain(asyncio.to_thread(stream.read, CHUNK_LIMIT))
                        try:
                            response = await peer.request("POST", f"/api/migration/receive/chunk/{session_id}/{name}", content=chunk, headers={
                                "Content-Type": "application/octet-stream", "X-Chunk-Index": str(index),
                                "X-Chunk-Final": "1" if index == count - 1 else "0",
                                "X-Archive-Bytes": str(size), "X-Archive-SHA256": digest})
                        except MigrationError:
                            if index != count - 1:
                                raise
                            receipt = await _wait_for_receipt(peer, session_id, name, poll_interval)
                            response = {"ok": True, "session_id": session_id, "next_index": index + 1, "receipt": receipt}
                        if (response.get("ok") is not True or response.get("session_id") != session_id
                                or type(response.get("next_index")) is not int or response.get("next_index") != index + 1):
                            raise MigrationError("transfer")
                    expected = {"app_name": name, "bytes": size, "sha256": digest, "complete": True}
                    receipt = response.get("receipt")
                    if receipt is None:
                        receipt = await _wait_for_receipt(peer, session_id, name, poll_interval)
                    if (receipt != expected or type(receipt.get("bytes")) is not int or receipt.get("complete") is not True):
                        raise MigrationError("transfer")
                archive.unlink()
            status.update(phase="finalizing", current_app=None)
            final_body = {"version": 4, "session_id": session_id}
            try:
                await peer.request("POST", "/api/migration/receive/finalize", body=final_body)
            except MigrationError:
                pass  # status reconciles lost finalize response; no duplicate installs
            while True:
                try:
                    remote = await peer.request("GET", f"/api/migration/receive/status/{session_id}")
                except MigrationError:
                    await asyncio.sleep(poll_interval)
                    continue
                if remote.get("session_id") != session_id:
                    raise MigrationError("session")
                phase = remote.get("phase")
                if phase == "receiving":
                    # Finalize may never have reached the server. Its session
                    # state transition is idempotent and owns one retained job.
                    await peer.request("POST", "/api/migration/receive/finalize", body=final_body)
                elif phase == "complete":
                    outcome = type(remote.get("result")) is dict and remote["result"].get("ok") is True
                    break
                elif phase != "finalizing":
                    raise MigrationError("failed")
                await asyncio.sleep(poll_interval)
    except asyncio.CancelledError:
        status.update(phase="interrupted", ok=False)
        raise
    except Exception as error:
        code = "collision" if isinstance(error, MigrationError) and error.code == "collision" else "failed"
        logger.exception("Outgoing migration failed; the source journal keeps this recoverable")
        status.update(phase="failed", ok=False, error=str(MigrationError(code)))
    finally:
        async def cleanup():
            nonlocal outcome
            if source is not None:
                try:
                    result = await source.restart_unaffected()
                    status["source_recovery"] = result
                    if any(not p["selected"] and p["previous_status"] == "running" and p["restart"] != "confirmed" for p in result["paused_apps"]):
                        outcome = False
                except Exception:
                    # Paused apps stay stopped and the record requires attention;
                    # the reason belongs in the log, not only in the outcome.
                    logger.exception("Could not resume unaffected source apps; the source journal keeps the paused apps")
                    outcome = False
            if not outcome and peer is not None and session_id is not None:
                try:
                    await peer.request("POST", "/api/migration/receive/abort", body={"version": 4, "session_id": session_id})
                except Exception:
                    # A live destination finalization cannot be cancelled remotely;
                    # its own recovery record decides what it left behind.
                    logger.warning("Could not ask the destination to abort", exc_info=True)
            if temporary is not None:
                await drain(asyncio.to_thread(shutil.rmtree, temporary))
        try:
            await drain(cleanup())
        except Exception:
            logger.exception("Outgoing migration could not finalize its cleanup")
            outcome = False
            status.update(phase="failed", ok=False, error=str(MigrationError("failed")))
        finally:
            if record is not None:
                try:
                    if source_intent:
                        outcome = record.finish(outcome, status.get("source_recovery", {}))
                except OSError:
                    outcome = False  # durable stopping intent still holds the gate
                finally:
                    record.mark_live(False)
            if ping is not None:
                ping.cancel()
            heartbeat.cancel()
            try:
                # Draining runs under one guard so a cancellation while draining
                # one heartbeat cannot skip the operation lock release: a lock
                # that is never handed back blocks every other operation until
                # the process restarts. Cancellation still propagates.
                if ping is not None:
                    await heartbeat_cancelled(ping)
                await heartbeat_cancelled(heartbeat)
            finally:
                lock.release(OpKind.MIGRATION)
    status.update(phase="done" if outcome else "failed", ok=outcome)
    log.append("Migration complete." if outcome else "Migration incomplete; inspect recovery status.")
    return outcome


async def heartbeat_cancelled(task):
    try:
        await task
    except asyncio.CancelledError:
        pass
