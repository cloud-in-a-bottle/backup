"""Private recovery metadata stored in the same encrypted snapshot as app data."""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import stat
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from configuration import MAX_CONFIGURATION_BYTES, parse_configuration, serialize_configuration
from restic_process import kill_and_drain

CONFIGURATION_TAG = "bottle-configuration-v1"
RUNTIME_TAG = "bottle-runtime-v1"
# An explicit restic input outside app data, including the excluded backup repo.
# This container-local path is stable across instances and never browsable through
# the app-data file browser. It is removed as soon as the backup process settles.
CONFIGURATION_FILE = Path("/tmp/bottle-backup-configuration/configuration.json")
_SNAPSHOT_ID = re.compile(r"[a-f0-9]{8,64}")
_FULL_SNAPSHOT_ID = re.compile(r"[a-f0-9]{64}")


class SnapshotConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class Snapshot:
    id: str
    paths: tuple[str, ...]
    has_configuration: bool
    has_runtime: bool = False


def snapshot_metadata(snapshot_id: str, data: bytes) -> Snapshot:
    """Resolve a prefix once, rejecting ambiguous and non-Bottle snapshots."""
    try:
        if type(snapshot_id) is not str or not _SNAPSHOT_ID.fullmatch(snapshot_id):
            raise ValueError
        if len(data) > MAX_CONFIGURATION_BYTES:
            raise ValueError
        entries = json.loads(data.decode("utf-8"))
        if type(entries) is not list or len(entries) != 1 or type(entries[0]) is not dict:
            raise ValueError
        entry = entries[0]
        identifier, paths, tags = entry.get("id"), entry.get("paths"), entry.get("tags", [])
        if (
            type(identifier) is not str or not _FULL_SNAPSHOT_ID.fullmatch(identifier)
            or not identifier.startswith(snapshot_id)
            or type(paths) is not list or not paths
            or any(type(path) is not str or not path.startswith("/") for path in paths)
            or len(set(paths)) != len(paths)
            or type(tags) is not list or any(type(tag) is not str for tag in tags)
            or not {"bottle", "openhost"}.intersection(tags)
        ):
            raise ValueError
        configuration_tags = [tag for tag in tags if tag.startswith("bottle-configuration-")]
        runtime_tags = [tag for tag in tags if tag.startswith("bottle-runtime-")]
        if configuration_tags and configuration_tags != [CONFIGURATION_TAG]:
            raise ValueError
        if runtime_tags and (runtime_tags != [RUNTIME_TAG] or not configuration_tags):
            raise ValueError
        has_configuration = CONFIGURATION_TAG in tags
        if (str(CONFIGURATION_FILE) in paths) != has_configuration:
            raise ValueError
        return Snapshot(identifier, tuple(paths), has_configuration, bool(runtime_tags))
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise SnapshotConfigurationError("The snapshot metadata is missing, ambiguous, or unsupported.") from None


def _fsync_directory(path: Path) -> None:
    """Make a rename or a fresh directory entry survive an abrupt host reboot."""
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        raise SnapshotConfigurationError("Could not durably persist recovery progress.") from None


def _private_directory(path: Path) -> None:
    created = not path.exists()
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise SnapshotConfigurationError("Cannot create a private configuration staging directory.")
    path.chmod(0o700)
    if created:
        _fsync_directory(path.parent)


def clear_configuration_file() -> None:
    # Unlinking this single known artifact also safely removes a stale symlink.
    if CONFIGURATION_FILE.parent.is_symlink():
        raise SnapshotConfigurationError("Invalid configuration staging directory.")
    CONFIGURATION_FILE.unlink(missing_ok=True)


@contextmanager
def configuration_file(bundle: dict):
    """Publish complete private bytes atomically; caller holds the operation lock."""
    data = serialize_configuration(bundle)
    _private_directory(CONFIGURATION_FILE.parent)
    descriptor, temporary = tempfile.mkstemp(prefix=".capture-", dir=CONFIGURATION_FILE.parent)
    try:
        # fdopen takes ownership of the descriptor; if it cannot, the finally
        # below removes the file and the descriptor must be closed here.
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, CONFIGURATION_FILE)
        _fsync_directory(CONFIGURATION_FILE.parent)
        yield CONFIGURATION_FILE
    finally:
        if descriptor is not None:
            os.close(descriptor)
        Path(temporary).unlink(missing_ok=True)
        clear_configuration_file()


async def read_configuration(snapshot_id: str, env: dict, *, timeout: float = 60.0) -> dict:
    """Read a bounded metadata blob without logging/decrypting it to a public path."""
    if not _FULL_SNAPSHOT_ID.fullmatch(snapshot_id):
        raise SnapshotConfigurationError("Invalid snapshot identifier.")
    proc = await asyncio.create_subprocess_exec(
        "restic", "dump", snapshot_id, str(CONFIGURATION_FILE), "--no-lock",
        env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        async with asyncio.timeout(timeout):
            output = bytearray()
            assert proc.stdout is not None
            while chunk := await proc.stdout.read(65536):
                if len(output) + len(chunk) > MAX_CONFIGURATION_BYTES:
                    raise SnapshotConfigurationError("The snapshot configuration exceeds the supported size limit.")
                output.extend(chunk)
            await proc.wait()
            if proc.returncode != 0:
                raise SnapshotConfigurationError("Could not read private configuration from this snapshot.")
            return parse_configuration(bytes(output))
    except TimeoutError:
        raise SnapshotConfigurationError("Reading the snapshot configuration timed out.") from None
    finally:
        if proc.returncode is None or (proc.stdout is not None and not proc.stdout.at_eof()):
            await kill_and_drain(proc)


def save_journal(path: Path, state: dict) -> None:
    """Persist only caller-supplied safe progress, never a private bundle or token.

    A durable publication is all-or-nothing: if the replacement cannot be made
    durable, the previous journal is restored so a later restart never reads a
    cleared attention gate that the caller still reports as a failure.
    """
    _private_directory(path.parent)
    descriptor, temporary = tempfile.mkstemp(prefix=".journal-", dir=path.parent)
    previous = None
    try:
        # fdopen takes ownership of the descriptor; if it cannot, the cleanup
        # below removes the file and the descriptor must be closed here.
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = None
            json.dump(state, stream, ensure_ascii=False, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        if path.exists() and not path.is_symlink():
            previous = path.parent / f".journal-previous-{secrets.token_hex(16)}"
            os.link(path, previous)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        if previous is not None:
            try:
                os.replace(previous, path)
                _fsync_directory(path.parent)
            except OSError:
                pass
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)
        Path(temporary).unlink(missing_ok=True)
        if previous is not None:
            Path(previous).unlink(missing_ok=True)
