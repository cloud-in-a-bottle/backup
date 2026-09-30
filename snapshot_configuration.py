"""Private recovery metadata stored in the same encrypted snapshot as app data."""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from configuration import MAX_CONFIGURATION_BYTES, parse_configuration, serialize_configuration
from journal import save_journal as _save_journal
import restic_process

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
    try:
        output = await restic_process.read(
            ["dump", snapshot_id, str(CONFIGURATION_FILE), "--no-lock"], env,
            limit=MAX_CONFIGURATION_BYTES, timeout=timeout,
        )
        return parse_configuration(output)
    except restic_process.ReadError as error:
        message = {"size": "The snapshot configuration exceeds the supported size limit.",
                   "timeout": "Reading the snapshot configuration timed out.",
                   "exit": "Could not read private configuration from this snapshot."}[str(error)]
        raise SnapshotConfigurationError(message) from None


def save_journal(path: Path, state: dict) -> None:
    """Create restore's private directory, then use the common journal writer."""
    _private_directory(path.parent)
    try:
        _save_journal(path, state)
    except OSError:
        raise SnapshotConfigurationError("Could not durably persist recovery progress.") from None
