"""Whole-tree promotion shared by backup restore and snapshot-based migration.

Restic owns archive parsing, integrity checking, and file metadata restoration.
This module only publishes verified trees and retains their predecessors until
the recovery journal commits.
"""
from __future__ import annotations

import asyncio
import ctypes
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile

from operations import drain

_syncfs = ctypes.CDLL(None, use_errno=True).syncfs
_syncfs.argtypes = [ctypes.c_int]
_syncfs.restype = ctypes.c_int


class DataError(ValueError):
    def __init__(self):
        super().__init__("App data could not be safely staged or replaced.")


def durability_barrier(*directories: Path):
    """Linux syncfs reports writeback errors without reopening mode-0000 files."""
    if not directories:
        raise ValueError("A recovery filesystem must be specified")
    for path in directories:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            if _syncfs(fd) != 0:
                error = ctypes.get_errno()
                raise OSError(error, os.strerror(error))
            os.fsync(fd)
        finally:
            os.close(fd)


def app_name(name):
    if type(name) is not str or len(name.encode()) > 200 or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", name):
        raise DataError()
    return name


def directory(path: Path):
    if path.is_symlink() or not path.is_dir():
        raise DataError()


def owned_directory(path: Path):
    try:
        info = path.lstat()
    except OSError:
        raise DataError() from None
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise DataError()
    return info


def private_work_dir(root: Path, work: Path, backup_name: str) -> Path:
    directory(root)
    executor = root / app_name(backup_name)
    owned_directory(executor)
    try:
        relative = work.absolute().relative_to(executor.absolute())
    except ValueError:
        raise DataError() from None
    if not relative.parts or ".." in relative.parts:
        raise DataError()
    current = executor
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise DataError()
        current.mkdir(mode=0o700, exist_ok=True)
        owned_directory(current)
        current.chmod(0o700)
    if not work.resolve().is_relative_to(executor.resolve()):
        raise DataError()
    return work


def _replace(staged_root: Path, destination_root: Path, names):
    directory(staged_root)
    directory(destination_root)
    if staged_root.resolve() == destination_root.resolve():
        raise DataError()
    if type(names) not in (tuple, list) or len(set(names)) != len(names):
        raise DataError()
    for name in names:
        app_name(name)
        source, target = staged_root / name, destination_root / name
        directory(source)
        if target.is_symlink() or (target.exists() and not target.is_dir()):
            raise DataError()
        if source.stat().st_dev != destination_root.stat().st_dev:
            raise DataError()
        if source.resolve().is_relative_to(target.resolve()) or target.resolve().is_relative_to(source.resolve()):
            raise DataError()
    old_root = Path(tempfile.mkdtemp(prefix=".migration-old-", dir=staged_root))
    promoted, moved = [], []
    try:
        durability_barrier(staged_root, destination_root)
        for name in names:
            target = destination_root / name
            if target.exists():
                target.rename(old_root / name)
                moved.append(name)
            (staged_root / name).rename(target)
            promoted.append(name)
        durability_barrier(staged_root, destination_root, old_root)
    except BaseException:
        rollback_failed = False
        for name in reversed(names):
            try:
                if name in promoted:
                    (destination_root / name).rename(staged_root / name)
                if name in moved:
                    (old_root / name).rename(destination_root / name)
            except OSError:
                rollback_failed = True
        if not rollback_failed:
            old_root.rmdir()
        try:
            durability_barrier(staged_root, destination_root)
        except OSError:
            pass
        raise DataError() from None
    return old_root


async def replace_app_trees(staged_root: Path, destination_root: Path, names) -> Path:
    """Caller owns the operation lock; retain the stage until durable completion."""
    return await drain(asyncio.to_thread(_replace, staged_root, destination_root, names))


async def discard_app_trees(rollback: Path) -> None:
    """Discard only a token from replace_app_trees, after committing recovery."""
    def discard():
        directory(rollback.parent)
        directory(rollback)
        if not rollback.name.startswith(".migration-old-"):
            raise DataError()
        shutil.rmtree(rollback)
        durability_barrier(rollback.parent)
    await drain(asyncio.to_thread(discard))
