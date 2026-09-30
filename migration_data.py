"""Private, bounded archive staging and whole-tree promotion for recovery."""
from __future__ import annotations

import asyncio
import hashlib
import gzip
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tarfile
import tempfile
import time

from operations import drain  # noqa: F401  (re-exported for callers)

MAX_ARCHIVE_BYTES = 1024**4
MAX_MEMBERS = 1_000_000
FILE_WORK_TIMEOUT = 900.0


class _DeadlineIO:
    """Check deadlines during bounded file I/O, including gzip seek emulation."""
    def __init__(self, stream, deadline):
        self.stream, self.deadline = stream, deadline

    def check(self):
        if time.monotonic() > self.deadline:
            raise DataError()

    def read(self, size=-1):
        self.check()
        # tarfile reads PAX/GNU extension bodies using their declared header
        # size before returning a member to our validator. Bound that allocation
        # here, not only the final filename or member count.
        if not 0 <= size <= 1024 * 1024:
            raise DataError()
        return self.stream.read(size)

    def write(self, value):
        self.check()
        return self.stream.write(value)

    def tell(self):
        return self.stream.tell()

    def seek(self, offset, whence=0):
        self.check()
        if whence == 1:
            offset += self.tell()
        elif whence != 0:
            raise DataError()
        if offset < self.tell():
            self.stream.seek(0)
        while self.tell() < offset:
            if not self.read(min(1024 * 1024, offset - self.tell())):
                raise DataError()
        return self.tell()


class _DeadlineTar(tarfile.TarFile):
    def addfile(self, tarinfo, fileobj=None):
        if fileobj is not None:
            fileobj = _DeadlineIO(fileobj, self.fileobj.deadline)
        return super().addfile(tarinfo, fileobj)


class DataError(ValueError):
    def __init__(self):
        super().__init__("App data could not be safely staged or replaced.")


def _fsync_path(path: Path, *, expect_directory: bool = False) -> None:
    """Flush one entry with an error-reporting barrier, never a silent one."""
    flags = os.O_RDONLY | (os.O_DIRECTORY if expect_directory else 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def flush_tree(root: Path, deadline: float | None = None) -> int:
    """fsync every file and directory written below root, reporting failures.

    os.sync() flushes host-wide but cannot report writeback errors, so it cannot
    justify discarding retained originals. Archived restrictive modes would also
    block reopening entries, so extraction defers them until after this flush,
    and an unreadable directory is a reported failure rather than os.walk's
    default silent skip.
    """
    def scan_error(error):
        raise DataError() from error

    flushed = 0
    for parent, directories, files in os.walk(root, topdown=True, onerror=scan_error):
        parent_path = Path(parent)
        for name in files:
            entry = parent_path / name
            if entry.is_symlink():
                continue
            _fsync_path(entry)
            flushed += 1
            if flushed > MAX_MEMBERS:
                raise DataError()
        for name in directories:
            entry = parent_path / name
            if entry.is_symlink():
                continue
            _fsync_path(entry, expect_directory=True)
            flushed += 1
        _fsync_path(parent_path, expect_directory=True)
        flushed += 1
        if deadline is not None and time.monotonic() > deadline:
            raise DataError()
    return flushed


def flush_entries(*roots: Path):
    """fsync only the named directories, not the trees below them.

    A rename or removal makes the parent directory's entries change, so fsyncing
    exactly those parents is what makes the rename durable. Walking below them
    would instead depend on os.walk's silent skip of unreadable directories, so
    an archived restrictive-mode tree would report as flushed without being
    flushed.
    """
    for root in roots:
        _fsync_path(root, expect_directory=True)


def durability_barrier(*roots: Path, recurse: bool = True):
    """Flush data and metadata, including restrictive-mode trees and hardlinks.

    Host-wide sync keeps the cost of unrelated dirty pages off this path, then
    each tree the caller is about to trust is flushed with fsync so writeback
    errors surface instead of passing silently. ``recurse`` walks below each
    root, which is only meaningful while every entry is still readable; use it
    for freshly written data and not for trees that already carry archived
    modes. Call in a drained worker, never on the event loop; a failed barrier
    must fail recovery closed.
    """
    os.sync()
    if not roots:
        return
    if recurse:
        for root in roots:
            flush_tree(root)
    else:
        flush_entries(*roots)


def app_name(name):
    if type(name) is not str or len(name.encode()) > 200 or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", name):
        raise DataError()
    return name


def directory(path: Path):
    if path.is_symlink() or not path.is_dir():
        raise DataError()


def owned_directory(path: Path):
    """A co-located user must not own or share any migration state directory."""
    try:
        info = path.lstat()
    except OSError:
        # A missing path is a data problem here, not an unhandled OS error.
        raise DataError() from None
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise DataError()
    return info


def private_work_dir(root: Path, work: Path, backup_name: str) -> Path:
    directory(root)
    executor = root / app_name(backup_name)
    owned_directory(executor)
    # Reject symlink components even when they happen to point back inside.
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
        # Migration journals, archives and retained originals are private, so
        # an existing directory must be tightened rather than trusted.
        current.chmod(0o700)
    if work.resolve().is_relative_to(executor.resolve()):
        return work
    raise DataError()


def _path(value: str) -> str:
    if (not value or len(value.encode("utf-8")) > 4096 or value.startswith("/")
            or "\\" in value or "\x00" in value or ".." in value.split("/")
            or any(len(p.encode("utf-8")) > 255 for p in value.split("/"))):
        raise DataError()
    return str(PurePosixPath(value))


def _members(tar: tarfile.TarFile, deadline: float):
    members, total = {}, 0
    for member in tar:
        if time.monotonic() > deadline or len(members) >= MAX_MEMBERS:
            raise DataError()
        name = _path(member.name)
        if name in members or not (member.isdir() or member.isreg() or member.issym() or member.islnk()):
            raise DataError()
        if name == "." and not member.isdir():
            raise DataError()
        total += member.size
        if member.size < 0 or total > MAX_ARCHIVE_BYTES or member.uid < 0 or member.gid < 0:
            raise DataError()
        if os.geteuid() != 0 and (member.uid != os.geteuid() or member.gid not in {os.getegid(), *os.getgroups()}):
            raise DataError()  # tarfile otherwise silently loses numeric ownership
        members[name] = member
    for name, member in members.items():
        for parent in PurePosixPath(name).parents:
            entry = members.get(str(parent))
            if entry is not None and not entry.isdir():
                raise DataError()
        if member.issym():
            # Resolve lexical relative links; reject escape, absolute and long links.
            link = member.linkname
            if not link or link.startswith("/") or "\\" in link or "\x00" in link or len(link.encode()) > 4096:
                raise DataError()
            parts = list(PurePosixPath(name).parent.parts)
            for part in link.split("/"):
                if len(part.encode()) > 255:
                    raise DataError()
                if part == "..":
                    if not parts:
                        raise DataError()
                    parts.pop()
                elif part not in {"", "."}:
                    parts.append(part)
                    # Check every traversed component BEFORE a later '..' can
                    # erase it. The whole member map includes forward links.
                    traversed = members.get(str(PurePosixPath(*parts)))
                    if traversed is not None and traversed.issym():
                        raise DataError()
            # Symlink chains can escape despite lexical checks. Disallow links
            # through any other archive symlink, including cycles.
            target = PurePosixPath(*parts)
            for candidate in (target, *target.parents):
                linked = members.get(str(candidate))
                if linked is not None and linked.issym():
                    raise DataError()
        if member.islnk():
            target = members.get(_path(member.linkname))
            if target is None or not target.isreg():
                raise DataError()
            if (member.uid, member.gid, member.mode & 0o777) != (target.uid, target.gid, target.mode & 0o777):
                raise DataError()
    return list(members.values())


def extract_archive(archive: Path, destination: Path):
    """Validate every entry before extracting anything into a private empty tree."""
    deadline = time.monotonic() + FILE_WORK_TIMEOUT
    if destination.exists() or destination.is_symlink():
        raise DataError()
    with gzip.open(archive, "rb") as compressed, tarfile.open(fileobj=_DeadlineIO(compressed, deadline), mode="r:", errorlevel=2) as tar:
        members = _members(tar, deadline)
        # tarfile stops at the end-of-archive blocks, before gzip necessarily
        # validates its trailer/CRC. Drain the bounded remainder first.
        expanded = tar.fileobj.tell()
        while block := tar.fileobj.read(1024 * 1024):
            expanded += len(block)
            if expanded > MAX_ARCHIVE_BYTES + MAX_MEMBERS * 4096:
                raise DataError()
        destination.mkdir(mode=0o700)

        def safe(member, path):
            if time.monotonic() > deadline:
                raise DataError()
            tarfile.tar_filter(member, path)  # resolved containment, including links
            # Extraction must stay owner-accessible so the data can be flushed
            # with an error-reporting barrier; recorded modes are reapplied after.
            accessible = member.mode & 0o777 | (0o700 if member.isdir() else 0o600)
            return member.replace(mode=accessible, uname=None, gname=None)

        # Regulars first permits forward hardlinks without tarfile's fallback
        # recursive extraction. Directories retain their recorded final modes.
        members.sort(key=lambda m: m.islnk())
        tar.extractall(destination, members=members, numeric_owner=True, filter=safe)
        # tarfile skips chown entirely for non-root. Apply permitted archived
        # group ownership explicitly and verify every numeric owner, including
        # symlinks. This is per-record metadata restoration, never blanket chown.
        for member in members:
            tar.fileobj.check()
            target = destination / _path(member.name)
            if os.geteuid() != 0:
                os.chown(target, -1, member.gid, follow_symlinks=False)
            actual = target.lstat()
            if (actual.st_uid, actual.st_gid) != (member.uid, member.gid):
                raise DataError()
        # Replaced data must reach storage before the barrier can be trusted, and
        # archived modes would block reopening entries, so flush then restrict.
        flush_tree(destination, deadline)
        # Archived modes are reapplied deepest first, so an unreadable tree is
        # closed before its parent. Each entry is opened while it is still
        # reachable and flushed through that descriptor: reopening a restored
        # mode would fail for a legitimately private 0000 file or 0500 tree, and
        # a mode change is only durable once the inode itself is flushed.
        for member in sorted((m for m in members if not m.issym()),
                             key=lambda m: PurePosixPath(m.name).parts, reverse=True):
            target = destination / _path(member.name)
            if target.is_symlink():
                raise DataError()
            flags = os.O_RDONLY | (os.O_DIRECTORY if target.is_dir() else 0)
            descriptor = os.open(target, flags)
            try:
                os.fchmod(descriptor, member.mode & 0o777)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            actual = target.lstat()
            if (actual.st_uid, actual.st_gid) != (member.uid, member.gid):
                raise DataError()
            if actual.st_mode & 0o777 != member.mode & 0o777:
                raise DataError()
        # Only the destination's own entries changed here; the tree below it is
        # deliberately unreadable now and was already flushed above.
        durability_barrier(destination, recurse=False)
        tar.fileobj.check()


def build_archive(source: Path, archive: Path) -> tuple[int, str]:
    deadline = time.monotonic() + FILE_WORK_TIMEOUT
    if source.is_symlink() or (source.exists() and not source.is_dir()):
        raise DataError()
    with gzip.open(archive, "wb") as compressed, _DeadlineTar.open(fileobj=_DeadlineIO(compressed, deadline), mode="w:", dereference=False) as tar:
        def checked(member):
            if time.monotonic() > deadline:
                raise DataError()
            _path(member.name)
            if not (member.isdir() or member.isreg() or member.issym() or member.islnk()):
                raise DataError()
            return member
        if source.exists():
            tar.add(source, arcname=".", filter=checked)
        else:
            empty = tarfile.TarInfo(".")
            empty.type, empty.mode = tarfile.DIRTYPE, 0o755
            empty.uid, empty.gid = os.geteuid(), os.getegid()
            tar.addfile(empty)
    size = archive.stat().st_size
    if not 0 < size <= MAX_ARCHIVE_BYTES:
        raise DataError()
    digest = hashlib.sha256()
    with archive.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            if time.monotonic() > deadline:
                raise DataError()
            digest.update(chunk)
    return size, digest.hexdigest()


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
        durability_barrier()  # also supports callers staging without extraction

        for name in names:
            target = destination_root / name
            if target.exists():
                target.rename(old_root / name)
                moved.append(name)
            (staged_root / name).rename(target)
            promoted.append(name)
        # Promoted trees keep archived modes, so flush only the roots whose
        # entries changed; the file data itself was flushed at extraction and
        # their archived modes are deliberately not reopened.
        durability_barrier(staged_root, destination_root, old_root, recurse=False)
    except BaseException:
        # Best effort rollback; any unrecoverable originals stay under old_root.
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
        # Never claim successful rollback until its directory entries are durable.
        # A failing barrier here must not mask the fail-closed error with a raw
        # writeback error, and the originals are already back in place.
        try:
            durability_barrier(staged_root, destination_root, recurse=False)
        except OSError:
            pass
        raise DataError() from None
    return old_root


async def replace_app_trees(staged_root: Path, destination_root: Path, names: tuple[str, ...] | list[str]) -> Path:
    """Return app-private rollback directory; NEVER discard originals here.

    Caller holds OperationLock through the entire multi-root recovery boundary.
    Keep the staged root on failure (including cancellation before token return).
    Only discard after all promotions, activation and cleanup are confirmed.
    """
    return await drain(asyncio.to_thread(_replace, staged_root, destination_root, names))


async def discard_app_trees(rollback: Path) -> None:
    """Discard a token returned by replace_app_trees, never a request-supplied path."""
    def discard():
        directory(rollback.parent)
        directory(rollback)
        if not rollback.name.startswith(".migration-old-"):
            raise DataError()
        shutil.rmtree(rollback)
        durability_barrier(rollback.parent, recurse=False)
    await drain(asyncio.to_thread(discard))
