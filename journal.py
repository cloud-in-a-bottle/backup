"""Durable JSON publication shared by restore and both ends of migration."""

import json
import logging
import os
from pathlib import Path
import secrets

logger = logging.getLogger(__name__)


def save_journal(path: Path, record: dict, *, retain_previous: bool = False) -> None:
    """Publish under the operation lock; failure restores the old file or absence.

    The caller supplies an app-owned private directory and non-secret progress.
    Originals must remain intact until this function returns successfully.
    Layout migrations retain the predecessor for manual recovery inspection.
    """
    temporary = path.with_suffix(".tmp")
    previous = path.parent / ("journal-rollback-" + secrets.token_hex(16) + ".json")
    linked = published = False
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with open(temporary, "w", opener=lambda p, f: os.open(p, f | os.O_NOFOLLOW, 0o600)) as stream:
            json.dump(record, stream, ensure_ascii=False, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        if path.exists() or path.is_symlink():
            os.link(path, previous, follow_symlinks=False)
            linked = True
            if retain_previous:
                os.fsync(directory_fd)
        temporary.replace(path)
        published = True
        os.fsync(directory_fd)
    except BaseException:
        if published:
            try:
                if linked:
                    previous.replace(path)
                else:
                    path.unlink(missing_ok=True)
                os.fsync(directory_fd)
            except OSError:
                # Keep the old inode reachable if rollback itself fails.
                logger.error("Journal rollback failed; retained predecessor at %s", previous, exc_info=True)
        raise
    else:
        if linked and not retain_previous:
            try:
                previous.unlink()
            except OSError:
                logger.warning("Could not reclaim a superseded journal", exc_info=True)
    finally:
        os.close(directory_fd)
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            logger.warning("Could not reclaim an unpublished journal", exc_info=True)
