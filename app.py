import asyncio
import json
import logging
import os
import re
import shutil
import sqlite3
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote

from quart import Quart, Response, jsonify, render_template, request

import migration
import migration_data
import restic_process
import snapshot_configuration
from configuration import (ConfigurationError, RouterClient, _inventory, capture_configuration,
                        confirm_owner)
from operations import OperationLock, OpKind, drain
from recovery import RecoverySession, journal_progress

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.info("backup app module loaded")

app = Quart(__name__)
# Encrypted repository uploads stay below the platform's 16 MiB proxy limit.
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024

# ---------------------------------------------------------------------------
# Paths & configuration
# ---------------------------------------------------------------------------

BASE_PATH = os.environ.get("BOTTLE_APP_BASE_PATH", os.environ.get("OPENHOST_APP_BASE_PATH", "/backup"))
APP_NAME = os.environ.get("BOTTLE_APP_NAME", os.environ.get("OPENHOST_APP_NAME", "backup"))
APP_TOKEN = os.environ.get("BOTTLE_APP_TOKEN", os.environ.get("OPENHOST_APP_TOKEN", ""))
APP_DATA_DIR = Path(os.environ.get("OPENHOST_APP_DATA_DIR", "/data/app_data/backup"))
ALL_APP_DATA = Path("/data/app_data")
APP_TEMP_DATA = Path("/data/app_temp_data")
APP_ARCHIVE = Path("/data/app_archive")
VM_DATA_DIR = Path("/data/vm_data")

# Roots captured by default, only when present as directories in this container.
# Standard ``access_all_app_data = true`` mounts expose app data, not router
# state or host SSH keys. ``vm_data`` is deliberately not captured: it is a
# host-side, nonstandard location that this mount does not provide, and it
# stays restorable only for snapshots an older version did capture.
# Order is significant only for UI display (``list_snapshot_files`` surfaces
# these as the top-level entries when ``root`` is unset). Missing roots are
# skipped silently at backup time.
BACKUP_ROOTS = (ALL_APP_DATA, APP_TEMP_DATA)

# ``access_all_app_data = true`` mounts ``/data/app_archive`` into the
# container so the backup app can see it for migration / inspection,
# but the archive tier is intentionally NOT backed up for either backend:
#
# - ``local`` archive backend: data stays on the instance's disk, so it is
#   not an off-machine copy.
# - ``s3`` archive backend: file data is stored through JuiceFS; recovery
#   requires JuiceFS metadata as well as the S3 objects.
#
# The entire app_data/backup directory is also excluded: configuration,
# history, and any local restic repository stored there. Excluding a local
# repository avoids recursive self-inclusion.
#
# Restic still receives this as an explicit ``--exclude`` (in addition
# to ``/data/app_archive`` not being in BACKUP_ROOTS), so a future
# refactor that adds it to the roots list won't silently start
# capturing the archive.
RESTORE_WORK_NAME = ".bottle-backup-restore"
BACKUP_EXCLUDES = (
    ALL_APP_DATA / APP_NAME,
    APP_TEMP_DATA / APP_NAME,
    APP_ARCHIVE,
    *(root / RESTORE_WORK_NAME for root in BACKUP_ROOTS),
)
ROUTER_URL = os.environ.get("OPENHOST_ROUTER_URL", "http://host.docker.internal:8080")
ZONE_DOMAIN = os.environ.get("OPENHOST_ZONE_DOMAIN", "")
# Hostname recorded on every snapshot (`restic backup --host`). The container's
# own hostname is random and changes on each restart/redeploy, which would
# fragment `restic forget --group-by host` into per-container groups. Pinning it
# to the (stable) zone domain gives every snapshot from this instance one
# identity — and, in a shared repo, keeps instances distinguishable. Falls back
# to a constant when the zone domain isn't set so we never pass an empty --host.
BACKUP_HOST = ZONE_DOMAIN or "bottle"
# Router API token — the backup app needs this to call the local router API.
# The OPENHOST_APP_TOKEN is for cross-app service communication and does NOT
# grant access to router management endpoints (/api/apps, /reload_app, etc.).
# This can be set in config.json as "router_api_token" or via environment.
ROUTER_API_TOKEN = os.environ.get("OPENHOST_ROUTER_API_TOKEN", "")

CONFIG_DIR = APP_DATA_DIR
CONFIG_FILE = CONFIG_DIR / "config.json"
DB_FILE = CONFIG_DIR / "backups.db"
# Restic repository lives inside the backup app's data dir by default. This
# path is excluded from backups (see `--exclude` in run_backup).
RESTIC_REPO_DIR = APP_DATA_DIR / "restic-repo"

DEFAULT_CONFIG = {
    "interval_seconds": 0,
    "repo": "",
    "repo_password": "",
    "env": {},
    # Retention policy (restic `forget` keep-* flags). 0 = tier unset. When
    # every tier is 0 no retention runs, so the default is "keep everything".
    "keep_last": 0,
    "keep_hourly": 0,
    "keep_daily": 0,
    "keep_weekly": 0,
    "keep_monthly": 0,
    "keep_yearly": 0,
}

# Config key -> restic forget flag. Order is cosmetic (matches restic docs).
KEEP_FLAGS = {
    "keep_last": "--keep-last",
    "keep_hourly": "--keep-hourly",
    "keep_daily": "--keep-daily",
    "keep_weekly": "--keep-weekly",
    "keep_monthly": "--keep-monthly",
    "keep_yearly": "--keep-yearly",
}

# Snapshot IDs are hex strings; restic emits 8-char short IDs and 64-char long
# ones. Accept either (plus anything in between) for validation on API input.
SNAPSHOT_ID_RE = re.compile(r"[a-f0-9]{8,64}\Z")


# New snapshots are tagged ``bottle`` plus ``zone:<domain>``. Legacy snapshots
# used ``openhost``; list/stats/forget still accept that tag. No zone tag in
# local dev (OPENHOST_ZONE_DOMAIN unset) matches the old unscoped scheme.
SNAPSHOT_TAG = "bottle"
LEGACY_SNAPSHOT_TAG = "openhost"
SNAPSHOT_TAGS = (SNAPSHOT_TAG, LEGACY_SNAPSHOT_TAG)


def _zone_tag() -> str | None:
    return f"zone:{quote(ZONE_DOMAIN, safe='.-:')}" if ZONE_DOMAIN else None


def _restic_tag_args() -> list[str]:
    """``--tag bottle --tag openhost``. restic ORs repeated ``--tag`` flags."""
    args: list[str] = []
    for tag in SNAPSHOT_TAGS:
        args += ["--tag", tag]
    return args


def _has_app_tag(tags: list[str]) -> bool:
    return any(t in SNAPSHOT_TAGS for t in tags)


def _backup_tags(name: str | None = None) -> list[str]:
    """Encode user components: restic splits every --tag value on commas."""
    tags = [SNAPSHOT_TAG]
    zone = _zone_tag()
    if zone:
        tags.append(zone)
    if name:
        tags.append(f"name-uri:{quote(name, safe='')}")
    return tags


def _snapshot_name(tags: list[str]) -> str | None:
    for tag in tags:
        if tag.startswith("name-uri:"):
            return unquote(tag[len("name-uri:"):])
        if tag.startswith("name:"):
            return tag[len("name:"):]
    return None


def classify_repo(repo: str) -> dict:
    """Return ``{"type": <label>, "remote": bool, "location": <display>}``.

    Used by the UI so users can tell at a glance whether their snapshots are
    stored on a remote backend (e.g. S3) or just on the same instance's local
    disk. The latter is risky: if the instance dies, so does the backup.
    """
    if not repo:
        return {"type": "unknown", "remote": False, "location": ""}
    # Restic backend prefixes (see https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html)
    if repo.startswith("s3:"):
        return {"type": "s3", "remote": True, "location": repo[3:]}
    if repo.startswith("b2:"):
        return {"type": "b2", "remote": True, "location": repo[3:]}
    if repo.startswith("azure:"):
        return {"type": "azure", "remote": True, "location": repo[6:]}
    if repo.startswith("gs:"):
        return {"type": "gcs", "remote": True, "location": repo[3:]}
    if repo.startswith("swift:"):
        return {"type": "swift", "remote": True, "location": repo[6:]}
    if repo.startswith("sftp:"):
        return {"type": "sftp", "remote": True, "location": repo[5:]}
    if repo.startswith("rest:"):
        return {"type": "rest-server", "remote": True, "location": repo[5:]}
    if repo.startswith("rclone:"):
        return {"type": "rclone", "remote": True, "location": repo[7:]}
    # Anything else is a local path (no scheme, or `local:`).
    if repo.startswith("local:"):
        repo_path = repo[6:]
    else:
        repo_path = repo
    return {"type": "local", "remote": False, "location": repo_path}


# Single lock for mutual exclusion across backup / restore / migration.
op_lock = OperationLock()

# A migration holds op_lock across many separate receive_* requests (start ->
# chunks -> finalize). If the source dies mid-transfer the destination never
# gets a finalize, so the lock would otherwise stay held until an app restart
# (issue #14). Treat a migration idle this long as abandoned and reclaim it.
MIGRATION_IDLE_TIMEOUT_SECONDS = 30 * 60
_migration_receiver: migration.MigrationReceiver | None = None


async def _reclaim_abandoned_migration() -> None:
    """Release a migration lock orphaned by a dead/stopped source (issue #14).

    Safe to call before starting any operation: it only clears a *migration*
    lock idle past the timeout, so a live transfer (kept fresh via
    ``op_lock.touch()`` on each receive) is never disturbed.
    """
    if _migration_receiver is not None:
        await _migration_receiver.expire_stale()


def _receiver() -> migration.MigrationReceiver:
    global _migration_receiver
    if _migration_receiver is None:
        _migration_receiver = migration.MigrationReceiver(
            lock=op_lock, all_app_data=ALL_APP_DATA,
            work_dir=APP_DATA_DIR / ".migration", router_url=ROUTER_URL,
            backup_app_name=APP_NAME, restore=_restore_migration_snapshot,
        )
    return _migration_receiver


# Restore-specific status (not part of the lock itself).
restore_last_snapshot = None
restore_last_status = None
restore_progress: dict | None = None
_restore_needs_attention = False
_restore_session: RecoverySession | None = None

# Most recent `restic check` result, surfaced via /api/check/status.
check_last_status = None
check_last_output = None
check_last_at = None
check_running = False

scheduler_task = None

# Fire-and-forget background tasks (e.g. the post-delete repo-stats refresh).
# We keep a strong reference until each finishes — asyncio only holds a weak
# reference to running tasks, so without this a task can be garbage-collected
# mid-flight and silently cancelled. Each removes itself on completion.
_background_tasks: set[asyncio.Task] = set()


def _spawn_background(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


# ---------------------------------------------------------------------------
# Live operation-status push (Server-Sent Events)
# ---------------------------------------------------------------------------
# The status banner reflects op_lock state. Rather than relying only on the
# UI's slow poll, each connected browser holds an SSE stream (/api/events);
# op_lock's on_change callback wakes every stream so the banner updates the
# instant an operation starts or finishes.

# One queue per connected SSE client. A notification pushes a sentinel into
# each; the stream coroutine wakes, reads the current status, and emits it.
_status_subscribers: set[asyncio.Queue] = set()


def _lock_status() -> dict:
    """Current op_lock state — the payload the banner needs. Shared by
    /api/status and the SSE stream so the two never disagree."""
    active = op_lock.active
    return {
        "busy": op_lock.busy,
        "active_op": active.value if active else None,
        "busy_message": op_lock.busy_message(),
    }


def _notify_status_change() -> None:
    """Wake every SSE subscriber so it pushes the new status immediately.

    Runs synchronously from op_lock.try_acquire/release (same event-loop
    thread), so a non-blocking put_nowait is safe. A full queue already has a
    pending wake-up, so dropping the extra is fine.
    """
    for q in list(_status_subscribers):
        try:
            q.put_nowait(None)
        except asyncio.QueueFull:
            pass


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def init_db():
    """Create tables if they don't exist.  Call once at startup."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            snapshot_id TEXT,
            data_added_bytes INTEGER,
            total_size_bytes INTEGER,
            file_count INTEGER,
            name TEXT
        )
    """)
    # Incremental schema upgrades (for installs where the old table existed).
    cursor = conn.execute("PRAGMA table_info(backups)")
    columns = {row[1] for row in cursor.fetchall()}
    for col, ddl in [
        ("snapshot_id", "ALTER TABLE backups ADD COLUMN snapshot_id TEXT"),
        ("data_added_bytes", "ALTER TABLE backups ADD COLUMN data_added_bytes INTEGER"),
        ("total_size_bytes", "ALTER TABLE backups ADD COLUMN total_size_bytes INTEGER"),
        ("file_count", "ALTER TABLE backups ADD COLUMN file_count INTEGER"),
        ("name", "ALTER TABLE backups ADD COLUMN name TEXT"),
        ("repo_size_bytes", "ALTER TABLE backups ADD COLUMN repo_size_bytes INTEGER"),
        (
            "repo_uncompressed_bytes",
            "ALTER TABLE backups ADD COLUMN repo_uncompressed_bytes INTEGER",
        ),
        ("repo_blob_count", "ALTER TABLE backups ADD COLUMN repo_blob_count INTEGER"),
        (
            "repo_snapshots_count",
            "ALTER TABLE backups ADD COLUMN repo_snapshots_count INTEGER",
        ),
        (
            "repo_compression_ratio",
            "ALTER TABLE backups ADD COLUMN repo_compression_ratio REAL",
        ),
        ("repo_stats_at", "ALTER TABLE backups ADD COLUMN repo_stats_at TEXT"),
    ]:
        if col not in columns:
            conn.execute(ddl)
    conn.commit()
    conn.close()


def get_db():
    """Get a database connection."""
    return sqlite3.connect(str(DB_FILE))


def record_backup(
    timestamp,
    status,
    error_message=None,
    snapshot_id=None,
    data_added_bytes=None,
    total_size_bytes=None,
    file_count=None,
    name=None,
    repo_stats=None,
):
    """Insert a backup record into the database.

    When ``repo_stats`` (a dict from ``repo_stats()``) is supplied, the
    repo-wide size cache is written into the *same* INSERT, so a successful
    backup records both its per-run figures and the current repo footprint
    in one row — no separate connection or ``MAX(id)`` update needed. The
    delete path, which has no INSERT to piggyback on, instead re-stamps the
    newest surviving row inline (see ``delete_snapshot``).
    """
    cols = [
        "timestamp",
        "status",
        "error_message",
        "snapshot_id",
        "data_added_bytes",
        "total_size_bytes",
        "file_count",
        "name",
    ]
    vals = [
        timestamp,
        status,
        error_message,
        snapshot_id,
        data_added_bytes,
        total_size_bytes,
        file_count,
        name,
    ]
    if repo_stats is not None:
        cols += [
            "repo_size_bytes",
            "repo_uncompressed_bytes",
            "repo_blob_count",
            "repo_snapshots_count",
            "repo_compression_ratio",
            "repo_stats_at",
        ]
        vals += [
            repo_stats.get("total_size_bytes"),
            repo_stats.get("total_uncompressed_size_bytes"),
            repo_stats.get("total_blob_count"),
            repo_stats.get("snapshots_count"),
            repo_stats.get("compression_ratio"),
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ]
    placeholders = ", ".join(["?"] * len(vals))
    conn = get_db()
    try:
        conn.execute(
            f"INSERT INTO backups ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        conn.commit()
    finally:
        conn.close()


def get_last_backup():
    """Return the most recent backup record, or None."""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT timestamp, status, error_message FROM backups ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            return {"timestamp": row[0], "status": row[1], "error_message": row[2]}
        return None
    finally:
        conn.close()


def load_repo_stats_cache() -> dict | None:
    """Return the last cached repo stats (newest stamped row), or None.

    The cache is written by whoever changes the repo footprint, folded into
    that operation's own DB write — ``record_backup`` for a backup,
    ``delete_snapshot`` for a prune — so there is no standalone writer here.
    Keys mirror what ``repo_stats()`` returns so /api/repo/stats can serve
    this verbatim, plus ``computed_at`` so the UI can show its age.
    """
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT repo_size_bytes, repo_uncompressed_bytes, repo_blob_count, "
            "repo_snapshots_count, repo_compression_ratio, repo_stats_at "
            "FROM backups WHERE repo_stats_at IS NOT NULL ORDER BY id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        logger.exception("Failed to read cached repo stats")
        return None
    finally:
        conn.close()
    if not row:
        return None
    return {
        "total_size_bytes": row[0],
        "total_uncompressed_size_bytes": row[1],
        "total_blob_count": row[2],
        "snapshots_count": row[3],
        "compression_ratio": row[4],
        "computed_at": row[5],
    }


def invalidate_repo_stats_cache() -> None:
    """Drop the cached repo-size stamp from every backup row.

    The cache describes a specific repository; call this when the configured
    repo changes so ``load_repo_stats_cache`` returns ``None`` and the next
    ``/api/repo/stats`` read computes live against the new repo instead of
    serving the old repo's size. Leaves the backup history itself untouched —
    only the auxiliary ``repo_stats_*`` columns are cleared.
    """
    conn = get_db()
    try:
        conn.execute(
            "UPDATE backups SET repo_size_bytes = NULL, "
            "repo_uncompressed_bytes = NULL, repo_blob_count = NULL, "
            "repo_snapshots_count = NULL, repo_compression_ratio = NULL, "
            "repo_stats_at = NULL WHERE repo_stats_at IS NOT NULL"
        )
        conn.commit()
    except sqlite3.Error:
        logger.exception("Failed to invalidate repo stats cache")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            saved = json.load(f)
        return {**DEFAULT_CONFIG, **saved}
    return dict(DEFAULT_CONFIG)


def save_config(conf):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(conf, f, indent=2)
    # The password lives in this file so restrict perms.
    try:
        os.chmod(CONFIG_FILE, 0o600)
    except OSError:
        pass


def get_router_api_token():
    """Get the router API token from config or environment.

    Priority: config.json > OPENHOST_ROUTER_API_TOKEN env var.
    """
    conf = load_config()
    token = conf.get("router_api_token", "")
    if token:
        return token
    return ROUTER_API_TOKEN


def _extract_bearer_token() -> str | None:
    """Extract the Bearer token from the current request's Authorization header."""
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return None


# Owner-only, read-only probe: the platform's app-definitions parse endpoint
# rejects an app token, so a successful parse proves owner authority without
# changing anything. The configured router_api_token is deliberately absent
# here: it proves what this app may do unattended, never who is calling.
async def _caller_is_owner() -> str | None:
    """Return the caller's token when the router confirms owner authority."""
    token = _extract_bearer_token()
    if not token:
        return None
    return token if await confirm_owner(ROUTER_URL, token) else None


def _owner_required_response() -> tuple:
    return jsonify(
        ok=False,
        error="Owner authorization required: send a valid owner Router API token as a Bearer token.",
    ), 401


async def _verify_admin_token(supplied: str | None) -> bool:
    """Return True iff ``supplied`` is a valid admin Bearer token.

    The backup app is reachable unauthenticated from inside the container
    network (co-located apps on the Docker bridge can hit
    ``http://backup:8080/...`` directly, bypassing the Cloud in a Bottle router's
    auth layer). Sensitive operations — password reveal, writing the
    stored router_api_token or repo_password — must therefore require an
    explicit caller token.

    We accept any token that the local Cloud in a Bottle router accepts. The
    router validates the token by checking it against the owner API
    tokens table, so this gives us real auth even though the backup app
    itself doesn't have a user database.
    """
    if not supplied:
        return False
    try:
        import httpx

        async with httpx.AsyncClient(verify=False, timeout=5) as client:
            r = await client.get(
                f"{ROUTER_URL}/api/apps",
                headers={"Authorization": f"Bearer {supplied}"},
            )
            return r.status_code == 200 and "json" in r.headers.get("content-type", "")
    except Exception:
        logger.exception("Admin token verification failed")
        return False


# ---------------------------------------------------------------------------
# Restic helpers
# ---------------------------------------------------------------------------


def _restic_env(conf: dict) -> dict:
    """Environment for invoking the restic binary with repo + password set."""
    env = os.environ.copy()
    if conf.get("_isolated_restic"):
        env = {key: value for key, value in env.items() if not key.startswith("RESTIC_")}
    env["RESTIC_REPOSITORY"] = conf["repo"]
    env["RESTIC_PASSWORD"] = conf.get("repo_password", "")
    # Suppress progress output in unattended runs; JSON flag gives structured
    # output where we need it.
    env["RESTIC_PROGRESS_FPS"] = "0"
    # Forward any configured backend credentials (S3 keys, etc.). Only keys
    # in ALLOWED_ENV_KEYS are accepted via the API; anything already in
    # config is trusted.
    for k, v in (conf.get("env") or {}).items():
        if v is None or v == "":
            continue
        env[k] = str(v)
    return env


async def _run_restic(args: list[str], conf: dict, timeout: float | None = None):
    """Run `restic <args>` with configured repo, return (returncode, stdout, stderr).

    Raises asyncio.TimeoutError if the subprocess exceeds ``timeout``. On
    either timeout OR task cancellation, the subprocess is killed so we
    don't leak a live restic process holding the repo lock.

    Every invocation is logged at INFO (command on start, exit code +
    elapsed time on completion) so the app's console — ``oh app logs
    backup`` — shows exactly what restic ran. The args never carry secrets:
    the repo URL and password go through the environment (see
    ``_restic_env``), not argv.
    """
    env = _restic_env(conf)
    logger.info("restic %s", " ".join(args))
    started = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        "restic",
        *args,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    communication = asyncio.create_task(proc.communicate())
    try:
        stdout, stderr = await asyncio.wait_for(asyncio.shield(communication), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        logger.warning(
            "restic %s killed after %.1fs (%s)",
            args[0] if args else "?",
            time.monotonic() - started,
            type(e).__name__,
        )
        restic_process.kill_group(proc)
        await restic_process.finish_communication(communication)
        raise
    logger.info(
        "restic %s -> rc=%s (%.1fs)",
        args[0] if args else "?",
        proc.returncode,
        time.monotonic() - started,
    )
    return proc.returncode, stdout, stderr


def _parse_ndjson(data: bytes):
    """Iterate over NDJSON messages in ``data``, skipping blank/invalid lines."""
    for raw in data.decode(errors="replace").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            yield json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("restic: non-JSON stdout line: %s", raw)


# Long enough for multi-GB S3 uploads on slow links but still finite — a
# wedged TCP connection can't permanently brick the scheduler.
BACKUP_TIMEOUT_SECONDS = 6 * 60 * 60  # 6 hours
RESTORE_TIMEOUT_SECONDS = 12 * 60 * 60  # 12 hours
CHECK_TIMEOUT_SECONDS = 2 * 60 * 60  # 2 hours
FORGET_TIMEOUT_SECONDS = 10 * 60  # 10 minutes — forget only rewrites metadata
PRUNE_TIMEOUT_SECONDS = 6 * 60 * 60  # 6 hours — prune repacks, can be slow on S3

# Duration lock-taking commands wait for the repo lock before giving up
# (restic `--retry-lock`, added in 0.16). Without it restic fails instantly if
# anything else holds a lock; a short retry rides out the brief window where a
# concurrent op is finishing, instead of surfacing "repository is already
# locked" to the user. Read-only commands use `--no-lock` and don't need this.
RETRY_LOCK = "1m"

# Guard against concurrent `restic init` calls. When the UI loads, multiple
# API endpoints (snapshots, stats, check) call ensure_repo_initialized at
# the same time. Without this lock, two concurrent `restic init` invocations
# can corrupt the repo (the second init races with the first, producing keys
# that fail ciphertext verification).
_init_lock = asyncio.Lock()


async def ensure_repo_initialized(
    conf: dict, *, auto_init: bool | None = None
) -> tuple[bool, str | None]:
    """Ensure the restic repo exists; run `restic init` if not.

    Returns (initialized_now, error_message).

    ``auto_init`` controls what happens when ``cat config`` fails with a
    "repo does not exist" signal:

    - ``True``  — always run ``restic init`` (used by ``run_backup`` so the
      first scheduled backup creates the repo regardless of backend).
    - ``False`` — never auto-init; report a "not initialized" error so the
      caller / UI can prompt the user explicitly.
    - ``None``  — auto-init only when the repo is local (no remote backend
      prefix).  Safer default for read-only operations: a typo'd S3 URL
      won't silently create an empty bucket-side repo at the wrong path,
      but a fresh local install still "just works" when the user clicks
      a UI button.
    """
    async with _init_lock:
        # `cat config` is a cheap way to confirm the repo exists and the password
        # is correct. It returns non-zero on either missing repo or wrong password.
        # --no-lock: this is a pure read (existence/password probe), so it must
        # not take a restic repo lock — that keeps the invariant "op_lock is
        # held whenever a restic lock is held" true without gating this behind
        # op_lock, and lets it run even while a prune holds the exclusive lock.
        rc, _stdout, stderr = await _run_restic(
            ["cat", "config", "--no-lock"], conf, timeout=30
        )
        if rc == 0:
            return False, None

        err = stderr.decode(errors="replace").strip()
        # If the repo simply doesn't exist, decide whether to init. Heuristic on
        # the error text; restic doesn't expose a clean "not found" exit code.
        err_lower = err.lower()
        is_not_initialized = (
            "does not exist" in err_lower
            or "unable to open config" in err_lower
            or "no such file" in err_lower
        )
        if is_not_initialized:
            info = classify_repo(conf["repo"])
            should_init = auto_init if auto_init is not None else not info["remote"]
            if not should_init:
                return False, (
                    f"Repository not initialized at {conf['repo']!r}. Run a backup "
                    f"to create it, or pass auto_init=True for this operation."
                )
            # Local repo path: make sure parent exists. classify_repo already
            # strips any `local:` prefix, so we use its `location` as the on-disk
            # path rather than the raw repo string.
            if info["type"] == "local" and info["location"]:
                Path(info["location"]).parent.mkdir(parents=True, exist_ok=True)
            rc2, _out2, err2 = await _run_restic(["init"], conf, timeout=60)
            if rc2 != 0:
                return (
                    False,
                    f"restic init failed: {err2.decode(errors='replace').strip()}",
                )
            return True, None
        return False, f"restic repo check failed: {err}"


async def _restic_unlock_if_stale(conf: dict) -> None:
    """Best-effort remove stale repo locks at startup.

    Uses ``--remove-all`` so that locks left by a previous container
    incarnation are cleared even if the hostname changed between restarts
    (which is the normal case for Docker containers — each restart gets a
    new random hostname, so plain ``restic unlock`` would only remove locks
    matching the current hostname and silently leave the stale one behind).
    """
    try:
        rc, stdout, stderr = await _run_restic(
            ["unlock", "--remove-all"], conf, timeout=30
        )
        if rc == 0:
            out = (
                stdout.decode(errors="replace") + stderr.decode(errors="replace")
            ).strip()
            if out:
                logger.info("restic unlock --remove-all: %s", out)
            else:
                logger.info("restic unlock --remove-all: no stale locks found")
        else:
            err = (
                stdout.decode(errors="replace") + stderr.decode(errors="replace")
            ).strip()
            logger.warning("restic unlock --remove-all failed (rc=%d): %s", rc, err)
    except Exception:
        logger.warning("restic unlock failed on startup", exc_info=True)


async def test_restic_connection(
    conf: dict, *, timeout: float = 10.0
) -> tuple[bool, str, str]:
    """Run ``restic cat config`` once with a short timeout, no retries.

    Returns ``(ok, output)`` where ``output`` is the raw stderr restic
    produced. We drain stderr incrementally in a background task so that
    when we kill restic at the timeout, all the bytes it printed up to
    that point — typically several ``retrying after Xs: <backend error>``
    lines — are already in our buffer.
    """
    env = _restic_env(conf)
    logger.info("restic cat config (connection test, timeout=%.0fs)", timeout)
    proc = await asyncio.create_subprocess_exec(
        "restic",
        "cat",
        "config",
        "--no-lock",  # pure read: never take a restic repo lock (see invariant)
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,  # own the group so backend helpers die with it
    )
    buf = bytearray()

    async def read_stderr() -> None:
        assert proc.stderr is not None
        try:
            while True:
                chunk = await proc.stderr.read(4096)
                if not chunk:
                    return
                buf.extend(chunk)
        except BaseException:
            restic_process.abort_failed_reader(proc, proc.stderr)
            raise

    async def collect() -> None:
        # This task is the sole owner of the pipe, including timeout/cancellation
        # cleanup. A second drain cannot read the same StreamReader concurrently.
        results = await asyncio.gather(read_stderr(), proc.wait(), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result

    communication = asyncio.create_task(collect())
    timed_out = False
    try:
        await asyncio.wait_for(asyncio.shield(communication), timeout=timeout)
    except asyncio.TimeoutError:
        timed_out = True
    finally:
        if proc.returncode is None or not communication.done():
            restic_process.kill_group(proc)
        await restic_process.finish_communication(communication)

    text = bytes(buf).decode(errors="replace")
    if timed_out:
        text = (text + f"\n\n[killed after {timeout:.0f}s — no retries]").lstrip()
    return _classify_restic_test(proc.returncode, text, timed_out)


def _classify_restic_test(
    returncode: int | None, output: str, timed_out: bool
) -> tuple[bool, str, str]:
    """Decide whether a restic test should read as success or failure.

    Exit 0 → success.
    "repository does not exist" → success ("reachable, just no repo yet" —
    a backup will create it). The bucket / path / creds all worked; the
    only "missing" thing is the user hasn't initialized a repo there yet,
    which is the expected state on first run.
    Anything else → failure.
    """
    if returncode == 0 and not timed_out:
        return True, "Connection OK", output or "(no output)"
    if not timed_out and "repository does not exist" in output.lower():
        return (
            True,
            "Reachable — no repository at this location yet (a backup will create it)",
            output,
        )
    return False, "Connection failed", output or f"restic exited with code {returncode}"


def _build_restic_debug(conf: dict) -> dict:
    """Return the restic command + env used for testing.

    All values are included in plaintext — this app has no public routes,
    so callers are already authed as the owner by the Cloud in a Bottle router.
    The UI hides the secret-looking values behind a 'Show secrets' button
    purely as a shoulder-surfing guard.
    """
    env = _restic_env(conf)
    # Only the keys restic actually reads from us, not the whole process env.
    keys: list[str] = ["RESTIC_REPOSITORY", "RESTIC_PASSWORD"]
    for k in conf.get("env") or {}:
        if k not in keys:
            keys.append(k)
    entries = []
    for k in keys:
        v = env.get(k, "")
        entries.append({"key": k, "value": v})
    return {
        "command": "restic cat config",
        "env": entries,
    }


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------


def _backup_blocked_reason() -> str | None:
    """Public reason a new snapshot must wait for an owner, or None."""
    if _restore_needs_attention:
        return "Retry or acknowledge the incomplete recovery before running another backup."
    # This construction is deliberate rather than lazy: the receiver loads its
    # journal once, so building it here is what makes an interrupted incoming
    # migration visible to the very first request after a restart. Startup
    # already refuses to serve when the private work directory is unusable, so
    # this cannot start failing for a reason the process would survive.
    if _receiver().needs_attention:
        return (
            "Inspect and acknowledge the interrupted incoming migration before running another backup."
        )
    if _source_needs_attention():
        return (
            "Inspect and acknowledge the interrupted outgoing migration before running another backup."
        )
    return None


def _source_needs_attention() -> bool:
    """Outgoing cutover attention, safe to read on the request path."""
    record = migration.source_recovery
    return bool(record is not None and record.needs_attention)


async def run_backup(name: str | None = None, *, lock_acquired: bool = False) -> bool:
    if lock_acquired and op_lock.active != OpKind.BACKUP:
        raise RuntimeError("Backup operation ownership was not reserved")
    try:
        blocked = _backup_blocked_reason()
    except Exception:
        # The attention records live on disk, so a filesystem failure here must
        # read as a blocked backup rather than escape into the scheduler.
        logger.exception(
            "Could not read the recovery attention records; backups stay blocked until "
            "the private migration directory is readable and owned by this app's user")
        if lock_acquired:
            op_lock.release(OpKind.BACKUP)
        return False
    if blocked:
        logger.warning("Backup paused: %s", blocked)
        if lock_acquired:
            op_lock.release(OpKind.BACKUP)
        return False
    if not lock_acquired:
        await _reclaim_abandoned_migration()
    err = None if lock_acquired else op_lock.try_acquire(OpKind.BACKUP)
    if err:
        logger.warning("Skipping backup: %s", err)
        return False

    try:
        conf = load_config()
    except Exception:
        logger.exception("Failed to load backup config")
        op_lock.release(OpKind.BACKUP)
        return False

    if not conf.get("repo") or not conf.get("repo_password"):
        logger.error("Restic repo or password not configured, skipping backup")
        op_lock.release(OpKind.BACKUP)
        return False

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    logger.info("Starting restic backup to %s", conf["repo"])

    removed = 0
    try:
        # Backup always creates the repo if missing — that's the operation
        # users opt into knowing it'll write to the configured location.
        init_err = (await ensure_repo_initialized(conf, auto_init=True))[1]
        if init_err:
            record_backup(timestamp, "error", init_err, name=name)
            logger.error("Backup failed: %s", init_err)
            return False

        tags = _backup_tags(name)

        # Back up the default roots, and only the ones present as directories
        # in this container.
        roots = [p for p in BACKUP_ROOTS if p.is_dir()]
        if not roots:
            msg = "No backup roots available — expected one of: " + ", ".join(
                str(p) for p in BACKUP_ROOTS
            )
            record_backup(timestamp, "error", msg, name=name)
            logger.error(msg)
            return False

        # Missing approval or failed private export is a failed backup, never a
        # silently incomplete recovery point. No owner key is needed to export;
        # a configured key additionally captures runtime permissions and states.
        bundle = await capture_configuration(
            ROUTER_URL, APP_TOKEN, get_router_api_token() or None, APP_NAME
        )
        tags.append(snapshot_configuration.CONFIGURATION_TAG)
        if bundle["runtime"] is not None:
            tags.append(snapshot_configuration.RUNTIME_TAG)

        args = ["backup", "--json", "--retry-lock", RETRY_LOCK]
        args += [str(p) for p in roots]
        # Pin the recorded hostname so every snapshot from this instance shares
        # one stable identity (see BACKUP_HOST) instead of the container's
        # random per-restart hostname.
        args += ["--host", BACKUP_HOST]
        # Exclude the entire backup app data directory and app_archive
        # (scope documented at the BACKUP_EXCLUDES definition).
        for ex in BACKUP_EXCLUDES:
            args += ["--exclude", str(ex)]
        if APP_DATA_DIR not in BACKUP_EXCLUDES:
            args += ["--exclude", str(APP_DATA_DIR)]
        for t in tags:
            args += ["--tag", t]

        # Go through the shared helper so the subprocess has a bounded
        # timeout and gets properly killed on cancellation. BACKUP_TIMEOUT
        # is generous for large instances but still finite — a wedged S3
        # connection would otherwise hold the op lock forever.
        try:
            with snapshot_configuration.configuration_file(bundle) as metadata_file:
                rc, stdout, stderr = await _run_restic(
                    [*args, str(metadata_file)], conf, timeout=BACKUP_TIMEOUT_SECONDS
                )
        except asyncio.TimeoutError:
            msg = f"restic backup timed out after {BACKUP_TIMEOUT_SECONDS}s"
            record_backup(timestamp, "error", msg, name=name)
            logger.error(msg)
            return False

        summary = None
        if stdout:
            # Restic emits NDJSON to stdout with --json; the last `summary`
            # message contains the snapshot ID and byte counts.
            for msg in _parse_ndjson(stdout):
                if msg.get("message_type") == "summary":
                    summary = msg

        if stderr:
            for line in stderr.decode(errors="replace").splitlines():
                if line.strip():
                    logger.info("restic stderr: %s", line)

        if rc == 0:
            snapshot_id = await snapshot_configuration.complete_capture(
                (summary or {}).get("snapshot_id"), _restic_env(conf)
            )
            summary["snapshot_id"] = snapshot_id
            # Backup succeeded. Apply the retention policy first (forget only,
            # under the lock) so the footprint we stamp reflects the
            # post-retention snapshot set. Best-effort — a retention failure
            # must not fail the backup itself.
            try:
                removed = await run_retention(conf)
            except Exception:
                logger.exception("Retention failed")

            # Compute the repo footprint now, while we still hold the op lock
            # (so the stats read stays serialized), and fold it into the same
            # row record_backup inserts — no second connection or MAX(id)
            # update. repo_stats() is best-effort and never raises; on failure
            # repo_stats_data is None and the row just carries no fresh cache
            # (the reader falls back to the previous stamped row). The size is
            # still pre-prune here; the background prune re-stamps it once it
            # reclaims space.
            repo_stats_data, _ = await repo_stats()
            record_backup(
                timestamp,
                "success",
                snapshot_id=summary.get("snapshot_id") if summary else None,
                data_added_bytes=summary.get("data_added") if summary else None,
                total_size_bytes=(
                    summary.get("total_bytes_processed") if summary else None
                ),
                file_count=summary.get("total_files_processed") if summary else None,
                name=name,
                repo_stats=repo_stats_data,
            )
            if summary is not None:
                logger.info(
                    "Backup completed: snapshot=%s data_added=%s total=%s",
                    summary.get("snapshot_id", "?"),
                    summary.get("data_added", "?"),
                    summary.get("total_bytes_processed", "?"),
                )
            else:
                # Succeeded but we somehow missed the summary line.
                logger.info("Backup completed (no summary parsed)")
            return True

        error_msg = stderr.decode(errors="replace").strip() or f"restic exit code {rc}"
        record_backup(timestamp, "error", error_msg, name=name)
        logger.error("Backup failed: %s", error_msg)
        return False
    except (ConfigurationError, snapshot_configuration.SnapshotConfigurationError) as e:
        record_backup(timestamp, "error", str(e), name=name)
        logger.error("Backup configuration capture failed: %s", e)
        return False
    except Exception as e:
        record_backup(timestamp, "error", str(e), name=name)
        logger.exception("Backup failed")
        return False
    finally:
        op_lock.release(OpKind.BACKUP)
        # Retention forgot snapshots but didn't prune — reclaim the space in
        # a background worker after releasing the backup lock. The worker
        # acquires its own operation lock for the prune's full duration.
        if removed:
            schedule_prune()


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------


async def list_snapshots() -> tuple[list[dict], bool]:
    """Return (snapshots, repo_ok) for every instance in the configured repo.

    Each snapshot entry has: {id, short_id, time, paths, tags, hostname}.
    """
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return [], False
    # Auto-init for local repos so the snapshots panel doesn't render
    # "unable to open config file" on a freshly-configured install where
    # the user hasn't triggered a backup yet.  Remote repos are NOT
    # auto-inited from a read endpoint — that's reserved for run_backup
    # so a typo'd S3/B2/SFTP URL can't silently create an empty repo at
    # the wrong location.
    init_err = (await ensure_repo_initialized(conf))[1]
    if init_err:
        logger.info("list_snapshots: %s", init_err)
        return [], False
    try:
        # Keep this app's snapshots (bottle or legacy openhost), regardless of
        # zone: a replacement instance must be able to discover and restore
        # backups made under the original instance's domain.
        rc, stdout, stderr = await _run_restic(
            ["snapshots", "--json", *_restic_tag_args(), "--no-lock"],
            conf,
            timeout=60,
        )
        if rc != 0:
            logger.error(
                "restic snapshots failed: %s", stderr.decode(errors="replace").strip()
            )
            return [], False
        entries = json.loads(stdout.decode(errors="replace") or "[]")
        out = []
        for e in entries:
            tags = e.get("tags", []) or []
            if not _has_app_tag(tags):
                continue
            out.append(
                {
                    "id": e.get("id", ""),
                    "short_id": e.get("short_id", ""),
                    "time": e.get("time", ""),
                    "paths": e.get("paths", []),
                    "tags": tags,
                    "hostname": e.get("hostname", ""),
                    "name": _snapshot_name(tags),
                    "has_configuration": snapshot_configuration.CONFIGURATION_TAG in tags,
                    "has_runtime": snapshot_configuration.RUNTIME_TAG in tags,
                    "capture_complete": snapshot_configuration.is_complete_capture(e),
                }
            )
        # Newest first
        out.sort(key=lambda x: x["time"], reverse=True)
        # Reconcile the history DB against reality: this listing just
        # succeeded, so any backups row whose snapshot isn't here (retention
        # forgot it, or it was deleted out of band) is stale. Reuses this
        # call's result — no extra restic invocation.
        _reconcile_snapshots_db({e["id"] for e in out if e.get("id")})
        return out, True
    except Exception:
        logger.exception("Failed to list snapshots")
        return [], False


async def repo_stats() -> tuple[dict | None, str | None]:
    """Return (stats, error) — how much space the restic repo is using.

    Uses ``restic stats --mode raw-data`` which reports the deduplicated /
    compressed on-disk footprint of the repository (this is the number that
    matters for S3 cost / local disk usage). Scopes to the ``bottle`` tag and
    the legacy ``openhost`` tag — the *total* app footprint across zones. We
    intentionally don't narrow to a single zone here: restic dedups blobs
    across all snapshots, so per-zone size attribution is ill-defined, and the
    cost-relevant number is the whole bottle footprint (which also naturally
    includes legacy snapshots).
    """
    try:
        conf = load_config()
        if not conf.get("repo") or not conf.get("repo_password"):
            return None, "Restic repo not configured"
        # Auto-init only for local repos (see list_snapshots for the rationale).
        init_err = (await ensure_repo_initialized(conf))[1]
        if init_err:
            return None, init_err
        rc, stdout, stderr = await _run_restic(
            ["stats", "--mode", "raw-data", "--json", *_restic_tag_args(), "--no-lock"],
            conf,
            timeout=60,
        )
        if rc != 0:
            return None, stderr.decode(errors="replace").strip() or f"restic exit {rc}"
        data = json.loads(stdout.decode(errors="replace") or "{}")
        # Returns stats on compressed binary blobs, not original content
        return {
            "total_size_bytes": data.get("total_size", 0),
            "total_uncompressed_size_bytes": data.get("total_uncompressed_size", 0),
            "total_blob_count": data.get("total_blob_count", 0),
            "snapshots_count": data.get("snapshots_count", 0),
            "compression_ratio": data.get("compression_ratio"),
        }, None
    except Exception as e:
        logger.exception("repo_stats failed")
        return None, str(e)


def validate_subpath(path: str) -> bool:
    if not isinstance(path, str) or "\x00" in path:
        return False
    if not path:
        return True
    for seg in path.split("/"):
        if seg in ("..", ".") or not seg:
            return False
    return True


# Named shortcuts for root-specific restores and API browsing.
_ROOT_NAMES = {
    "app_data": ALL_APP_DATA,
    "app_temp_data": APP_TEMP_DATA,
    "vm_data": VM_DATA_DIR,
}


async def _run_restic_streaming(
    args: list[str],
    conf: dict,
    timeout: float | None,
    on_line,
) -> tuple[int | None, bytes]:
    """Run ``restic <args>`` and feed each stdout line to ``on_line`` as it
    arrives, returning ``(returncode, stderr_bytes)``.

    Unlike ``_run_restic`` (which buffers all of stdout via ``communicate``),
    this reads stdout incrementally so a large ``restic ls`` doesn't
    materialise the whole listing in memory — the caller keeps only
    what it needs. stderr is drained concurrently to avoid a pipe-buffer
    deadlock, and the subprocess is killed on timeout/cancellation so we don't
    leak a restic process holding the repo lock.
    """
    env = _restic_env(conf)
    proc = await asyncio.create_subprocess_exec(
        "restic",
        *args,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=2**20,  # allow long JSON lines (default readline limit is 64K)
    )
    stderr_buf = bytearray()

    async def _drain_stderr() -> None:
        assert proc.stderr is not None
        while True:
            chunk = await proc.stderr.read(4096)
            if not chunk:
                return
            stderr_buf.extend(chunk)

    async def _pump_stdout() -> None:
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            on_line(line.decode(errors="replace"))
        await proc.wait()

    drain_task = asyncio.create_task(_drain_stderr())
    try:
        await asyncio.wait_for(_pump_stdout(), timeout=timeout)
        await asyncio.wait_for(drain_task, timeout=5)
    except BaseException:
        # Any failure — timeout, cancellation, or an on_line callback raising —
        # must still tear down the subprocess and drain task so we don't leak a
        # restic process holding the repo lock. Then re-raise unchanged.
        drain_task.cancel()
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await proc.wait()
        except Exception:
            pass
        raise
    return proc.returncode, bytes(stderr_buf)


async def list_snapshot_files(
    snapshot_id: str, subpath: str = "", root: str | None = None
):
    """List direct children anywhere in the snapshot's actual filesystem tree.

    Paths are relative to the snapshot's /, or to an optional named data root.
    Reads only the restic repository, never the live filesystem.
    """
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return [], "Restic repo not configured"

    if not validate_subpath(subpath):
        return [], "Invalid path"
    if root is not None and root not in _ROOT_NAMES:
        return [], f"Unknown root: {root}"

    target_path = str(_ROOT_NAMES[root]) if root else "/"
    if subpath:
        target_path = target_path.rstrip("/") + "/" + subpath

    args = ["ls", "--json", snapshot_id, target_path, "--no-lock"]

    files: list[dict] = []
    target_norm = target_path.rstrip("/")

    def _collect(raw: str) -> None:
        raw = raw.strip()
        if not raw:
            return
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if msg.get("struct_type") != "node":
            return
        path = msg.get("path", "")
        # Only immediate children of target_path.
        if not path.startswith(target_norm + "/"):
            # Could also be an exact match of the target (the dir itself) — skip.
            return
        rest = path[len(target_norm) + 1 :]
        if not rest or "/" in rest:
            return  # nested deeper, not a direct child
        files.append(
            {
                "path": rest,
                "size": msg.get("size", 0) or 0,
                "is_dir": msg.get("type") == "dir",
                "mod_time": msg.get("mtime", ""),
            }
        )

    # An explicit directory filter without --recursive bounds traversal to this
    # level. Stream the output so large directories don't duplicate the listing.
    try:
        rc, stderr = await _run_restic_streaming(args, conf, 120, _collect)
    except Exception as e:
        return [], f"restic error: {e}"

    if rc != 0:
        err = stderr.decode(errors="replace").strip()
        if "not found" in err.lower() or "no matching" in err.lower():
            return [], "Snapshot or path not found"
        return [], f"restic error: {err}"
    return files, None


async def list_snapshot_contents(snapshot_id: str):
    """Put backup contents side by side, retaining their actual browse paths."""
    entries, error = await list_snapshot_files(snapshot_id)
    if error:
        return [], error
    aliases = {
        "data/app_data": "app_data",
        "data/app_temp_data": "app_temp_data",
        "data/vm_data": "vm_data",
        "tmp/bottle-backup-configuration": "platform_configuration",
    }
    contents = []
    for entry in entries:
        path = entry["path"]
        if entry["is_dir"] and path in {"data", "tmp"}:
            children, error = await list_snapshot_files(snapshot_id, path)
            if error:
                return [], error
            if children:
                for child in children:
                    actual = path + "/" + child["path"]
                    label = aliases.get(actual, actual) if child["is_dir"] else actual
                    contents.append({**child, "path": label, "browse_path": actual})
                continue
        # Keep other captured paths and empty directories visible too.
        contents.append({**entry, "browse_path": path})
    labels = Counter(entry["path"] for entry in contents)
    for entry in contents:
        if labels[entry["path"]] > 1:
            entry["path"] = entry["browse_path"]
    return contents, None


async def delete_snapshot(snapshot_id: str) -> bool:
    """Remove a snapshot.

    Runs ``restic forget --prune`` so disk/object-store space is reclaimed
    immediately. Prune on a large repo can be slow (several minutes on an
    S3 repo with a lot of data) — we set a generous but bounded timeout so
    a wedged prune can't permanently hold the UI.

    Holds ``op_lock`` for its whole span (prune + DB cleanup + stats refresh)
    so a delete is a first-class operation: it shows in the status banner and
    mutually excludes backup/restore/migration. The route fires this via
    ``_spawn_background`` and returns immediately, so the prune runs in the
    background and the user tracks it through the banner.
    """
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return False

    err = op_lock.try_acquire(OpKind.DELETE)
    if err:
        logger.warning("Skipping delete: %s", err)
        return False
    try:
        try:
            rc, _out, stderr = await _run_restic(
                ["forget", "--prune", "--retry-lock", RETRY_LOCK, snapshot_id],
                conf,
                timeout=30 * 60,
            )
            if rc != 0:
                logger.warning(
                    "restic forget failed for %s: %s",
                    snapshot_id,
                    stderr.decode(errors="replace").strip(),
                )
                return False
        except Exception:
            logger.exception("restic forget failed")
            return False

        # DB cleanup. Snapshot IDs stored here are always the full 64-char IDs
        # that restic emits in its --json summary, so an exact match on the
        # user-supplied ID is sufficient when they pass a full ID. When they
        # pass a short (8-char) ID, match by prefix with length >= 8 to avoid
        # accidental matches on arbitrary substrings.
        conn = get_db()
        try:
            if len(snapshot_id) >= 40:
                conn.execute(
                    "DELETE FROM backups WHERE snapshot_id = ?", (snapshot_id,)
                )
            else:
                conn.execute(
                    "DELETE FROM backups WHERE substr(snapshot_id, 1, ?) = ?",
                    (len(snapshot_id), snapshot_id),
                )
            conn.commit()
        except sqlite3.Error:
            # The restic forget already succeeded; don't fail the operation.
            logger.exception("DB cleanup failed for snapshot %s", snapshot_id)
        finally:
            conn.close()

        # The prune reclaimed space, so the cached repo size is now stale.
        # Awaited inline (we hold the lock and the request has already
        # returned) so the banner stays up until the size is current.
        await _refresh_repo_stats_cache()

        logger.info("Deleted snapshot %s", snapshot_id)
        return True
    finally:
        op_lock.release(OpKind.DELETE)


async def _refresh_repo_stats_cache() -> None:
    """Recompute the repo footprint and re-stamp it onto the newest backup row.

    Runs as a background task after a delete/prune so the expensive
    ``restic stats`` read stays off the request path. Best-effort: on any
    failure (stats unavailable, no rows to stamp, DB error) the cache simply
    isn't refreshed and readers fall back to the previous stamped row.

    Unlike a backup (which folds its stats into its own INSERT), a delete has
    no row of its own, so we re-stamp the newest *surviving* row via MAX(id).
    """
    repo_stats_data = (await repo_stats())[0]
    if repo_stats_data is None:
        return
    conn = get_db()
    try:
        conn.execute(
            "UPDATE backups SET repo_size_bytes = ?, repo_uncompressed_bytes = ?, "
            "repo_blob_count = ?, repo_snapshots_count = ?, "
            "repo_compression_ratio = ?, repo_stats_at = ? "
            "WHERE id = (SELECT MAX(id) FROM backups)",
            (
                repo_stats_data.get("total_size_bytes"),
                repo_stats_data.get("total_uncompressed_size_bytes"),
                repo_stats_data.get("total_blob_count"),
                repo_stats_data.get("snapshots_count"),
                repo_stats_data.get("compression_ratio"),
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
        )
        conn.commit()
    except sqlite3.Error:
        logger.exception("Failed to re-stamp repo stats cache after delete")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Retention (restic forget) + background prune
# ---------------------------------------------------------------------------


def _forget_args(conf: dict) -> list[str] | None:
    """Build ``restic forget`` args from the configured keep-* policy.

    Returns None when no tier is set — meaning "keep everything", so no
    forget runs. This is also the safety floor: we never issue a forget with
    zero keep flags, which would delete every snapshot. ``--prune`` is
    intentionally omitted — it runs in the background afterwards (see
    ``schedule_prune``).

    Scoping: ``--tag bottle --tag openhost`` selects our snapshots (OR) and
    ``--group-by ''`` (empty) treats them all as ONE group so the policy
    applies across the whole set. We assume a single instance per repo, so no
    per-host/paths grouping is needed — and grouping would only fragment
    retention (paths vary when a root like vm_data is absent; host varied on
    older snapshots before we began pinning ``--host BACKUP_HOST``). Backups
    are pinned to the zone host from now on for a stable snapshot identity.

    Values are stored as validated ints by ``post_config`` and seeded by
    ``DEFAULT_CONFIG``, so we can read them directly.
    """
    keeps: list[str] = []
    for key, flag in KEEP_FLAGS.items():
        if conf.get(key):
            keeps += [flag, str(conf[key])]
    if not keeps:
        return None
    return [
        "forget",
        "--json",
        "--retry-lock",
        RETRY_LOCK,
        *_restic_tag_args(),
        "--group-by",
        "",
        *keeps,
    ]


def _reconcile_snapshots_db(present_ids: set[str]) -> None:
    """Drop history rows whose snapshot no longer exists in the repo.

    The backstop that keeps the ``backups`` table a subset of restic reality
    — covers retention's forgotten snapshots plus anything deleted out of
    band. Rows with no ``snapshot_id`` (successful backups whose summary was
    missing) are left alone. Only call with the ids from a *successful*
    listing: an empty set from a failed list would wipe every row.
    """
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, snapshot_id FROM backups WHERE snapshot_id IS NOT NULL"
        ).fetchall()
        stale = [(r[0],) for r in rows if r[1] not in present_ids]
        if stale:
            conn.executemany("DELETE FROM backups WHERE id = ?", stale)
            conn.commit()
            logger.info("Reconciled DB: removed %d stale backup row(s)", len(stale))
    except sqlite3.Error:
        logger.exception("Snapshot DB reconcile failed")
    finally:
        conn.close()


async def run_retention(conf: dict) -> int:
    """Apply the keep-* policy via ``restic forget`` and reconcile the DB.

    Returns the number of snapshots forgotten. Prune (the expensive step
    that reclaims space) is deliberately NOT run here — the caller schedules
    it in the background only when this returns > 0. Must be called while
    holding the operation lock.
    """
    args = _forget_args(conf)
    if args is None:
        return 0
    try:
        rc, stdout, stderr = await _run_restic(
            args, conf, timeout=FORGET_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        logger.error("restic forget timed out after %ss", FORGET_TIMEOUT_SECONDS)
        return 0
    if rc != 0:
        logger.error(
            "restic forget failed: %s", stderr.decode(errors="replace").strip()
        )
        return 0

    # forget --json returns one object per group, each with a "remove" list of
    # snapshot objects (absent/empty when nothing was removed).
    removed_ids: list[str] = []
    try:
        for group in json.loads(stdout.decode(errors="replace") or "[]"):
            for snap in group.get("remove") or []:
                sid = snap.get("id")
                if sid:
                    removed_ids.append(sid)
    except (json.JSONDecodeError, AttributeError):
        logger.warning("Could not parse restic forget --json output")

    if removed_ids:
        conn = get_db()
        try:
            conn.executemany(
                "DELETE FROM backups WHERE snapshot_id = ?",
                [(sid,) for sid in removed_ids],
            )
            conn.commit()
        except sqlite3.Error:
            logger.exception("DB cleanup after forget failed")
        finally:
            conn.close()
        logger.info("Retention forgot %d snapshot(s)", len(removed_ids))
    return len(removed_ids)


# Background prune coordination. restic prune reclaims the space freed by
# forget; it's slow (repacks pack files) so we run it off the backup path as a
# single coalesced worker. _prune_needed lets a forget that lands while a prune
# is already running request a follow-up pass.
_prune_needed = False
_prune_task: "asyncio.Task | None" = None


def schedule_prune() -> None:
    """Request a background prune. Coalesces: at most one worker runs at once."""
    global _prune_needed, _prune_task
    _prune_needed = True
    if _prune_task is None or _prune_task.done():
        _prune_task = asyncio.create_task(_prune_worker())


async def _prune_worker() -> None:
    global _prune_needed
    while _prune_needed:
        _prune_needed = False
        # A prune must not run concurrently with a backup/restore/migration —
        # restic takes an exclusive repo lock — so wait for the operation lock.
        waited = 0.0
        while op_lock.try_acquire(OpKind.PRUNE) is not None:
            if waited >= PRUNE_TIMEOUT_SECONDS:
                logger.warning("Prune gave up waiting for the operation lock")
                _prune_needed = True  # retry on the next schedule_prune
                return
            await asyncio.sleep(5)
            waited += 5
        try:
            await _run_prune_locked()
        finally:
            op_lock.release(OpKind.PRUNE)


async def _run_prune_locked() -> None:
    """Run ``restic prune`` and re-stamp the repo-size cache. Lock held by caller."""
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return
    try:
        rc, _out, stderr = await _run_restic(
            ["prune", "--retry-lock", RETRY_LOCK], conf, timeout=PRUNE_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        logger.error("restic prune timed out after %ss", PRUNE_TIMEOUT_SECONDS)
        return
    if rc != 0:
        logger.error("restic prune failed: %s", stderr.decode(errors="replace").strip())
        return
    logger.info("Prune completed")
    # Prune reclaimed space, so the cached repo size is stale — recompute and
    # re-stamp the newest surviving backup row.
    stats = (await repo_stats())[0]
    if stats is not None:
        conn = get_db()
        try:
            conn.execute(
                "UPDATE backups SET repo_size_bytes = ?, repo_uncompressed_bytes = ?, "
                "repo_blob_count = ?, repo_snapshots_count = ?, "
                "repo_compression_ratio = ?, repo_stats_at = ? "
                "WHERE id = (SELECT MAX(id) FROM backups)",
                (
                    stats.get("total_size_bytes"),
                    stats.get("total_uncompressed_size_bytes"),
                    stats.get("total_blob_count"),
                    stats.get("snapshots_count"),
                    stats.get("compression_ratio"),
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            )
            conn.commit()
        except sqlite3.Error:
            logger.exception("Failed to re-stamp repo stats after prune")
        finally:
            conn.close()


def get_backup_history(limit=20, offset=0):
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) FROM backups").fetchone()[0]
        rows = conn.execute(
            "SELECT id, timestamp, status, error_message, created_at, snapshot_id, "
            "data_added_bytes, total_size_bytes, file_count, name "
            "FROM backups ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        history = [
            {
                "id": r[0],
                "timestamp": r[1],
                "status": r[2],
                "error_message": r[3],
                "created_at": r[4],
                "snapshot_id": r[5],
                "data_added_bytes": r[6],
                "total_size_bytes": r[7],
                "file_count": r[8],
                "name": r[9],
            }
            for r in rows
        ]
        return history, total
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------


def _restore_journal_path() -> Path:
    return APP_DATA_DIR / ".recovery" / "restore-state.json"


def _checkpoint_restore(phase: str, *, needs_attention: bool = False) -> None:
    global restore_progress, _restore_needs_attention
    restore_progress = {
        **(restore_progress or {}),
        "phase": phase,
        "needs_attention": needs_attention,
        "recovery": _restore_session.progress if _restore_session else None,
    }
    _restore_needs_attention = needs_attention
    snapshot_configuration.save_journal(_restore_journal_path(), restore_progress)


def _pending_restart_records(saved: dict, *, infer_interrupted: bool = False) -> list[dict]:
    records = {}

    def add(name, app_id):
        if (
            type(name) is not str or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", name)
            or type(app_id) is not str or not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{12}", app_id)
        ):
            raise ValueError
        records[name, app_id] = {"name": name, "app_id": app_id}

    pending = saved.get("pending_restarts", [])
    if type(pending) is not list:
        raise ValueError
    for entry in pending:
        if type(entry) is not dict or set(entry) != {"name", "app_id"}:
            raise ValueError
        add(entry["name"], entry["app_id"])
    recovery = saved.get("recovery") or {}
    if type(recovery) is not dict:
        raise ValueError
    if infer_interrupted:
        if saved.get("phase") == "stopping":
            selected = set(saved.get("affected_apps", []))
            for name, entry in _inventory(recovery.get("destination_apps_before", [])).items():
                if name not in selected and name != APP_NAME and entry["status"] == "running":
                    add(name, entry["app_id"])
        paused = recovery.get("paused_apps", [])
        if type(paused) is not list:
            raise ValueError
        for entry in paused:
            if type(entry) is not dict:
                raise ValueError
            if entry.get("selected") is False and entry.get("previous_status") == "running" and entry.get("restart") != "confirmed":
                add(entry.get("name"), entry.get("app_id"))
    return list(records.values())


def _load_restore_journal() -> None:
    global restore_progress, restore_last_status, restore_last_snapshot, _restore_needs_attention
    path = _restore_journal_path()
    if not path.exists():
        return
    try:
        if path.is_symlink() or path.stat().st_size > 8 * 1024 * 1024:
            raise ValueError
        saved = json.loads(path.read_text(encoding="utf-8"))
        if (
            type(saved) is not dict or saved.get("journal_version") != 1
            or type(saved.get("snapshot")) is not str
            or not SNAPSHOT_ID_RE.fullmatch(saved["snapshot"])
            or type(saved.get("job_id")) is not str
            or not re.fullmatch(r"[a-f0-9]{32}", saved["job_id"])
            or type(saved.get("phase")) is not str
            or type(saved.get("affected_apps", [])) is not list
            or any(type(name) is not str or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", name) for name in saved.get("affected_apps", []))
            or type(saved.get("affected_roots", [])) is not list
            or any(root not in {"app_data", "app_temp_data"} for root in saved.get("affected_roots", []))
        ):
            raise ValueError
        retained = saved.get("retained_stages", [])
        if type(retained) is not list or any(
            type(entry) is not dict or set(entry) != {"root", "job_id"}
            or type(entry.get("root")) is not str or entry["root"] not in {"app_data", "app_temp_data"}
            or type(entry.get("job_id")) is not str or not re.fullmatch(r"[a-f0-9]{32}", entry["job_id"])
            for entry in retained
        ):
            raise ValueError
        active = saved["phase"] not in {"complete", "incomplete", "error", "acknowledged", "interrupted"}
        pending_restarts = _pending_restart_records(saved, infer_interrupted=active)
        recovery = journal_progress(saved.get("recovery"))
        _restore_needs_attention = bool(saved.get("needs_attention")) or active or bool(pending_restarts)
        restore_progress = {
            "journal_version": 1, "snapshot": saved["snapshot"], "job_id": saved["job_id"],
            "phase": "interrupted" if active else saved["phase"],
            "needs_attention": _restore_needs_attention, "recovery": recovery,
            "affected_apps": saved.get("affected_apps", []),
            "affected_roots": saved.get("affected_roots", []),
            "retained_stages": retained,
            "pending_restarts": pending_restarts,
        }
        if _restore_needs_attention:
            restore_last_status = "Recovery was interrupted or incomplete. Check the router and retry the snapshot."
        elif saved["phase"] == "complete":
            restore_last_status, restore_last_snapshot = "success", saved["snapshot"]
    except (OSError, ValueError, TypeError, UnicodeError):
        _restore_needs_attention = True
        restore_last_status = "The recovery journal could not be read. Check recovery state before running another backup."


async def _snapshot_for_restore(snapshot_id: str, conf: dict) -> snapshot_configuration.Snapshot:
    rc, output, _ = await _run_restic(
        ["snapshots", "--json", snapshot_id, "--no-lock"], conf, timeout=60
    )
    if rc != 0:
        raise snapshot_configuration.SnapshotConfigurationError("Could not read the selected snapshot.")
    return snapshot_configuration.snapshot_metadata(snapshot_id, output)


async def _snapshot_needs_owner(snapshot_id: str) -> bool:
    """Whether a whole-snapshot restore of this snapshot needs owner authority.

    A lookup failure answers False: ``run_restore`` re-reads the metadata and
    still refuses to apply configuration without authority, so an unreadable
    repository here can only cost a file-only restore, never a bypass.
    """
    try:
        snapshot = await _snapshot_for_restore(snapshot_id, load_config())
    except Exception:
        return False
    return snapshot.has_configuration


async def _restore_configuration_snapshot(snapshot: snapshot_configuration.Snapshot, conf: dict, owner_token: str, *, session=None) -> bool:
    global restore_progress, restore_last_status, restore_last_snapshot, _restore_session
    if not owner_token:
        raise snapshot_configuration.SnapshotConfigurationError(
            "Configuration recovery requires owner authorization from the caller."
        )
    if not snapshot.capture_complete:
        raise snapshot_configuration.SnapshotConfigurationError(
            "This snapshot has no confirmed complete capture. Use file-only recovery or select a completed backup."
        )
    allowed = {str(path) for path in BACKUP_ROOTS} | {str(snapshot_configuration.CONFIGURATION_FILE)}
    if any(path not in allowed for path in snapshot.paths):
        raise snapshot_configuration.SnapshotConfigurationError("This snapshot contains unsupported data roots.")
    bundle = await snapshot_configuration.read_configuration(snapshot.id, _restic_env(conf))
    if snapshot.has_runtime != (bundle["runtime"] is not None):
        raise snapshot_configuration.SnapshotConfigurationError("The configuration does not match the snapshot's runtime metadata tag.")
    preflighted = session is not None
    session = session or RecoverySession(ROUTER_URL, owner_token, bundle, APP_NAME)
    previous_attention = _restore_needs_attention
    previous_apps = list((restore_progress or {}).get("affected_apps", [])) if previous_attention else []
    previous_roots = list((restore_progress or {}).get("affected_roots", [])) if previous_attention else []
    previous_stages = list((restore_progress or {}).get("retained_stages", []))
    previous_restarts = list((restore_progress or {}).get("pending_restarts", [])) if previous_attention else []
    captured_roots = {name for name, path in _ROOT_NAMES.items() if str(path) in snapshot.paths}
    if previous_attention and (
        not restore_progress or "affected_apps" not in restore_progress
        or "affected_roots" not in restore_progress
        or not set(previous_apps) <= set(session.restore_app_names)
        or not set(previous_roots) <= captured_roots
    ):
        raise snapshot_configuration.SnapshotConfigurationError(
            "Retry a snapshot covering the interrupted apps, or inspect and acknowledge the previous recovery."
        )
    _restore_session = session
    job_id = uuid.uuid4().hex
    restore_progress = {
        "journal_version": 1, "job_id": job_id, "snapshot": snapshot.id,
        "affected_apps": previous_apps if previous_attention else [],
        "affected_roots": previous_roots if previous_attention else [],
        "retained_stages": previous_stages,
        "pending_restarts": previous_restarts,
    }
    stages: dict[Path, Path] = {}
    rollbacks: dict[Path, Path] = {}
    modifying = False
    data_promoted = False
    cleanup_failed = False
    committed = False

    async def finalize() -> None:
        nonlocal cleanup_failed, committed
        global _restore_session, _restore_needs_attention
        needs_attention = previous_attention
        try:
            try:
                await session.restart_unaffected()
            except Exception:
                # The recovery is reported incomplete either way; the paused-app
                # journal is what an operator reads, so the cause belongs here.
                logger.exception("Could not confirm unaffected-app cleanup after recovery")
                cleanup_failed = True
            pending = _pending_restart_records(
                {"pending_restarts": previous_restarts, "recovery": session.progress}, infer_interrupted=True
            )
            if pending:
                try:
                    inventory = _inventory(await RouterClient(ROUTER_URL, owner_token).get("/api/apps"))
                    recovered = {app["name"]: app for app in session.summary.get("apps", []) if app.get("ok")}
                    pending = [entry for entry in pending if not (
                        entry["name"] in inventory and inventory[entry["name"]]["app_id"] == entry["app_id"]
                        and (inventory[entry["name"]]["status"] == "running" or (
                            entry["name"] in recovered
                            and recovered[entry["name"]].get("app_id") == entry["app_id"]
                            and recovered[entry["name"]].get("status") == inventory[entry["name"]]["status"]
                        ))
                    )]
                except ConfigurationError:
                    # An unreachable router cannot confirm these restarts, so
                    # they stay pending and the recovery needs attention. The
                    # conservative outcome is kept, but silently so would look
                    # like apps had restarted.
                    logger.exception("Could not confirm pending restarts against the router; they stay pending")
            restore_progress["pending_restarts"] = pending
            cleanup_failed = cleanup_failed or bool(pending)
            success = data_promoted and session.summary["ok"] and not cleanup_failed
            if success:
                # Every promoted byte must reach stable storage before the
                # journal may claim this recovery is complete.
                try:
                    await drain(asyncio.to_thread(migration_data.durability_barrier, *(stages or [APP_DATA_DIR])))
                except Exception:
                    logger.error("Recovery durability barrier failed", exc_info=True)
                    cleanup_failed = True
                    success = False
            if success:
                try:
                    _checkpoint_restore("complete", needs_attention=False)
                    committed = True
                except Exception:
                    # Not durably committed, so the promoted originals stay
                    # recoverable under their stages.
                    logger.error("Recovery progress could not be persisted")
                    cleanup_failed = True
            needs_attention = False if committed else (
                previous_attention or modifying or cleanup_failed
                or bool(restore_progress.get("pending_restarts"))
            )
            if not committed and not needs_attention:
                # Nothing was modified and no earlier job needs review, so a
                # failed pre-mutation checkpoint must not block every future
                # backup. Record the error without demanding attention.
                try:
                    _checkpoint_restore("error", needs_attention=False)
                except Exception:
                    logger.error("Recovery progress could not be persisted")
            for data_root, stage in stages.items():
                record = {"root": next(name for name, path in _ROOT_NAMES.items() if path == data_root), "job_id": job_id}
                if committed:
                    # Post-commit disposal is garbage collection only. An
                    # already committed recovery stays successful, and any
                    # failure keeps the stage and its retained record.
                    try:
                        rollback = rollbacks.get(data_root)
                        if rollback is not None:
                            await migration_data.discard_app_trees(rollback)
                        await asyncio.to_thread(shutil.rmtree, stage)
                    except Exception:
                        logger.warning("Retaining staged originals after committed recovery", exc_info=True)
                        if record not in restore_progress["retained_stages"]:
                            restore_progress["retained_stages"].append(record)
                        continue
                    if record in restore_progress["retained_stages"]:
                        restore_progress["retained_stages"].remove(record)
                elif modifying:
                    # Failed rollback may retain the only original data under
                    # this stage. Never discard it before a durable commit.
                    if record not in restore_progress["retained_stages"]:
                        restore_progress["retained_stages"].append(record)
                else:
                    # Nothing was promoted, so the stage holds only a verified
                    # download that a retry will recreate.
                    try:
                        await asyncio.to_thread(shutil.rmtree, stage)
                    except Exception:
                        if record not in restore_progress["retained_stages"]:
                            restore_progress["retained_stages"].append(record)
                    else:
                        if record in restore_progress["retained_stages"]:
                            restore_progress["retained_stages"].remove(record)
            _restore_needs_attention = needs_attention
        finally:
            final_phase = "complete" if committed else ("incomplete" if needs_attention else "error")
            try:
                _checkpoint_restore(final_phase, needs_attention=needs_attention)
            except Exception:
                logger.error("Final recovery progress could not be persisted", exc_info=True)
                # After the durable complete checkpoint, this publication only
                # records garbage collection. Its failure cannot undo the commit
                # or tell the source that a confirmed migration is incomplete.
                if not committed and (needs_attention or modifying):
                    _restore_needs_attention = True
            _restore_session = None
    try:
        _checkpoint_restore("preflight", needs_attention=previous_attention)
        if not preflighted:
            await session.preflight()
        _checkpoint_restore("staging", needs_attention=previous_attention)
        # Download/verify before stopping any application. Each staging tree is
        # on the destination filesystem so the final directory promotion is an
        # atomic rename, including when persistent and temporary mounts differ.
        for data_root in (ALL_APP_DATA, APP_TEMP_DATA):
            if str(data_root) not in snapshot.paths:
                continue
            if not data_root.is_dir() or data_root.is_symlink():
                raise snapshot_configuration.SnapshotConfigurationError("A destination app-data root is unavailable.")
            parent = data_root / RESTORE_WORK_NAME
            snapshot_configuration._private_directory(parent)
            stage = parent / job_id
            stage.mkdir(mode=0o700)
            stages[data_root] = stage
            rc, _, _ = await _run_restic(
                ["restore", "--retry-lock", RETRY_LOCK, snapshot.id,
                 "--target", str(stage), "--include", str(data_root), "--verify"],
                conf, timeout=RESTORE_TIMEOUT_SECONDS,
            )
            payload = stage / str(data_root).lstrip("/")
            if rc != 0 or not payload.is_dir() or payload.is_symlink():
                raise snapshot_configuration.SnapshotConfigurationError("App data could not be fully staged and verified.")
            # A missing app directory is explicit empty data in a captured root.
            for name in session.restore_app_names:
                candidate = payload / name
                if candidate.is_symlink() or (candidate.exists() and not candidate.is_dir()):
                    raise snapshot_configuration.SnapshotConfigurationError("A snapshot app-data root is not a directory.")
                candidate.mkdir(exist_ok=True)
            if data_root == ALL_APP_DATA:
                captured = {entry.name for entry in payload.iterdir()
                            if entry.is_dir() and not entry.is_symlink()}
                session.note_omitted_data(captured - set(session.restore_app_names))
        restore_progress["affected_apps"] = sorted(set(previous_apps) | set(session.restore_app_names))
        restore_progress["affected_roots"] = sorted(set(previous_roots) | captured_roots)
        _checkpoint_restore("stopping", needs_attention=True)
        modifying = True
        await session.stop_apps()
        # Persist the location before promotion can move original directories
        # into rollback storage. A process crash cannot erase this breadcrumb.
        for data_root in stages:
            record = {"root": next(name for name, path in _ROOT_NAMES.items() if path == data_root), "job_id": job_id}
            if record not in restore_progress["retained_stages"]:
                restore_progress["retained_stages"].append(record)
        _checkpoint_restore("restoring_data", needs_attention=True)
        for data_root, stage in stages.items():
            payload = stage / str(data_root).lstrip("/")
            # Keep the rollback token for every promoted root until the whole
            # recovery is durably committed; a later root can still fail.
            rollbacks[data_root] = await migration_data.replace_app_trees(
                payload, data_root, session.restore_app_names
            )
        data_promoted = True
        _checkpoint_restore("activating", needs_attention=True)
        await session.activate()
    finally:
        await drain(finalize())
    result = session.summary
    if result["ok"] and not cleanup_failed and committed:
        restore_last_snapshot, restore_last_status = snapshot.id, "success"
        return True
    restore_last_status = "Recovery is incomplete. Review the app results before retrying or acknowledging it."
    return False


async def run_restore(snapshot_id: str, root: str | None = None, owner_token: str | None = None, *, lock_acquired: bool = False) -> bool:
    """Restore a snapshot.

    With ``root`` None a snapshot that carries configuration is recovered
    through the owner-only configuration path, which needs ``owner_token``
    confirmed by the router. A configuration snapshot requested with a named
    root uses file-only restore, applying no definitions, API keys or app state.
    Whole-snapshot configuration recovery refuses callers without owner authority.

    A named root (``app_data``, ``app_temp_data``, or the legacy ``vm_data``)
    is selected with restic ``--exclude`` filters for the other captured
    paths, never ``--include``, so the backup executor and its repository stay
    protected in every mode. ``vm_data`` is only ever present in snapshots
    taken by a version that captured it.
    """
    global restore_last_snapshot, restore_last_status, restore_progress

    if lock_acquired and op_lock.active != OpKind.RESTORE:
        raise RuntimeError("Restore operation ownership was not reserved")
    if not lock_acquired:
        await _reclaim_abandoned_migration()
    err = None if lock_acquired else op_lock.try_acquire(OpKind.RESTORE)
    if err:
        logger.warning("Skipping restore: %s", err)
        return False

    try:
        conf = load_config()
    except Exception:
        logger.exception("Failed to load restore config")
        op_lock.release(OpKind.RESTORE)
        return False

    if not conf.get("repo") or not conf.get("repo_password"):
        restore_last_status = "error: restic repo not configured"
        op_lock.release(OpKind.RESTORE)
        return False

    if not SNAPSHOT_ID_RE.match(snapshot_id):
        restore_last_status = "error: invalid snapshot id"
        op_lock.release(OpKind.RESTORE)
        return False

    if root is not None and root not in _ROOT_NAMES:
        restore_last_status = f"error: unknown root '{root}'"
        op_lock.release(OpKind.RESTORE)
        return False

    logger.info("Starting restic restore from %s (root=%s)", snapshot_id, root or "all")
    restore_last_snapshot, restore_last_status = None, None

    try:
        snapshot = await _snapshot_for_restore(snapshot_id, conf)
        if root is None and snapshot.has_configuration:
            if not owner_token:
                # Reached only when a caller without owner authority asked for
                # a configuration snapshot. Refuse before reading the bundle.
                raise snapshot_configuration.SnapshotConfigurationError(
                    "Configuration recovery requires owner authorization from the caller."
                )
            return await _restore_configuration_snapshot(snapshot, conf, owner_token)
        if _restore_needs_attention:
            raise snapshot_configuration.SnapshotConfigurationError("Retry the interrupted full recovery before restoring individual roots.")
        restore_progress = {
            **(restore_progress or {}),
            "phase": "files_only", "has_configuration": snapshot.has_configuration,
            "snapshot": snapshot.id, "recovery": None,
            "warnings": ["File-only restore: app definitions, API keys, and application state are not applied."],
        }
        args = [
            "restore",
            "--retry-lock",
            RETRY_LOCK,
            snapshot.id,
            "--target",
            "/",  # restic restores the absolute paths as they were captured
        ]
        # Use exclusions only, including for a single selected root, so the
        # backup executor and its repository stay protected in every mode.
        if root:
            if str(_ROOT_NAMES[root]) not in snapshot.paths:
                raise snapshot_configuration.SnapshotConfigurationError("The selected root was not captured in this snapshot.")
            for path in snapshot.paths:
                if path != str(_ROOT_NAMES[root]):
                    args += ["--exclude", path]
        for ex in (*BACKUP_EXCLUDES, APP_DATA_DIR, snapshot_configuration.CONFIGURATION_FILE):
            args += ["--exclude", str(ex)]
        try:
            rc, _stdout, stderr = await _run_restic(
                args, conf, timeout=RESTORE_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            restore_last_status = (
                f"error: restore timed out after {RESTORE_TIMEOUT_SECONDS}s"
            )
            logger.error(restore_last_status)
            return False

        if rc == 0:
            restore_last_snapshot = snapshot_id
            restore_last_status = "success"
            logger.info("Restore completed successfully")
        else:
            restore_last_status = f"error: {stderr.decode(errors='replace').strip() or f'restic exit {rc}'}"
            logger.error("Restore failed: %s", restore_last_status)
    except (ConfigurationError, snapshot_configuration.SnapshotConfigurationError) as e:
        restore_last_status = f"error: {e}"
        logger.error("Recovery failed: %s", e)
    except asyncio.CancelledError:
        restore_last_status = "Recovery was interrupted. Check the router and retry the snapshot."
        raise
    except Exception as e:
        restore_last_status = "error: recovery failed; inspect the backup and router status before retrying"
        # Only the exception type: restic and router messages can carry
        # repository paths, credentials or captured data.
        logger.error("Restore failed (%s)", type(e).__name__)
    finally:
        op_lock.release(OpKind.RESTORE)

    return restore_last_status == "success"


# ---------------------------------------------------------------------------
# Check (repo integrity)
# ---------------------------------------------------------------------------


async def run_check() -> bool:
    """Run `restic check`. Updates module-level state.

    The integrity scan uses ``--no-lock`` and does not claim ``op_lock``.
    The /api/check route rejects a start while op_lock is busy or
    ``check_running`` is set, but operations started after the check begins
    can overlap the scan.
    """
    global check_last_status, check_last_output, check_last_at, check_running
    # Set the flag inside the try so that any exception from load_config /
    # _run_restic still runs the finally clause that clears it. Without
    # this, a corrupt config.json would leave check_running=True forever.
    try:
        check_running = True
        conf = load_config()
        if not conf.get("repo") or not conf.get("repo_password"):
            check_last_status = "error"
            check_last_output = "Restic repo not configured"
            check_last_at = datetime.now(timezone.utc).isoformat()
            return False
        # Auto-init only for local repos so a fresh-install user clicking
        # "Run check" doesn't see a confusing "unable to open config file"
        # error on a repo that simply hasn't been backed up yet.  Remote
        # repos still error here so we don't silently create them.
        init_err = (await ensure_repo_initialized(conf))[1]
        if init_err:
            check_last_status = "error"
            check_last_output = init_err
            check_last_at = datetime.now(timezone.utc).isoformat()
            logger.info("run_check: %s", init_err)
            return False
        try:
            rc, stdout, stderr = await _run_restic(
                # No repo lock or op_lock is held during this scan. The route
                # gates only its start; later operations can modify the repo
                # while the check is still running.
                ["check", "--no-lock"], conf, timeout=CHECK_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            check_last_status = "error"
            check_last_output = f"restic check timed out after {CHECK_TIMEOUT_SECONDS}s"
            check_last_at = datetime.now(timezone.utc).isoformat()
            logger.error(check_last_output)
            return False
        output = (
            stdout.decode(errors="replace") + stderr.decode(errors="replace")
        ).strip()
        check_last_output = output[-4000:]  # cap
        check_last_at = datetime.now(timezone.utc).isoformat()
        if rc == 0:
            check_last_status = "ok"
            logger.info("restic check ok")
            return True
        check_last_status = "error"
        logger.error("restic check failed: %s", output)
        return False
    except Exception as e:
        check_last_status = "error"
        check_last_output = str(e)
        check_last_at = datetime.now(timezone.utc).isoformat()
        logger.exception("restic check failed")
        return False
    finally:
        check_running = False


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


async def scheduler_loop():
    first_run = True
    while True:
        try:
            conf = load_config()
            interval = conf["interval_seconds"]
        except Exception:
            # A scheduler that exits on a transient read error never runs
            # another automatic backup, so keep the loop alive instead.
            logger.exception("Could not read the backup schedule")
            await asyncio.sleep(30)
            continue

        if not interval or not conf.get("repo"):
            await asyncio.sleep(30)
            continue

        if first_run:
            first_run = False
            last = get_last_backup()
            if last and last["timestamp"]:
                try:
                    last_dt = datetime.strptime(
                        last["timestamp"], "%Y-%m-%dT%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                    elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
                    wait = max(0, interval - elapsed)
                except (ValueError, TypeError):
                    wait = interval
            else:
                wait = interval
        else:
            wait = interval

        logger.info("Next backup in %d seconds", int(wait))
        await asyncio.sleep(wait)
        try:
            await run_backup()
        except Exception:
            # Only cancellation may leave this loop; a failed backup retries.
            logger.exception("Scheduled backup failed unexpectedly")


def ensure_default_config():
    """Make sure config.json exists with default values.

    Does not auto-generate a password or set a repo — the user configures
    those through the UI. Backups won't run until configured.
    """
    if not CONFIG_FILE.exists():
        save_config(load_config())


@app.before_serving
async def startup():
    global scheduler_task
    init_db()
    ensure_default_config()
    snapshot_configuration.clear_configuration_file()
    _load_restore_journal()
    # Outgoing cutover intent must be visible before any capture can run, so an
    # interrupted migration blocks new work from the first request. Without that
    # record a capture could copy half-migrated data, so fail closed.
    try:
        migration.initialize_source_recovery(
            lock=op_lock, all_app_data=ALL_APP_DATA,
            work_dir=APP_DATA_DIR / ".migration", router_url=ROUTER_URL,
            backup_app_name=APP_NAME,
        )
    except (migration.MigrationError, migration_data.DataError) as error:
        # The record is what makes an interrupted cutover visible, so an
        # unusable private directory must be named rather than surfacing as an
        # opaque data error from deep in the staging helpers.
        logger.error(
            "Outgoing migration state unavailable in %s; refusing to start because a "
            "capture could copy half-migrated data. That directory must exist, be a "
            "real directory, and be owned by this app's user (mode 0700): %s",
            APP_DATA_DIR / ".migration", error,
        )
        raise
    # Push op_lock transitions to connected SSE clients so the status banner
    # updates the instant an operation starts or finishes.
    op_lock.set_on_change(_notify_status_change)
    # Best-effort unlock in case a previous run died mid-operation.
    try:
        conf = load_config()
        if conf.get("repo") and conf.get("repo_password"):
            await _restic_unlock_if_stale(conf)
    except Exception:
        logger.warning("startup unlock skipped", exc_info=True)
    scheduler_task = asyncio.create_task(scheduler_loop())
    logger.info("Backup scheduler started")


@app.after_serving
async def shutdown():
    if scheduler_task:
        scheduler_task.cancel()


# ---------------------------------------------------------------------------
# Route helper
# ---------------------------------------------------------------------------


def route(path, **kwargs):
    """Register a route at both /path and BASE_PATH/path to handle proxies."""

    def decorator(func):
        app.route(path, **kwargs)(func)
        if BASE_PATH and BASE_PATH != "/":
            prefixed = BASE_PATH.rstrip("/") + path
            app.route(prefixed, **kwargs)(func)
        return func

    return decorator


# ---------------------------------------------------------------------------
# Backup / restore routes
# ---------------------------------------------------------------------------


@route("/")
async def index():
    conf = load_config()
    last = get_last_backup()
    state = {
        "running": op_lock.backup_running,
        "last_backup": last["timestamp"] if last else None,
        "last_status": last["status"] if last else None,
        "last_error": last["error_message"] if last else None,
    }
    backend = classify_repo(conf.get("repo", ""))
    env_pairs = conf.get("env") or {}
    # Render env as "KEY=val;KEY2=val2" — same shape the input accepts on save.
    env_string = ";".join(f"{k}={v}" for k, v in env_pairs.items())
    return await render_template(
        "index.html",
        base_path=BASE_PATH,
        config=conf,
        env_string=env_string,
        state=state,
        backend=backend,
        scope=_backup_scope_summary(),
        app_name=APP_NAME,
    )


def _backup_scope_summary() -> dict:
    """Snapshot the scope of what backup currently captures and skips.

    Surfaced in the UI so the user can tell, at a glance, that
    ``/data/app_archive`` is intentionally outside the snapshot —
    important because access_all_app_data mounts the archive into the
    backup container and the file-browser path can otherwise leave
    the impression that those bytes will be in the next snapshot.

    Built off the same ``BACKUP_ROOTS`` / ``BACKUP_EXCLUDES`` tuples
    that the backup + restore code paths use, so the UI can never
    drift from the actual restic command line.  Each entry carries
    a short ``reason`` string suitable for inline rendering.

    ``present`` reflects current presence in this container (included roots
    must be directories). The backup loop skips missing roots; this is not
    a check of which permissions the platform has granted.
    """
    included = []
    for p in BACKUP_ROOTS:
        included.append({"path": str(p), "present": p.is_dir()})

    excluded = []
    for p in BACKUP_EXCLUDES:
        # ``user_facing=False`` marks an exclude that's an
        # implementation detail (the backup app's own data dir).
        # Surfaced this way so the
        # snapshots-browser note can hide self-references without
        # the JS having to hard-code which path that is — the JS
        # filters on ``user_facing`` and stays in lockstep with
        # whatever the helper decides counts as operator-relevant.
        if p == APP_ARCHIVE:
            reason = (
                "Archive data is intentionally excluded for both local and S3 "
                "archive backends. Local archive data stays on the instance's "
                "disk, not in an off-machine copy. S3 archive recovery requires "
                "JuiceFS metadata as well as the S3 objects."
            )
            user_facing = True
        elif p in {ALL_APP_DATA / APP_NAME, APP_TEMP_DATA / APP_NAME} or p.name == RESTORE_WORK_NAME:
            reason = (
                "The entire backup app data directory is excluded, including "
                "configuration, backup history, and any local restic repository "
                "stored here. This also avoids recursive self-inclusion of "
                "a repository stored in this directory."
            )
            user_facing = False
        else:
            reason = ""
            user_facing = True
        excluded.append(
            {
                "path": str(p),
                "present": p.exists(),
                "reason": reason,
                "user_facing": user_facing,
            }
        )

    return {
        "included": included, "excluded": excluded,
        "configuration": {"included": True, "runtime_configured": bool(get_router_api_token())},
    }


@route("/api/config", methods=["GET"])
async def get_config():
    conf = load_config()
    return jsonify(config={**conf, "backend": classify_repo(conf.get("repo", ""))})


@route("/api/config", methods=["POST"])
async def post_config():
    data = await request.get_json()
    current_conf = load_config()

    if "router_api_token" in data and current_conf.get("router_api_token"):
        bearer = _extract_bearer_token()
        new_token = data.get("router_api_token") or ""
        authorized = (
            await _verify_admin_token(bearer)
            or (bool(new_token) and await _verify_admin_token(new_token))
        )
        if not authorized:
            return jsonify(
                ok=False,
                error="Bearer token required to rotate router_api_token",
            ), 401

    # The cached repo size describes whatever repo was configured. If the repo
    # URL changes, that figure belongs to the old repo, so remember the old
    # value now (before we overwrite it) to invalidate the cache below.
    old_repo = current_conf.get("repo", "")

    conf = current_conf
    for key in ("repo", "repo_password", "router_api_token"):
        if key in data:
            conf[key] = data[key] or ""
    if "env" in data:
        if not isinstance(data["env"], dict):
            return jsonify(ok=False, error="'env' must be an object"), 400
        conf["env"] = {
            k: str(v) for k, v in data["env"].items() if v != "" and v is not None
        }
    if "interval_seconds" in data:
        try:
            interval = int(data["interval_seconds"])
        except (TypeError, ValueError):
            return jsonify(ok=False, error="interval_seconds must be an integer"), 400
        conf["interval_seconds"] = 0 if interval <= 0 else max(60, interval)
    # Retention policy (restic forget keep-* tiers). Each is a non-negative
    # int; 0 = tier unset. Stored as ints so _forget_args can read them
    # directly.
    for key in KEEP_FLAGS:
        if key in data:
            try:
                n = int(data[key])
            except (TypeError, ValueError):
                return jsonify(ok=False, error=f"{key} must be an integer"), 400
            conf[key] = max(0, n)
    save_config(conf)
    # Pointing at a different repo makes the cached size stale — drop it so the
    # next /api/repo/stats read computes live against the new repo.
    if conf.get("repo", "") != old_repo:
        invalidate_repo_stats_cache()
    return jsonify(ok=True)


@route("/api/repo/test", methods=["POST"])
async def api_repo_test():
    """Test the restic connection once, no retries.

    Body (all optional): ``repo``, ``repo_password`` — override the saved
    values for the test, so the user can try a new URL/password before
    committing them with Save. The saved ``env`` is always used.
    """
    data = await request.get_json(silent=True) or {}
    conf = load_config()
    repo = (data.get("repo") or conf.get("repo") or "").strip()
    if not repo:
        return jsonify(ok=False, error="No repo URL configured"), 400
    repo_password = data.get("repo_password") or conf.get("repo_password", "")
    # Merge env overrides on top of saved env so the user can test credentials
    # they've typed into the page (AWS quick setup, raw env setter) without
    # having to Apply/Save first.
    env_override = data.get("env") or {}
    merged_env = {**(conf.get("env") or {})}
    for k, v in env_override.items():
        if v is None or v == "":
            continue
        merged_env[k] = v
    test_conf = {
        **conf,
        "repo": repo,
        "repo_password": repo_password,
        "env": merged_env,
    }

    ok, message, output = await test_restic_connection(test_conf)
    debug = _build_restic_debug(test_conf)
    return jsonify(ok=ok, message=message, output=output, debug=debug)


@route("/api/backup", methods=["POST"])
async def trigger_backup():
    blocked = _backup_blocked_reason()
    if blocked:
        return jsonify(ok=False, error=blocked), 409
    data = await request.get_json(silent=True) or {}
    if type(data) is not dict or (data.get("name") is not None and type(data["name"]) is not str):
        return jsonify(ok=False, error="Invalid backup request."), 400
    name = (data.get("name") or "").strip() or None
    await _reclaim_abandoned_migration()
    # Reclaiming can adopt an interrupted migration, so the reason is read
    # once, after the await, and used for both the decision and the response.
    blocked = _backup_blocked_reason()
    if blocked:
        return jsonify(ok=False, error=blocked), 409
    error = op_lock.try_acquire(OpKind.BACKUP)
    if error:
        return jsonify(ok=False, error=error), 409
    _spawn_background(run_backup(name=name, lock_acquired=True))
    return jsonify(ok=True, message="Backup started")


@route("/api/status")
async def status():
    conf = load_config()
    last = get_last_backup()
    return jsonify(
        running=op_lock.backup_running,
        migration_running=op_lock.migration_running,
        restore_running=op_lock.restore_running,
        delete_running=op_lock.delete_running,
        # Generic lock state so the UI can render one always-on banner for
        # whatever operation currently holds op_lock, without knowing each kind.
        **_lock_status(),
        last_backup=last["timestamp"] if last else None,
        last_status=last["status"] if last else None,
        last_error=last["error_message"] if last else None,
        interval_seconds=conf["interval_seconds"],
        repo=conf.get("repo", ""),
        backend=classify_repo(conf.get("repo", "")),
    )


@route("/api/events")
async def events():
    """Server-Sent Events stream of op_lock state.

    Emits the current status on connect, then again on every lock transition
    (pushed via ``_notify_status_change``), with a periodic comment keepalive
    so idle connections survive proxies. The UI uses this to update the banner
    instantly; its slow poll is only a fallback.
    """

    async def stream():
        q: asyncio.Queue = asyncio.Queue()
        _status_subscribers.add(q)
        try:
            yield f"data: {json.dumps(_lock_status())}\n\n"
            while True:
                try:
                    await asyncio.wait_for(q.get(), timeout=25)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                # Coalesce a burst of notifications into one status emit.
                while not q.empty():
                    q.get_nowait()
                yield f"data: {json.dumps(_lock_status())}\n\n"
        finally:
            _status_subscribers.discard(q)

    return Response(
        stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            # Disable proxy buffering (nginx) so events aren't held back.
            "X-Accel-Buffering": "no",
        },
    )


@route("/api/snapshots")
async def api_snapshots():
    snapshots, repo_ok = await list_snapshots()
    return jsonify(ok=True, snapshots=snapshots, repo_ok=repo_ok)


@route("/api/repo/stats")
async def api_repo_stats():
    # Serve the cached value stamped by the last backup/delete — running
    # `restic stats` on every request is slow enough to 504 behind the proxy
    # on large repos. `?refresh=1` bypasses the cache for a one-off live read
    # (used e.g. when the repo was changed out of band); it does not persist —
    # the stored cache updates only on the next backup or delete.
    force = request.args.get("refresh") in ("1", "true", "yes")
    if not force:
        cached = load_repo_stats_cache()
        if cached is not None:
            return jsonify(ok=True, stats=cached, cached=True)
    # No cache yet (fresh install, or first load before any backup) or a
    # forced refresh: compute live.
    stats, error = await repo_stats()
    if error:
        return jsonify(ok=False, error=error), 500
    return jsonify(ok=True, stats=stats, cached=False)


@route("/api/restore", methods=["POST"])
async def trigger_restore():
    data = await request.get_json()
    if type(data) is not dict:
        return jsonify(ok=False, error="Invalid restore request."), 400
    snapshot_id = data.get("snapshot", "")
    if type(snapshot_id) is not str or not SNAPSHOT_ID_RE.fullmatch(snapshot_id):
        return jsonify(ok=False, error="Invalid snapshot id"), 400
    root = data.get("root")
    if root is not None and type(root) is not str:
        return jsonify(ok=False, error="Invalid restore root."), 400
    root = root or None
    if root is not None and root not in _ROOT_NAMES:
        return jsonify(ok=False, error=f"Unknown root: {root}"), 400
    # A co-located container can reach this app directly, so a snapshot that
    # carries app definitions and API-key verifiers may only be applied for a
    # caller the router confirms is the owner. Legacy file-only restores keep
    # their existing behavior.
    caller_token = _extract_bearer_token()
    if caller_token and not await _caller_is_owner():
        return _owner_required_response()
    if not caller_token and root is None and await _snapshot_needs_owner(snapshot_id):
        # Applying captured definitions and key verifiers is owner-only, so say
        # so now rather than letting a background job fail after it started.
        # Legacy file-only snapshots still restore without a token.
        return _owner_required_response()
    await _reclaim_abandoned_migration()
    error = op_lock.try_acquire(OpKind.RESTORE)
    if error:
        return jsonify(ok=False, error=error), 409
    _spawn_background(run_restore(snapshot_id, root=root, owner_token=caller_token, lock_acquired=True))
    return jsonify(ok=True, message="Restore started")


@route("/api/restore/status")
async def restore_status_endpoint():
    return jsonify(
        running=op_lock.restore_running or _restore_session is not None,
        last_restore=restore_last_snapshot,
        last_status=restore_last_status,
        needs_attention=_restore_needs_attention,
        progress={**(restore_progress or {}), "recovery": _restore_session.progress} if _restore_session else restore_progress,
    )


@route("/api/restore/acknowledge", methods=["POST"])
async def acknowledge_restore():
    global _restore_needs_attention, restore_progress
    # Acknowledging unblocks the destructive retry, so it needs the same owner
    # authority as the recovery it clears.
    if not await _caller_is_owner():
        return _owner_required_response()
    if op_lock.busy:
        return jsonify(ok=False, error=op_lock.busy_message()), 409
    if not _restore_needs_attention:
        return jsonify(ok=True)
    candidate = {
        **(restore_progress or {}), "phase": "acknowledged", "needs_attention": False,
        "affected_apps": [], "affected_roots": [], "pending_restarts": [], "recovery": None,
    }
    try:
        if restore_progress and "job_id" in restore_progress:
            snapshot_configuration.save_journal(_restore_journal_path(), candidate)
        else:
            if _restore_journal_path().parent.is_symlink():
                raise ValueError
            _restore_journal_path().unlink(missing_ok=True)
    except Exception:
        return jsonify(ok=False, error="The recovery notice could not be acknowledged. Check the backup's writable storage."), 500
    restore_progress = candidate
    _restore_needs_attention = False
    return jsonify(ok=True)


@route("/api/snapshot/files")
async def snapshot_files():
    snapshot_id = request.args.get("snapshot", "")
    if not snapshot_id or not SNAPSHOT_ID_RE.match(snapshot_id):
        return jsonify(ok=False, error="Invalid snapshot id"), 400
    # Optional named-root shortcuts; otherwise browse from the snapshot's /.
    root = request.args.get("root") or None
    if root is not None and root not in _ROOT_NAMES:
        return jsonify(ok=False, error=f"Unknown root: {root}"), 400
    subpath = request.args.get("path", "")
    if not validate_subpath(subpath):
        return jsonify(ok=False, error="Invalid path"), 400
    view = request.args.get("view", "tree")
    if view not in {"tree", "backup"}:
        return jsonify(ok=False, error="Unknown snapshot view"), 400
    try:
        if view == "backup" and root is None and not subpath:
            files, error = await list_snapshot_contents(snapshot_id)
        else:
            files, error = await list_snapshot_files(snapshot_id, subpath, root=root)
        if error:
            status_code = 404 if "not found" in error.lower() else 500
            return jsonify(ok=False, error=error), status_code
        return jsonify(ok=True, files=files, root=root)
    except Exception as e:
        logger.exception("Failed to list snapshot files")
        return jsonify(ok=False, error=str(e)), 500


@route("/api/snapshot/delete", methods=["POST"])
async def snapshot_delete():
    data = await request.get_json()
    snapshot_id = data.get("snapshot", "")
    if not snapshot_id or not SNAPSHOT_ID_RE.match(snapshot_id):
        return jsonify(ok=False, error="Invalid snapshot id"), 400
    if op_lock.busy:
        return jsonify(ok=False, error=op_lock.busy_message()), 409
    # Run the prune in the background (it can take minutes) and return
    # immediately. delete_snapshot acquires op_lock(DELETE), so the status
    # banner reflects it and other operations get a clean busy rejection; the
    # UI watches the banner and refreshes the snapshot list when it clears.
    _spawn_background(delete_snapshot(snapshot_id))
    return jsonify(ok=True, message="Delete started")


@route("/api/check", methods=["POST"])
async def trigger_check():
    # run_check runs `restic check --no-lock`, so it holds no repo lock and
    # doesn't claim op_lock
    if op_lock.busy:
        return jsonify(ok=False, error=op_lock.busy_message()), 409
    if check_running:
        return jsonify(ok=False, error="check already running"), 409
    _spawn_background(run_check())
    return jsonify(ok=True, message="Check started")


@route("/api/check/status")
async def check_status_endpoint():
    return jsonify(
        running=check_running,
        last_status=check_last_status,
        last_output=check_last_output,
        last_at=check_last_at,
    )


@route("/api/history")
async def backup_history():
    limit = min(int(request.args.get("limit", 20)), 100)
    offset = int(request.args.get("offset", 0))
    history, total = get_backup_history(limit, offset)
    return jsonify(ok=True, history=history, total=total)


@route("/api/backup/rename", methods=["POST"])
async def rename_backup():
    data = await request.get_json()
    backup_id = data.get("id")
    new_name = (data.get("name") or "").strip() or None
    if not backup_id:
        return jsonify(ok=False, error="Missing backup id"), 400
    conn = get_db()
    try:
        conn.execute("UPDATE backups SET name = ? WHERE id = ?", (new_name, backup_id))
        conn.commit()
        if conn.total_changes == 0:
            return jsonify(ok=False, error="Backup not found"), 404
    finally:
        conn.close()
    return jsonify(ok=True)


# ---------------------------------------------------------------------------
# App management & chown routes (pre-migration helpers)
# ---------------------------------------------------------------------------


async def _get_router_apps(router_token: str) -> dict:
    """Fetch app list from the local router as a name-keyed dict."""
    import httpx

    async with httpx.AsyncClient(verify=False, timeout=10) as client:
        r = await client.get(
            f"{ROUTER_URL}/api/apps",
            headers={"Authorization": f"Bearer {router_token}"},
        )
        if r.status_code in (401, 403):
            raise RuntimeError(f"Router API token is invalid or unauthorized (HTTP {r.status_code})")
        if r.status_code != 200:
            raise RuntimeError(f"Router returned HTTP {r.status_code}")
        if "json" not in r.headers.get("content-type", ""):
            raise RuntimeError("Router API token is invalid or unauthorized (non-JSON response)")
        listing = r.json()
        return {
            a["name"]: a
            for a in migration._normalize_app_listing(listing)
            if isinstance(a, dict) and a.get("name")
        }


@route("/api/router/test", methods=["POST"])
async def api_router_test():
    """Test router API token validity against the local router."""
    data = await request.get_json(silent=True) or {}
    token = data.get("token") or _extract_bearer_token() or get_router_api_token()
    if not token:
        return jsonify(ok=False, error="No router API token provided or configured"), 400
    try:
        apps = await _get_router_apps(token)
        app_names = [n for n in apps if n != APP_NAME]
        return jsonify(
            ok=True,
            message=f"Connected to router successfully ({len(app_names)} apps found)",
            app_count=len(app_names),
        )
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


@route("/api/apps-status")
async def apps_status():
    """Return the status of all apps from the local router."""
    router_token = _extract_bearer_token() or get_router_api_token()
    if not router_token:
        return jsonify(ok=False, error="No router API token configured"), 400
    try:
        apps = await _get_router_apps(router_token)
        return jsonify(ok=True, apps=apps)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


def _parse_selected_apps(raw: object) -> list[str] | None:
    """Normalize app selection inputs to a list of app names or None for all."""
    if raw is None:
        return None
    if isinstance(raw, str):
        val = raw.strip()
        if not val or val.lower() == "all" or val == "*":
            return None
        return [a.strip() for a in val.split(",") if a.strip()]
    if isinstance(raw, list):
        if not raw or "all" in raw or "*" in raw:
            return None
        return [str(a).strip() for a in raw if str(a).strip()]
    return None


# Statuses that mean an app still holds its data dir open. ``building`` and
# ``starting`` count: a container that is about to run is as unsafe to copy
# from as one already running.
_ACTIVE_STATUSES = ("running", "building", "starting")


async def _running_selected_apps(
    router_token: str, selected_apps: list[str] | None
) -> list[str]:
    """Names of targeted non-backup apps that are not stopped.

    This app is always excluded under its own (configurable) name: it serves
    the request doing the asking, so it can never be stopped first.
    """
    apps = await _get_router_apps(router_token)
    return [
        name
        for name, info in apps.items()
        if name != APP_NAME
        and (selected_apps is None or name in selected_apps)
        and info.get("status") in _ACTIVE_STATUSES
    ]


@route("/api/stop-all-apps", methods=["POST"])
@route("/api/stop-apps", methods=["POST"])
async def stop_all_apps():
    """Stop running apps (all or a selected list), excluding backup."""
    router_token = _extract_bearer_token() or get_router_api_token()
    if not router_token:
        return jsonify(ok=False, error="No router API token configured"), 400
    data = await request.get_json(silent=True) or {}
    selected_apps = _parse_selected_apps(data.get("apps"))
    try:
        import httpx

        apps = await _get_router_apps(router_token)
        stopped = []
        async with httpx.AsyncClient(verify=False, timeout=30) as client:
            for app_name, info in apps.items():
                if app_name == APP_NAME:
                    continue
                if selected_apps and app_name not in selected_apps:
                    continue
                if info.get("status") in _ACTIVE_STATUSES:
                    app_id = info.get("app_id") or info.get("id") or app_name
                    try:
                        sr = await client.post(
                            f"{ROUTER_URL}/stop_app/{app_id}",
                            headers={"Authorization": f"Bearer {router_token}"},
                        )
                        if sr.status_code == 200:
                            stopped.append(app_name)
                        else:
                            logger.warning(
                                "Failed to stop %s: HTTP %s", app_name, sr.status_code
                            )
                    except Exception as e:
                        logger.warning("Failed to stop %s: %s", app_name, e)
        return jsonify(ok=True, stopped=stopped)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


# UIDs at or above this value are taken to be subuid-mapped — i.e. a host-side
# representation of a non-root user inside a rootless container's user
# namespace.  Distros conventionally allocate subuid ranges starting at
# 100000 (Debian/Ubuntu) or 165536 (the kernel-recommended floor); real
# interactive users are well below this.  Anything in this range is owned by
# a process running inside a container under a non-root in-container user
# (postgres, rabbitmq, mysql, etc.) and chowning it to the host user destroys
# the user-namespace mapping, leaving the in-container user unable to read
# its own data.
_SUBUID_FLOOR: int = 100000


@route("/api/chown-app-data", methods=["POST"])
async def chown_app_data():
    """Recursively chown app_data to the host user, skipping subuid-mapped files."""
    router_token = _extract_bearer_token() or get_router_api_token()
    if not router_token:
        return jsonify(ok=False, error="No router API token configured"), 400

    data = await request.get_json(silent=True) or {}
    selected_apps = _parse_selected_apps(data.get("apps"))

    try:
        running = await _running_selected_apps(router_token, selected_apps)
    except Exception as e:
        return jsonify(ok=False, error=f"Could not check app status: {e}"), 500
    if running:
        return jsonify(
            ok=False,
            error=f"Apps still running: {', '.join(running)}. "
            "Stop them before fixing ownership.",
        ), 400

    if not ALL_APP_DATA.is_dir():
        return jsonify(
            ok=False, error=f"app_data directory not found: {ALL_APP_DATA}"
        ), 404

    target_uid = 1000
    target_gid = 1000
    count = 0
    skipped = 0
    errors = 0

    def _chown_one(path: str) -> None:
        nonlocal count, skipped, errors
        try:
            st = os.lstat(path)
        except OSError as e:
            errors += 1
            logger.warning("stat failed for %s: %s", path, e)
            return
        if st.st_uid >= _SUBUID_FLOOR or st.st_gid >= _SUBUID_FLOOR:
            skipped += 1
            return
        try:
            os.chown(path, target_uid, target_gid, follow_symlinks=False)
            count += 1
        except OSError as e:
            errors += 1
            logger.warning("chown failed for %s: %s", path, e)

    def _chown_tree(dir_path: Path) -> None:
        if not dir_path.exists():
            return
        path_str = str(dir_path)
        for root, dirs, files in os.walk(path_str):
            for name in dirs + files:
                _chown_one(os.path.join(root, name))
        _chown_one(path_str)

    if selected_apps:
        for app_name in selected_apps:
            _chown_tree(ALL_APP_DATA / app_name)
    else:
        _chown_tree(ALL_APP_DATA)

    logger.info(
        "chown complete: %d items fixed, %d skipped, %d errors", count, skipped, errors
    )
    return jsonify(
        ok=True,
        message=f"Ownership fixed on {count} items (uid={target_uid}, gid={target_gid}); "
        f"skipped {skipped} subuid-mapped items",
        count=count,
        skipped=skipped,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Migration routes
# ---------------------------------------------------------------------------


@route("/api/migration/status")
async def migration_status_endpoint():
    idle = op_lock.idle_seconds() if op_lock.migration_running else None
    incoming = _receiver().journal_status
    if incoming and incoming.get("snapshot") == (restore_progress or {}).get("snapshot") and incoming.get("snapshot"):
        incoming["recovery"] = _restore_session.progress if _restore_session else restore_progress.get("recovery")
    displayed = migration.status
    incoming_newer = incoming and incoming.get("started_at", 0) > (displayed or {}).get("started_at", 0)
    if incoming and (displayed is None or incoming_newer or incoming.get("phase") in {"preflighting", "receiving", "finalizing"}):
        phase = incoming.get("phase", "interrupted")
        displayed = {
            "phase": "done" if phase == "complete" else "error" if phase in {"failed", "incomplete", "interrupted", "aborted"} else phase,
            "progress": 100 if phase == "complete" else 0,
            "error": None if phase == "complete" else "Review incoming migration results.",
        }
    return jsonify(
        version=migration.MIGRATION_PROTOCOL_VERSION,
        running=op_lock.migration_running,
        stale=idle is not None and idle > MIGRATION_IDLE_TIMEOUT_SECONDS,
        idle_seconds=round(idle) if idle is not None else None,
        status=displayed,
        log=migration.log[-50:],
        receive=incoming,
        source_recovery=_source_recovery_status(),
    )


def _source_recovery_status() -> dict | None:
    """Outgoing cutover record for the UI; carries no tokens or bundle data."""
    record = migration.source_recovery
    return record.journal_status if record is not None else None


# ---------------------------------------------------------------------------
# Direct push migration
# ---------------------------------------------------------------------------


@route("/api/migration/push", methods=["POST"])
async def trigger_direct_push():
    """The retained source job owns capture, quiescence, transfer and cleanup."""
    data = await request.get_json(silent=True) or {}
    if type(data) is not dict or set(data) - {"target_url", "target_token", "apps"}:
        return jsonify(ok=False, error="Invalid migration request."), 400
    if type(data.get("target_url")) is not str or type(data.get("target_token")) is not str:
        return jsonify(ok=False, error="Destination URL and API token are required."), 400
    target_url = (data.get("target_url") or "").rstrip("/")
    target_token = data.get("target_token") or ""
    if not target_url.strip() or not target_token.strip():
        return jsonify(ok=False, error="Destination URL and API token are required."), 400
    raw_apps = data.get("apps")
    if raw_apps is not None and not isinstance(raw_apps, (list, str)):
        return jsonify(ok=False, error="'apps' must be a list or string"), 400
    if raw_apps == []:
        return jsonify(ok=False, error="Select at least one app."), 400
    selected_apps = _parse_selected_apps(raw_apps)
    # Outgoing migration captures definitions and API-key verifiers and sends
    # them to a destination URL, so only a caller the router confirms as owner
    # may start one. The configured token is never proof of caller identity.
    router_token = await _caller_is_owner()
    if not router_token:
        return _owner_required_response()
    await _reclaim_abandoned_migration()
    if _restore_needs_attention:
        # Source preflight only checks router state, so it cannot see app trees
        # left uncertain by an interrupted restore on this instance.
        return jsonify(
            ok=False,
            error="Retry or acknowledge the incomplete recovery before migrating this instance.",
        ), 409
    if _receiver().needs_attention:
        # This instance's own data is in an uncertain state. Migrating it
        # outward would copy half-replaced app trees to the destination.
        return jsonify(
            ok=False,
            error="Inspect and acknowledge the interrupted incoming migration before migrating again.",
        ), 409
    if _source_needs_attention():
        # The previous cutover from this instance is unresolved, so apps may be
        # stopped and app states unverified.
        return jsonify(
            ok=False,
            error="Inspect and acknowledge the interrupted outgoing migration before migrating again.",
        ), 409
    err = op_lock.try_acquire(OpKind.MIGRATION)
    if err:
        return jsonify(ok=False, error=err), 409
    _spawn_background(
        migration.run_direct_push(
            target_url=target_url,
            target_token=target_token,
            selected_apps=selected_apps,
            lock=op_lock,
            all_app_data=ALL_APP_DATA,
            work_dir=APP_DATA_DIR / ".migration",
            router_url=ROUTER_URL,
            app_token=APP_TOKEN,
            owner_token=router_token,
            backup_app_name=APP_NAME,
            lock_acquired=True,
        )
    )
    return jsonify(ok=True, message="Direct push migration started")


# ---------------------------------------------------------------------------
# Receive endpoints (target side — called by source during direct push)
# ---------------------------------------------------------------------------


@route("/api/migration/receive/start", methods=["POST"])
async def receive_start():
    token = _receive_owner_token()
    return jsonify(await _receiver().start(await _migration_json(), owner_token=token))


@route("/api/migration/receive/app/<app_name>", methods=["POST"])
async def receive_app(app_name):
    raise migration.MigrationError("protocol")


@route("/api/migration/receive/object/<session_id>/<kind>/<identifier>", methods=["POST"])
async def receive_object(session_id, kind, identifier):
    token = _receive_owner_token()
    offset = request.headers.get("X-Object-Offset", "")
    if not re.fullmatch(r"[0-9]{1,13}", offset):
        raise migration.MigrationError("invalid")
    return jsonify(await _receiver().upload(
        session_id, kind, identifier, request.body, offset=int(offset), owner_token=token,
    ))


@route("/api/migration/receive/chunk/<app_name>", methods=["POST"])
@route("/api/migration/receive/chunk/<session_id>/<app_name>", methods=["POST"])
async def receive_legacy_chunk(app_name, session_id=None):
    raise migration.MigrationError("protocol")


@route("/api/migration/receive/data", methods=["POST"])
async def receive_data():
    raise migration.MigrationError("protocol")


@route("/api/migration/receive/finalize", methods=["POST"])
async def receive_finalize():
    token = _receive_owner_token()
    return jsonify(await _receiver().finalize(await _migration_json(), owner_token=token))


async def _restore_migration_snapshot(snapshot, repository, password, owner_token, session):
    """Incoming migration uses the ordinary restore transaction and journal."""
    global restore_last_snapshot, restore_last_status
    restore_last_snapshot, restore_last_status = None, None
    conf = {"repo": str(repository), "repo_password": password, "env": {}, "_isolated_restic": True}
    try:
        ok = await _restore_configuration_snapshot(snapshot, conf, owner_token, session=session)
    except BaseException:
        restore_last_status = "error: Incoming snapshot recovery did not complete."
        raise
    return {"ok": ok, "recovery": (restore_progress or {}).get("recovery")}


def _receive_owner_token() -> str:
    token = _extract_bearer_token()
    if not token:
        raise migration.MigrationError("auth")
    return token


async def _migration_json() -> dict:
    if not request.is_json:
        raise migration.MigrationError("invalid")
    content = bytearray()
    async for chunk in request.body:
        if len(content) + len(chunk) > migration.MAX_JSON_BYTES:
            raise migration.MigrationError("invalid")
        content.extend(chunk)

    def unique_pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    try:
        value = json.loads(content.decode("utf-8"), object_pairs_hook=unique_pairs)
        if type(value) is not dict:
            raise ValueError
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise migration.MigrationError("invalid") from None


@app.errorhandler(migration.MigrationError)
async def migration_error(error):
    return jsonify(ok=False, error=str(error)), error.status_code


@route("/api/migration/receive/capabilities")
async def receive_capabilities():
    token = _receive_owner_token()
    return jsonify(await _receiver().capabilities(owner_token=token))


@route("/api/migration/receive/status/<session_id>")
async def receive_status(session_id):
    token = _receive_owner_token()
    return jsonify(await _receiver().status(session_id, owner_token=token))


@route("/api/migration/acknowledge", methods=["POST"])
async def migration_acknowledge():
    """Clear an interrupted incoming-migration notice without touching data."""
    token = await _caller_is_owner()
    if not token:
        return _owner_required_response()
    incoming = _receiver().journal_status or {}
    if incoming.get("snapshot") and incoming["snapshot"] == (restore_progress or {}).get("snapshot"):
        # One owner acknowledgment covers this migration's ordinary restore
        # journal too; an unrelated restore notice is never cleared here.
        response = await app.make_response(await acknowledge_restore())
        if response.status_code != 200:
            return response
    state = await _receiver().acknowledge(owner_token=token)
    return jsonify(ok=True, needs_attention=False, journal_status=state)


@route("/api/migration/source-acknowledge", methods=["POST"])
async def migration_source_acknowledge():
    """Clear an interrupted outgoing-migration notice without touching data."""
    token = await _caller_is_owner()
    if not token:
        return _owner_required_response()
    record = migration.source_recovery
    if record is None:
        return jsonify(ok=True, needs_attention=False, journal_status=None)
    state = await record.acknowledge(owner_token=token)
    return jsonify(ok=True, needs_attention=False, journal_status=state)


@route("/api/migration/receive/abort", methods=["POST"])
async def receive_abort():
    token = _receive_owner_token()
    return jsonify(await _receiver().abort(await _migration_json(), owner_token=token))


@route("/api/migration/receive/keepalive", methods=["POST"])
async def receive_keepalive():
    token = _receive_owner_token()
    return jsonify(await _receiver().keepalive(await _migration_json(), owner_token=token))


@route("/health")
async def health():
    return "ok"
