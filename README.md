# bottle-backup

User-controlled incremental backups and cross-instance migration for Cloud in a Bottle, powered by [restic](https://restic.net).

## What it does

This app backs up app data and private application configuration on a Cloud in a Bottle instance to a storage backend you control: S3, Backblaze B2, SFTP, Google Cloud Storage, Azure Blob, OpenStack Swift, rclone remotes, a restic REST server, or a local directory. Backups are encrypted, incremental and deduplicated by restic. It also migrates apps, persistent data and configuration directly between instances.

The app has `access_all_app_data = true` in its manifest, which exposes apps' persistent, temporary, and archive data to the container. Restic snapshots include the persistent and temporary data roots subject to the exclusions below; the archive tier (`/data/app_archive`) is excluded for both local and S3 archive backends.

## Getting started

1. Install the app from the Cloud in a Bottle dashboard and approve its global Private app-definitions export grant.
2. Open the Backup UI at `https://backup.<your-zone>/`.
3. Enter your restic repository URL and password.
4. Optionally configure backend credentials (AWS keys, B2 keys, etc.) in the environment variables section.
5. Click "Test" to verify access.
6. Optionally save a local owner Router API Token in the Backups tab to capture desired states, global service grants and provider selections too. Full configuration recovery and migration require owner authority.
7. Click "Run backup now" or set an automatic interval.

The manifest requests a global `Private` grant for `github.com/cloud-in-a-bottle/cloud-in-a-bottle/services/app-definitions`. For an older installation, use native Update & Reload and approve the permission request. Missing approval or failed export fails the backup instead of silently creating a data-only recovery point. Basic definition and API-key backups need only the app token and approved export grant; an invalid configured owner token also fails the backup.

## Backup scope

Each backup captures these directories only if they are present in the backup app's container, subject to the exclusions below:

| Path | Contents |
|------|----------|
| `/data/app_data` | Persistent app data (databases, config, user files) |
| `/data/app_temp_data` | App temp data (caches, build artifacts) |

The same encrypted restic snapshot also contains a format-v1 private JSON bundle with canonical schema-v2 app definitions and API-key verifier records. JSON is accepted by the platform's YAML importer. Importing these verifiers preserves the original raw client API keys and absolute expiry semantics: clients can keep using their original unexpired keys. The original raw API keys are not exported, and verifiers are not authentication keys.

With a configured owner Router API Token, the bundle additionally captures desired running/stopped states, exact global service grants and provider selections. Without it, recovery restores captured definitions and key records but cannot recover missing grants or desired states. The UI labels this limited runtime scope.

Standard mounts never exposed the router database or host SSH keys. New snapshots do not capture `/data/vm_data`; that optional nonstandard path remains available for file-only recovery of old snapshots.

Excluded from backups:

| Path | Reason |
|------|--------|
| `/data/app_data/backup` | The entire backup app data directory, including configuration, backup history, and any local restic repository stored there |
| `/data/app_temp_data/backup` | The backup app's own temporary storage |
| `.bottle-backup-restore` within data roots | Internal recovery staging, which may retain original data after an incomplete restore |
| `/data/app_archive` | Archive data is intentionally excluded for both local and S3 archive backends |

The executor exclusions follow the installed backup app name. The local archive backend stores data on the instance's disk; it is not an off-machine copy. The S3 archive backend stores file data through JuiceFS, and recovery requires JuiceFS metadata as well as the S3 objects. These snapshots do not capture archive contents; archive backup policy is outside this feature's scope.

Backups read live files without stopping apps or coordinating database snapshots. Files changing during a backup are not guaranteed to form an application-consistent recovery point.

## Supported backends

Any backend restic supports works here. The repository URL format follows restic's conventions:

| Backend | URL format | Example |
|---------|-----------|---------|
| Amazon S3 | `s3:s3.amazonaws.com/bucket` | `s3:s3.us-east-1.amazonaws.com/my-backups` |
| Backblaze B2 | `b2:bucket-name:path` | `b2:my-backups:/bottle` |
| SFTP | `sftp:user@host:/path` | `sftp:backup@nas.local:/backups` |
| Google Cloud Storage | `gs:bucket:/path` | `gs:my-backups:/bottle` |
| Azure Blob | `azure:container:path` | `azure:backups:/bottle` |
| OpenStack Swift | `swift:container:/path` | `swift:my-backups:/bottle` |
| REST server | `rest:http://host:port/` | `rest:https://restic.example.com/` |
| rclone | `rclone:remote:path` | `rclone:b2-remote:backups/bottle` |
| Local path | `/path/to/repo` | `/data/app_data/backup/local-repo` |

Backend credentials (like `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` for S3) are set through the environment variables field in the UI. The UI includes inline examples for common setups like S3.

A local-path repo stores backups on the same disk as the instance, which provides no protection against disk failure. The UI warns about this. Use a remote backend for real disaster recovery.

## Automatic backups

Set an interval (in seconds) in the configuration. The scheduler runs in the background and triggers a backup at each interval. The minimum interval is 60 seconds. On startup, the scheduler checks when the last backup ran and waits only the remaining time before the next one, so restarting the app does not reset the countdown.

Set the interval to 0 (or leave it blank) to disable automatic backups.

## Retention (expiring old backups)

After each successful backup, old snapshots are expired according to a retention policy you set in the UI (the **Retention policy** section of the configuration). It maps directly onto restic's [`forget`](https://restic.readthedocs.io/en/stable/060_forget.html) keep-* rules:

| Field | restic flag | Meaning |
|-------|-------------|---------|
| Keep last | `--keep-last` | The N most recent snapshots, regardless of time |
| Keep hourly | `--keep-hourly` | The newest snapshot from each of the last N hours that have one |
| Keep daily | `--keep-daily` | The newest snapshot from each of the last N days that have one |
| Keep weekly | `--keep-weekly` | The newest snapshot from each of the last N weeks that have one |
| Keep monthly | `--keep-monthly` | The newest snapshot from each of the last N months that have one |
| Keep yearly | `--keep-yearly` | The newest snapshot from each of the last N years that have one |

A snapshot is kept if it matches **any** rule (the rules are OR'd), so tiers combine additively. For example, `keep_last=5, keep_daily=7, keep_weekly=4` keeps the 5 most recent plus one per day for 7 days plus one per week for 4 weeks, deduplicated where they overlap. Set a field to 0 to disable that tier. **If every field is 0, nothing is expired**: the app never issues a `forget` with zero keep rules.

The policy is applied across all `bottle`-tagged snapshots (and legacy `openhost`-tagged ones) as a single group (`--group-by ''`), which assumes one instance per repository. Every snapshot is recorded with a stable host (`--host`, set to the zone domain) so its identity doesn't change when the backup container is redeployed.

Retention runs `forget` inline after the backup and reconciles the backup history database to match. The actual space is reclaimed by a `restic prune`, which runs **in the background** only when snapshots were actually expired. The prune waits for and holds the operation lock while it runs: other lock-taking operations cannot start, and scheduled backups attempted during that time are skipped.

## Snapshots

Each successful backup creates a restic snapshot tagged with `bottle`. Older snapshots tagged `openhost` are still listed, restored, and expired. The UI lists snapshots newest-first and lets you:

- Browse `app_data`, `app_temp_data`, and `platform_configuration` side by side, along with any additional captured paths
- Restore a full snapshot (all captured data roots, with the exclusions described below; single-root restore is API-only)
- Delete a snapshot (runs `restic forget --prune` to reclaim space)
- Name or rename a snapshot for easier identification

Select a snapshot and expand its contents summary to see which files and settings were captured. Legacy snapshots remain usable but cannot recreate missing app definitions or API keys.

The browser groups stored paths under these folder names, including for existing snapshots. The original filesystem tree remains available through `/api/snapshot/files`; `view=backup` returns the grouped top level with each entry's original `browse_path` for navigation.

The Status panel shows the **repo size**, the deduplicated, compressed on-disk footprint (`restic stats --mode raw-data`). Because computing it is slow on large/remote repos, the value is cached: it is recomputed and stored after each backup and after a snapshot delete/prune, and served from the cache on page load. The backup history database is reconciled against restic on every snapshot listing, so rows for snapshots that no longer exist are cleaned up automatically.

## Restoring

The UI always requests a full snapshot restore, regardless of the root open in the file browser. Configuration-aware recovery requires the **caller** to be the owner: paste a valid owner Router API Token in the Backups tab (or send it as a Bearer token) and the backup app confirms it with the router before starting. The token saved in the app's own configuration authorizes unattended backups; it never authorizes a caller, because co-located containers can reach this app directly. Legacy files-only restores keep their existing behavior. Recovery preflights definitions, stages and verifies data before stopping apps, and replaces whole selected app trees so stale files and database WALs are removed. It imports API-key records additively, restores providers and global grants before consumers, waits for application readiness and resumes unaffected apps. An accepted request is not a completed restore.

Existing selected apps must have the same source and published ports as the snapshot. Conflicts fail preflight; reconcile or remove the conflicting app, or recover onto a fresh destination. Existing apps reload saved/local code with `update:false`, rather than fetching an update. Both existing and newly installed selected apps start during recovery, and saved stopped states are reapplied only after unaffected apps are running, so a consumer never needs an app that is recorded stopped. Intentionally stopped apps can run while other apps become ready. Private sources may need bootstrap authorization. Provider-scoped OAuth grants require manual reauthorization and make recovery incomplete. Owner passwords, sessions and platform settings are not restored.

Restored files keep their recorded modes, including private `0000` files and `0500` trees. Durability is established while a restored entry is still reachable, and each rename is flushed through the directory that holds it, so a legitimately unreadable tree is never reopened to justify discarding the originals.

A snapshot can contain app-data directories for apps that have no exported definition. Whole-tree promotion only covers apps the recovery restores, so those directories are reported in the recovery result instead of being silently omitted; recover them from the same snapshot with a root-specific restore. Promoted data is flushed to storage with an error-reporting barrier before retained originals are released, and the recovery is only reported complete after the final per-app results, pending restarts and journal entry are durable.

The UI shows phases, per-app outcomes, API-key import counts, limited-runtime warnings, retained staging and pending restarts. Incomplete or interrupted recovery remains visible after reload. Inspect the apps and retained original data, resolve conflicts or approvals, then retry the snapshot. Acknowledge after inspection only clears the notice; it does not start apps or delete retained original data.

Legacy snapshots and root-specific API restores are files-only. They write captured files in place, leave stale files absent from the snapshot, do not coordinate running apps and cannot recreate missing definitions or API keys. Stop or reload apps manually as needed. Use `POST /api/restore` with `snapshot` and optional `root` (`app_data`, `app_temp_data`, or legacy `vm_data`). Executor storage, internal staging, private bundle files and archive remain excluded in every mode, protecting the backup app and its repository. The operation lock prevents concurrent backup or migration jobs during restore.

## Integrity checks

The "Run restic check" button runs `restic check`, which verifies the internal consistency of the repository (pack files, index, snapshots). The result and output are shown in the UI. This does not verify individual file contents against their original hashes, only that the repository structure is intact.

## Migration

The Migrate tab moves selected apps, persistent data, private definitions, API-key records, desired states, exact global service grants and provider selections to another instance. Both backup apps must be upgraded to protocol v5. Older receivers are rejected before side effects.

### How migration works

1. The browser submits only `POST /api/migration/push`. The backend preflights destination protocol and owner authentication and captures configuration and desired states before pausing all non-backup source apps that may write shared data, including unselected apps.
2. Restic captures the selected persistent data and private configuration in a temporary encrypted repository. Its objects are sent directly to the destination in requests of at most 14 MiB; interrupted requests can be replayed. No shared storage account is needed.
3. The destination runs a full restic integrity check, confirms the captured configuration, and uses the same staged restore transaction as a backup restore. That transaction imports key records additively, restores provider selections and global grants, activates apps and waits for readiness. Existing source/port conflicts fail preflight. Provider-scoped approvals need manual reauthorization.
4. Unaffected apps resume together, then saved running/stopped states are applied, and every paused app is rechecked at the final boundary. Both existing and newly installed selected apps start before saved stopped states can be applied and can run while other apps become ready. The source reports success only after confirmed destination recovery and unaffected-source cleanup.
5. Selected source apps remain stopped after cutover. Inspect incomplete results before retrying. Owner passwords, sessions and platform settings are not transferred.

An interrupted outgoing migration is recorded on the source. Backups and new migrations stay blocked until an owner inspects the recorded app states and acknowledges the record with `POST /api/migration/source-acknowledge`; acknowledging does not start apps or confirm the destination.

### Migration requirements

- A local owner Router API Token, configured in the Backups tab, and the approved Private definitions export grant.
- A destination owner API token, entered in the Migrate tab.
- Both backup apps installed, running and upgraded to protocol v5.

Migration pauses non-backup source writers, including unselected running apps, before copying persistent data. Previously running unselected apps normally resume during source cleanup after destination recovery. It does not transfer temporary or archive storage. Ordinary restic backups still read live files and are not a universal application-consistent snapshot. Restic handles file integrity and metadata preservation for both workflows; there is no browser-side stop or blanket ownership-fix step. Each instance needs temporary disk space for the encrypted repository, and the destination also needs restore staging space.

## Configuration

Configuration is stored in `/data/app_data/backup/config.json` with permissions restricted to 0600. The config file holds:

| Field | Description |
|-------|-------------|
| `repo` | Restic repository URL |
| `repo_password` | Restic repository encryption password |
| `env` | Backend credential environment variables (e.g., AWS keys) |
| `interval_seconds` | Automatic backup interval (0 = disabled) |
| `keep_last` / `keep_hourly` / `keep_daily` / `keep_weekly` / `keep_monthly` / `keep_yearly` | Retention policy tiers (0 = tier disabled; all 0 = keep everything) |
| `router_api_token` | Owner token for runtime capture, configuration recovery and migration; optional for basic definition/key backups |

The config API (`POST /api/config`) requires a valid Bearer token to rotate the `router_api_token` after it has been set, preventing co-located containers from silently replacing it.

## API

All routes are registered at both `/path` and `/backup/path` to handle the Cloud in a Bottle base-path proxy.

### Backup and restore

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Web UI |
| GET | `/api/status` | Current status (running, last backup, interval, backend type) |
| GET | `/api/config` | Current configuration |
| POST | `/api/config` | Update configuration |
| POST | `/api/backup` | Trigger a backup (accepts optional `name` in JSON body) |
| GET | `/api/snapshots` | List snapshots with `has_configuration` and `has_runtime` recovery scope |
| GET | `/api/repo/stats` | Repository size and compression stats |
| POST | `/api/repo/test` | Test restic connection (accepts optional repo/password overrides) |
| POST | `/api/restore` | Restore a snapshot (JSON: `snapshot`, optional `root`); configuration snapshots require a caller confirmed as owner |
| GET | `/api/restore/status` | `running`, `last_restore`, `last_status`, `needs_attention` and safe `progress` with recovery outcomes |
| POST | `/api/restore/acknowledge` | Clear an incomplete recovery notice after inspection (owner authority required); does not restart apps or remove retained originals |
| GET | `/api/snapshot/files` | Browse the snapshot tree (`snapshot`, `path` relative to its root; optional named `root` shortcut) |
| POST | `/api/snapshot/delete` | Delete a snapshot |
| POST | `/api/check` | Run `restic check` |
| GET | `/api/check/status` | Last check result |
| GET | `/api/history` | Backup history (query: `limit`, `offset`) |
| POST | `/api/backup/rename` | Rename a backup record |
| GET | `/health` | Health check (returns `ok`) |

### Migration

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/migration/status` | Source progress/log, local incoming `receive` recovery state and the interrupted outgoing `source_recovery` record |
| POST | `/api/migration/push` | Start a direct-push migration to another instance (owner authority required) |
| POST | `/api/migration/acknowledge` | Clear an interrupted incoming-migration notice (owner authority required); keeps retained originals and app states untouched |
| POST | `/api/migration/source-acknowledge` | Clear an interrupted outgoing-migration notice (owner authority required); unblocks backups and migrations without starting apps |
| GET | `/api/apps-status` | List apps via the local router API |
| POST | `/api/stop-all-apps` (also `/api/stop-apps`) | Stop running apps, all or a selected list, never this app itself |
| POST | `/api/chown-app-data` | Fix ownership on app_data (skips subuid-mapped files) |
| POST | `/api/router/test` | Test a supplied or configured local Router API Token |

### Migration receive endpoints (called by source instance)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/migration/receive/capabilities` | Authenticate owner and report protocol v5 and request limit |
| POST | `/api/migration/receive/start` | Preflight private bundle and reserve a receive session without stopping apps |
| POST | `/api/migration/receive/object/<session_id>/<kind>/<identifier>` | Upload an encrypted repository object range with an `X-Object-Offset` header |
| POST | `/api/migration/receive/finalize` | Start or reconcile retained activation job after verified data transfer |
| GET | `/api/migration/receive/status/<session_id>` | Poll receiver; only terminal `result.ok=true` confirms success |
| POST | `/api/migration/receive/keepalive` | Keep an active receiving session alive while compressing source data |
| POST | `/api/migration/receive/abort` | Abort a receive session, except a live finalizer |

## Files

| File | Description |
|------|-------------|
| `app.py` | Quart web application: routes, restic wrappers, scheduler, config management |
| `operations.py` | Mutual-exclusion lock ensuring only one backup, restore, migration, or prune runs at a time |
| `migration.py` | Direct encrypted-snapshot transfer and source cutover; destination recovery uses the ordinary restore path |
| `migration_data.py` | Shared whole-tree promotion, retained originals, and private work directories |
| `journal.py` | Atomic durable progress publication shared by restore and migration |
| `configuration.py` | Canonical private definition export, validation and supplemental runtime capture |
| `recovery.py` | Owner-authorized preflight, provider/grant recovery and confirmed activation |
| `snapshot_configuration.py` | Private restic bundle and credential-free recovery journal helpers |
| `restic_process.py` | Cancellation-safe restic subprocess execution shared by capture and recovery |
| `Dockerfile` | Python 3.12 Alpine image with restic and uv |
| `cloudinabottle.toml` | App manifest with all-app-data access and the Private definitions export grant |
| `templates/index.html` | Single-page web UI with Backups and Migrate tabs |
| `tests/` | Pytest test suite covering routes, exclude logic, and migration |

## Data

All persistent state lives in `$OPENHOST_APP_DATA_DIR` (defaults to `/data/app_data/backup/`):

```
/data/app_data/backup/
  config.json      # Restic repo URL, password, backend credentials, schedule
  backups.db       # SQLite database tracking backup history
  restic-repo/     # Default local restic repository (only used for local backends)
```

## Concurrency and timeouts

Only one lock-taking operation (backup, restore, migration, snapshot deletion, or the background retention prune) can run at a time within this app. The `OperationLock` in `operations.py` enforces this. The background prune waits for this lock and holds it until it finishes. The check route rejects a start while the operation lock is busy, but `restic check --no-lock` holds neither the operation lock nor a restic repository lock. Operations started after the check begins can therefore overlap it.

Timeouts for restic operations:

| Operation | Timeout |
|-----------|---------|
| Backup | 6 hours |
| Restore | 12 hours |
| Check | 2 hours |
| Connection test | 10 seconds |
| Retention forget | 10 minutes |
| Prune | 6 hours |
| Snapshot forget/prune (manual delete) | 30 minutes |

If a restic process exceeds its timeout, it is killed and the operation is marked as failed.

Read-only commands (`snapshots`, `stats`, `ls`, `cat config`, `check`) run with `--no-lock` so they do not contend on the repository lock or leave a stale lock behind if a request is aborted. Lock-taking commands (`backup`, `restore`, `forget`, `prune`) run with `--retry-lock 1m` so that if another operation is briefly holding the lock, restic waits and retries for up to a minute instead of failing immediately with "repository is already locked". (Both flags require restic ≥ 0.16.)

Every restic invocation is logged (the command on start, exit code and elapsed time on completion), visible via `oh app logs backup`.

## Running tests

Install restic 0.19.1 on `PATH`, then install the locked development dependencies and matching Chromium:

```sh
uv sync --frozen --group dev
uv run --frozen playwright install --with-deps chromium
npm install --prefix "${TMPDIR:-/tmp}/browser-a11y" --no-audit --no-fund axe-core@4.10.3
export AXE_CORE_PATH="${TMPDIR:-/tmp}/browser-a11y/node_modules/axe-core/axe.min.js"
restic version
uv run --frozen --group dev pytest tests/ -v
```

On a host with locally supplied Chromium libraries, load that environment and use `playwright install chromium` without `--with-deps`. Browser tests run an isolated local Quart/Hypercorn server with Chromium route mocks for expensive backend actions. They check recovery scopes, acceptance versus completion, incomplete notices and retries, private-field suppression, push-only migration, keyboard/mobile operation and axe accessibility. They fail rather than skip when Chromium or axe is missing. CI installs the pinned restic and browser versions and runs the complete suite, including real-restic transfer and restore tests.
