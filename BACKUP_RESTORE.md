# Backup and restore

Beacon supports:

- SQLite snapshots (`.db`, `.sqlite`, `.sqlite3`) — full verified replacement, with an automatic pre-restore backup.
- JSON backups — validated merge import of non-secret operational records; password hashes are deliberately redacted and are not restored.
- `.beaconbackup.zip` — contains database, JSON export, PDF snapshot and a SHA-256 manifest.
- PDF reports — summary, incidents, vehicles, overspeed, errors (admin) and backup snapshot.

Every SQLite backup is copied using SQLite's backup API and checked with `PRAGMA integrity_check`. Uploaded ZIP member paths are validated to prevent path traversal.

After a successful SQLite restore, runtime/live caches are cleared so the new database becomes authoritative instead of mixing old in-memory records with restored records.
