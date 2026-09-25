# Beacon Cloud repair validation

Validated before packaging:

- Python AST / bytecode syntax checks pass for `app.py` and `app_legacy.py`.
- The `Watchlist` model exists and is imported by the repaired entrypoint.
- Police and GK Socket.IO channels use role sessions rather than an admin token.
- Device heartbeat authentication, mock telemetry blocking and LPR authentication checks are present.
- System errors, SQLite/JSON backups and PDF reports are present.
- SQLite restore verifies integrity, removes WAL/SHM sidecars, replaces the database, clears runtime cache and recreates missing repaired tables.
- JSON restore supports current exports and common older/plain table-list exports and merges rows with per-row savepoints.
- `/pulse_receiver` no longer bypasses the hardened `/heartbeat` path; empty requests are read-only probes, while telemetry requires authentication.
- The Render Blueprint uses Python 3.13.5, Gunicorn gthread and a persistent `/var/data` disk.

A full runtime import/build against the production dependency set could not be executed inside the repair workspace because external package/Gradle downloads and the Android SDK are not available here.
