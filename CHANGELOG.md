# Beacon repair pass — 2026-09

## Reliability
- Real device identity is required for heartbeats.
- Telemetry has explicit integrity metadata and sequence handling.
- Live state uses a cache and stale-state cutoff.
- Overspeed/accident events are surfaced to authority rooms and operational incidents are persisted.

## Authority experience
- Police and GK sessions use their own role-aware live channels.
- Reports & Backups is centralized and linked from authority consoles.
- System Errors & Health is administrator-only and searchable.

## Data protection
- Public lookup no longer exposes device tokens.
- LPR ingestion is authenticated.
- Default secrets/bootstrap credentials are removed from the repaired entrypoint.
- JSON exports redact password hashes.

## Exports
- SQLite online-backup download for administrators.
- Portable JSON export for authorized authority users.
- PDF summary, incidents, vehicles, overspeeds, errors and backup snapshot reports.
