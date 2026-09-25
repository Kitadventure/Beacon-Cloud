# Beacon Cloud — repaired authority platform

Beacon is a real-time road-safety, vehicle-intelligence and incident-coordination platform. This repaired distribution keeps the legacy feature set in `app_legacy.py` while using `app.py` as the hardened entrypoint.

## Major repairs

- Device heartbeats no longer auto-create missing devices.
- Live telemetry accepts accuracy, timing, sequence and mock-location integrity metadata.
- Mock/simulated location is blocked by default.
- Nearby/risk evaluation uses the active-device cache rather than repeatedly scanning the full snapshot table.
- Snapshot cleanup is throttled instead of running on every heartbeat.
- Police and GK Socket.IO authentication is role-aware and no longer asks officers for an admin token.
- GK accounts can actually be created from access management.
- Public vehicle lookup never returns a device credential.
- Plate/LPR ingestion requires authenticated authority access or the configured integration token.
- A persistent System Errors center records unhandled server errors plus authenticated web/mobile client errors.
- Authority Reports & Backups provides role-aware PDF reports, JSON export and an admin-only SQLite backup.
- The dashboard has a fixed map-style null reference and links to diagnostics/reports.
- Request IDs and basic security headers are attached to responses.
- Legacy full-app PDF generation is repaired through the new report engine.

## First-time setup

1. Copy `.env.example` to your deployment environment.
2. Set a strong `FLASK_SECRET`. The server refuses a production start without it.
3. Install dependencies with Python 3.11+.
4. Start the service with the Procfile command or `python app.py` for local development.
5. Visit `/register` once to create the first administrator. Public registration closes after the first admin exists.
6. From Access Management, create Police and GK/Command accounts.
7. Provision devices through an authenticated `/onboard` request and keep the returned device token private.

## Authority screens

- `/dashboard` — administrator operations dashboard
- `/police` — role-aware Police Operations console
- `/gk/dashboard` — GK / Command Center
- `/authority/reports` — reports and backups available to the logged-in authority role
- `/admin/errors` — administrator-only system error and health center
- `/admin/users` — authority account management

## Backups and reports

PDF reports are generated on demand and are available from the central Reports page, Police console, GK console, the main admin dashboard tools, and the administrator error center.

SQLite backup uses SQLite's online backup API, producing a consistent `.db` copy instead of simply copying the live file. JSON export is portable and redacts password hashes. The SQLite backup is administrator-only; operational JSON/PDF views are role-aware.

## Environment safety

`ALLOW_SIMULATION=0` is the default and should remain disabled on any live deployment. A controlled simulator can set it to `1` only for isolated demonstrations/testing.

Set `SOCKET_ALLOWED_ORIGINS` to a comma-separated allow-list in an internet-facing deployment. Avoid the old wildcard CORS configuration where practical.

## Migration note

The new entrypoint is intentionally a compatibility wrapper so an existing Beacon database and the legacy road/report/device features can be upgraded without discarding the earlier work. `app_legacy.py` is kept as a recovery/reference copy. New functionality should be added to modular files rather than growing the legacy monolith further.

## Verification in this distribution

The repaired Python entrypoint and preserved legacy source are syntax-checked with `py_compile`. Full runtime/integration testing requires the declared Python dependencies and a configured database/server environment. The supplied Android archive did not contain its original `app/src` source tree, so the mobile ZIP contains a clean protocol-compatible source reconstruction and cannot be honestly described as byte-for-byte recovery of unavailable source.
