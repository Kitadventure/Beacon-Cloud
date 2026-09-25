# Beacon Cloud — Complete Authority Upgrade

This release consolidates the live-communication repairs and the authority UI into one server package.

## Authority UI
- Maroon header and footer branded for Toror Technology and Innovation’s Limited Company.
- Expand/collapse sidebar groups.
- Lightweight dashboard with 15-second refreshes.
- Vehicles & Devices directory with prominent search and phone numbers.
- Messages center with single-vehicle, all-vehicle and overspeeder targeting.
- Bidirectional Authority Inbox.
- Incident and alert view.
- Traffic operations hub.
- Reports / downloads and protected Backups & Restore.
- System Errors screen with resolution and JSON export.

## Communication
- Existing Socket.IO events remain in use for immediate delivery.
- Persistent device-warning queue provides an HTTP fallback through `/nearby`.
- Overspeed and accident warnings are persisted for fallback delivery.
- Accident-zone warnings are sent only to moving vehicles approaching the active incident zone.
- Driver-to-authority messages are stored with owner, phone, plate and device context.

## Performance
- Dashboard no longer scans historical snapshots on every refresh.
- Vehicle directory uses indexed device/live-state queries instead of full snapshot history scans.
- Gunicorn is configured as one `gthread` worker with 24 threads to reduce the memory pressure seen with the previous 100-thread setup.

## Backup & Restore
- SQLite, JSON, full `.beaconbackup.zip`, and PDF snapshot downloads.
- Restore accepts `.db`, `.sqlite`, `.sqlite3`, `.json`, and full ZIP backups.
- A pre-restore safety backup is created before database replacement.
- JSON exports redact password hashes.
- Full archives include a checksum manifest.

## Render
Recommended Start Command:

```bash
gunicorn --workers 1 --worker-class gthread --threads 24 --bind 0.0.0.0:$PORT app:app
```

If a Render service has a manually configured Start Command, set it to the command above; changing only `Procfile` does not override a manually entered service command.

Chief administrator credentials remain in Render Environment Variables:

```text
ADMIN_NAME
ADMIN_PASSWORD
```

No public authority registration is enabled.
