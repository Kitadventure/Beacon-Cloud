# Beacon Cloud — Render deployment

## Build command

`pip install -r requirements.txt`

## Start command

`gunicorn -w 1 --threads 50 app:app --bind 0.0.0.0:$PORT`

## Health check

Set Render Health Check Path to `/healthz`. Do not use `/pulse_receiver` as a health check; it is an authenticated legacy ingestion endpoint and will correctly return HTTP 401 when no pulse credential is supplied.

## Required environment

Set a strong `FLASK_SECRET` (or `SECRET_KEY`). For protected production enrollment, set `PUBLIC_ENROLLMENT=0` and configure `ENROLLMENT_KEY`; the Android build can continue using the public demo enrollment mode for a controlled demonstration. Set `ADMIN_API_TOKEN` only for approved integrations.

For durable SQLite data/backups on Render, attach a persistent disk and mount it at `/var/data`, then set `BEACON_DATA_DIR=/var/data`. Without a persistent disk, a local SQLite database/backups can be lost when the service filesystem is replaced.

## First boot

1. Deploy.
2. Open `/healthz` and verify `ok: true`.
3. Open `/admin/login` and create the first administrator if the database has no admin.
4. Create Police/GK users from Access Management.
5. Create or enroll a test vehicle from the Android app.
6. Open Reports & Backups and create a full backup before demonstrations.

## 401 pulse logs

Requests to `/pulse_receiver` with no `X-Pulse-Token` are intentionally rejected. Existing external jobs that still call this legacy endpoint must be updated to send the configured `PULSE_TOKEN`; the Android app does not use this endpoint.
