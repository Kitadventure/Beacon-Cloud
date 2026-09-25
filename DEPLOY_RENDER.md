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
3. Open `/admin/login` and sign in with the chief administrator username/password stored in Render Environment Variables. Public registration is disabled.
4. Create Police/GK users from Access Management.
5. Create or enroll a test vehicle from the Android app.
6. Open Reports & Backups and create a full backup before demonstrations.

## Pulse receiver compatibility

Empty health probes to `/pulse_receiver` return a successful probe response. Real mobile telemetry can authenticate with its device token in the JSON body, `Authorization: Token ...`, `Authorization: Bearer ...`, or `X-Device-Token`. Integration-style callers can continue using `PULSE_TOKEN` or `ADMIN_API_TOKEN`.


## Chief administrator login

Set the chief administrator credentials in Render Environment Variables. Supported names are `ADMIN_USER` + `ADMIN_PASS` (preferred), or `ADMIN_USERNAME` + `ADMIN_PASSWORD`; the legacy `BOOTSTRAP_ADMIN_USERNAME` + `BOOTSTRAP_ADMIN_PASSWORD` names are also accepted. Public registration remains disabled. On boot and on login, the configured chief-admin username/password is synchronized to the persistent SQLite database, so a password changed in Render does not remain blocked by an older stored hash.

The application also accepts a device token in `Authorization: Token ...`, `Authorization: Bearer ...`, `X-Device-Token`, or the JSON `token` field for mobile telemetry. Integration pulse callers can continue using `PULSE_TOKEN` or `ADMIN_API_TOKEN`.
