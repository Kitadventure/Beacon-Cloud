# Beacon Cloud — Render deployment

## Build command

`pip install -r requirements.txt`

## Start command

`gunicorn -w 1 --worker-class gthread --threads 100 app:app --bind 0.0.0.0:$PORT`

## Health check

Set Render Health Check Path to `/healthz`. Do not use `/pulse_receiver` as a health check; it is an authenticated legacy ingestion endpoint and will correctly return HTTP 401 when no pulse credential is supplied.

## Required environment

For the chief administrator, set only `ADMIN_NAME` and `ADMIN_PASSWORD` in Render Environment Variables. No admin registration is required. The application generates its session secret locally on first boot and keeps it with the persistent data directory; it is not a required Render credential. For protected device enrollment, set `PUBLIC_ENROLLMENT=0` and configure `ENROLLMENT_KEY`.

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

Set exactly these two Render Environment Variables:

- `ADMIN_NAME` — chief administrator username
- `ADMIN_PASSWORD` — chief administrator password

The login path checks this pair directly before consulting SQLite, so an old persistent password hash cannot reject a newly configured Render password. Public administrator registration is disabled.

The application also accepts a device token in `Authorization: Token ...`, `Authorization: Bearer ...`, `X-Device-Token`, or the JSON `token` field for mobile telemetry. Integration pulse callers can continue using `PULSE_TOKEN` or `ADMIN_API_TOKEN`.
