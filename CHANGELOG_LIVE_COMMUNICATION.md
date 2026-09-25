Beacon Cloud live communication/performance pass

- First-class Device.phone_number with migration/backfill from legacy extra.
- User directory loads on page open, prominently visible search, phone included.
- Driver -> Authority inbox with persistent SQLite storage + real-time authority event.
- Authority -> Driver history now includes delivered messages instead of disappearing after first poll.
- Live-device state distinguishes REAL-TIME socket from TELEMETRY ONLINE heartbeat.
- Grouped latest-snapshot SQL replaces full-history Python scan.
- Authority status caches SQLite integrity check and status for faster polling.
- Optimized admin device/vehicle JSON payloads omit raw heartbeat data from list calls.
- Explicit Gunicorn threaded start command for Socket.IO on Render.
