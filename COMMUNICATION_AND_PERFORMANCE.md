# Live communication + performance repair

## Authority -> driver
- Admin/GK message dispatch still persists BroadcastDelivery rows.
- `/device/messages` remains pending-only for background polling, preventing repeated popups.
- `/device/messages/view` now renders message history, so a message does not disappear merely because the background poll marked it delivered.
- WebSocket `admin_message` remains supported for real-time delivery.

## Driver -> authority
- POST `/device/authority-message` stores an AuthorityInboxMessage and broadcasts `authority_inbox_message` to authenticated authority rooms.
- `/authority/inbox` shows owner, plate, registered phone, device and message history.

## User directory
- `/admin/message/search-devices` returns the full matching registered-device directory up to 5000 entries and searches phone numbers.
- Search is shown as a prominent full-width control and loads automatically on page open.

## Phone
- `Device.phone_number` is a first-class field. Existing phone data is backfilled from legacy `extra`.

## Connection state
- `REAL-TIME` = authenticated Socket.IO connection.
- `TELEMETRY ONLINE` = recent verified HTTP telemetry but no current socket connection.
- `OFFLINE` = neither is currently fresh.

## Performance
- Latest snapshots use grouped SQL instead of loading the entire snapshot history.
- Authority status caches its expensive SQLite integrity check for 60 seconds and status metrics for 2 seconds.
- Device list JSON no longer embeds raw heartbeat payloads; detail endpoints fetch them on demand.

## Render
The repository uses: `gunicorn -w 1 --threads 100 app:app --bind 0.0.0.0:$PORT`.
If Render Service Settings has a separate Start Command, it must be changed there too; a sync worker will not match the WebSocket deployment used by this service.
