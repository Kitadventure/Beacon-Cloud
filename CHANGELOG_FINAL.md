# Beacon Cloud — final server pass

## 2026-09-25
- Fixed `/modern/api/map`: undefined `cutoff` and malformed SQL parameter mapping were removed.
- Live authority map now uses `live_vehicle_state` as the last-known vehicle source, excludes revoked devices, and clusters dense viewports server-side.
- Restored/offline vehicles remain represented on the map with an explicit connection state.
- Removed the embedded map card from the dashboard; the map remains a dedicated sidebar destination.
- Added a backward-compatible `admin_traffic_zones` endpoint alias for older authority links.
- Bridge `no live telemetry for device` is now treated as a normal availability state rather than a repeated System Error.
- Authority directory/place display prefers readable cached place names and only exposes exact coordinates inside deliberate technical details.
- Reduced browser/API work in the dashboard and kept historical telemetry out of high-frequency list refreshes.
- Retained role-specific access: device clients remain bound to their own device ID/token; authority users use role-authenticated server sessions.
