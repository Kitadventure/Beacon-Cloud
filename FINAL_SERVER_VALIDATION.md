# Final server validation

This package is the final server-side master for the current Android contract.

Validated in this workspace:
- Python syntax compilation for `app.py`, `app_legacy.py`, `modern_ui.py`, simulator and backup tools.
- No remaining `M_ID` frontend reference in the modern authority code.
- No remaining malformed `cutoff` map parameter usage.
- Map API endpoint and sidebar route are registered in `install_modern_ui`.
- Dashboard no longer embeds a map.
- Normal `no live telemetry for device` bridge state is not recorded as a System Error.
- Device-targeted APIs continue to require device identity/token and filter by the authenticated device.
- Backup/restore and PDF/report routes remain present.

Runtime note: the build environment used for this validation did not have Flask/SQLAlchemy installed, so a live Gunicorn boot and Render-host integration test could not be executed here.
