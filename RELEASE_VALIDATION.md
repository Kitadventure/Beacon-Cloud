# Release validation

Static release checks completed:

- `app.py` Python AST: PASS
- `app_legacy.py` Python AST: PASS
- `modern_ui.py` Python AST: PASS
- Modern authority UI integration present: PASS
- Durable `DeviceAlert` model present: PASS
- JSON/SQLite/full backup routes present: PASS
- Restore form uses expected `backup` multipart field: PASS
- Gunicorn `gthread` / 24-thread deployment configuration present: PASS
- No compiled Android or RealMart source is part of this server package.

Runtime verification still depends on the deployment environment and database state; the workspace used to prepare this package does not include the Render runtime itself.
