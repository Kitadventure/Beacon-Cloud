from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parent
app = (ROOT / 'app.py').read_text()
legacy = (ROOT / 'app_legacy.py').read_text()
checks = {
    'secure secret requirement': 'must be configured' in app and 'dev-secret-change-me' not in app,
    'no bootstrap default': 'ChangeMeTemp@123' not in app and 'ChangeMeTemp@123' not in legacy,
    'police role socket': 'police_auth_v2' in app and 'session.get("auth_role")' in app,
    'gk role socket': 'gk_auth_v2' in app,
    'system errors': 'class SystemError' in app and '/admin/errors' in app,
    'sqlite backup': '/admin/backup/sqlite' in app and 'source.backup(target)' in app,
    'json authority backup': '/authority/backup/json' in app,
    'pdf reports': '/authority/report/summary.pdf' in app and 'reportlab' in app,
    'mock telemetry gate': 'SIMULATION_DISABLED' in app,
    'device heartbeat auth': 'DEVICE_AUTH_FAILED' in app,
    'lpr authentication': 'secure_ingest_plate' in app,
}
failed = [name for name, ok in checks.items() if not ok]
ast.parse(app); ast.parse(legacy)
print('Python AST: OK')
for name, ok in checks.items(): print(('OK   ' if ok else 'FAIL '), name)
if failed: raise SystemExit(1)
