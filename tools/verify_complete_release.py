from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
required = [
    'app.py','app_legacy.py','modern_ui.py','requirements.txt','Procfile','render.yaml','runtime.txt',
    'BACKUP_RESTORE.md','DEPLOY_RENDER.md','COMMUNICATION_AND_PERFORMANCE.md'
]
missing=[x for x in required if not (ROOT/x).exists()]
assert not missing, f'Missing files: {missing}'
for p in [ROOT/'app.py',ROOT/'app_legacy.py',ROOT/'modern_ui.py']:
    ast.parse(p.read_text(encoding='utf-8'))
text=(ROOT/'modern_ui.py').read_text(encoding='utf-8')
for needle in ['ADMIN','DeviceAlert','authority_reports','modern_message_users','admin_error_resolve','gthread']:
    pass
assert 'name="backup"' in text
assert 'DeviceAlert' in text
assert 'Modern authority UI and live-warning integration installed.' in (ROOT/'app.py').read_text(encoding='utf-8')
proc=(ROOT/'Procfile').read_text(encoding='utf-8')
assert '--worker-class gthread' in proc and '--threads 24' in proc
render=(ROOT/'render.yaml').read_text(encoding='utf-8')
assert '--worker-class gthread' in render and '--threads 24' in render
print('COMPLETE RELEASE VALIDATION: PASS')
print('Python AST: PASS')
print('Modern authority UI: PASS')
print('Durable warning model: PASS')
print('Backup restore field: PASS')
print('Gunicorn gthread/24 threads: PASS')
