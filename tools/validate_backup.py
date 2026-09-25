#!/usr/bin/env python3
"""Validate Beacon SQLite/JSON/full-backup files without starting the web app."""
from __future__ import annotations
import hashlib, json, sqlite3, sys, zipfile
from pathlib import Path

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def validate_sqlite(path: Path) -> list[str]:
    with sqlite3.connect(path) as c:
        result = c.execute("PRAGMA integrity_check").fetchone()
        if not result or str(result[0]).lower() != "ok":
            raise RuntimeError(f"SQLite integrity check failed: {result[0] if result else 'no result'}")
        tables = {str(x[0]) for x in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"device", "snapshot"}.issubset(tables):
        raise RuntimeError("Not a Beacon database: required device/snapshot tables are missing")
    return sorted(tables)

def validate_json_bytes(data: bytes) -> int:
    payload = json.loads(data.decode("utf-8"))
    tables = payload.get("tables") if isinstance(payload, dict) else None
    if not isinstance(tables, dict):
        tables = {k:v for k,v in payload.items() if isinstance(v, list)} if isinstance(payload, dict) else {}
    if not tables:
        raise RuntimeError("JSON backup contains no table records")
    return sum(len(v) for v in tables.values() if isinstance(v, list))

def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python tools/validate_backup.py FILE", file=sys.stderr)
        return 2
    path = Path(sys.argv[1]).expanduser().resolve()
    if not path.is_file():
        print(f"ERROR: file not found: {path}", file=sys.stderr); return 2
    try:
        suffix = path.suffix.lower()
        if suffix in {".db", ".sqlite", ".sqlite3"}:
            tables = validate_sqlite(path)
            print(f"OK SQLite: {path.name}; tables={len(tables)}; sha256={sha256_bytes(path.read_bytes())}")
        elif suffix == ".json":
            rows = validate_json_bytes(path.read_bytes())
            print(f"OK JSON: {path.name}; rows={rows}; sha256={sha256_bytes(path.read_bytes())}")
        elif suffix == ".zip" or path.name.lower().endswith(".beaconbackup.zip"):
            with zipfile.ZipFile(path) as zf:
                names = [n for n in zf.namelist() if not n.endswith("/")]
                if "manifest.json" not in names:
                    raise RuntimeError("Full backup is missing manifest.json")
                manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
                verified = []
                for item in manifest.get("files", []):
                    name = item.get("name")
                    if not name or name not in names:
                        raise RuntimeError(f"Manifest file missing: {name}")
                    data = zf.read(name)
                    got = sha256_bytes(data)
                    if got != item.get("sha256"):
                        raise RuntimeError(f"Checksum mismatch: {name}")
                    verified.append(name)
                    if name.lower().endswith((".db", ".sqlite", ".sqlite3")):
                        tmp = path.parent / (path.stem + "__validate.sqlite3")
                        tmp.write_bytes(data)
                        try: validate_sqlite(tmp)
                        finally: tmp.unlink(missing_ok=True)
                    elif name.lower().endswith(".json"):
                        validate_json_bytes(data)
                    elif name.lower().endswith(".pdf") and not data.startswith(b"%PDF"):
                        raise RuntimeError(f"Invalid PDF member: {name}")
                print(f"OK full backup: {path.name}; verified={', '.join(verified)}; sha256={sha256_bytes(path.read_bytes())}")
        else:
            raise RuntimeError("Supported types: .db .sqlite .sqlite3 .json .zip")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
