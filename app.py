"""Beacon Cloud — hardened compatibility wrapper around the original service.

This version keeps the original feature set while adding:
- safer authentication/provisioning (no default bootstrap credentials)
- live telemetry integrity fields and no implicit device recreation
- improved live-cache performance for nearby calculations
- role-aware police/GK Socket.IO channels
- persistent system-error dashboard with browser/device error reporting
- SQLite and JSON backup downloads
- role-aware PDF reporting + a centralized authority Reports & Backups page
- persistent operational incidents for accident/watch events
- modern security headers and request IDs

The original implementation is preserved in app_legacy.py so the new service can
remain backward-compatible while giving the repaired entrypoint a clean boundary.
"""
from __future__ import annotations

import io
import json
import os
import re
import sqlite3
import tempfile
import threading
import shutil
import zipfile
import hashlib
import secrets
import time
import traceback
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from flask import Response, abort, flash, g, has_request_context, jsonify, redirect, render_template_string, request, send_file, session, url_for
from werkzeug.exceptions import HTTPException

import app_legacy as legacy

app = legacy.app
db = legacy.db

# Public model aliases retained for convenience.
Admin = legacy.Admin
Device = legacy.Device
Snapshot = legacy.Snapshot
Road = legacy.Road
OverspeedEvent = legacy.OverspeedEvent
PoliceUser = legacy.PoliceUser
GKUser = legacy.GKUser
TrafficZone = legacy.TrafficZone
BroadcastMessage = legacy.BroadcastMessage
BroadcastDelivery = legacy.BroadcastDelivery
Watchlist = legacy.Watchlist
PlateSighting = legacy.PlateSighting

# ---------------------------------------------------------------------------
# Configuration / process state
# ---------------------------------------------------------------------------
# Prefer an explicitly supplied secret. When omitted, generate a durable secret
# beside the database so the service can boot cleanly and retain sessions when
# a persistent data disk is attached.
_secret_value = os.environ.get("FLASK_SECRET") or os.environ.get("SECRET_KEY")
if not _secret_value:
    _secret_dir = Path(os.environ.get("BEACON_DATA_DIR") or ("/var/data" if Path("/var/data").is_dir() else str(Path(app.instance_path))))
    _secret_dir.mkdir(parents=True, exist_ok=True)
    _secret_file = _secret_dir / ".flask_secret"
    try:
        _secret_value = _secret_file.read_text(encoding="utf-8").strip() if _secret_file.exists() else ""
        if not _secret_value:
            _secret_value = secrets.token_urlsafe(48)
            _secret_file.write_text(_secret_value, encoding="utf-8")
            try: _secret_file.chmod(0o600)
            except Exception: pass
    except Exception:
        _secret_value = secrets.token_urlsafe(48)
app.secret_key = _secret_value

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "0") == "1",
    MAX_CONTENT_LENGTH=int(os.environ.get("MAX_UPLOAD_MB", "100")) * 1024 * 1024,
    JSON_SORT_KEYS=False,
)

# Narrow Socket.IO CORS when the deployment provides an explicit allow-list.
_socket_origins = [x.strip() for x in os.environ.get("SOCKET_ALLOWED_ORIGINS", "").split(",") if x.strip()]
if _socket_origins:
    try:
        legacy.socketio.server.eio.cors_allowed_origins = _socket_origins
    except Exception:
        pass

LIVE_CACHE_TTL_S = float(os.environ.get("LIVE_CACHE_TTL_S", "15"))
SNAPSHOT_RETENTION_S = float(os.environ.get("SNAPSHOT_RETENTION_S", str(7 * 24 * 3600)))
CLEANUP_INTERVAL_S = float(os.environ.get("CLEANUP_INTERVAL_S", "60"))
HEARTBEAT_MIN_INTERVAL_S = float(os.environ.get("HEARTBEAT_MIN_INTERVAL_S", "0.75"))
MAX_HEARTBEAT_JSON_BYTES = int(os.environ.get("MAX_HEARTBEAT_JSON_BYTES", "65536"))
NEARBY_DEFAULT_RADIUS_M = float(os.environ.get("NEARBY_DEFAULT_RADIUS_M", "1000"))
UNSAFE_TTC_SECONDS = float(os.environ.get("UNSAFE_TTC_SECONDS", "6.0"))
CONFIRMATION_RADIUS_M = float(os.environ.get("CONFIRMATION_RADIUS_M", "30.0"))
ALLOW_SIMULATION = os.environ.get("ALLOW_SIMULATION", "0") == "1"
ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN")
ENROLLMENT_KEY = os.environ.get("ENROLLMENT_KEY")
PUBLIC_ENROLLMENT = os.environ.get("PUBLIC_ENROLLMENT", "1") == "1"
BACKUP_RETENTION_COUNT = int(os.environ.get("BACKUP_RETENTION_COUNT", "30"))
DATA_DIR = Path(os.environ.get("BEACON_DATA_DIR") or ("/var/data" if Path("/var/data").is_dir() else str(Path(app.instance_path))))
DATA_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR = Path(os.environ.get("BEACON_BACKUP_DIR") or (DATA_DIR / "backups"))
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR = BACKUP_DIR / "incoming"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

_last_heartbeat_at: dict[str, float] = {}
_last_sequence: dict[tuple[str, str], int] = {}
_last_cleanup_at = 0.0
_live_cache: dict[str, dict[str, Any]] = {}
_live_cache_lock = threading.RLock()
_connected_lock = threading.Lock()

# The legacy module already owns this dict; use it so legacy routes/events remain compatible.
connected_sockets = legacy.connected_sockets

# ---------------------------------------------------------------------------
# Additional operational models
# ---------------------------------------------------------------------------
class SystemError(db.Model):
    __tablename__ = "system_error"
    id = db.Column(db.Integer, primary_key=True)
    occurred_at = db.Column(db.DateTime, default=datetime.utcnow, index=True, nullable=False)
    severity = db.Column(db.String(16), default="ERROR", index=True)
    source = db.Column(db.String(128), default="server", index=True)
    error_type = db.Column(db.String(255), index=True)
    message = db.Column(db.Text)
    route = db.Column(db.String(512))
    method = db.Column(db.String(16))
    request_id = db.Column(db.String(64), index=True)
    username = db.Column(db.String(128))
    resolved = db.Column(db.Boolean, default=False, index=True)
    traceback_text = db.Column(db.Text)


class LiveVehicleState(db.Model):
    __tablename__ = "live_vehicle_state"
    device_id = db.Column(db.String(36), primary_key=True)
    snapshot_id = db.Column(db.Integer, nullable=True, index=True)
    ts = db.Column(db.DateTime, nullable=False, index=True)
    lat = db.Column(db.Float, nullable=False)
    lon = db.Column(db.Float, nullable=False)
    speed_mps = db.Column(db.Float, nullable=False, default=0.0)
    bearing_deg = db.Column(db.Float, nullable=False, default=0.0)
    heading_deg = db.Column(db.Float, nullable=False, default=0.0)
    accuracy_m = db.Column(db.Float, nullable=True)
    sequence = db.Column(db.Integer, nullable=True)
    session_id = db.Column(db.String(96), nullable=True)
    source = db.Column(db.String(32), nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class OperationalIncident(db.Model):
    __tablename__ = "operational_incident"
    id = db.Column(db.String(36), primary_key=True, default=lambda: uuid.uuid4().hex)
    type = db.Column(db.String(64), index=True, nullable=False)
    severity = db.Column(db.String(16), default="medium", index=True)
    confidence = db.Column(db.Float, default=0.0)
    device_id = db.Column(db.String(36), index=True)
    plate = db.Column(db.String(64))
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    reason = db.Column(db.Text)
    status = db.Column(db.String(24), default="active", index=True)
    evidence = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    resolved_at = db.Column(db.DateTime, nullable=True)

with app.app_context():
    db.create_all()
    if db.engine.url.get_backend_name() == "sqlite":
        try:
            with db.engine.begin() as conn:
                conn.exec_driver_sql("PRAGMA journal_mode=WAL")
                conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
                conn.exec_driver_sql("PRAGMA busy_timeout=15000")
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _role() -> str | None:
    return session.get("auth_role") or ("admin" if session.get("admin_logged") else None)


def _require_authority(allowed: set[str] | None = None) -> str:
    current = _role()
    allowed = allowed or {"admin", "police", "gk"}
    if current not in allowed:
        abort(401, "Authorized authority login required")
    return current


def _request_id() -> str:
    return getattr(g, "beacon_request_id", None) or str(uuid.uuid4())


def _safe_text(value: Any, limit: int = 12000) -> str:
    return str(value if value is not None else "")[:limit]


def _api_error(message: str, status: int = 400, code: str | None = None):
    payload = {"ok": False, "error": message, "request_id": _request_id()}
    if code:
        payload["code"] = code
    return jsonify(payload), status


def record_system_error(
    exc: BaseException,
    *,
    source: str = "server",
    severity: str = "ERROR",
    route: str | None = None,
    request_id: str | None = None,
    username: str | None = None,
) -> int | None:
    """Persist diagnostics, with a file fallback when the DB is unavailable."""
    rid = request_id or _request_id()
    in_request = has_request_context()
    route_value = route or (request.path if in_request else "")
    method = request.method if in_request else ""
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    try:
        db.session.rollback()
        row = SystemError(
            severity=severity,
            source=_safe_text(source, 128),
            error_type=type(exc).__name__,
            message=_safe_text(exc),
            route=_safe_text(route_value, 512),
            method=_safe_text(method, 16),
            request_id=_safe_text(rid, 64),
            username=_safe_text(username or (session.get("username") if in_request else ""), 128),
            traceback_text=_safe_text(tb, 20000),
        )
        db.session.add(row)
        db.session.commit()
        return row.id
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        try:
            fallback = Path(app.instance_path) / "system_errors_fallback.log"
            fallback.parent.mkdir(parents=True, exist_ok=True)
            with fallback.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({
                    "occurred_at": datetime.utcnow().isoformat() + "Z",
                    "severity": severity,
                    "source": source,
                    "error_type": type(exc).__name__,
                    "message": _safe_text(exc, 4000),
                    "route": route_value,
                    "method": method,
                    "request_id": rid,
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        return None


def _parse_json_body() -> dict[str, Any]:
    if not request.is_json:
        return {}
    try:
        body = request.get_json(silent=False)
    except Exception as exc:
        record_system_error(exc, source="json", severity="WARNING")
        raise ValueError("Request body contains invalid JSON") from exc
    if body is None:
        return {}
    if not isinstance(body, dict):
        raise ValueError("JSON request body must be an object")
    return body


def _db_is_sqlite() -> bool:
    try:
        return db.engine.url.get_backend_name() == "sqlite"
    except Exception:
        return str(db.engine.url).startswith("sqlite:")


def _sqlite_path() -> Path | None:
    if not _db_is_sqlite():
        return None
    db_name = db.engine.url.database
    if not db_name or db_name == ":memory:":
        return None
    path = Path(db_name)
    if not path.is_absolute():
        path = Path(app.instance_path) / path
    return path


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return value


def _device_connected(device_id: str) -> bool:
    with _connected_lock:
        return bool(connected_sockets.get(device_id))


def _latest_live_entries(exclude: str | None = None) -> list[tuple[str, dict[str, Any]]]:
    now = datetime.utcnow()
    cutoff = now - timedelta(seconds=LIVE_CACHE_TTL_S)
    rows: dict[str, dict[str, Any]] = {}
    with _live_cache_lock:
        stale = [k for k, v in _live_cache.items() if v.get("ts") is None or v["ts"] < cutoff]
        for k in stale:
            _live_cache.pop(k, None)
        for device_id, value in _live_cache.items():
            if device_id != exclude and value.get("ts") and value["ts"] >= cutoff:
                rows[device_id] = dict(value)
    try:
        cached_ids = set(rows)
        live_rows = LiveVehicleState.query.filter(LiveVehicleState.updated_at >= cutoff).all()
        for row in live_rows:
            if row.device_id == exclude:
                continue
            if row.device_id not in cached_ids or row.updated_at > rows[row.device_id].get("ts", datetime.min):
                rows[row.device_id] = {
                    "lat": row.lat, "lon": row.lon, "speed_mps": row.speed_mps,
                    "bearing_deg": row.bearing_deg, "heading_deg": row.heading_deg,
                    "ts": row.ts, "raw": None, "accuracy_m": row.accuracy_m,
                    "sequence": row.sequence, "session_id": row.session_id, "source": row.source
                }
    except Exception as exc:
        record_system_error(exc, source="cache:live-state", severity="WARNING")
    return list(rows.items())

def _haversine_m(lat1, lon1, lat2, lon2):
    return legacy.haversine_m(lat1, lon1, lat2, lon2)


def _classify_risk(self_snap, other_snap):
    if not self_snap or not other_snap:
        return {"decision": "no_decision", "confidence": 0.0, "reason": "missing_data"}
    d = _haversine_m(self_snap.lat, self_snap.lon, other_snap.lat, other_snap.lon)
    direction = legacy.classify_direction(self_snap.bearing_deg or 0.0, other_snap.bearing_deg or 0.0)
    close = legacy.closing_speed_mps(
        self_snap.lat, self_snap.lon, self_snap.speed_mps or 0.0, self_snap.bearing_deg or 0.0,
        other_snap.lat, other_snap.lon, other_snap.speed_mps or 0.0, other_snap.bearing_deg or 0.0,
    )
    ttc = d / close if close > 0.05 else float("inf")
    age_self = max(0.0, (datetime.utcnow() - self_snap.ts).total_seconds())
    age_other = max(0.0, (datetime.utcnow() - other_snap.ts).total_seconds())
    # Accuracy is optionally stored in the raw telemetry metadata.
    def accuracy(obj):
        try:
            raw = json.loads(obj.raw) if isinstance(obj.raw, str) else (obj.raw or {})
            return float(raw.get("_server_integrity", {}).get("accuracy_m") or raw.get("accuracy_m") or 999.0)
        except Exception:
            return 999.0
    acc_self = accuracy(self_snap)
    acc_other = accuracy(other_snap)
    recency = max(0.0, 1.0 - max(age_self, age_other) / max(1.0, CLEANUP_STALE_SECONDS))
    accuracy_score = max(0.0, min(1.0, 1.0 - (acc_self + acc_other) / 200.0))
    distance_score = max(0.0, 1.0 - d / max(1.0, NEARBY_DEFAULT_RADIUS_M))
    confidence = max(0.0, min(1.0, 0.35 * recency + 0.35 * accuracy_score + 0.30 * distance_score))

    if max(age_self, age_other) > CLEANUP_STALE_SECONDS:
        return {"decision": "no_decision", "confidence": round(confidence, 2), "reason": "stale_telemetry"}
    if min(acc_self, acc_other) > 100:
        return {"decision": "no_decision", "confidence": round(confidence, 2), "reason": "poor_location_accuracy"}

    if direction == "opposite":
        if ttc != float("inf") and ttc < UNSAFE_TTC_SECONDS:
            return {"decision": "red", "confidence": round(min(1.0, confidence + 0.25), 2), "reason": f"opposite_ttc_{ttc:.1f}s"}
        return {"decision": "clear", "confidence": round(confidence, 2), "reason": "opposite_no_imminent_collision"}

    if direction == "same":
        required = legacy.estimate_overtake_time_mps(self_snap.speed_mps or 0.0, other_snap.speed_mps or 0.0)
        if self_snap.speed_mps <= other_snap.speed_mps + 0.01:
            return {"decision": "no_decision", "confidence": round(confidence, 2), "reason": "same_no_overtake_advantage"}
        if ttc != float("inf") and ttc < max(UNSAFE_TTC_SECONDS, required * 1.5):
            return {"decision": "red", "confidence": round(min(1.0, confidence + 0.20), 2), "reason": f"same_gap_ttc_{ttc:.1f}s"}
        gap = d
        if gap < max(12.0, (self_snap.speed_mps or 0.0) * required * 0.5):
            return {"decision": "caution", "confidence": round(confidence, 2), "reason": "same_gap_low"}
        return {"decision": "clear", "confidence": round(confidence, 2), "reason": "same_direction_gap_sufficient"}

    if direction == "cross" and ttc != float("inf") and ttc < UNSAFE_TTC_SECONDS * 0.8:
        return {"decision": "red", "confidence": round(min(1.0, confidence + 0.10), 2), "reason": f"cross_ttc_{ttc:.1f}s"}
    return {"decision": "caution", "confidence": round(confidence, 2), "reason": "crossing_trajectory"}


def compute_nearby_v2(device_id: str, radius_m: float = NEARBY_DEFAULT_RADIUS_M) -> dict[str, Any]:
    self_snap = None
    with _live_cache_lock:
        entry = _live_cache.get(device_id)
    if entry and entry.get("ts") and entry["ts"] >= datetime.utcnow() - timedelta(seconds=LIVE_CACHE_TTL_S):
        class Tmp: pass
        self_snap = Tmp()
        self_snap.device_id = device_id
        self_snap.lat = entry["lat"]
        self_snap.lon = entry["lon"]
        self_snap.speed_mps = entry.get("speed_mps") or 0.0
        self_snap.bearing_deg = entry.get("bearing_deg") or 0.0
        self_snap.ts = entry["ts"]
        self_snap.raw = entry.get("raw")
    if not self_snap:
        state = LiveVehicleState.query.filter_by(device_id=device_id).first()
        if state and state.updated_at and state.updated_at >= datetime.utcnow() - timedelta(seconds=LIVE_CACHE_TTL_S):
            class Tmp: pass
            self_snap = Tmp()
            self_snap.device_id = device_id; self_snap.lat = state.lat; self_snap.lon = state.lon
            self_snap.speed_mps = state.speed_mps or 0.0; self_snap.bearing_deg = state.bearing_deg or 0.0
            self_snap.ts = state.ts; self_snap.raw = None
    if not self_snap:
        self_snap = Snapshot.query.filter_by(device_id=device_id).order_by(Snapshot.ts.desc()).first()
    if not self_snap:
        with legacy.active_devices_lock:
            entry = legacy.active_devices.get(device_id)
        if not entry:
            raise RuntimeError("no live telemetry for device")
        class Tmp: pass
        self_snap = Tmp(); self_snap.device_id = device_id; self_snap.lat = entry["lat"]; self_snap.lon = entry["lon"]
        self_snap.speed_mps = entry.get("speed_mps") or 0.0; self_snap.bearing_deg = entry.get("bearing_deg") or 0.0
        self_snap.ts = entry["ts"]; self_snap.raw = entry.get("raw")

    results = []
    device_rows: dict[str, Device] = {}
    live_entries = _latest_live_entries(exclude=device_id)
    if live_entries:
        ids = [i for i, _ in live_entries]
        for row in Device.query.filter(Device.id.in_(ids)).all():
            device_rows[row.id] = row

    for other_id, entry in live_entries:
        try:
            d = _haversine_m(self_snap.lat, self_snap.lon, entry["lat"], entry["lon"])
        except Exception:
            continue
        if d > radius_m:
            continue
        class Tmp2: pass
        other = Tmp2()
        other.device_id = other_id
        other.lat = entry["lat"]
        other.lon = entry["lon"]
        other.speed_mps = entry.get("speed_mps") or 0.0
        other.bearing_deg = entry.get("bearing_deg") or 0.0
        other.ts = entry["ts"]
        other.raw = entry.get("raw")
        direction = legacy.classify_direction(self_snap.bearing_deg or 0.0, other.bearing_deg or 0.0)
        closing = legacy.closing_speed_mps(
            self_snap.lat, self_snap.lon, self_snap.speed_mps or 0.0, self_snap.bearing_deg or 0.0,
            other.lat, other.lon, other.speed_mps, other.bearing_deg,
        )
        risk = _classify_risk(self_snap, other)
        row = device_rows.get(other_id)
        results.append({
            "device_id": other_id,
            "ts": other.ts.isoformat(),
            "lat": other.lat,
            "lon": other.lon,
            "distance_m": round(d, 2),
            "direction": direction,
            "speed_mps": round(other.speed_mps, 2),
            "bearing_deg": round(other.bearing_deg, 1),
            "closing_mps": round(closing, 2),
            "decision": risk["decision"],
            "confidence": risk["confidence"],
            "reason": risk["reason"],
            "owner": row.owner if row else None,
            "car_name": row.car_name if row else None,
            "car_model": row.car_model if row else None,
            "plate": row.plate if row else None,
        })
    results.sort(key=lambda x: x["distance_m"])

    own = Device.query.get(device_id)
    payload = {
        "self": {
            "device_id": device_id,
            "lat": self_snap.lat,
            "lon": self_snap.lon,
            "speed_mps": round(self_snap.speed_mps or 0.0, 2),
            "bearing_deg": round(self_snap.bearing_deg or 0.0, 1),
            "ts": self_snap.ts.isoformat(),
            "meta": {
                "owner": own.owner if own else None,
                "car_name": own.car_name if own else None,
                "car_model": own.car_model if own else None,
                "plate": own.plate if own else None,
            },
        },
        "nearby": results,
    }
    return payload


legacy.compute_nearby_for_device = compute_nearby_v2
legacy.classify_risk = _classify_risk
legacy.restore_device_if_missing = lambda *args, **kwargs: None


def _send_ws(device_id: str, event: str, payload: dict[str, Any]) -> bool:
    with _connected_lock:
        sids = set(connected_sockets.get(device_id, set()))
    if not sids:
        return False
    failed = set()
    for sid in sids:
        try:
            legacy.socketio.emit(event, payload, room=sid)
        except Exception as exc:
            failed.add(sid)
            record_system_error(exc, source="socket", severity="WARNING")
    if failed:
        with _connected_lock:
            current = connected_sockets.get(device_id, set())
            current.difference_update(failed)
            if not current:
                connected_sockets.pop(device_id, None)
    return bool(sids - failed)


legacy.send_ws_to_device = _send_ws

# ---------------------------------------------------------------------------
# Authentication / provisioning compatibility overrides
# ---------------------------------------------------------------------------

# Render/production chief administrator credentials.
# The deployment may keep these in Render Environment Variables rather than in the database.
# Supporting both naming conventions keeps older deployments working while avoiding public registration.
def _render_admin_credentials():
    username = (
        os.environ.get("ADMIN_USER")
        or os.environ.get("ADMIN_USERNAME")
        or os.environ.get("BOOTSTRAP_ADMIN_USERNAME")
    )
    password = (
        os.environ.get("ADMIN_PASS")
        or os.environ.get("ADMIN_PASSWORD")
        or os.environ.get("BOOTSTRAP_ADMIN_PASSWORD")
    )
    return (username.strip() if username else None), (password if password else None)


def _sync_render_chief_admin():
    """Make Render's chief-admin variables authoritative for the first admin account.

    This intentionally does not create a public registration flow. On a persistent database,
    it updates the hash for the configured chief-admin username so changing the Render secret
    is immediately reflected without requiring a manual database reset.
    """
    username, password = _render_admin_credentials()
    if not username or not password:
        return None
    try:
        existing = Admin.query.filter_by(username=username).first()
        if existing is None:
            existing = Admin(username=username, password_hash=legacy.generate_password_hash(password))
            db.session.add(existing)
        else:
            # Always resync the stored hash when the deployment explicitly supplies the secret.
            if not legacy.check_password_hash(existing.password_hash, password):
                existing.password_hash = legacy.generate_password_hash(password)
        db.session.commit()
        try:
            row = getattr(legacy, "_bootstrap_state_row", lambda **_: None)(create_if_missing=True)
            if row is not None:
                row.value = "0"
                db.session.commit()
        except Exception:
            db.session.rollback()
        return existing
    except Exception as exc:
        db.session.rollback()
        record_system_error(exc, source="auth:render-admin-sync", severity="ERROR")
        return None

SECURE_LOGIN_HTML = """
<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Beacon Authority Login</title>
<style>body{font-family:Inter,system-ui,-apple-system,"Segoe UI",Roboto,Arial;background:radial-gradient(circle at top,#14324b,#071522 60%);color:#e5eefb;margin:0;min-height:100vh;display:grid;place-items:center}.card{width:min(560px,calc(100% - 36px));background:#0d1d2e;border:1px solid #29435a;border-radius:22px;padding:28px;box-shadow:0 20px 60px #0007}.eyebrow{font-size:12px;letter-spacing:.12em;color:#7dd3fc;text-transform:uppercase}.muted{color:#94a3b8;line-height:1.5}label{display:block;margin-top:14px;font-weight:700}input{width:100%;box-sizing:border-box;margin-top:7px;padding:13px;border:1px solid #29435a;background:#081525;color:#fff;border-radius:12px}button,a{display:inline-block;margin-top:18px;padding:12px 16px;border-radius:12px;border:0;background:#0e86ad;color:#fff;text-decoration:none;font-weight:800;cursor:pointer}.flash{margin-top:14px;background:#3b1e24;color:#fecaca;border:1px solid #7f1d1d;padding:12px;border-radius:12px}</style></head>
<body><div class="card"><div class="eyebrow">Beacon Cloud</div><h1>Authority access</h1><p class="muted">Use the chief administrator credentials configured in Render, or an authority account created by an administrator. Public registration is disabled.</p>{% with messages=get_flashed_messages() %}{% if messages %}<div class="flash">{{ messages[0] }}</div>{% endif %}{% endwith %}<form method="post"><label>Username</label><input name="username" autocomplete="username" required><label>Password</label><input name="password" type="password" autocomplete="current-password" required><input type="hidden" name="next" value="{{ next_path or '' }}"><button type="submit">Sign in</button></form>{% if allow_register %}<p class="muted">This installation has no administrator yet.</p><a href="{{ url_for('admin_register') }}">Create first administrator</a>{% endif %}</div></body></html>
"""


with app.app_context():
    _sync_render_chief_admin()

def _pick_user(username: str):
    for model, role in ((Admin, "admin"), (PoliceUser, "police"), (GKUser, "gk")):
        obj = model.query.filter_by(username=username).first()
        if obj:
            return obj, role
    return None, None


def _set_session(username: str, role: str):
    session.clear()
    session.update({"username": username, "user_id": username, "auth_role": role,
                    "admin_logged": role == "admin", "police_logged": role == "police",
                    "gk_logged": role == "gk", "admin_user": username if role == "admin" else None})


def secure_admin_login():
    if request.method == "GET":
        # Public registration is never exposed. The configured Render credentials are the
        # deployment's chief-admin login and are synchronized during startup/login.
        return render_template_string(SECURE_LOGIN_HTML, allow_register=False, next_path=request.args.get("next", ""))

    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    if not username or not password:
        flash("Enter the administrator username and password.")
        return render_template_string(SECURE_LOGIN_HTML, allow_register=False, next_path=request.form.get("next", "")), 400

    env_username, env_password = _render_admin_credentials()
    # Render environment variables are authoritative for the chief administrator.
    # This works even when a persistent SQLite database contains an older password hash.
    if env_username and env_password and username == env_username and password == env_password:
        with db.session.no_autoflush:
            _sync_render_chief_admin()
        _set_session(env_username, "admin")
        next_path = request.form.get("next") or request.args.get("next")
        if next_path and next_path.startswith("/") and not next_path.startswith("//"):
            return redirect(next_path)
        return redirect(url_for("dashboard"))

    user, role = _pick_user(username)
    if not user or not legacy.check_password_hash(user.password_hash, password):
        flash("Invalid credentials")
        return render_template_string(SECURE_LOGIN_HTML, allow_register=False, next_path=request.form.get("next", "")), 401

    _set_session(user.username, role)
    next_path = request.form.get("next") or request.args.get("next")
    if next_path and next_path.startswith("/") and not next_path.startswith("//"):
        return redirect(next_path)
    if role == "admin":
        return redirect(url_for("dashboard"))
    if role == "police":
        return redirect(url_for("police_dashboard"))
    return redirect(url_for("gk_dashboard"))


def secure_admin_register():
    if Admin.query.count() > 0:
        flash("Administrator registration is closed. An existing admin must create additional accounts.")
        return redirect(url_for("admin_login"))
    if request.method == "GET":
        return render_template_string("""
        <!doctype html><html><head><meta name='viewport' content='width=device-width,initial-scale=1'><title>First Administrator</title><style>body{font-family:Inter,system-ui;background:#071522;color:#e5eefb;margin:0;padding:30px}.card{max-width:580px;margin:auto;background:#0d1d2e;border:1px solid #29435a;border-radius:18px;padding:24px}input{width:100%;box-sizing:border-box;margin:7px 0 14px;padding:12px;border-radius:10px;border:1px solid #29435a;background:#081525;color:#fff}button{padding:12px 16px;border:0;border-radius:10px;background:#1188ae;color:#fff;font-weight:800}</style></head><body><div class='card'><h1>First administrator</h1><p>This one-time setup closes as soon as an administrator exists.</p><form method='post'><label>Username</label><input name='username' minlength='3' required><label>Password</label><input name='password' type='password' minlength='10' required><label>Confirm password</label><input name='password2' type='password' minlength='10' required><button>Create administrator</button></form></div></body></html>
        """)
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    password2 = request.form.get("password2") or ""
    if len(username) < 3 or len(password) < 10:
        flash("Use at least 3 characters for the username and 10 for the password.")
        return redirect(url_for("admin_register"))
    if password != password2:
        flash("Passwords do not match.")
        return redirect(url_for("admin_register"))
    if Admin.query.count() > 0:
        return redirect(url_for("admin_login"))
    try:
        obj = Admin(username=username, password_hash=legacy.generate_password_hash(password))
        db.session.add(obj)
        db.session.commit()
        _set_session(username, "admin")
        return redirect(url_for("dashboard"))
    except Exception as exc:
        db.session.rollback()
        record_system_error(exc, source="auth:registration")
        flash("Administrator creation failed. See System Errors.")
        return redirect(url_for("admin_register"))


app.view_functions["admin_login"] = secure_admin_login
app.view_functions["admin_register"] = secure_admin_register


def _enrollment_authorized() -> bool:
    role = _role()
    if role in {"admin", "police", "gk"}:
        return True
    auth = request.headers.get("Authorization", "")
    supplied = auth.split(" ", 1)[1].strip() if auth.lower().startswith("bearer ") else request.headers.get("X-Enrollment-Key")
    if ENROLLMENT_KEY and supplied and supplied == ENROLLMENT_KEY:
        return True
    if ADMIN_API_TOKEN and supplied and supplied == ADMIN_API_TOKEN:
        return True
    return PUBLIC_ENROLLMENT


def secure_onboard():
    if not _enrollment_authorized():
        return _api_error("Device enrollment authorization required", 401, "PROVISIONING_AUTH_REQUIRED")
    try:
        payload = _parse_json_body()
    except ValueError as exc:
        return _api_error(str(exc), 400, "INVALID_JSON")
    owner = payload.get("owner")
    car_name = payload.get("car_name") or payload.get("vehicle_make") or payload.get("vehicle_type")
    car_model = payload.get("car_model") or payload.get("vehicle_model_name") or payload.get("vehicle_category")
    plate = payload.get("plate")
    extra = payload.get("extra")
    if extra is not None and not isinstance(extra, (dict, list, str, int, float, bool)):
        return _api_error("extra must be JSON-serializable", 400, "INVALID_EXTRA")
    device_id = str(payload.get("device_id") or uuid.uuid4())[:36]
    token = legacy.create_device_token()
    while Device.query.filter_by(token=token).first():
        token = legacy.create_device_token()
    try:
        existing = Device.query.filter_by(id=device_id).first()
        if existing:
            # Idempotent retry: preserve identity, rotate the credential, and update metadata.
            if (existing.plate or "").strip().upper() not in {"", str(plate or "").strip().upper()}:
                return _api_error("device_id is already registered to another vehicle", 409, "DEVICE_ID_CONFLICT")
            existing.token = token
            existing.owner = owner or existing.owner
            existing.car_name = car_name or existing.car_name
            existing.car_model = car_model or existing.car_model
            existing.plate = plate or existing.plate
            existing.extra = json.dumps(extra) if extra is not None else existing.extra
            existing.revoked = False
            db.session.commit()
            _clear_runtime_caches()
            device = existing
        else:
            device = Device(id=device_id, token=token, owner=owner, car_name=car_name,
                            car_model=car_model, plate=plate,
                            extra=json.dumps(extra) if extra is not None else None)
            db.session.add(device)
            db.session.commit()
        return jsonify({"ok": True, "device_id": device.id, "token": device.token,
                        "owner": device.owner, "car_name": device.car_name,
                        "car_model": device.car_model, "plate": device.plate})
    except Exception as exc:
        db.session.rollback()
        record_system_error(exc, source="device:onboard")
        return _api_error("Device provisioning failed", 500, "ONBOARD_FAILED")


app.view_functions["onboard"] = secure_onboard


@app.route("/enrollment/status")
def enrollment_status():
    return jsonify({
        "ok": True,
        "enrollment_available": bool(PUBLIC_ENROLLMENT or ENROLLMENT_KEY),
        "requires_key": bool(ENROLLMENT_KEY and not PUBLIC_ENROLLMENT),
        "mode": "public-demo" if PUBLIC_ENROLLMENT else "protected",
    })


def authority_vehicle_lookup():
    if _role() not in {"admin", "police", "gk"}:
        return _api_error("Authorized authority login required", 401, "AUTH_REQUIRED")
    try:
        body = _parse_json_body()
    except ValueError as exc:
        return _api_error(str(exc), 400, "INVALID_JSON")
    try:
        device = legacy._find_matching_device(body.get("owner"), body.get("plate"), body.get("phone_number"))
    except Exception as exc:
        record_system_error(exc, source="vehicle:lookup", severity="WARNING")
        return _api_error("Vehicle lookup failed", 500, "LOOKUP_FAILED")
    if not device:
        return _api_error("No matching vehicle found", 404, "VEHICLE_NOT_FOUND")
    return jsonify({"ok": True, "device_id": device.id, "owner": device.owner,
                    "car_name": device.car_name, "car_model": device.car_model,
                    "plate": device.plate, "extra": legacy._device_extra_object(device)})


app.view_functions["vehicle_lookup"] = authority_vehicle_lookup


@app.route("/vehicle/recover", methods=["POST"])
def vehicle_recover():
    if not _enrollment_authorized():
        return _api_error("Vehicle recovery authorization required", 401, "RECOVERY_AUTH_REQUIRED")
    try:
        body = _parse_json_body()
    except ValueError as exc:
        return _api_error(str(exc), 400, "INVALID_JSON")
    try:
        device = legacy._find_matching_device(body.get("owner") or body.get("name"), body.get("plate"), body.get("phone_number") or body.get("phone"))
    except Exception as exc:
        record_system_error(exc, source="vehicle:recover", severity="WARNING")
        return _api_error("Vehicle recovery lookup failed", 500, "RECOVERY_LOOKUP_FAILED")
    if not device or device.revoked:
        return _api_error("No matching active vehicle found", 404, "VEHICLE_NOT_FOUND")
    try:
        token = legacy.create_device_token()
        while Device.query.filter_by(token=token).first():
            token = legacy.create_device_token()
        device.token = token
        db.session.commit()
        _clear_runtime_caches()
        return jsonify({"ok": True, "device_id": device.id, "token": device.token,
                        "owner": device.owner, "car_name": device.car_name,
                        "car_model": device.car_model, "plate": device.plate, "rotated": True})
    except Exception as exc:
        db.session.rollback()
        record_system_error(exc, source="vehicle:recover", severity="ERROR")
        return _api_error("Vehicle credential recovery failed", 500, "RECOVERY_FAILED")


# ---------------------------------------------------------------------------
# Heartbeat / device channel
# ---------------------------------------------------------------------------
def _record_incident(kind: str, *, device_id=None, plate=None, lat=None, lon=None, severity="medium", confidence=0.0, reason="", evidence=None) -> OperationalIncident:
    # Deduplicate noisy repeated observations inside a short active window.
    cutoff = datetime.utcnow() - timedelta(seconds=20)
    existing = OperationalIncident.query.filter(
        OperationalIncident.type == kind,
        OperationalIncident.device_id == device_id,
        OperationalIncident.status == "active",
        OperationalIncident.created_at >= cutoff,
    ).order_by(OperationalIncident.created_at.desc()).first()
    if existing:
        existing.confidence = max(existing.confidence or 0.0, confidence)
        existing.reason = reason or existing.reason
        existing.evidence = json.dumps(evidence or {})
        db.session.commit()
        return existing
    incident = OperationalIncident(type=kind, device_id=device_id, plate=plate, lat=lat, lon=lon,
                                   severity=severity, confidence=confidence, reason=reason,
                                   evidence=json.dumps(evidence or {}), status="active")
    db.session.add(incident)
    db.session.commit()
    return incident


def _cache_live_state(snap: Snapshot, *, accuracy_m=None, sequence=None, session_id=None, source=None):
    state = LiveVehicleState.query.filter_by(device_id=snap.device_id).first()
    if state is None:
        state = LiveVehicleState(device_id=snap.device_id)
        db.session.add(state)
    state.snapshot_id = snap.id
    state.ts = snap.ts
    state.lat = snap.lat
    state.lon = snap.lon
    state.speed_mps = snap.speed_mps or 0.0
    state.bearing_deg = snap.bearing_deg or 0.0
    state.heading_deg = snap.heading_deg or state.bearing_deg or 0.0
    state.accuracy_m = float(accuracy_m) if accuracy_m is not None else None
    state.sequence = sequence
    state.session_id = str(session_id)[:96] if session_id else None
    state.source = str(source)[:32] if source else None
    state.updated_at = datetime.utcnow()
    with _live_cache_lock:
        _live_cache[snap.device_id] = {
            "lat": snap.lat, "lon": snap.lon, "speed_mps": snap.speed_mps or 0.0,
            "bearing_deg": snap.bearing_deg or 0.0, "heading_deg": snap.heading_deg or 0.0,
            "ts": snap.ts, "raw": None, "accuracy_m": accuracy_m,
            "sequence": sequence, "session_id": session_id, "source": source
        }


def _clear_runtime_caches():
    _last_heartbeat_at.clear()
    _last_sequence.clear()
    with _live_cache_lock:
        _live_cache.clear()
    with legacy.active_devices_lock:
        legacy.active_devices.clear()


def secure_heartbeat():
    global _last_cleanup_at
    if request.content_length and request.content_length > MAX_HEARTBEAT_JSON_BYTES:
        return _api_error("Heartbeat payload too large", 413, "PAYLOAD_TOO_LARGE")
    try:
        body = _parse_json_body()
    except ValueError as exc:
        return _api_error(str(exc), 400, "INVALID_JSON")

    device = None
    try:
        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith(("token ", "bearer ")):
            token = auth.split(" ", 1)[1].strip()
        else:
            token = (request.headers.get("X-Device-Token") or body.get("token"))
        device_id = body.get("device_id")
        if token:
            device = legacy.find_device_by_token(token)
        if not device or not device_id:
            if device_id and token:
                device = Device.query.filter_by(id=device_id, token=token, revoked=False).first()
        if not device or device.revoked or (device_id and device.id != device_id):
            return _api_error("Missing, invalid or revoked device token", 401, "DEVICE_AUTH_FAILED")
    except Exception as exc:
        record_system_error(exc, source="heartbeat:auth")
        return _api_error("Device authentication failed", 500, "DEVICE_AUTH_ERROR")

    device_id = device.id
    now = time.time()
    previous = _last_heartbeat_at.get(device_id)
    if previous and now - previous < HEARTBEAT_MIN_INTERVAL_S:
        return jsonify({"ok": True, "saved_at": None, "accepted": False, "note": "rate_limited", "request_id": _request_id()}), 202
    _last_heartbeat_at[device_id] = now

    try:
        lat = float(body.get("lat")); lon = float(body.get("lon"))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return _api_error("lat/lon out of range", 400, "INVALID_COORDINATES")
        speed_mps = body.get("speed_mps")
        if speed_mps is None:
            speed_kmh = body.get("speed_kmh")
            speed_mps = float(speed_kmh) / 3.6 if speed_kmh is not None else 0.0
        speed_mps = float(speed_mps)
        if not (0 <= speed_mps <= 120):
            return _api_error("speed_mps outside accepted range", 400, "INVALID_SPEED")
        bearing = float(body.get("bearing", 0.0)) % 360
        heading = float(body.get("heading", bearing)) % 360
    except (TypeError, ValueError) as exc:
        return _api_error(str(exc), 400, "INVALID_TELEMETRY")

    is_mock = bool(body.get("is_mock", False))
    if is_mock and not ALLOW_SIMULATION:
        return _api_error("Simulated/mock telemetry is disabled", 403, "SIMULATION_DISABLED")

    sequence = body.get("sequence")
    session_id = str(body.get("session_id") or "legacy")[:96]
    if sequence is not None:
        try:
            sequence = int(sequence)
        except (TypeError, ValueError):
            return _api_error("sequence must be an integer", 400, "INVALID_SEQUENCE")
        seq_key = (device_id, session_id)
        last_seq = _last_sequence.get(seq_key)
        if last_seq is not None and sequence <= last_seq:
            return _api_error("Duplicate or out-of-order telemetry sequence", 409, "STALE_SEQUENCE")
        _last_sequence[seq_key] = sequence

    accuracy_m = body.get("accuracy_m", body.get("accuracy"))
    try:
        accuracy_m = float(accuracy_m) if accuracy_m is not None else None
        if accuracy_m is not None and not (0 <= accuracy_m <= 10000):
            return _api_error("accuracy_m outside accepted range", 400, "INVALID_ACCURACY")
    except (TypeError, ValueError):
        return _api_error("invalid accuracy_m", 400, "INVALID_ACCURACY")
    telemetry = {
        "timestamp": body.get("timestamp") or body.get("ts"),
        "sequence": sequence,
        "session_id": session_id,
        "accuracy_m": accuracy_m,
        "speed_accuracy_mps": body.get("speed_accuracy_mps"),
        "bearing_accuracy_deg": body.get("bearing_accuracy_deg"),
        "elapsed_realtime_ms": body.get("elapsed_realtime_ms"),
        "is_mock": is_mock,
        "integrity": "SIMULATION" if is_mock else ("VERIFIED" if sequence is not None and accuracy_m is not None else "LEGACY_COMPAT"),
        "received_at": datetime.utcnow().isoformat() + "Z",
    }
    raw = dict(body)
    raw.pop("token", None)
    raw["_server_integrity"] = telemetry

    try:
        snap = Snapshot(device_id=device_id, ts=datetime.utcnow(), lat=lat, lon=lon,
                        speed_mps=speed_mps, bearing_deg=bearing, heading_deg=heading,
                        source=str(body.get("source") or "android")[:32], raw=json.dumps(raw))
        db.session.add(snap)
        db.session.flush()
        _cache_live_state(snap, accuracy_m=accuracy_m, sequence=sequence, session_id=session_id, source=body.get("source") or "android")
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        record_system_error(exc, source="database:heartbeat", severity="ERROR")
        return _api_error("Telemetry could not be stored", 500, "TELEMETRY_SAVE_FAILED")

    try:
        legacy.update_active_device_from_snapshot(snap)
    except Exception as exc:
        record_system_error(exc, source="active-cache", severity="WARNING")

    if time.time() - _last_cleanup_at >= CLEANUP_INTERVAL_S:
        try:
            Snapshot.query.filter(Snapshot.ts < datetime.utcnow() - timedelta(seconds=SNAPSHOT_RETENTION_S)).delete(synchronize_session=False)
            db.session.commit()
            _last_cleanup_at = time.time()
        except Exception as exc:
            db.session.rollback()
            record_system_error(exc, source="database:cleanup", severity="WARNING")

    payload = None
    try:
        payload = compute_nearby_v2(device_id)
        _send_ws(device_id, "nearby_update", payload)
    except Exception as exc:
        record_system_error(exc, source="risk-engine", severity="WARNING")

    # Overspeed remains compatible with the legacy road engine, but the event becomes persistent/observable.
    try:
        legacy.check_overspeed_for_snapshot(snap)
        recent = OverspeedEvent.query.filter_by(snapshot_id=snap.id).all()
        for event in recent:
            data = {"type": "overspeed", "event_id": event.id, "device_id": event.device_id,
                    "road_id": event.road_id, "speed_kmh": event.speed_kmh, "lat": event.lat,
                    "lon": event.lon, "ts": event.ts.isoformat()}
            _send_ws(device_id, "overspeed_alert", data)
            legacy.socketio.emit("overspeed_alert", data, room="police")
            legacy.socketio.emit("overspeed_alert", data, room="gk")
    except Exception as exc:
        record_system_error(exc, source="risk:overspeed", severity="WARNING")

    # Persist and fan out server-side inferred accident observations, deduplicated.
    try:
        acc = legacy.detect_accident_for_device(device_id)
        if acc:
            incident = _record_incident("possible_accident", device_id=device_id, lat=lat, lon=lon,
                                        severity=acc.get("severity", "medium"), confidence=float(acc.get("confidence", 0)),
                                        reason=acc.get("reason", ""), evidence=acc)
            acc = dict(acc, incident_id=incident.id, device_id=device_id)
            _send_ws(device_id, "accident_alert", acc)
            legacy.socketio.emit("accident_alert", acc, room="police")
            legacy.socketio.emit("accident_alert", acc, room="gk")
    except Exception as exc:
        record_system_error(exc, source="risk:accident", severity="WARNING")

    return jsonify({"ok": True, "accepted": True, "saved_at": snap.ts.isoformat() + "Z", "request_id": _request_id(), "nearby": payload or {}})


app.view_functions["heartbeat"] = secure_heartbeat
legacy.heartbeat = secure_heartbeat


def secure_nearby():
    try:
        device = legacy.require_auth_token()
    except Exception:
        return _api_error("Device token required", 401, "DEVICE_AUTH_REQUIRED")
    try:
        radius = min(max(float(request.args.get("radius_m", NEARBY_DEFAULT_RADIUS_M)), 50.0), 5000.0)
        return jsonify(compute_nearby_v2(device.id, radius))
    except Exception as exc:
        record_system_error(exc, source="risk:nearby", severity="WARNING")
        return _api_error("Nearby calculation failed", 500, "NEARBY_FAILED")


app.view_functions["nearby"] = secure_nearby

# ---------------------------------------------------------------------------
# Modern authority report / backup center
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Durable backup / restore subsystem
# ---------------------------------------------------------------------------
def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sqlite_integrity(path: Path) -> tuple[bool, str]:
    conn = None
    try:
        conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        result = conn.execute("PRAGMA integrity_check").fetchone()
        ok = bool(result and str(result[0]).lower() == "ok")
        return ok, str(result[0] if result else "no result")
    except Exception as exc:
        return False, str(exc)
    finally:
        if conn is not None:
            conn.close()


def _make_sqlite_backup_file() -> Path:
    src = _sqlite_path()
    if not src or not src.exists():
        raise RuntimeError("SQLite backend is not active or database file does not exist")
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    dest = BACKUP_DIR / f"beacon_{ts}.sqlite3"
    source = target = None
    try:
        source = sqlite3.connect(str(src), timeout=15)
        target = sqlite3.connect(str(dest), timeout=15)
        source.backup(target)
        target.execute("PRAGMA wal_checkpoint(FULL)")
        target.commit()
        ok, detail = _sqlite_integrity(dest)
        if not ok:
            raise RuntimeError(f"backup integrity check failed: {detail}")
        return dest
    finally:
        if target is not None:
            target.close()
        if source is not None:
            source.close()


def _write_json_backup(path: Path) -> None:
    path.write_bytes(generate_json_export_bytes())


def _write_pdf_backup(path: Path) -> None:
    path.write_bytes(generate_authority_pdf("backup", admin_sensitive=True))


def _build_backup_archive() -> Path:
    sqlite_file = _make_sqlite_backup_file() if _sqlite_path() else None
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    json_file = BACKUP_DIR / f"beacon_{ts}.json"
    pdf_file = BACKUP_DIR / f"beacon_{ts}.pdf"
    _write_json_backup(json_file)
    _write_pdf_backup(pdf_file)
    archive = BACKUP_DIR / f"beacon_full_{ts}.beaconbackup.zip"
    manifest = {
        "format": "beacon-backup-v2",
        "product": "Beacon Road Safety & Coordination Platform",
        "created_at": datetime.utcnow().isoformat() + "Z",
        "database_backend": db.engine.url.get_backend_name(),
        "files": [],
    }
    files = [(sqlite_file, "database.sqlite3"), (json_file, "data.json"), (pdf_file, "summary.pdf")]
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for source, name in files:
            if source and source.exists():
                zf.write(source, name)
                manifest["files"].append({"name": name, "sha256": _sha256(source), "bytes": source.stat().st_size})
        zf.writestr("manifest.json", json.dumps(manifest, indent=2))
    _prune_backups()
    return archive


def _prune_backups():
    files = sorted([p for p in BACKUP_DIR.iterdir() if p.is_file() and p.name != "incoming"], key=lambda p: p.stat().st_mtime, reverse=True)
    keep = max(BACKUP_RETENTION_COUNT * 4, BACKUP_RETENTION_COUNT)
    for path in files[keep:]:
        try:
            path.unlink()
        except Exception:
            pass


def _safe_zip_members(zf: zipfile.ZipFile) -> dict[str, str]:
    selected = {}
    for info in zf.infolist():
        if info.is_dir():
            continue
        name = Path(info.filename)
        if name.is_absolute() or ".." in name.parts:
            raise ValueError("backup archive contains unsafe path")
        lower = name.name.lower()
        suffix = name.suffix.lower()
        if suffix in {".sqlite", ".sqlite3", ".db"} and "sqlite" not in selected:
            selected["sqlite"] = info.filename
        elif suffix == ".json" and lower != "manifest.json" and "json" not in selected:
            selected["json"] = info.filename
    return selected


def _restore_sqlite_file(source: Path) -> dict[str, Any]:
    target = _sqlite_path()
    if not target:
        raise RuntimeError("SQLite restore requires a SQLite database")
    if not source.exists():
        raise RuntimeError("backup file does not exist")
    ok, detail = _sqlite_integrity(source)
    if not ok:
        raise RuntimeError(f"source database failed integrity check: {detail}")
    with sqlite3.connect(str(source)) as conn:
        tables = {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    required = {"device", "snapshot"}
    if not required.issubset(tables):
        raise RuntimeError("backup is not a Beacon database (required tables missing)")

    pre = _make_sqlite_backup_file() if target.exists() else None
    tmp = target.with_suffix(target.suffix + ".restore-tmp")
    legacy.db.session.remove()
    legacy.db.engine.dispose()
    try:
        if tmp.exists():
            tmp.unlink()
        src_conn = sqlite3.connect(str(source), timeout=30)
        dst_conn = sqlite3.connect(str(tmp), timeout=30)
        try:
            src_conn.backup(dst_conn)
            dst_conn.commit()
        finally:
            dst_conn.close(); src_conn.close()
        ok2, detail2 = _sqlite_integrity(tmp)
        if not ok2:
            raise RuntimeError(f"restored database failed verification: {detail2}")
        # SQLite WAL/SHM sidecars belong to the database image. Never leave old
        # sidecars beside a restored file, or SQLite could replay stale pages.
        for sidecar in (Path(str(target) + "-wal"), Path(str(target) + "-shm"),
                        Path(str(tmp) + "-wal"), Path(str(tmp) + "-shm")):
            try:
                if sidecar.exists(): sidecar.unlink()
            except Exception: pass
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, target)
        _clear_runtime_caches()
        with app.app_context():
            db.create_all()
        return {"restored": True, "target": str(target), "pre_restore_backup": str(pre) if pre else None}
    except Exception:
        if tmp.exists():
            try: tmp.unlink()
            except Exception: pass
        raise


def _restore_json_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise RuntimeError("not a Beacon JSON backup")
    tables_payload = payload.get("tables")
    if not isinstance(tables_payload, dict):
        # Accept older/plain JSON exports shaped as {table_name: [rows...]}.
        tables_payload = {k: v for k, v in payload.items() if isinstance(v, list)}
    if not tables_payload:
        raise RuntimeError("not a Beacon JSON backup")
    aliases = {
        "devices": "device", "snapshots": "snapshot", "roads": "road",
        "overspeed_events": "overspeed_event", "overspeedevents": "overspeed_event",
        "police_users": "police_user", "policeusers": "police_user",
        "gk_users": "gk_user", "gkusers": "gk_user",
        "watchlists": "watchlist", "plate_sightings": "plate_sighting",
        "traffic_zones": "traffic_zone", "broadcast_messages": "broadcast_message",
        "broadcast_deliveries": "broadcast_delivery", "admins": "admin",
    }
    restored = 0
    skipped = 0
    redacted_auth = {"admin", "police_user", "gk_user"}
    for raw_table_name, rows in tables_payload.items():
        table_name = aliases.get(str(raw_table_name).lower(), raw_table_name)
        table = db.metadata.tables.get(table_name)
        if table is None or not isinstance(rows, list):
            continue
        columns = {c.name: c for c in table.columns}
        pks = [c.name for c in table.primary_key.columns]
        for row in rows:
            if not isinstance(row, dict):
                skipped += 1; continue
            if table_name in redacted_auth and row.get("password_hash") == "[redacted]":
                skipped += 1; continue
            values = {k: v for k, v in row.items() if k in columns and not (k == "password_hash" and v == "[redacted]")}
            if not values:
                skipped += 1; continue
            try:
                with db.session.begin_nested():
                    existing = None
                    if pks and all(values.get(k) is not None for k in pks):
                        stmt = db.select(table).where(*[columns[k] == values[k] for k in pks]).limit(1)
                        existing = db.session.execute(stmt).mappings().first()
                    if existing:
                        db.session.execute(table.update().where(*[columns[k] == values[k] for k in pks]).values(**{k:v for k,v in values.items() if k not in pks}))
                    else:
                        db.session.execute(table.insert().values(**values))
                restored += 1
            except Exception:
                skipped += 1
    db.session.commit()
    return {"restored_rows": restored, "skipped_rows": skipped, "note": "JSON imports merge data; redacted password hashes are intentionally not restored."}

def generate_json_export_bytes() -> bytes:
    data = {}
    for table in db.metadata.sorted_tables:
        rows = []
        for row in db.session.execute(table.select()).mappings():
            item = {}
            for key, value in row.items():
                item[key] = "[redacted]" if key == "password_hash" else _json_safe(value)
            rows.append(item)
        data[table.name] = rows
    payload = {
        "product": "Beacon Road Safety & Coordination Platform",
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "database_backend": db.engine.url.get_backend_name(),
        "note": "Password hashes are redacted. Use the SQLite backup for full restoration when SQLite is configured.",
        "tables": data,
    }
    return json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")


def system_status() -> dict[str, Any]:
    db_ok = True
    db_err = None
    try:
        db.session.execute(db.text("SELECT 1"))
    except Exception as exc:
        db_ok = False
        db_err = str(exc)
        record_system_error(exc, source="database:health", severity="CRITICAL")
    with _connected_lock:
        socket_connections = sum(len(v) for v in connected_sockets.values())
        connected_devices = len(connected_sockets)
    with legacy.active_devices_lock:
        live_devices = len(legacy.active_devices)
    open_errors = SystemError.query.filter_by(resolved=False).count() if db_ok else None
    sqlite_integrity = None
    if db_ok and _db_is_sqlite():
        path = _sqlite_path()
        if path and path.exists():
            ok_i, detail_i = _sqlite_integrity(path)
            sqlite_integrity = {"ok": ok_i, "detail": detail_i}
    with _live_cache_lock:
        cache_count = len(_live_cache)
    return {
        "ok": db_ok and (sqlite_integrity is None or sqlite_integrity.get("ok", False)),
        "time": datetime.utcnow().isoformat() + "Z",
        "database": {"ok": db_ok, "backend": db.engine.url.get_backend_name(), "error": db_err, "sqlite_integrity": sqlite_integrity},
        "live": {"devices": live_devices, "connected_devices": connected_devices, "socket_connections": socket_connections, "cache_entries": cache_count},
        "errors": {"open": open_errors},
        "vehicles": Device.query.count() if db_ok else None,
        "roads": Road.query.count() if db_ok else None,
        "overspeed_events": OverspeedEvent.query.count() if db_ok else None,
        "incidents": OperationalIncident.query.filter_by(status="active").count() if db_ok else None,
    }


def _pdf_table(elems, headers, rows):
    from reportlab.lib import colors
    from reportlab.platypus import Table, TableStyle
    table_data = [headers] + [["" if v is None else str(v)[:90] for v in r] for r in rows]
    t = Table(table_data, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f4c5c")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#94a3b8")),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    elems.append(t)


def generate_authority_pdf(kind: str, *, admin_sensitive: bool = False) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
    except Exception as exc:
        raise RuntimeError("reportlab is required for PDF generation") from exc

    buff = io.BytesIO()
    doc = SimpleDocTemplate(buff, pagesize=landscape(A4), leftMargin=26, rightMargin=26, topMargin=26, bottomMargin=26)
    styles = getSampleStyleSheet()
    elems = [Paragraph("Beacon — Authority Report", styles["Heading1"]),
             Paragraph(f"Report: {kind} · Generated {datetime.utcnow().isoformat()}Z", styles["Normal"]), Spacer(1, 10)]

    if kind in {"summary", "backup"}:
        st = system_status()
        elems.append(Paragraph("System status", styles["Heading2"]))
        _pdf_table(elems, ["Metric", "Value"], [
            ("Database", json.dumps(st["database"])),
            ("Live devices", st["live"]["devices"]),
            ("Socket connections", st["live"]["socket_connections"]),
            ("Open errors", st["errors"]["open"]),
            ("Active incidents", st["incidents"]),
            ("Registered vehicles", st["vehicles"]),
            ("Roads", st["roads"]),
        ])
        elems.append(Spacer(1, 10))

    if kind in {"summary", "vehicles", "backup"}:
        elems.append(Paragraph("Vehicles", styles["Heading2"]))
        rows = []
        for d in Device.query.order_by(Device.created_at.desc()).limit(1000).all():
            snap = Snapshot.query.filter_by(device_id=d.id).order_by(Snapshot.ts.desc()).first()
            owner = d.owner if admin_sensitive else ("[restricted]" if d.owner else "")
            rows.append((d.id[:12], d.plate or "", d.car_name or d.car_model or "", owner,
                         snap.ts.isoformat() if snap and snap.ts else ""))
        _pdf_table(elems, ["Device", "Plate", "Vehicle", "Owner", "Last seen"], rows)
        elems.append(Spacer(1, 10))

    if kind in {"summary", "overspeeds", "backup"}:
        elems.append(Paragraph("Recent overspeed events", styles["Heading2"]))
        rows = [(o.ts.isoformat() if o.ts else "", o.device_id[:12] if o.device_id else "",
                 o.road_id[:12] if o.road_id else "", o.speed_kmh, o.lat, o.lon)
                for o in OverspeedEvent.query.order_by(OverspeedEvent.ts.desc()).limit(500).all()]
        _pdf_table(elems, ["Time", "Device", "Road", "km/h", "Lat", "Lon"], rows)

    if kind in {"summary", "incidents", "backup"}:
        elems.append(Paragraph("Operational incidents", styles["Heading2"]))
        rows = [(i.created_at.isoformat() if i.created_at else "", i.id[:12], i.type, i.severity,
                 round(i.confidence or 0, 2), i.device_id[:12] if i.device_id else "", i.status, i.reason or "")
                for i in OperationalIncident.query.order_by(OperationalIncident.created_at.desc()).limit(500).all()]
        _pdf_table(elems, ["Time", "Incident", "Type", "Severity", "Confidence", "Device", "State", "Reason"], rows)

    if kind == "errors":
        elems.append(Paragraph("System errors", styles["Heading2"]))
        rows = [(e.occurred_at.isoformat() if e.occurred_at else "", e.severity, e.source,
                 e.error_type, e.message, e.route, "resolved" if e.resolved else "open")
                for e in SystemError.query.order_by(SystemError.occurred_at.desc()).limit(1000).all()]
        _pdf_table(elems, ["Time", "Severity", "Source", "Type", "Message", "Route", "State"], rows)

    doc.build(elems)
    buff.seek(0)
    return buff.getvalue()


REPORTS_HTML = """
<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Beacon — Reports & Backups</title>
<style>body{font-family:Inter,system-ui,-apple-system,"Segoe UI",Roboto,Arial;background:#06111f;color:#e5eefb;margin:0}.wrap{max-width:1200px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:center}.nav{display:flex;gap:8px;flex-wrap:wrap}.nav a,.btn{display:inline-block;text-decoration:none;color:#fff;background:#11698e;border:1px solid #1f8fb8;padding:10px 14px;border-radius:10px;font-weight:800}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-top:18px}.card{background:#0d1d2e;border:1px solid #1e354a;border-radius:16px;padding:18px}.muted{color:#94a3b8;line-height:1.5}.list{display:flex;flex-direction:column;gap:8px;margin-top:12px}.list a{background:#081827;border:1px solid #29435a;color:#e5eefb;text-decoration:none;padding:12px;border-radius:10px}.tag{font-size:11px;padding:4px 7px;border-radius:999px;background:#173047;color:#bfe8ff}@media(max-width:900px){.grid{grid-template-columns:1fr 1fr}}@media(max-width:600px){.grid{grid-template-columns:1fr}}</style></head>
<body><div class='wrap'><div class='top'><div><div style='font-size:12px;letter-spacing:.1em;color:#7dd3fc'>BEACON AUTHORITY</div><h1 style='margin:.1em 0'>Reports & Backups</h1><div class='muted'>Operational exports for briefings, records, diagnostics and recovery.</div></div><div class='nav'><a href='{{ home }}'>Dashboard</a><a href='{{ url_for("authority_reports") }}'>Reports</a>{% if role=='admin' %}<a href='{{ url_for("admin_errors") }}'>System errors</a>{% endif %}<a href='{{ url_for("admin_logout") }}'>Logout</a></div></div>
<div class='grid'>
<div class='card'><h2>PDF reports</h2><div class='muted'>Role-aware authority reports. Admin-only error details stay out of operational reports for police/GK.</div><div class='list'><a href='{{ url_for("authority_report_summary_pdf") }}'>System summary <span class='tag'>PDF</span></a><a href='{{ url_for("authority_report_incidents_pdf") }}'>Incidents & alerts <span class='tag'>PDF</span></a><a href='{{ url_for("authority_report_vehicles_pdf") }}'>Vehicle register <span class='tag'>PDF</span></a><a href='{{ url_for("authority_report_overspeeds_pdf") }}'>Overspeed events <span class='tag'>PDF</span></a>{% if role=='admin' %}<a href='{{ url_for("authority_report_errors_pdf") }}'>System errors <span class='tag'>PDF</span></a>{% endif %}<a href='{{ url_for("authority_report_backup_pdf") }}'>Backup snapshot <span class='tag'>PDF</span></a></div></div>
<div class='card'><h2>Data backups</h2><div class='muted'>Operational exports are available to authority roles. Full database restore and bundled backups are administrator-only.</div><div class='list'>{% if role=='admin' %}<a href='{{ url_for("admin_backup_full") }}'>Full Beacon backup <span class='tag'>.zip</span></a><a href='{{ url_for("admin_backup_restore_page") }}'>Restore / manage backups</a>{% endif %}{% if sqlite_available %}<a href='{{ url_for("admin_backup_sqlite") if role=="admin" else "#" }}' {% if role!='admin' %}onclick='return false' style='opacity:.55'{% endif %}>SQLite snapshot <span class='tag'>.db</span></a>{% endif %}<a href='{{ url_for("authority_backup_json") }}'>Operational data <span class='tag'>.json</span></a></div></div></div></div>
<div class='card'><h2>Road & legacy exports</h2><div class='muted'>Existing per-road exports remain available from the road monitoring area. New authority PDFs use the hardened report engine above.</div><div class='list'><a href='{{ url_for("admin_traffic") if role=="admin" else url_for("all_vehicles") }}'>Open vehicle / road monitoring</a>{% if role=='admin' %}<a href='{{ url_for("report_all_xlsx") }}'>Legacy full workbook <span class='tag'>XLSX</span></a>{% endif %}</div></div>
</div></div></body></html>
"""


@app.route("/authority/reports")
def authority_reports():
    role = _require_authority()
    home = url_for("dashboard") if role == "admin" else url_for("police_dashboard") if role == "police" else url_for("gk_dashboard")
    return render_template_string(REPORTS_HTML, role=role, home=home, sqlite_available=bool(_sqlite_path()), backend=db.engine.url.get_backend_name())

@app.route("/authority/status")
def authority_status():
    _require_authority()
    try:
        return jsonify(system_status())
    except Exception as exc:
        record_system_error(exc, source="status", severity="WARNING")
        return _api_error("Health data unavailable", 503, "STATUS_UNAVAILABLE")

@app.route("/authority/report/<kind>.pdf")
def authority_report(kind):
    role = _require_authority()
    allowed = {"summary", "incidents", "vehicles", "overspeeds", "errors", "backup"}
    if kind not in allowed or (kind == "errors" and role != "admin"):
        abort(404)
    try:
        data = generate_authority_pdf(kind, admin_sensitive=(role == "admin"))
        return send_file(io.BytesIO(data), as_attachment=True, download_name=f"beacon_{kind}.pdf", mimetype="application/pdf")
    except Exception as exc:
        record_system_error(exc, source=f"pdf:{kind}")
        return _api_error("PDF generation failed", 500, "PDF_GENERATION_FAILED")


def secure_pulse_receiver():
    """Authenticated legacy ingestion alias that never bypasses hardened heartbeat checks."""
    configured = os.environ.get("PULSE_TOKEN") or os.environ.get("ADMIN_API_TOKEN")
    body = request.get_json(silent=True) or {}
    auth_header = request.headers.get("Authorization", "")
    token = None
    if auth_header.lower().startswith(("token ", "bearer ")):
        token = auth_header.split(" ", 1)[1].strip()
    presented = request.headers.get("X-Pulse-Token") or request.args.get("pulse_token") or token or body.get("pulse_token") or body.get("integration_token")
    if not body:
        return jsonify({"ok": True, "service": "pulse_receiver", "authenticated": False, "probe": True})
    # A normal mobile heartbeat can authenticate with its own device token in the JSON body.
    # Only integration-style pulse callers are required to present PULSE_TOKEN/ADMIN_API_TOKEN.
    body_device_token = body.get("token")
    device_auth_present = bool(body.get("device_id") and body_device_token)
    if not device_auth_present and body.get("device_id") and auth_header.lower().startswith("bearer "):
        device_auth_present = True
    if configured and not device_auth_present:
        try:
            valid = bool(presented) and secrets.compare_digest(str(presented), str(configured))
        except Exception:
            valid = False
        if not valid:
            return _api_error("pulse authentication required", 401, "PULSE_AUTH_REQUIRED")
    return secure_heartbeat()

app.view_functions["pulse_receiver"] = secure_pulse_receiver

# Named aliases used by the Reports HTML.
@app.route("/authority/report/summary.pdf", endpoint="authority_report_summary_pdf")
def authority_report_summary_pdf():
    return authority_report("summary")

@app.route("/authority/report/incidents.pdf", endpoint="authority_report_incidents_pdf")
def authority_report_incidents_pdf():
    return authority_report("incidents")

@app.route("/authority/report/vehicles.pdf", endpoint="authority_report_vehicles_pdf")
def authority_report_vehicles_pdf():
    return authority_report("vehicles")

@app.route("/authority/report/overspeeds.pdf", endpoint="authority_report_overspeeds_pdf")
def authority_report_overspeeds_pdf():
    return authority_report("overspeeds")

@app.route("/authority/report/errors.pdf", endpoint="authority_report_errors_pdf")
def authority_report_errors_pdf():
    return authority_report("errors")

@app.route("/authority/report/backup.pdf", endpoint="authority_report_backup_pdf")
def authority_report_backup_pdf():
    return authority_report("backup")

@app.route("/admin/backup/sqlite")
def admin_backup_sqlite():
    if _role() != "admin":
        abort(401, "Admin access required")
    try:
        path = _make_sqlite_backup_file()
        return send_file(path, as_attachment=True, download_name=path.name, mimetype="application/x-sqlite3")
    except Exception as exc:
        record_system_error(exc, source="backup:sqlite")
        return _api_error("SQLite backup failed", 500, "SQLITE_BACKUP_FAILED")

@app.route("/admin/backup/json", endpoint="admin_backup_json")
def admin_backup_json():
    if _role() != "admin":
        abort(401, "Admin access required")
    try:
        data = generate_json_export_bytes()
        return send_file(io.BytesIO(data), as_attachment=True,
                         download_name=f"beacon_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
                         mimetype="application/json")
    except Exception as exc:
        record_system_error(exc, source="backup:json")
        return _api_error("JSON export failed", 500, "JSON_BACKUP_FAILED")


@app.route("/admin/backup/full")
def admin_backup_full():
    if _role() != "admin":
        abort(401, "Admin access required")
    try:
        archive = _build_backup_archive()
        return send_file(archive, as_attachment=True, download_name=archive.name, mimetype="application/zip")
    except Exception as exc:
        record_system_error(exc, source="backup:full")
        return _api_error("Full backup failed", 500, "FULL_BACKUP_FAILED")


@app.route("/admin/backups")
def admin_backups():
    if _role() != "admin":
        return _api_error("Admin access required", 401, "ADMIN_REQUIRED")
    items = []
    for p in sorted(BACKUP_DIR.iterdir(), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True):
        if p.is_file():
            items.append({"name": p.name, "bytes": p.stat().st_size, "sha256": _sha256(p), "modified": datetime.utcfromtimestamp(p.stat().st_mtime).isoformat() + "Z"})
    return jsonify({"directory": str(BACKUP_DIR), "backups": items[:200]})


@app.route("/admin/backup/restore", methods=["POST"])
def admin_backup_restore():
    if _role() != "admin":
        abort(401, "Admin access required")
    upload = request.files.get("backup")
    if upload is None or not upload.filename:
        return _api_error("Select a .sqlite/.db/.json/.zip backup", 400, "BACKUP_FILE_REQUIRED")
    original = re.sub(r"[^A-Za-z0-9_.-]", "_", Path(upload.filename).name)
    incoming = UPLOAD_DIR / f"{uuid.uuid4().hex}_{original}"
    upload.save(incoming)
    try:
        suffix = incoming.suffix.lower()
        if suffix in {".sqlite", ".sqlite3", ".db"}:
            result = _restore_sqlite_file(incoming)
        elif suffix == ".json":
            payload = json.loads(incoming.read_text(encoding="utf-8"))
            result = _restore_json_payload(payload)
            result["mode"] = "merge"
        elif suffix == ".zip" or incoming.name.lower().endswith(".beaconbackup.zip"):
            with zipfile.ZipFile(incoming, "r") as zf:
                selected = _safe_zip_members(zf)
                if "sqlite" in selected:
                    temp_source = UPLOAD_DIR / f"restore_{uuid.uuid4().hex}.sqlite3"
                    with temp_source.open("wb") as fh:
                        fh.write(zf.read(selected["sqlite"]))
                    try:
                        result = _restore_sqlite_file(temp_source)
                        result["mode"] = "full"
                    finally:
                        try: temp_source.unlink()
                        except Exception: pass
                elif "json" in selected:
                    payload = json.loads(zf.read(selected["json"]).decode("utf-8"))
                    result = _restore_json_payload(payload)
                    result["mode"] = "merge"
                else:
                    raise RuntimeError("backup archive has no database.sqlite3 or data.json")
        else:
            return _api_error("Unsupported backup file type", 400, "BACKUP_TYPE_UNSUPPORTED")
        _prune_backups()
        return jsonify({"ok": True, **result})
    except Exception as exc:
        record_system_error(exc, source="backup:restore", severity="ERROR")
        return _api_error(str(exc), 400, "BACKUP_RESTORE_FAILED")
    finally:
        try: incoming.unlink()
        except Exception: pass


BACKUP_RESTORE_HTML = """
<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Beacon — Backup & Restore</title>
<style>body{font-family:Inter,system-ui,-apple-system,'Segoe UI',Roboto,Arial;background:#06111f;color:#e5eefb;margin:0}.wrap{max-width:1000px;margin:auto;padding:24px}.card{background:#0d1d2e;border:1px solid #20394f;border-radius:18px;padding:20px;margin-top:16px}.muted{color:#94a3b8;line-height:1.5}.btn{display:inline-block;background:#11698e;color:#fff;border:1px solid #1f8fb8;padding:11px 14px;border-radius:10px;font-weight:800;text-decoration:none;cursor:pointer}.warn{background:#3b2610;border-color:#8a5a13}.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}input[type=file]{width:100%;padding:14px;border:1px dashed #36556d;border-radius:12px;background:#081827;color:#cfe7f3;box-sizing:border-box}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;background:#081827;padding:12px;border-radius:10px;overflow:auto}</style></head>
<body><div class='wrap'><div class='row'><a class='btn' href='{{ url_for("dashboard") }}'>Dashboard</a><a class='btn' href='{{ url_for("authority_reports") }}'>Reports</a><a class='btn' href='{{ url_for("admin_errors") }}'>System errors</a></div>
<div class='card'><h1>Backup & Restore</h1><p class='muted'>Full backup bundles SQLite, JSON and PDF. SQLite/.db/.sqlite3 restores are full verified replacements and automatically create a pre-restore backup. JSON restores merge non-sensitive records and skip redacted credentials.</p>
<div class='row'><a class='btn' href='{{ url_for("admin_backup_full") }}'>Create full backup</a><a class='btn' href='{{ url_for("admin_backup_sqlite") }}'>SQLite only</a><a class='btn' href='{{ url_for("admin_backup_json") }}'>JSON export</a><a class='btn' href='{{ url_for("authority_report_backup_pdf") }}'>PDF snapshot</a></div></div>
<div class='card'><h2>Restore a backup</h2><form method='post' enctype='multipart/form-data' action='{{ url_for("admin_backup_restore") }}'><input type='file' name='backup' accept='.sqlite,.sqlite3,.db,.json,.zip' required><div class='row' style='margin-top:12px'><button class='btn warn' type='submit' onclick='return confirm("A verified SQLite restore will replace the current database after creating a pre-restore backup. Continue?")'>Validate & Restore</button></div></form><div id='msg' class='muted' style='margin-top:12px'></div></div>
<div class='card'><h2>Saved backup files</h2><pre id='files' class='mono'>Loading…</pre></div>
<script>async function load(){const r=await fetch('{{ url_for("admin_backups") }}',{cache:'no-store'});const j=await r.json();document.getElementById('files').textContent=(j.backups||[]).map(x=>x.name+'\n  '+x.bytes+' bytes\n  '+x.sha256).join('\n\n')||'No backups yet';}load();</script>
</div></body></html>
"""

@app.route("/admin/backup-restore")
def admin_backup_restore_page():
    if _role() != "admin":
        return redirect(url_for("admin_login", next=request.path))
    return render_template_string(BACKUP_RESTORE_HTML)

@app.route("/authority/backup/json", endpoint="authority_backup_json")
def authority_backup_json():
    _require_authority()
    try:
        data = generate_json_export_bytes()
        return send_file(io.BytesIO(data), as_attachment=True,
                         download_name=f"beacon_operational_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
                         mimetype="application/json")
    except Exception as exc:
        record_system_error(exc, source="backup:authority-json")
        return _api_error("JSON export failed", 500, "JSON_BACKUP_FAILED")

def repaired_legacy_all_pdf():
    if _role() != "admin":
        abort(401, "Admin access required")
    try:
        data = generate_authority_pdf("summary", admin_sensitive=True)
        return send_file(io.BytesIO(data), as_attachment=True,
                         download_name="beacon_full_report.pdf", mimetype="application/pdf")
    except Exception as exc:
        record_system_error(exc, source="report:legacy-all-pdf")
        return _api_error("PDF report failed", 500, "PDF_REPORT_FAILED")

app.view_functions["report_all_pdf"] = repaired_legacy_all_pdf

# ---------------------------------------------------------------------------
# System error center
# ---------------------------------------------------------------------------
ERRORS_HTML = """
<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Beacon — System Errors</title><style>body{font-family:Inter,system-ui,-apple-system,'Segoe UI',Roboto,Arial;background:#06111f;color:#e5eefb;margin:0}.wrap{max-width:1400px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;gap:15px;flex-wrap:wrap;align-items:center}.nav{display:flex;gap:8px;flex-wrap:wrap}.nav a,.btn{display:inline-block;background:#11698e;border:1px solid #1f8fb8;color:#fff;padding:10px 13px;border-radius:10px;text-decoration:none;font-weight:800;cursor:pointer}.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:18px 0}.card{background:#0d1d2e;border:1px solid #20394f;border-radius:16px;padding:16px}.k{font-size:11px;color:#8da5bb;text-transform:uppercase;letter-spacing:.1em}.v{font-size:30px;font-weight:900;margin-top:5px}.toolbar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}.toolbar input,.toolbar select{padding:10px;border:1px solid #29435a;background:#081525;color:#fff;border-radius:10px;min-width:200px}.table{width:100%;border-collapse:collapse}.table th,.table td{padding:10px 8px;border-bottom:1px solid #20394f;text-align:left;vertical-align:top;font-size:12px}.pill{display:inline-block;padding:4px 8px;border-radius:999px;background:#4c1d1d;color:#fecaca}.warning{background:#49370a;color:#fde68a}.trace{white-space:pre-wrap;max-width:520px;max-height:180px;overflow:auto;color:#cbd5e1}.muted{color:#94a3b8}.empty{text-align:center;padding:30px;color:#94a3b8}@media(max-width:900px){.stats{grid-template-columns:repeat(2,1fr)}}@media(max-width:600px){.stats{grid-template-columns:1fr}.table{font-size:11px}}</style></head>
<body><div class='wrap'><div class='top'><div><div style='font-size:12px;letter-spacing:.1em;color:#7dd3fc'>BEACON OPERATIONS</div><h1 style='margin:.1em 0'>System Errors & Health</h1><div class='muted'>One place for backend exceptions, database failures and authority dashboard errors.</div></div><div class='nav'><a href='{{ url_for("dashboard") }}'>Dashboard</a><a href='{{ url_for("authority_reports") }}'>Reports & backups</a><a href='{{ url_for("admin_logout") }}'>Logout</a></div></div>
<div class='stats'><div class='card'><div class='k'>Open errors</div><div class='v' id='open'>—</div></div><div class='card'><div class='k'>Database</div><div class='v' id='db'>—</div></div><div class='card'><div class='k'>Live devices</div><div class='v' id='live'>—</div></div><div class='card'><div class='k'>Sockets</div><div class='v' id='sockets'>—</div></div></div>
<div class='card'><div class='toolbar'><input id='q' placeholder='Search source, route, error type, message'><select id='state'><option value='all'>All errors</option><option value='open'>Open</option><option value='resolved'>Resolved</option></select><button class='btn' onclick='loadErrors()'>Refresh</button><button class='btn' onclick='window.location="{{ url_for("authority_report_errors_pdf") }}"'>PDF</button><button class='btn' onclick='window.location="{{ url_for("admin_backup_json") }}"'>JSON backup</button></div><div style='overflow:auto'><table class='table'><thead><tr><th>Time</th><th>Severity</th><th>Source</th><th>Type</th><th>Message</th><th>Route</th><th>State</th><th>Trace / action</th></tr></thead><tbody id='rows'></tbody></table></div></div></div>
<script>
function esc(v){return String(v??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));}
async function loadStatus(){try{const r=await fetch('/authority/status',{cache:'no-store'});const j=await r.json();document.getElementById('open').textContent=j.errors?.open??'—';document.getElementById('db').textContent=j.database?.ok?'OK':'DOWN';document.getElementById('live').textContent=j.live?.devices??'—';document.getElementById('sockets').textContent=j.live?.socket_connections??'—';}catch(e){document.getElementById('db').textContent='ERROR';}}
async function loadErrors(){const q=document.getElementById('q').value.trim();const state=document.getElementById('state').value;const r=await fetch('/admin/errors/json?limit=500&state='+encodeURIComponent(state)+'&q='+encodeURIComponent(q),{cache:'no-store'});const j=await r.json();const rows=document.getElementById('rows');rows.innerHTML='';(j.errors||[]).forEach(e=>{const tr=document.createElement('tr');tr.innerHTML='<td>'+esc(e.occurred_at)+'</td><td><span class="pill '+(e.severity==='WARNING'?'warning':'')+'">'+esc(e.severity)+'</span></td><td>'+esc(e.source)+'</td><td>'+esc(e.error_type)+'</td><td>'+esc(e.message)+'</td><td>'+esc(e.route)+'</td><td>'+esc(e.resolved?'Resolved':'Open')+'</td><td>'+(e.resolved?'':'<button class="btn" onclick="resolveError('+e.id+')">Resolve</button>')+'<details><summary>Trace</summary><pre class="trace">'+esc(e.traceback)+'</pre></details></td>';rows.appendChild(tr)});if(!j.errors?.length)rows.innerHTML='<tr><td colspan="8" class="empty">No errors found.</td></tr>';loadStatus();}
async function resolveError(id){await fetch('/admin/errors/'+id+'/resolve',{method:'POST'});loadErrors();}
setInterval(loadStatus,5000);loadErrors();
</script></body></html>
"""

@app.route("/admin/errors", endpoint="admin_errors")
def admin_errors():
    if _role() != "admin":
        return redirect(url_for("admin_login", next=request.path))
    return render_template_string(ERRORS_HTML)

@app.route("/admin/errors/json")
def admin_errors_json():
    if _role() != "admin":
        return _api_error("Admin access required", 401, "ADMIN_REQUIRED")
    try:
        limit = min(max(int(request.args.get("limit", 250)), 1), 1000)
    except Exception:
        limit = 250
    state = (request.args.get("state") or "all").lower()
    q = (request.args.get("q") or "").strip().lower()
    query = SystemError.query.order_by(SystemError.occurred_at.desc())
    if state == "open": query = query.filter_by(resolved=False)
    if state == "resolved": query = query.filter_by(resolved=True)
    out = []
    for e in query.limit(2000).all():
        hay = " ".join(str(v or "") for v in (e.source, e.error_type, e.message, e.route)).lower()
        if q and q not in hay:
            continue
        out.append({"id": e.id, "occurred_at": e.occurred_at.isoformat() if e.occurred_at else None,
                    "severity": e.severity, "source": e.source, "error_type": e.error_type,
                    "message": e.message, "route": e.route, "method": e.method,
                    "request_id": e.request_id, "username": e.username, "resolved": bool(e.resolved),
                    "traceback": e.traceback_text})
        if len(out) >= limit: break
    return jsonify({"errors": out, "count": len(out)})

@app.route("/admin/errors/<int:error_id>/resolve", methods=["POST"])
def admin_error_resolve(error_id):
    if _role() != "admin": return _api_error("Admin access required", 401, "ADMIN_REQUIRED")
    row = SystemError.query.get_or_404(error_id)
    row.resolved = True
    db.session.commit()
    return jsonify({"ok": True, "id": row.id, "resolved": True})

@app.route("/authority/client-error", methods=["POST"])
def authority_client_error():
    if _role() not in {"admin", "police", "gk"}:
        return _api_error("Authentication required", 401, "AUTH_REQUIRED")
    body = request.get_json(silent=True) or {}
    exc = RuntimeError(_safe_text(body.get("message") or "Client-side error", 4000))
    record_system_error(exc, source=f"web:{_role()}", severity="WARNING", route=_safe_text(body.get("source") or request.path, 512))
    return jsonify({"ok": True, "request_id": _request_id()})

@app.route("/device/client-error", methods=["POST"])
def device_client_error():
    try: device = legacy.require_auth_token()
    except Exception: return _api_error("Device token required", 401, "DEVICE_AUTH_REQUIRED")
    body = request.get_json(silent=True) or {}
    exc = RuntimeError(_safe_text(body.get("message") or "Mobile client error", 4000))
    record_system_error(exc, source=f"android:{device.id}", severity="WARNING", route=_safe_text(body.get("source") or "mobile", 512))
    return jsonify({"ok": True, "request_id": _request_id()})

# ---------------------------------------------------------------------------
# User administration override (fixes GK creation flow)
# ---------------------------------------------------------------------------
USER_ADMIN_HTML = """
<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Beacon — Access Management</title><style>body{font-family:Inter,system-ui;background:#071522;color:#e5eefb;margin:0}.wrap{max-width:1100px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap}.nav{display:flex;gap:8px;flex-wrap:wrap}.nav a,.btn{background:#11698e;color:#fff;text-decoration:none;border:1px solid #1f8fb8;padding:10px 13px;border-radius:10px;font-weight:800}.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.card{background:#0d1d2e;border:1px solid #20394f;border-radius:16px;padding:16px;margin-top:14px}input,select{width:100%;box-sizing:border-box;padding:12px;background:#081525;border:1px solid #29435a;color:#fff;border-radius:10px;margin:7px 0 12px}.table{width:100%;border-collapse:collapse}.table th,.table td{padding:10px;border-bottom:1px solid #20394f;text-align:left}.muted{color:#94a3b8}.flash{background:#173047;padding:10px;border-radius:10px}@media(max-width:800px){.grid{grid-template-columns:1fr}}</style></head><body><div class='wrap'><div class='top'><div><div style='font-size:12px;color:#7dd3fc;letter-spacing:.1em'>BEACON SECURITY</div><h1>Access management</h1></div><div class='nav'><a href='{{ url_for("dashboard") }}'>Dashboard</a><a href='{{ url_for("authority_reports") }}'>Reports</a><a href='{{ url_for("admin_errors") }}'>System errors</a><a href='{{ url_for("admin_logout") }}'>Logout</a></div></div>{% with messages=get_flashed_messages() %}{% if messages %}<div class='flash'>{{ messages[0] }}</div>{% endif %}{% endwith %}<div class='card'><h2>Create authority account</h2><form method='post'><div class='grid'><div><label>Role</label><select name='role'><option value='admin'>Administrator</option><option value='police'>Police</option><option value='gk'>GK / Command</option></select></div><div><label>Username</label><input name='username' required></div><div><label>Password</label><input type='password' name='password' minlength='10' required></div><div><label>Confirm</label><input type='password' name='password2' minlength='10' required></div></div><button class='btn' type='submit'>Create account</button></form></div><div class='grid'><div class='card'><h2>Administrators</h2><table class='table'><thead><tr><th>User</th><th>Created</th><th></th></tr></thead><tbody>{% for a in admins %}<tr><td>{{ a.username }}</td><td>{{ a.created_at.isoformat() if a.created_at else '' }}</td><td><form method='post' action='{{ url_for("admin_delete_user",role="admin",user_id=a.id) }}'><button class='btn' type='submit' onclick='return confirm("Delete this administrator?")'>Delete</button></form></td></tr>{% endfor %}</tbody></table></div><div class='card'><h2>Police</h2><table class='table'><thead><tr><th>User</th><th>Created</th><th></th></tr></thead><tbody>{% for p in police_users %}<tr><td>{{ p.username }}</td><td>{{ p.created_at.isoformat() if p.created_at else '' }}</td><td><form method='post' action='{{ url_for("admin_delete_user",role="police",user_id=p.id) }}'><button class='btn' type='submit' onclick='return confirm("Delete this police account?")'>Delete</button></form></td></tr>{% endfor %}</tbody></table></div><div class='card' style='grid-column:1/-1'><h2>GK / Command</h2><table class='table'><thead><tr><th>User</th><th>Created</th><th></th></tr></thead><tbody>{% for g in gk_users %}<tr><td>{{ g.username }}</td><td>{{ g.created_at.isoformat() if g.created_at else '' }}</td><td><form method='post' action='{{ url_for("admin_delete_user",role="gk",user_id=g.id) }}'><button class='btn' type='submit' onclick='return confirm("Delete this GK account?")'>Delete</button></form></td></tr>{% endfor %}</tbody></table></div></div></div></body></html>
"""


def admin_users_v2():
    if _role() != "admin": return redirect(url_for("admin_login"))
    if request.method == "POST":
        username = (request.form.get("username") or "").strip(); password = request.form.get("password") or ""; password2 = request.form.get("password2") or ""
        role = (request.form.get("role") or "admin").lower()
        if role not in {"admin", "police", "gk"}:
            role = "admin"
        if len(username) < 3 or len(password) < 10:
            flash("Username must be at least 3 characters and password at least 10 characters.")
            return redirect(url_for("admin_admins"))
        if password != password2:
            flash("Passwords do not match.")
            return redirect(url_for("admin_admins"))
        if Admin.query.filter_by(username=username).first() or PoliceUser.query.filter_by(username=username).first() or GKUser.query.filter_by(username=username).first():
            flash("Username already exists.")
            return redirect(url_for("admin_admins"))
        try:
            cls = {"admin": Admin, "police": PoliceUser, "gk": GKUser}[role]
            obj = cls(username=username, password_hash=legacy.generate_password_hash(password))
            db.session.add(obj); db.session.commit(); flash(f"{role.upper()} account created.")
        except Exception as exc:
            db.session.rollback(); record_system_error(exc, source="auth:user-create"); flash("Account creation failed. See System Errors.")
        return redirect(url_for("admin_admins"))
    return render_template_string(USER_ADMIN_HTML, admins=Admin.query.order_by(Admin.created_at.asc()).all(),
                                  police_users=PoliceUser.query.order_by(PoliceUser.created_at.asc()).all(),
                                  gk_users=GKUser.query.order_by(GKUser.created_at.asc()).all())

app.view_functions["admin_admins"] = admin_users_v2

# ---------------------------------------------------------------------------
# Police / GK live consoles
# ---------------------------------------------------------------------------
POLICE_HTML = """
<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Beacon — Police Operations</title><style>body{font-family:Inter,system-ui;background:#06111f;color:#e5eefb;margin:0}.wrap{max-width:1320px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:center}.nav{display:flex;gap:8px;flex-wrap:wrap}.nav a{background:#11698e;color:#fff;text-decoration:none;border:1px solid #1f8fb8;padding:10px 13px;border-radius:10px;font-weight:800}.grid{display:grid;grid-template-columns:1.5fr .8fr;gap:14px;margin-top:18px}.card{background:#0d1d2e;border:1px solid #20394f;border-radius:16px;padding:16px}.feed{max-height:620px;overflow:auto;display:flex;flex-direction:column;gap:10px}.event{border:1px solid #29435a;background:#081827;border-radius:12px;padding:12px}.critical{border-color:#7f1d1d}.muted{color:#94a3b8}.tag{display:inline-block;padding:4px 7px;border-radius:999px;background:#173047;color:#bfe8ff;font-size:11px}.empty{text-align:center;padding:30px;color:#94a3b8}@media(max-width:900px){.grid{grid-template-columns:1fr}}</style></head><body><div class='wrap'><div class='top'><div><div style='font-size:12px;color:#7dd3fc;letter-spacing:.1em'>BEACON POLICE</div><h1>Police Operations</h1><div class='muted'>Live watchlist, LPR and safety event feed for the authenticated police session.</div></div><div class='nav'><a href='{{ url_for("all_vehicles") }}'>Vehicle search</a><a href='{{ url_for("authority_reports") }}'>Reports</a><a href='{{ url_for("admin_logout") }}'>Logout</a></div></div><div class='grid'><div class='card'><div style='display:flex;justify-content:space-between;align-items:center'><h2>Live operations</h2><span class='tag' id='authState'>Connecting…</span></div><div id='feed' class='feed'><div class='empty'>Waiting for live events…</div></div></div><div><div class='card'><h2>Session health</h2><p>Role: <strong>Police</strong></p><p>Live channel: <strong id='socket'>Connecting</strong></p><p>Platform: <strong id='health'>Checking</strong></p><a class='nav' style='display:inline-block;margin-top:8px' href='{{ url_for("authority_report_incidents_pdf") }}'>Download incident PDF</a></div><div class='card' style='margin-top:14px'><h2>What this console receives</h2><div class='muted'>Watchlist hits, plate sightings, overspeed events and server-inferred accident alerts. The channel uses the authenticated police session—no administrator API token is required.</div></div></div></div></div><script src='/socket.io/socket.io.js'></script><script>const s=io();const feed=document.getElementById('feed');function esc(v){return String(v??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));}function add(title,d,critical){if(feed.querySelector('.empty'))feed.innerHTML='';const e=document.createElement('div');e.className='event'+(critical?' critical':'');e.innerHTML='<strong>'+esc(title)+'</strong><div class="muted" style="margin-top:5px">'+esc(d.plate||d.watch_plate||d.device_id||'')+' · '+esc(d.ts||'')+'</div><div style="margin-top:7px">'+esc(d.watch_label||d.reason||'Operational event')+'</div>';feed.prepend(e);}function reportClientError(message,stack){fetch('/authority/client-error',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message,source:location.pathname,stack:stack||''})}).catch(()=>{});}window.addEventListener('error',e=>reportClientError(e.message||'window error',e.error?.stack));window.addEventListener('unhandledrejection',e=>reportClientError(String(e.reason||'unhandled promise rejection'),e.reason?.stack));s.on('connect',()=>{document.getElementById('socket').textContent='Connected';s.emit('police_auth_v2',{});});s.on('police_auth_ok_v2',()=>{document.getElementById('authState').textContent='Authorized';});s.on('police_auth_failed_v2',()=>{document.getElementById('authState').textContent='Denied';});s.on('disconnect',()=>{document.getElementById('socket').textContent='Disconnected';document.getElementById('authState').textContent='Disconnected';});s.on('watch_hit',d=>add('WATCHLIST HIT',d,true));s.on('plate_sighting',d=>add('PLATE SIGHTING',d,false));s.on('overspeed_alert',d=>add('OVERSPEED',d,false));s.on('accident_alert',d=>add('ACCIDENT ALERT',d,true));async function health(){try{const r=await fetch('/authority/status',{cache:'no-store'});const j=await r.json();document.getElementById('health').textContent=j.ok?'Healthy':'Degraded';}catch(e){document.getElementById('health').textContent='Unavailable';}}health();setInterval(health,5000);</script></body></html>
"""

GK_HTML = """
<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Beacon — Command Center</title><style>body{font-family:Inter,system-ui;background:#06111f;color:#e5eefb;margin:0}.wrap{max-width:1320px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:center}.nav{display:flex;gap:8px;flex-wrap:wrap}.nav a,.btn{background:#11698e;color:#fff;text-decoration:none;border:1px solid #1f8fb8;padding:10px 13px;border-radius:10px;font-weight:800}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:18px}.card{background:#0d1d2e;border:1px solid #20394f;border-radius:16px;padding:16px}.k{font-size:11px;color:#8da5bb;text-transform:uppercase;letter-spacing:.1em}.v{font-size:27px;font-weight:900;margin-top:4px}.feed{margin-top:14px;display:flex;flex-direction:column;gap:9px;max-height:500px;overflow:auto}.event{padding:12px;background:#081827;border:1px solid #29435a;border-radius:12px}.muted{color:#94a3b8;line-height:1.5}@media(max-width:900px){.cards{grid-template-columns:repeat(2,1fr)}}@media(max-width:600px){.cards{grid-template-columns:1fr}}</style></head><body><div class='wrap'><div class='top'><div><div style='font-size:12px;color:#7dd3fc;letter-spacing:.1em'>BEACON COMMAND</div><h1>GK / Command Center</h1><div class='muted'>System-wide situational view with authorized communications and reporting.</div></div><div class='nav'><a href='{{ url_for("authority_reports") }}'>Reports & backups</a><a href='{{ url_for("admin_logout") }}'>Logout</a></div></div><div class='cards'><div class='card'><div class='k'>Live devices</div><div class='v' id='live'>—</div></div><div class='card'><div class='k'>Active incidents</div><div class='v' id='incidents'>—</div></div><div class='card'><div class='k'>Open system errors</div><div class='v' id='errors'>—</div></div><div class='card'><div class='k'>Database</div><div class='v' id='db'>—</div></div></div><div class='card' style='margin-top:14px'><div style='display:flex;justify-content:space-between;align-items:center'><h2>Live operational feed</h2><a class='btn' href='{{ url_for("authority_report_incidents_pdf") }}'>Incident PDF</a></div><div id='feed' class='feed'><div class='muted'>Waiting for command events…</div></div></div></div><script src='/socket.io/socket.io.js'></script><script>const s=io();const feed=document.getElementById('feed');function esc(v){return String(v??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));}function add(t,d){if(feed.children.length===1&&feed.firstElementChild.classList.contains('muted'))feed.innerHTML='';const e=document.createElement('div');e.className='event';e.innerHTML='<strong>'+esc(t)+'</strong><div class="muted">'+esc(d.device_id||d.plate||'')+' · '+esc(d.ts||'')+'</div><div>'+esc(d.reason||d.watch_label||'Operational event')+'</div>';feed.prepend(e);}function reportClientError(message,stack){fetch('/authority/client-error',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message,source:location.pathname,stack:stack||''})}).catch(()=>{});}window.addEventListener('error',e=>reportClientError(e.message||'window error',e.error?.stack));window.addEventListener('unhandledrejection',e=>reportClientError(String(e.reason||'unhandled promise rejection'),e.reason?.stack));s.on('connect',()=>s.emit('gk_auth_v2',{}));s.on('watch_hit',d=>add('WATCHLIST HIT',d));s.on('overspeed_alert',d=>add('OVERSPEED',d));s.on('accident_alert',d=>add('ACCIDENT ALERT',d));async function load(){try{const r=await fetch('/authority/status',{cache:'no-store'});const j=await r.json();document.getElementById('live').textContent=j.live?.devices??'—';document.getElementById('incidents').textContent=j.incidents??'—';document.getElementById('errors').textContent=j.errors?.open??'—';document.getElementById('db').textContent=j.database?.ok?'OK':'DOWN';}catch(e){reportClientError(e.message,'health');}}load();setInterval(load,5000);</script></body></html>
"""


def police_dashboard_v2():
    _require_authority({"admin", "police"})
    return render_template_string(POLICE_HTML)


def gk_dashboard_v2():
    _require_authority({"admin", "gk"})
    return render_template_string(GK_HTML)

app.view_functions["police_dashboard"] = police_dashboard_v2
app.view_functions["gk_dashboard"] = gk_dashboard_v2

# Fix the known Dashboard JS null-reference and add a persistent tools strip/error reporting script.
legacy.DASHBOARD_HTML = legacy.DASHBOARD_HTML.replace(
    "mapStyle.value = key;",
    "if (mapStyle) { mapStyle.value = key; }",
)

def _inject_dashboard_tools(template: str, role: str) -> str:
    home = "{{ url_for('dashboard') }}" if role == "admin" else ("{{ url_for('police_dashboard') }}" if role == "police" else "{{ url_for('gk_dashboard') }}")
    extras = "<a href=\"{{ url_for('admin_errors') }}\">System errors</a>" if role == "admin" else ""
    snippet = f"""
<div style=\"position:fixed;right:18px;bottom:18px;z-index:9999;font-family:Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial\"><div style=\"background:#0b1b2a;color:#e5eefb;border:1px solid #2b4a61;border-radius:14px;padding:9px 11px;box-shadow:0 15px 40px #0007;display:flex;gap:7px;align-items:center;flex-wrap:wrap\"><strong style=\"font-size:12px\">BEACON</strong><a style=\"color:#bfe8ff;text-decoration:none;font-size:12px\" href=\"{home}\">Home</a><a style=\"color:#bfe8ff;text-decoration:none;font-size:12px\" href=\"{{{{ url_for('authority_reports') }}}}\">Reports & backups</a>{extras}</div></div>
<script>(function(){{window.addEventListener('error',function(e){{fetch('/authority/client-error',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{message:e.message||'window error',source:location.pathname,stack:e.error?.stack||''}})}}).catch(()=>{{}})}});window.addEventListener('unhandledrejection',function(e){{fetch('/authority/client-error',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{message:String(e.reason||'unhandled promise rejection'),source:location.pathname,stack:e.reason?.stack||''}})}}).catch(()=>{{}})}});}})();</script>
"""
    return template.replace("</body>", snippet + "</body>")

legacy.DASHBOARD_HTML = _inject_dashboard_tools(legacy.DASHBOARD_HTML, "admin")

# ---------------------------------------------------------------------------
# Socket.IO v2 authority channels and device registration channels
# ---------------------------------------------------------------------------
@legacy.socketio.on("police_auth_v2")
def police_auth_v2(_data):
    try:
        if _role() in {"admin", "police"}:
            legacy.join_room("police")
            legacy.emit("police_auth_ok_v2", {"ok": True, "role": _role()})
            return
    except Exception as exc:
        record_system_error(exc, source="socket:police-auth", severity="WARNING")
    legacy.emit("police_auth_failed_v2", {"ok": False})

@legacy.socketio.on("gk_auth_v2")
def gk_auth_v2(_data):
    try:
        if _role() in {"admin", "gk"}:
            legacy.join_room("gk")
            legacy.emit("gk_auth_ok_v2", {"ok": True, "role": _role()})
            return
    except Exception as exc:
        record_system_error(exc, source="socket:gk-auth", severity="WARNING")
    legacy.emit("gk_auth_failed_v2", {"ok": False})

@legacy.socketio.on("register_v2")
def register_v2(data):
    try:
        data = data or {}
        device = Device.query.filter_by(id=data.get("device_id"), token=data.get("token"), revoked=False).first()
        if not device:
            legacy.emit("error", {"error": "invalid token/device"})
            return
        sid = legacy.request.sid
        with _connected_lock:
            connected_sockets.setdefault(device.id, set()).add(sid)
        legacy.join_room(sid)
        legacy.emit("registered_v2", {"ok": True, "device_id": device.id})
    except Exception as exc:
        record_system_error(exc, source="socket:device-register")
        legacy.emit("error", {"error": "registration failed", "request_id": _request_id()})

@legacy.socketio.on("get_nearby_v2")
def get_nearby_v2(data):
    try:
        data = data or {}
        device = Device.query.filter_by(id=data.get("device_id"), token=data.get("token"), revoked=False).first()
        if not device:
            legacy.emit("error", {"error": "invalid device/token"})
            return
        legacy.emit("nearby", compute_nearby_v2(device.id))
    except Exception as exc:
        record_system_error(exc, source="socket:nearby", severity="WARNING")
        legacy.emit("error", {"error": "nearby calculation failed", "request_id": _request_id()})

# ---------------------------------------------------------------------------
# LPR ingestion security override
# ---------------------------------------------------------------------------
def secure_ingest_plate():
    if _role() not in {"admin", "police"}:
        try:
            legacy.require_admin_api()
        except Exception:
            return _api_error("Authenticated LPR integration required", 401, "LPR_AUTH_REQUIRED")
    try:
        body = _parse_json_body()
    except ValueError as exc:
        return _api_error(str(exc), 400, "INVALID_JSON")
    plate = re.sub(r"[^A-Z0-9]", "", str(body.get("plate") or "").upper())
    if not plate:
        return _api_error("plate required", 400, "PLATE_REQUIRED")
    try:
        lat = float(body["lat"]) if body.get("lat") is not None else None
        lon = float(body["lon"]) if body.get("lon") is not None else None
        if lat is not None and lon is not None and not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return _api_error("invalid coordinates", 400, "INVALID_COORDINATES")
    except (TypeError, ValueError):
        return _api_error("invalid coordinates", 400, "INVALID_COORDINATES")
    source = str(body.get("source") or "lpr")[:64]
    try:
        sighting = PlateSighting(plate=plate, lat=lat, lon=lon, source=source,
                                 raw=json.dumps(body.get("raw")) if body.get("raw") is not None else None)
        db.session.add(sighting); db.session.commit()
    except Exception as exc:
        db.session.rollback(); record_system_error(exc, source="database:lpr")
        return _api_error("LPR sighting could not be stored", 500, "LPR_SAVE_FAILED")

    try:
        entry = Watchlist.query.filter(db.func.upper(Watchlist.plate) == plate).first()
        event = {"type": "watch_hit", "plate": plate, "watch_plate": entry.plate if entry else plate,
                 "watch_label": entry.label if entry else "", "lat": lat, "lon": lon,
                 "ts": sighting.ts.isoformat() if sighting.ts else datetime.utcnow().isoformat(),
                 "source": source, "sighting_id": sighting.id}
        if entry:
            incident = _record_incident("watchlist_hit", plate=plate, lat=lat, lon=lon, severity="high", confidence=1.0,
                                        reason=entry.label or "Watchlist plate matched", evidence=event)
            event["incident_id"] = incident.id
            legacy.socketio.emit("watch_hit", event, room="police")
            legacy.socketio.emit("watch_hit", event, room="gk")
        legacy.socketio.emit("plate_sighting", event, room="police")
        legacy.socketio.emit("plate_sighting", event, room="gk")
        return jsonify({"ok": True, "sighting_id": sighting.id, "watch_hit": bool(entry)})
    except Exception as exc:
        record_system_error(exc, source="lpr:notify", severity="WARNING")
        return jsonify({"ok": True, "sighting_id": sighting.id, "watch_hit": False, "notification": "degraded"})

app.view_functions["ingest_plate"] = secure_ingest_plate

# ---------------------------------------------------------------------------
# Request/exception plumbing
# ---------------------------------------------------------------------------
@legacy.app.before_request
def _beacon_request_context():
    g.beacon_request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())

@legacy.app.after_request
def _beacon_headers(response):
    response.headers.setdefault("X-Request-ID", _request_id())
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "geolocation=(self), microphone=(), camera=()")
    if request.is_secure or app.config.get("SESSION_COOKIE_SECURE"):
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response

@legacy.app.errorhandler(Exception)
def _beacon_exception(exc):
    if isinstance(exc, HTTPException):
        return exc
    rid = _request_id()
    record_system_error(exc, source="server", severity="ERROR", request_id=rid)
    wants_json = request.path.startswith(("/api/", "/authority/", "/admin/", "/heartbeat", "/nearby", "/onboard", "/device/")) or request.accept_mimetypes.best == "application/json"
    if wants_json:
        return _api_error("Internal server error", 500, "INTERNAL_SERVER_ERROR")
    return render_template_string("""
    <!doctype html><html><head><meta name='viewport' content='width=device-width,initial-scale=1'><title>Beacon — System Error</title><style>body{font-family:Inter,system-ui;background:#06111f;color:#e5eefb;margin:0;padding:30px}.card{max-width:760px;margin:auto;background:#0d1d2e;border:1px solid #29435a;border-radius:18px;padding:24px}a{color:#7dd3fc}</style></head><body><div class='card'><h1>Beacon recovered from a system error</h1><p>The operation failed safely and was logged.</p><p><strong>Request ID:</strong> {{ rid }}</p><p><a href='{{ url_for("index") }}'>Return to Beacon</a></p></div></body></html>
    """, rid=rid), 500

@app.route("/healthz")
def healthz():
    try:
        db.session.execute(db.text("SELECT 1"))
        if _db_is_sqlite():
            path = _sqlite_path()
            if path and path.exists():
                ok_i, detail_i = _sqlite_integrity(path)
                if not ok_i:
                    return jsonify({"ok": False, "service": "beacon-cloud", "error": detail_i}), 503
        return jsonify({"ok": True, "service": "beacon-cloud", "version": BEACON_VERSION if "BEACON_VERSION" in globals() else "booting", "time": datetime.utcnow().isoformat() + "Z"})
    except Exception as exc:
        record_system_error(exc, source="healthz", severity="CRITICAL")
        return jsonify({"ok": False, "error": "database unavailable"}), 503


# ---------------------------------------------------------------------------
# Final initialization and metadata
# ---------------------------------------------------------------------------
with app.app_context():
    db.create_all()
    try:
        if _db_is_sqlite():
            with db.engine.connect() as conn:
                conn.exec_driver_sql("PRAGMA journal_mode=WAL")
                conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
                conn.exec_driver_sql("PRAGMA busy_timeout=10000")
                conn.commit()
    except Exception as exc:
        record_system_error(exc, source="database:sqlite-pragmas", severity="WARNING")

# Store a small version marker for status/reporting.
BEACON_VERSION = "2026.09.25-repair-v2"

if __name__ == "__main__":
    legacy.socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=os.environ.get("FLASK_DEBUG", "0") == "1")
