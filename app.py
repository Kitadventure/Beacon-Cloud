# app.py — Beacon backend with WebSocket (Flask + Flask-SocketIO)
# Threading async_mode for Windows/dev. For production and multi-worker, use a message queue + eventlet/gevent.

import os
import uuid
import threading
from math import radians, sin, cos, atan2, sqrt, degrees
from datetime import datetime, timedelta
import json
import time

from flask import (
    Flask, request, jsonify, render_template_string, abort,
    redirect, url_for, session, flash
)
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit, join_room
from werkzeug.security import generate_password_hash, check_password_hash

# -------------------------
# Configuration (tunable)
# -------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///beacon.db")
CLEANUP_STALE_SECONDS = int(os.environ.get("CLEANUP_STALE_SECONDS", "12"))  # remove snapshots older than this
NEARBY_DEFAULT_RADIUS_M = float(os.environ.get("NEARBY_DEFAULT_RADIUS_M", "1000"))  # 1 km default
HEARTBEAT_MIN_INTERVAL_S = float(os.environ.get("HEARTBEAT_MIN_INTERVAL_S", "0.5"))  # basic rate-limit
UNSAFE_TTC_SECONDS = float(os.environ.get("UNSAFE_TTC_SECONDS", "6.0"))  # threshold for opposite-direction unsafe
CONFIRMATION_RADIUS_M = float(os.environ.get("CONFIRMATION_RADIUS_M", "30.0"))  # support devices gathering radius

ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN")
ADMIN_USER = os.environ.get("ADMIN_USER")
ADMIN_PASS = os.environ.get("ADMIN_PASS")

VEHICLE_LENGTH_M = float(os.environ.get("VEHICLE_LENGTH_M", "5.0"))
OVERTAKE_EXTRA_M = float(os.environ.get("OVERTAKE_EXTRA_M", "5.0"))
SAFETY_FACTOR = float(os.environ.get("SAFETY_FACTOR", "1.5"))

# Accident/event settings
REPORTS_TO_CONFIRM = int(os.environ.get("REPORTS_TO_CONFIRM", "2"))
EVENT_MERGE_WINDOW_S = int(os.environ.get("EVENT_MERGE_WINDOW_S", "300"))
EVENT_MERGE_RADIUS_M = float(os.environ.get("EVENT_MERGE_RADIUS_M", "50.0"))

# -------------------------
# Flask + SQLAlchemy + SocketIO init
# -------------------------
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-change-me")

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

db = SQLAlchemy(app)

# -------------------------
# Models
# -------------------------
class Admin(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(128), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Device(db.Model):
    id = db.Column(db.String(36), primary_key=True)  # UUID4 hex
    token = db.Column(db.String(64), unique=True, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    owner = db.Column(db.String(128))
    car_name = db.Column(db.String(128))
    car_model = db.Column(db.String(128))
    plate = db.Column(db.String(64))
    extra = db.Column(db.Text)
    revoked = db.Column(db.Boolean, default=False)

class Snapshot(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    device_id = db.Column(db.String(36), db.ForeignKey('device.id'), index=True)
    ts = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    speed_mps = db.Column(db.Float)   # store m/s
    bearing_deg = db.Column(db.Float) # 0..360
    heading_deg = db.Column(db.Float) # optional
    source = db.Column(db.String(32), default="app")
    raw = db.Column(db.Text)

class EventReport(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    device_id = db.Column(db.String(36), index=True)
    ts = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    event_type = db.Column(db.String(64))
    g_force = db.Column(db.Float, nullable=True)
    speed_before = db.Column(db.Float, nullable=True)
    speed_after = db.Column(db.Float, nullable=True)
    raw = db.Column(db.Text)

class AccidentEvent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    event_type = db.Column(db.String(64))
    reports_count = db.Column(db.Integer, default=0)
    reporters = db.Column(db.Text)  # JSON list
    confirmed = db.Column(db.Boolean, default=False)
    confirmed_at = db.Column(db.DateTime, nullable=True)
    severity = db.Column(db.String(32), default="unknown")
    metadata = db.Column(db.Text)

class Hotspot(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128))
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    radius_m = db.Column(db.Float, default=50.0)
    risk_level = db.Column(db.String(32), default="high")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# -------------------------
# In-memory helpers & caches
# -------------------------
_last_heartbeat_at = {}
active_devices = {}
active_devices_lock = threading.Lock()
connected_sockets = {}  # { device_id: set(sid) }

def update_active_device_from_snapshot(snap):
    try:
        with active_devices_lock:
            active_devices[snap.device_id] = {
                "ts": snap.ts,
                "lat": snap.lat,
                "lon": snap.lon,
                "speed_mps": snap.speed_mps,
                "bearing_deg": snap.bearing_deg,
                "source": snap.source,
                "raw": (json.loads(snap.raw) if snap.raw else None)
            }
    except Exception:
        pass

def prune_active_devices():
    try:
        cutoff = datetime.utcnow() - timedelta(seconds=CLEANUP_STALE_SECONDS)
        with active_devices_lock:
            to_del = [k for k,v in active_devices.items() if v.get("ts") < cutoff]
            for k in to_del:
                active_devices.pop(k, None)
    except Exception:
        pass

# -------------------------
# Geodesy + decision helpers
# -------------------------
def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    dlat = radians(lat2-lat1)
    dlon = radians(lon2-lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def bearing_between(lat1, lon1, lat2, lon2):
    dLon = radians(lon2 - lon1)
    lat1r = radians(lat1)
    lat2r = radians(lat2)
    x = sin(dLon) * cos(lat2r)
    y = cos(lat1r) * sin(lat2r) - sin(lat1r) * cos(lat2r) * cos(dLon)
    br = degrees(atan2(x, y))
    return (br + 360) % 360

def angle_diff(a, b):
    d = (a - b + 540) % 360 - 180
    return abs(d)

def classify_direction(bearing_self, bearing_other):
    diff = angle_diff(bearing_self, bearing_other)
    if diff < 45:
        return "same"
    if diff > 135:
        return "opposite"
    return "cross"

def closing_speed_mps(lat1, lon1, v1, bearing1, lat2, lon2, v2, bearing2):
    bearing_line = radians(bearing_between(lat1, lon1, lat2, lon2))
    theta1 = abs((bearing1 - degrees(bearing_line) + 540) % 360 - 180)
    theta2 = abs((bearing2 - (degrees(bearing_line)+180) + 540) % 360 - 180)
    proj1 = v1 * cos(radians(theta1))
    proj2 = v2 * cos(radians(theta2))
    return proj1 + proj2

def estimate_overtake_time_mps(your_speed_mps, target_speed_mps):
    dist = VEHICLE_LENGTH_M + OVERTAKE_EXTRA_M
    rel = max(0.1, your_speed_mps - target_speed_mps)
    return dist / rel

# -------------------------
# Hotspot helpers
# -------------------------
def hotspots_near(lat, lon, radius_m=None):
    if radius_m is None:
        radius_m = CONFIRMATION_RADIUS_M
    res = []
    try:
        hs = Hotspot.query.all()
        for h in hs:
            d = haversine_m(lat, lon, h.lat, h.lon)
            if d <= max(radius_m, h.radius_m):
                res.append((h, d))
    except Exception:
        pass
    return res

def is_in_hotspot(lat, lon):
    hits = hotspots_near(lat, lon, radius_m=None)
    return hits[0] if hits else None

# -------------------------
# Confidence & decision logic (cloud authoritative)
# -------------------------
def _recent_snapshots_for_device(device_id, limit=5):
    return Snapshot.query.filter_by(device_id=device_id).order_by(Snapshot.ts.desc()).limit(limit).all()

def _smoothed_speed_and_bearing(device_id):
    snaps = _recent_snapshots_for_device(device_id, limit=5)
    if not snaps:
        return None, None
    spd = sum([s.speed_mps for s in snaps]) / len(snaps)
    xs = sum([cos(radians(s.bearing_deg)) for s in snaps])
    ys = sum([sin(radians(s.bearing_deg)) for s in snaps])
    avg_bearing = (degrees(atan2(ys, xs)) + 360) % 360
    return float(spd), float(avg_bearing)

def _devices_near_point(lat, lon, radius_m):
    cutoff = datetime.utcnow() - timedelta(seconds=CLEANUP_STALE_SECONDS)
    snaps = Snapshot.query.filter(Snapshot.ts >= cutoff).all()
    res = {}
    for s in snaps:
        d = haversine_m(lat, lon, s.lat, s.lon)
        if d <= radius_m:
            res[s.device_id] = s
    return res

def compute_warning(self_lat, self_lon, self_speed_mps, self_bearing,
                    other_lat, other_lon, other_speed_mps, other_bearing):
    d = haversine_m(self_lat, self_lon, other_lat, other_lon)
    cls = classify_direction(self_bearing, other_bearing)
    close = closing_speed_mps(self_lat, self_lon, self_speed_mps, self_bearing,
                              other_lat, other_lon, other_speed_mps, other_bearing)
    ttc = float('inf')
    if close > 0.1:
        ttc = d / close
    guidance = {
        'distance_m': round(d, 2),
        'direction': cls,
        'closing_mps': round(close, 2),
        'time_to_collision_s': round(ttc, 2) if ttc != float('inf') else None
    }
    if cls == "same":
        required = estimate_overtake_time_mps(self_speed_mps, other_speed_mps)
        guidance['overtake_time_required_s'] = round(required, 2)
    elif cls == "opposite":
        guidance['overtake_time_required_s'] = None
        if ttc != float('inf'):
            guidance['unsafe_if_overtaking'] = (ttc < UNSAFE_TTC_SECONDS)
    return guidance

def classify_risk(self_snap, other_snap):
    """
    Existing logic preserved — we add a small hotspot modifier:
    - if the midpoint lies inside an admin hotspot, increase confidence and possibly promote to RED if hotspot is high risk.
    """
    if not self_snap or not other_snap:
        return {"decision": "yellow", "confidence": 0.2, "reason": "missing_data"}

    d = haversine_m(self_snap.lat, self_snap.lon, other_snap.lat, other_snap.lon)
    direction = classify_direction(self_snap.bearing_deg, other_snap.bearing_deg)
    close = closing_speed_mps(self_snap.lat, self_snap.lon, self_snap.speed_mps, self_snap.bearing_deg,
                              other_snap.lat, other_snap.lon, other_snap.speed_mps, other_snap.bearing_deg)
    ttc = float('inf')
    if close > 0.05:
        ttc = d / close

    age_self = (datetime.utcnow() - self_snap.ts).total_seconds()
    age_other = (datetime.utcnow() - other_snap.ts).total_seconds()
    recency_score = max(0.0, 1.0 - max(age_self, age_other) / max(1.0, CLEANUP_STALE_SECONDS))
    dist_score = max(0.0, 1.0 - (d / max(1.0, NEARBY_DEFAULT_RADIUS_M)))

    mid_lat = (self_snap.lat + other_snap.lat) / 2.0
    mid_lon = (self_snap.lon + other_snap.lon) / 2.0
    supporters = _devices_near_point(mid_lat, mid_lon, CONFIRMATION_RADIUS_M)
    supporters = {k:v for k,v in supporters.items() if k not in (self_snap.device_id, other_snap.device_id)}
    support_count = len(supporters)
    support_score = min(1.0, support_count / 3.0)

    base_confidence = 0.35 * recency_score + 0.40 * dist_score + 0.25 * support_score
    base_confidence = max(0.0, min(1.0, base_confidence))

    # Hotspot influence:
    hs_hit = is_in_hotspot(mid_lat, mid_lon)
    hotspot_modifier = 0.0
    if hs_hit:
        hotspot, hd = hs_hit
        if hotspot.risk_level == "high":
            hotspot_modifier = 0.25
        elif hotspot.risk_level == "medium":
            hotspot_modifier = 0.12
        else:
            hotspot_modifier = 0.06
    base_confidence = min(1.0, base_confidence + hotspot_modifier)

    # existing logic with modified confidence...
    if direction == "opposite":
        if ttc == float('inf'):
            return {"decision": "green", "confidence": round(base_confidence * 0.6, 2), "reason": "opposite_no_closing"}
        if ttc < UNSAFE_TTC_SECONDS:
            conf = min(1.0, base_confidence + 0.25 * (1.0 - (ttc / UNSAFE_TTC_SECONDS)) + 0.1 * support_score)
            if hs_hit and hotspot.risk_level == "high" and conf > 0.45:
                return {"decision": "red", "confidence": round(conf, 2), "reason": f"opposite_ttc_{round(ttc,1)}s_hotspot"}
            return {"decision": "red", "confidence": round(conf, 2), "reason": f"opposite_ttc_{round(ttc,1)}s"}
        else:
            conf = base_confidence * (0.6 + 0.4 * max(0.0, (NEARBY_DEFAULT_RADIUS_M - d) / NEARBY_DEFAULT_RADIUS_M))
            return {"decision": "green", "confidence": round(conf, 2), "reason": f"opposite_ttc_safe_{round(ttc,1)}s"}

    if direction == "same":
        required = estimate_overtake_time_mps(self_snap.speed_mps, other_snap.speed_mps)
        if self_snap.speed_mps <= other_snap.speed_mps + 0.01:
            return {"decision": "green", "confidence": round(base_confidence * 0.6, 2), "reason": "same_no_overtake_possible"}
        cutoff = datetime.utcnow() - timedelta(seconds=CLEANUP_STALE_SECONDS)
        other_snaps = Snapshot.query.filter(Snapshot.device_id != self_snap.device_id, Snapshot.ts >= cutoff).all()
        imminent_opposing = False
        for s in other_snaps:
            dir_s = classify_direction(self_snap.bearing_deg, s.bearing_deg)
            if dir_s == "opposite":
                d2 = haversine_m(self_snap.lat, self_snap.lon, s.lat, s.lon)
                close2 = closing_speed_mps(self_snap.lat, self_snap.lon, self_snap.speed_mps, self_snap.bearing_deg,
                                           s.lat, s.lon, s.speed_mps, s.bearing_deg)
                if close2 > 0.05:
                    ttc2 = d2 / close2
                    if ttc2 < max(UNSAFE_TTC_SECONDS, required * SAFETY_FACTOR):
                        imminent_opposing = True
                        break
        if imminent_opposing:
            conf = min(1.0, base_confidence + 0.2 * support_score)
            return {"decision": "red", "confidence": round(conf, 2), "reason": f"same_opposing_imminent_req_{round(required,1)}s"}
        if (d / max(1.0, self_snap.speed_mps*3.6)) < (required + 2.0):
            return {"decision": "yellow", "confidence": round(base_confidence * 0.6 + 0.2 * support_score, 2), "reason": "same_gap_low"}
        return {"decision": "green", "confidence": round(base_confidence * 0.8 + 0.1 * support_score, 2), "reason": "same_safe"}

    if direction == "cross":
        if ttc != float('inf') and ttc < (UNSAFE_TTC_SECONDS * 0.8):
            conf = min(1.0, base_confidence + 0.15 * support_score)
            return {"decision": "red", "confidence": round(conf, 2), "reason": f"cross_ttc_{round(ttc,1)}s"}
        else:
            return {"decision": "yellow", "confidence": round(base_confidence * 0.6 + 0.1 * support_score, 2), "reason": "cross_caution"}

    return {"decision": "yellow", "confidence": round(base_confidence, 2), "reason": "fallback_uncertain"}

# -------------------------
# DB helpers
# -------------------------
def init_db():
    with app.app_context():
        db.create_all()
        try:
            if Admin.query.count() == 0 and ADMIN_USER and ADMIN_PASS:
                h = generate_password_hash(ADMIN_PASS)
                a = Admin(username=ADMIN_USER, password_hash=h)
                db.session.add(a)
                db.session.commit()
                app.logger.info("Admin user created from environment variable.")
        except Exception:
            pass

def create_device_token():
    return uuid.uuid4().hex

def cleanup_old_snapshots():
    cutoff = datetime.utcnow() - timedelta(seconds=CLEANUP_STALE_SECONDS)
    Snapshot.query.filter(Snapshot.ts < cutoff).delete()
    db.session.commit()

# -------------------------
# Event aggregation
# -------------------------
def create_or_update_accident_event_from_report(rep: EventReport):
    try:
        cutoff = datetime.utcnow() - timedelta(seconds=EVENT_MERGE_WINDOW_S)
        candidates = AccidentEvent.query.filter(AccidentEvent.created_at >= cutoff, AccidentEvent.confirmed == False).all()
        for ev in candidates:
            d = haversine_m(rep.lat, rep.lon, ev.lat, ev.lon)
            if d <= EVENT_MERGE_RADIUS_M:
                reporters = json.loads(ev.reporters or "[]")
                if rep.device_id not in reporters:
                    reporters.append(rep.device_id)
                    ev.reporters = json.dumps(reporters)
                    ev.reports_count = len(reporters)
                meta = json.loads(ev.metadata or "{}")
                meta.setdefault("last_report_at", datetime.utcnow().isoformat())
                meta.setdefault("samples", []).append({
                    "report_id": rep.id,
                    "device_id": rep.device_id,
                    "g_force": rep.g_force,
                    "speed_before": rep.speed_before,
                    "speed_after": rep.speed_after,
                    "ts": rep.ts.isoformat()
                })
                ev.metadata = json.dumps(meta)
                sev = ev.severity or "unknown"
                try:
                    for s in meta.get("samples", []):
                        if s.get("g_force") and float(s.get("g_force")) >= 6.0:
                            sev = "high"
                        elif s.get("g_force") and float(s.get("g_force")) >= 3.0 and sev != "high":
                            sev = "medium"
                except Exception:
                    pass
                ev.severity = sev
                db.session.add(ev)
                db.session.commit()
                if ev.reports_count >= REPORTS_TO_CONFIRM and not ev.confirmed:
                    ev.confirmed = True
                    ev.confirmed_at = datetime.utcnow()
                    db.session.add(ev)
                    db.session.commit()
                return ev

        reporters = [rep.device_id] if rep.device_id else []
        meta = {
            "first_report_id": rep.id,
            "samples": [{
                "report_id": rep.id,
                "device_id": rep.device_id,
                "g_force": rep.g_force,
                "speed_before": rep.speed_before,
                "speed_after": rep.speed_after,
                "ts": rep.ts.isoformat()
            }]
        }
        sev = "unknown"
        try:
            if rep.g_force and float(rep.g_force) >= 6.0:
                sev = "high"
            elif rep.g_force and float(rep.g_force) >= 3.0:
                sev = "medium"
            elif rep.g_force:
                sev = "low"
        except Exception:
            pass
        ev = AccidentEvent(
            lat=rep.lat,
            lon=rep.lon,
            event_type=rep.event_type or "impact",
            reports_count=len(reporters),
            reporters=json.dumps(reporters),
            confirmed=(len(reporters) >= REPORTS_TO_CONFIRM),
            confirmed_at=(datetime.utcnow() if len(reporters) >= REPORTS_TO_CONFIRM else None),
            severity=sev,
            metadata=json.dumps(meta)
        )
        db.session.add(ev)
        db.session.commit()
        return ev
    except Exception:
        app.logger.exception("create_or_update_accident_event_from_report error")
        try:
            db.session.rollback()
        except Exception:
            pass
        return None

# -------------------------
# Authentication helpers
# -------------------------
from functools import wraps

def require_auth_token():
    token = None
    auth = request.headers.get("Authorization")
    if auth and auth.lower().startswith("token "):
        token = auth.split(" ", 1)[1].strip()
    if not token:
        body = request.get_json(silent=True) or {}
        token = body.get("token") or request.args.get("token")
    if not token:
        abort(401, "Missing token")
    device = Device.query.filter_by(token=token, revoked=False).first()
    if not device:
        abort(401, "Invalid or revoked token")
    return device

def require_admin_api():
    if session.get('admin_logged'):
        return True
    auth = request.headers.get("Authorization", "")
    if auth and auth.lower().startswith("bearer "):
        token = auth.split(" ", 1)[1].strip()
        if ADMIN_API_TOKEN and token == ADMIN_API_TOKEN:
            return True
    abort(401, "Admin access required")

# -------------------------
# Socket helpers
# -------------------------
def send_ws_to_device(device_id, event, payload):
    sids = connected_sockets.get(device_id)
    if not sids:
        app.logger.debug("No connected sids for device %s", device_id)
        return False
    for sid in list(sids):
        try:
            socketio.emit(event, payload, room=sid)
        except Exception:
            sids.discard(sid)
    if not sids:
        connected_sockets.pop(device_id, None)
    return True

# -------------------------
# API routes: health/onboard/heartbeat/nearby
# -------------------------
@app.route("/health")
def health():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat()})

@app.route("/onboard", methods=["POST"])
def onboard():
    payload = request.get_json(force=True, silent=True) or {}
    owner = payload.get("owner")
    car_name = payload.get("car_name")
    car_model = payload.get("car_model")
    plate = payload.get("plate")
    extra = payload.get("extra")
    device_id = uuid.uuid4().hex
    token = create_device_token()
    d = Device(id=device_id, token=token, owner=owner, car_name=car_name, car_model=car_model, plate=plate, extra=(json.dumps(extra) if extra else None))
    db.session.add(d)
    db.session.commit()
    return jsonify({"device_id": device_id, "token": token})

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    device = require_auth_token()
    payload = request.get_json(force=True, silent=True) or {}
    device_id = payload.get("device_id") or device.id

    now_ts = time.time()
    last = _last_heartbeat_at.get(device_id)
    if last and (now_ts - last) < HEARTBEAT_MIN_INTERVAL_S:
        return jsonify({"ok": True, "saved_at": None, "note": "rate_limited"}), 202
    _last_heartbeat_at[device_id] = now_ts

    try:
        lat = float(payload.get("lat"))
        lon = float(payload.get("lon"))
    except Exception:
        return jsonify({"error": "lat & lon required and must be numbers"}), 400
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return jsonify({"error": "lat/lon out of range"}), 400

    speed_mps = payload.get("speed_mps")
    if speed_mps is None:
        speed_kmh = payload.get("speed_kmh")
        if speed_kmh is not None:
            try:
                speed_mps = float(speed_kmh) / 3.6
            except Exception:
                speed_mps = 0.0
        else:
            speed_mps = 0.0
    try:
        bearing = float(payload.get("bearing", 0.0))
    except Exception:
        bearing = 0.0
    try:
        heading = float(payload.get("heading", bearing))
    except Exception:
        heading = bearing
    src = payload.get("source", "app")

    snap = Snapshot(
        device_id=device_id,
        ts=datetime.utcnow(),
        lat=lat,
        lon=lon,
        speed_mps=float(speed_mps),
        bearing_deg=float(bearing) % 360.0,
        heading_deg=float(heading) % 360.0,
        source=str(src),
        raw=json.dumps(payload)
    )
    db.session.add(snap)
    db.session.commit()

    try:
        update_active_device_from_snapshot(snap)
        prune_active_devices()
    except Exception:
        app.logger.exception("active_devices update error")

    cleanup_old_snapshots()

    try:
        nearby_payload = compute_nearby_for_device(device_id, radius_m=NEARBY_DEFAULT_RADIUS_M)
        send_ws_to_device(device_id, 'nearby_update', nearby_payload)
    except Exception as e:
        app.logger.exception("compute_nearby error: %s", e)

    return jsonify({"ok": True, "saved_at": snap.ts.isoformat()})

@app.route("/nearby", methods=["GET"])
def nearby():
    token_device = None
    try:
        token_device = require_auth_token()
    except Exception:
        token_device = None
    device_id = request.args.get("device_id") or (token_device.id if token_device else None)
    if not device_id:
        return jsonify({"error": "device_id required (or provide Authorization token)"}), 400
    radius_m = float(request.args.get("radius_m", NEARBY_DEFAULT_RADIUS_M))
    try:
        payload = compute_nearby_for_device(device_id, radius_m=radius_m)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def compute_nearby_for_device(device_id, radius_m=NEARBY_DEFAULT_RADIUS_M):
    self_snap = Snapshot.query.filter_by(device_id=device_id).order_by(Snapshot.ts.desc()).first()
    if not self_snap:
        with active_devices_lock:
            entry = active_devices.get(device_id)
        if entry:
            class _Tmp: pass
            t = _Tmp()
            t.device_id = device_id
            t.lat = entry.get("lat")
            t.lon = entry.get("lon")
            t.speed_mps = entry.get("speed_mps")
            t.bearing_deg = entry.get("bearing_deg")
            t.ts = entry.get("ts")
            t.source = entry.get("source", "app")
            self_snap = t
        else:
            raise RuntimeError("no snapshot for device")

    cutoff = datetime.utcnow() - timedelta(seconds=CLEANUP_STALE_SECONDS)
    other_snaps = Snapshot.query.filter(Snapshot.device_id != device_id, Snapshot.ts >= cutoff).all()

    with active_devices_lock:
        for did, entry in active_devices.items():
            if did == device_id:
                continue
            seen = any(s.device_id == did for s in other_snaps)
            if seen:
                continue
            if entry.get("ts") >= cutoff:
                class _Tmp2: pass
                t = _Tmp2()
                t.device_id = did
                t.lat = entry.get("lat")
                t.lon = entry.get("lon")
                t.speed_mps = entry.get("speed_mps")
                t.bearing_deg = entry.get("bearing_deg")
                t.ts = entry.get("ts")
                t.source = entry.get("source", "app")
                other_snaps.append(t)

    results = []
    for s in other_snaps:
        d = haversine_m(self_snap.lat, self_snap.lon, s.lat, s.lon)
        if d > radius_m:
            continue
        direction = classify_direction(self_snap.bearing_deg, s.bearing_deg)
        closing = closing_speed_mps(self_snap.lat, self_snap.lon, self_snap.speed_mps, self_snap.bearing_deg,
                                    s.lat, s.lon, s.speed_mps, s.bearing_deg)
        guidance = compute_warning(self_snap.lat, self_snap.lon, self_snap.speed_mps, self_snap.bearing_deg,
                                   s.lat, s.lon, s.speed_mps, s.bearing_deg)
        risk = classify_risk(self_snap, s)
        results.append({
            "device_id": s.device_id,
            "ts": s.ts.isoformat() if hasattr(s, "ts") else None,
            "lat": s.lat,
            "lon": s.lon,
            "distance_m": round(d, 2),
            "direction": direction,
            "speed_mps": round(s.speed_mps, 2),
            "bearing_deg": round(s.bearing_deg, 1),
            "closing_mps": round(closing, 2),
            "guidance": guidance,
            "decision": risk.get("decision"),
            "confidence": float(risk.get("confidence", 0.0)),
            "reason": risk.get("reason")
        })
    results.sort(key=lambda x: x["distance_m"])
    return {
        "self": {
            "device_id": self_snap.device_id,
            "lat": self_snap.lat,
            "lon": self_snap.lon,
            "speed_mps": round(self_snap.speed_mps, 2),
            "bearing_deg": round(self_snap.bearing_deg, 1),
            "ts": self_snap.ts.isoformat() if hasattr(self_snap, "ts") else None
        },
        "nearby": results
    }

# -------------------------
# Event reporting endpoint (from app)
# -------------------------
@app.route("/report_event", methods=["POST"])
def report_event():
    device = require_auth_token()
    payload = request.get_json(force=True, silent=True) or {}
    try:
        lat = float(payload.get("lat"))
        lon = float(payload.get("lon"))
    except Exception:
        return jsonify({"error": "lat & lon required"}), 400
    event_type = payload.get("event_type", "impact")
    try:
        g_force = float(payload.get("g_force")) if payload.get("g_force") is not None else None
    except Exception:
        g_force = None
    try:
        spb = float(payload.get("speed_before")) if payload.get("speed_before") is not None else None
    except Exception:
        spb = None
    try:
        spa = float(payload.get("speed_after")) if payload.get("speed_after") is not None else None
    except Exception:
        spa = None
    ts_str = payload.get("ts")
    try:
        ts = datetime.fromisoformat(ts_str) if ts_str else datetime.utcnow()
    except Exception:
        ts = datetime.utcnow()

    rep = EventReport(
        device_id=device.id,
        ts=ts,
        lat=lat,
        lon=lon,
        event_type=event_type,
        g_force=g_force,
        speed_before=spb,
        speed_after=spa,
        raw=json.dumps(payload)
    )
    db.session.add(rep)
    db.session.commit()

    ev = create_or_update_accident_event_from_report(rep)

    return jsonify({
        "ok": True,
        "report_id": rep.id,
        "event_id": ev.id if ev else None,
        "event_confirmed": bool(ev.confirmed) if ev else False
    })

# -------------------------
# Admin templates (login + dashboard)
# -------------------------
ADMIN_LOGIN_HTML = """
<!doctype html>
<html>
<head><meta charset="utf-8"><title>Admin login</title></head>
<body style="font-family: sans-serif; margin: 20px;">
  <h2>Beacon Admin Login</h2>
  {% with messages = get_flashed_messages() %}
    {% if messages %}
      <div style="color: red;">{{ messages[0] }}</div>
    {% endif %}
  {% endwith %}
  <form method="post" action="{{ url_for('admin_login') }}">
    <label>Username: <input name="username" required></label><br/><br/>
    <label>Password: <input name="password" type="password" required></label><br/><br/>
    <button type="submit">Login</button>
  </form>
  <p style="margin-top: 1em;">
    If no admin exists, set environment variables <code>ADMIN_USER</code> and <code>ADMIN_PASS</code>
    before first run to create one automatically.
  </p>
</body>
</html>
"""

DASHBOARD_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Beacon — Dashboard (Events & Hotspots)</title>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    :root{ --bg:#f7f9fb; --card:#ffffff; --muted:#6b7280; --accent:#0b84ff; }
    body{ font-family: Inter, system-ui, -apple-system, "Segoe UI", Roboto, Arial; margin:0; background:var(--bg); color:#111827;}
    header{ background: linear-gradient(90deg,#0b84ff 0%, #00c6ff 100%); color:white; padding:14px 18px; display:flex; align-items:center; gap:12px;}
    header h1{ font-size:18px; margin:0;}
    .wrap{ display:flex; gap:12px; padding:12px; height: calc(100vh - 64px); box-sizing:border-box; }
    .left{ width:360px; display:flex; flex-direction:column; gap:12px; }
    .card{ background:var(--card); border-radius:10px; padding:12px; box-shadow:0 6px 18px rgba(15,23,42,0.06); overflow:auto; }
    #devicesList, #eventsList, #hotspotsList{ list-style:none; margin:0; padding:0; }
    li.item{ padding:10px; border-radius:8px; margin-bottom:8px; border:1px solid #eef2f7; display:flex; justify-content:space-between; align-items:flex-start; gap:8px; }
    .big{ font-weight:600; font-size:14px; }
    .muted{ color:var(--muted); font-size:13px; }
    #mapWrap{ flex:1; display:flex; flex-direction:column; gap:12px; }
    #map{ flex:1; border-radius:10px; overflow:hidden; }
    .row{ display:flex; justify-content:space-between; gap:8px; margin-top:6px; }
    .btn{ background:var(--accent); color:white; padding:8px 10px; border-radius:8px; border:none; cursor:pointer; }
    .btn.ghost{ background:transparent; color:var(--accent); border:1px solid #e6f2f7; }
    .small{ font-size:12px; padding:6px 8px; border-radius:6px; }
    .pill{ padding:6px 8px; border-radius:6px; background:#eef2f7; font-size:12px; }
    .legend{ font-size:13px; display:flex; gap:8px; align-items:center; }
    .legend .dot{ width:12px; height:12px; border-radius:6px; display:inline-block; }
    .controls{ display:flex; gap:8px; flex-wrap:wrap; }
    .form-row{ display:flex; gap:6px; margin-top:6px; }
    input[type="text"], input[type="number"]{ padding:6px 8px; border-radius:6px; border:1px solid #e6eef6; font-size:13px; }
  </style>
</head>
<body>
  <header>
    <h1>Beacon — Dashboard (Events & Hotspots)</h1>
    <div style="margin-left:auto; font-size:13px; opacity:0.95;">Auto-refresh every 5s — admin session required</div>
  </header>

  <div class="wrap">
    <div class="left">
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <div>
            <div class="muted">Devices</div>
            <div class="big" id="devicesCount">0 devices</div>
          </div>
          <div class="controls">
            <button id="btnRefresh" class="btn small">Refresh</button>
            <button id="btnCenter" class="btn small ghost">Center Map</button>
          </div>
        </div>
        <div id="statusBar" class="muted" style="margin-top:8px;">Status: idle</div>
        <hr style="margin:10px 0; border:none; border-top:1px solid #f1f5f9;" />
        <ul id="devicesList"></ul>
      </div>

      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <div>
            <div class="muted">Events (possible accidents)</div>
            <div class="big" id="eventsCount">0 events</div>
          </div>
          <div>
            <button id="btnClearEvents" class="btn small ghost">Clear selection</button>
          </div>
        </div>
        <ul id="eventsList"></ul>
      </div>

      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <div>
            <div class="muted">Hotspots</div>
            <div class="big" id="hotspotsCount">0</div>
          </div>
          <div>
            <button id="btnNewHotspot" class="btn small">Add Hotspot</button>
          </div>
        </div>
        <div id="hotspotForm" style="display:none; margin-top:8px;">
          <div class="form-row"><input id="hsName" type="text" placeholder="Name"/></div>
          <div class="form-row"><input id="hsLat" type="number" step="0.000001" placeholder="Lat"/><input id="hsLon" type="number" step="0.000001" placeholder="Lon"/></div>
          <div class="form-row"><input id="hsRadius" type="number" step="1" placeholder="Radius (m)" value="50"/><select id="hsRisk"><option value="high">high</option><option value="medium">medium</option><option value="low">low</option></select></div>
          <div class="form-row"><button id="btnSaveHotspot" class="btn small">Save</button><button id="btnCancelHotspot" class="btn small ghost">Cancel</button></div>
        </div>
        <hr style="margin:8px 0; border:none; border-top:1px solid #f1f5f9;" />
        <ul id="hotspotsList"></ul>
      </div>

      <div class="card" style="text-align:center;">
        <div class="legend"><span class="dot" style="background:#0b84ff"></span> Device &nbsp; <span class="dot" style="background:#e53935"></span> Event &nbsp; <span class="dot" style="background:#ffb300"></span> Hotspot</div>
      </div>
    </div>

    <div id="mapWrap" class="card">
      <div id="map"></div>
    </div>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
  /* Dashboard JS identical to the one you specified earlier.
     It fetches /admin/devices, /admin/events, /admin/hotspots and provides UI actions
     for confirming/dismissing events and adding/deleting hotspots.
     (Omitted here for brevity — use the client code you prepared.) */
  </script>
</body>
</html>
"""

# -------------------------
# Admin JSON endpoints
# -------------------------
@app.route('/admin/events')
def admin_events():
    require_admin_api()
    events = AccidentEvent.query.order_by(AccidentEvent.created_at.desc()).limit(200).all()
    out = []
    for e in events:
        try:
            reporters = json.loads(e.reporters or "[]")
        except Exception:
            reporters = []
        try:
            meta = json.loads(e.metadata or "{}")
        except Exception:
            meta = {}
        out.append({
            "id": e.id,
            "created_at": e.created_at.isoformat() if e.created_at else None,
            "lat": e.lat,
            "lon": e.lon,
            "event_type": e.event_type,
            "reports_count": e.reports_count,
            "reporters": reporters,
            "confirmed": bool(e.confirmed),
            "confirmed_at": e.confirmed_at.isoformat() if e.confirmed_at else None,
            "severity": e.severity,
            "metadata": meta
        })
    return jsonify({"events": out})

@app.route('/admin/event/<int:event_id>/confirm', methods=['POST'])
def admin_event_confirm(event_id):
    require_admin_api()
    e = AccidentEvent.query.get_or_404(event_id)
    if not e.confirmed:
        e.confirmed = True
        e.confirmed_at = datetime.utcnow()
        db.session.add(e)
        db.session.commit()
    return jsonify({"ok": True, "confirmed": True, "event_id": e.id})

@app.route('/admin/event/<int:event_id>/dismiss', methods=['POST'])
def admin_event_dismiss(event_id):
    require_admin_api()
    e = AccidentEvent.query.get_or_404(event_id)
    e.severity = "ignored"
    e.confirmed = False
    db.session.add(e)
    db.session.commit()
    return jsonify({"ok": True, "dismissed": True, "event_id": e.id})

@app.route('/admin/hotspots', methods=['GET', 'POST'])
def admin_hotspots():
    require_admin_api()
    if request.method == 'GET':
        hs = Hotspot.query.order_by(Hotspot.created_at.desc()).all()
        out = []
        for h in hs:
            out.append({
                "id": h.id,
                "name": h.name,
                "lat": h.lat,
                "lon": h.lon,
                "radius_m": h.radius_m,
                "risk_level": h.risk_level,
                "created_at": h.created_at.isoformat() if h.created_at else None
            })
        return jsonify({"hotspots": out})
    else:
        body = request.get_json(force=True, silent=True) or {}
        name = body.get("name", "Unnamed")
        lat = float(body.get("lat"))
        lon = float(body.get("lon"))
        radius_m = float(body.get("radius_m", body.get("radius", 50.0)))
        risk_level = body.get("risk_level", "high")
        h = Hotspot(name=name, lat=lat, lon=lon, radius_m=radius_m, risk_level=risk_level)
        db.session.add(h)
        db.session.commit()
        return jsonify({"ok": True, "hotspot_id": h.id})

@app.route('/admin/hotspot/<int:hid>/delete', methods=['POST'])
def admin_hotspot_delete(hid):
    require_admin_api()
    h = Hotspot.query.get_or_404(hid)
    db.session.delete(h)
    db.session.commit()
    return jsonify({"ok": True})

# -------------------------
# Admin web pages
# -------------------------
@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'GET':
        return render_template_string(ADMIN_LOGIN_HTML)
    username = request.form.get('username')
    password = request.form.get('password')
    if not username or not password:
        flash("Missing username or password")
        return render_template_string(ADMIN_LOGIN_HTML), 400
    admin = Admin.query.filter_by(username=username).first()
    if not admin or not check_password_hash(admin.password_hash, password):
        flash("Invalid credentials")
        return render_template_string(ADMIN_LOGIN_HTML), 401
    session['admin_logged'] = True
    session['admin_user'] = admin.username
    return redirect(url_for('dashboard'))

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged', None)
    session.pop('admin_user', None)
    return redirect(url_for('admin_login'))

@app.route('/dashboard')
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route('/friendly')
def friendly():
    return render_template_string(DASHBOARD_HTML)

@app.route('/admin/devices')
def admin_devices():
    require_admin_api()
    devices = Device.query.all()
    out = []
    prune_active_devices()
    for d in devices:
        snap = Snapshot.query.filter_by(device_id=d.id).order_by(Snapshot.ts.desc()).first()
        last = None
        if snap:
            parsed_raw = None
            if snap.raw:
                try:
                    parsed_raw = json.loads(snap.raw)
                except Exception:
                    parsed_raw = snap.raw
            last = {
                "ts": snap.ts.isoformat(),
                "lat": snap.lat,
                "lon": snap.lon,
                "speed_mps": round(snap.speed_mps, 3),
                "bearing_deg": round(snap.bearing_deg, 1),
                "raw": parsed_raw
            }
        else:
            with active_devices_lock:
                entry = active_devices.get(d.id)
            if entry:
                parsed_raw = entry.get("raw")
                last = {
                    "ts": entry.get("ts").isoformat() if entry.get("ts") else None,
                    "lat": entry.get("lat"),
                    "lon": entry.get("lon"),
                    "speed_mps": round(entry.get("speed_mps") or 0.0, 3),
                    "bearing_deg": round(entry.get("bearing_deg") or 0.0, 1),
                    "raw": parsed_raw
                }

        out.append({
            "id": d.id,
            "owner": d.owner,
            "car_name": d.car_name,
            "car_model": d.car_model,
            "plate": d.plate,
            "extra": d.extra,
            "created_at": d.created_at.isoformat() if d.created_at else None,
            "revoked": bool(d.revoked),
            "last_snapshot": last,
            "connected": bool(connected_sockets.get(d.id))
        })
    return jsonify({"devices": out})

@app.route('/admin/device/<device_id>/json')
def admin_device_json(device_id):
    require_admin_api()
    d = Device.query.get_or_404(device_id)
    snaps = _recent_snapshots_for_device(device_id, limit=20)
    snaps_out = []
    for s in snaps:
        parsed_raw = None
        if s.raw:
            try:
                parsed_raw = json.loads(s.raw)
            except Exception:
                parsed_raw = s.raw
        snaps_out.append({
            "ts": s.ts.isoformat(),
            "lat": s.lat,
            "lon": s.lon,
            "speed_mps": s.speed_mps,
            "bearing_deg": s.bearing_deg,
            "source": s.source,
            "raw": parsed_raw
        })

    if not snaps_out:
        with active_devices_lock:
            entry = active_devices.get(device_id)
        if entry:
            snaps_out.append({
                "ts": entry.get("ts").isoformat() if entry.get("ts") else None,
                "lat": entry.get("lat"),
                "lon": entry.get("lon"),
                "speed_mps": entry.get("speed_mps"),
                "bearing_deg": entry.get("bearing_deg"),
                "source": entry.get("source", "app"),
                "raw": entry.get("raw")
            })

    last = snaps_out[0] if snaps_out else None
    return jsonify({
        "device": {
            "id": d.id,
            "owner": d.owner,
            "car_name": d.car_name,
            "car_model": d.car_model,
            "plate": d.plate,
            "extra": d.extra,
            "created_at": d.created_at.isoformat() if d.created_at else None,
            "revoked": bool(d.revoked)
        },
        "last_snapshot": last,
        "snapshots": snaps_out,
        "connected": bool(connected_sockets.get(d.id))
    })

@app.route('/admin/device/<device_id>/revoke', methods=['POST'])
def admin_device_revoke(device_id):
    require_admin_api()
    d = Device.query.get_or_404(device_id)
    d.revoked = True
    db.session.commit()
    sids = connected_sockets.pop(device_id, None)
    with active_devices_lock:
        active_devices.pop(device_id, None)
    return jsonify({"ok": True, "revoked": True})

# -------------------------
# Socket.IO events
# -------------------------
@socketio.on('connect')
def ws_connect():
    sid = request.sid
    app.logger.debug('Socket connected: %s', sid)
    emit('connected', {'sid': sid})

@socketio.on('register')
def ws_register(data):
    sid = request.sid
    device_id = data.get('device_id')
    token = data.get('token')
    if not device_id or not token:
        emit('error', {'error': 'device_id and token required'})
        return
    device = Device.query.filter_by(id=device_id, token=token, revoked=False).first()
    if not device:
        emit('error', {'error': 'invalid token/device'})
        return
    sids = connected_sockets.get(device_id)
    if not sids:
        connected_sockets[device_id] = set()
    connected_sockets[device_id].add(sid)
    join_room(sid)
    emit('registered', {'ok': True, 'device_id': device_id})
    app.logger.info('Device %s registered socket %s', device_id, sid)

@socketio.on('disconnect')
def ws_disconnect():
    sid = request.sid
    app.logger.debug('Socket disconnected: %s', sid)
    to_remove = []
    for dev, sids in list(connected_sockets.items()):
        if sid in sids:
            sids.discard(sid)
        if not sids:
            to_remove.append(dev)
    for d in to_remove:
        connected_sockets.pop(d, None)

@socketio.on('get_nearby')
def ws_get_nearby(data):
    device_id = data.get('device_id')
    token = data.get('token')
    device = Device.query.filter_by(id=device_id, token=token, revoked=False).first()
    if not device:
        emit('error', {'error': 'invalid device/token'})
        return
    try:
        payload = compute_nearby_for_device(device_id, radius_m=NEARBY_DEFAULT_RADIUS_M)
        emit('nearby', payload)
    except Exception as e:
        emit('error', {'error': str(e)})

# -------------------------
# CLI entry
# -------------------------
if __name__ == "__main__":
    init_db()
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=os.environ.get("FLASK_DEBUG", "0") == "1")
