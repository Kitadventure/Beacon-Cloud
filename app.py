# app.py — Beacon backend with WebSocket (Flask + Flask-SocketIO)
# Threading async_mode for Windows/dev. For production and multi-worker, use a message queue + eventlet/gevent.

import os
import uuid
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
NEARBY_DEFAULT_RADIUS_M = float(os.environ.get("NEARBY_DEFAULT_RADIUS_M", "1000"))  # 1 km default per your spec
HEARTBEAT_MIN_INTERVAL_S = float(os.environ.get("HEARTBEAT_MIN_INTERVAL_S", "0.5"))  # basic rate-limit
UNSAFE_TTC_SECONDS = float(os.environ.get("UNSAFE_TTC_SECONDS", "6.0"))  # threshold for opposite-direction unsafe
CONFIRMATION_RADIUS_M = float(os.environ.get("CONFIRMATION_RADIUS_M", "30.0"))  # support devices gathering radius
# Optional API token for scripted admin calls
ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN")
# Optionally seed admin via env on first run:
ADMIN_USER = os.environ.get("ADMIN_USER")
ADMIN_PASS = os.environ.get("ADMIN_PASS")

VEHICLE_LENGTH_M = float(os.environ.get("VEHICLE_LENGTH_M", "5.0"))
OVERTAKE_EXTRA_M = float(os.environ.get("OVERTAKE_EXTRA_M", "5.0"))
SAFETY_FACTOR = float(os.environ.get("SAFETY_FACTOR", "1.5"))

# -------------------------
# Flask + SQLAlchemy + SocketIO init
# -------------------------
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-change-me")

# Use threading async_mode for Windows/dev. For production (multi-process) configure SocketIO with Redis and eventlet.
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
    source = db.Column(db.String(32), default="app") # e.g., "app", "web"
    raw = db.Column(db.Text) # JSON dump of raw payload (optional)

# -------------------------
# Simple in-memory rate tracking (per-device)
# -------------------------
_last_heartbeat_at = {}  # device_id -> timestamp (float)
# Note: for multi-process deployments, move this into Redis or DB.

# -------------------------
# Helpers: geodesy + relative computations
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
    # project velocities onto the line joining vehicles, sum projections (approx)
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
# New: Confidence + Decision logic (cloud authoritative)
# (unchanged from your original code)
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
    guidance = {}
    guidance['distance_m'] = round(d, 2)
    guidance['direction'] = cls
    guidance['closing_mps'] = round(close, 2)
    guidance['time_to_collision_s'] = round(ttc, 2) if ttc != float('inf') else None
    if cls == "same":
        required = estimate_overtake_time_mps(self_speed_mps, other_speed_mps)
        guidance['overtake_time_required_s'] = round(required, 2)
    elif cls == "opposite":
        guidance['overtake_time_required_s'] = None
        if ttc != float('inf'):
            guidance['unsafe_if_overtaking'] = (ttc < UNSAFE_TTC_SECONDS)
    return guidance

def classify_risk(self_snap, other_snap):
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

    if direction == "opposite":
        if ttc == float('inf'):
            return {"decision": "green", "confidence": base_confidence * 0.6, "reason": "opposite_no_closing"}
        if ttc < UNSAFE_TTC_SECONDS:
            conf = min(1.0, base_confidence + 0.25 * (1.0 - (ttc / UNSAFE_TTC_SECONDS)) + 0.1 * support_score)
            return {"decision": "red", "confidence": round(conf, 2), "reason": f"opposite_ttc_{round(ttc,1)}s"}
        else:
            conf = base_confidence * (0.6 + 0.4 * max(0.0, (NEARBY_DEFAULT_RADIUS_M - d) / NEARBY_DEFAULT_RADIUS_M))
            return {"decision": "green", "confidence": round(conf, 2), "reason": f"opposite_ttc_safe_{round(ttc,1)}s"}

    if direction == "same":
        required = estimate_overtake_time_mps(self_snap.speed_mps, other_snap.speed_mps)
        if self_snap.speed_mps <= other_snap.speed_mps + 0.01:
            return {"decision": "green", "confidence": base_confidence * 0.6, "reason": "same_no_overtake_possible"}
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
        # create initial admin from env if provided and no admins exist
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
    """
    API-level admin check: allow if logged in via session OR provide ADMIN_API_TOKEN in Bearer header.
    Use this for JSON admin endpoints. UI routes rely on session.
    """
    # session-based admin
    if session.get('admin_logged'):
        return True
    # bearer token fallback
    auth = request.headers.get("Authorization", "")
    if auth and auth.lower().startswith("bearer "):
        token = auth.split(" ", 1)[1].strip()
        if ADMIN_API_TOKEN and token == ADMIN_API_TOKEN:
            return True
    abort(401, "Admin access required")

# -------------------------
# In-memory socket tracking
# -------------------------
connected_sockets = {}  # { device_id: set(sid) }

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
# Routes: API (onboard, heartbeat, nearby, admin)
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
    """
    Device heartbeat endpoint. Server is authoritative: computes nearby & decisions and pushes back via socket.io.
    Returns saved_at and optionally immediate nearby decision payload for REST callers.
    """
    device = require_auth_token()
    payload = request.get_json(force=True, silent=True) or {}
    device_id = payload.get("device_id") or device.id

    # Rate-limiting (basic)
    now_ts = time.time()
    last = _last_heartbeat_at.get(device_id)
    if last and (now_ts - last) < HEARTBEAT_MIN_INTERVAL_S:
        # ignore or drop duplicates; respond success but do not spam DB
        return jsonify({"ok": True, "saved_at": None, "note": "rate_limited"}), 202
    _last_heartbeat_at[device_id] = now_ts

    # Validate coordinates / basic sanity checks
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

    cleanup_old_snapshots()

    # compute authoritative nearby + decisions and push via socket
    try:
        nearby_payload = compute_nearby_for_device(device_id, radius_m=NEARBY_DEFAULT_RADIUS_M)
        # Push: 'nearby_update' preserves compatibility but each nearby entry now includes decision+confidence
        send_ws_to_device(device_id, 'nearby_update', nearby_payload)
    except Exception as e:
        app.logger.exception("compute_nearby error: %s", e)

    return jsonify({"ok": True, "saved_at": snap.ts.isoformat()})

@app.route("/nearby", methods=["GET"])
def nearby():
    """
    REST nearby — returns authoritative decisions for nearby vehicles
    """
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
        raise RuntimeError("no snapshot for device")

    cutoff = datetime.utcnow() - timedelta(seconds=CLEANUP_STALE_SECONDS)
    other_snaps = Snapshot.query.filter(Snapshot.device_id != device_id, Snapshot.ts >= cutoff).all()

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
            "ts": s.ts.isoformat(),
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
            "ts": self_snap.ts.isoformat()
        },
        "nearby": results
    }

# -------------------------
# Admin/UI templates and routes
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

# Enhanced friendly admin UI: search/filter + leaflet map + live-updating device detail
FRIENDLY_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Beacon Admin — Devices</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
    integrity="sha256-sA+qH6t3n0lI3lJ5uJ6a2h5Gxv4nDAn3vYkP7kGQm0A=" crossorigin=""/>
  <style>
    body { font-family: system-ui, -apple-system, "Segoe UI", Roboto, Arial; margin: 12px; }
    .topbar { display:flex; gap:12px; align-items:center; margin-bottom:10px; }
    .controls { display:flex; gap:8px; align-items:center; }
    input[type="search"] { padding:6px; border-radius:6px; border:1px solid #ccc; width:320px; }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border: 1px solid #ddd; padding: 8px; vertical-align: middle; }
    th { background: #f6f6f6; text-align: left; }
    .muted { color: #666; font-size: 0.9em; }
    .btn { padding: 6px 10px; border-radius: 6px; cursor: pointer; border: 1px solid #ccc; background: #fff; }
    #panel { display:flex; gap:12px; margin-top: 12px; }
    #map { height: 360px; width: 60%; border: 1px solid #ddd; border-radius:6px; }
    #detail { width: 40%; max-height: 360px; overflow:auto; border:1px solid #eee; padding:8px; border-radius:6px; background:#fafafa; }
    pre { background:#fff; padding:8px; border-radius:6px; overflow:auto; }
  </style>
</head>
<body>
  <h2>Beacon — Admin Console</h2>
  <div class="topbar">
    <div class="controls">
      <button id="btnRefresh" class="btn">Refresh</button>
      <button id="btnLogout" class="btn">Logout</button>
      <span class="muted">Connected via socket: <span id="connectedCount">0</span></span>
    </div>
    <div style="flex:1"></div>
    <div>
      <input id="search" type="search" placeholder="Search by owner, car, plate, or device id">
    </div>
  </div>

  <table id="devicesTbl">
    <thead><tr>
      <th style="width:240px">Device ID / Owner</th><th>Car</th><th>Plate</th><th>Last seen</th><th>Location</th><th>Speed</th><th>Socket</th><th>Actions</th>
    </tr></thead>
    <tbody></tbody>
  </table>

  <div id="panel">
    <div id="map"></div>
    <div id="detail"><em>Select a device to view details & live position.</em></div>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
    integrity="sha256-o9N1j8wE5fhb3aC9t1gqgk4FpaNqB1h4Gxv7m2b0v4g=" crossorigin=""></script>

  <script>
    let devicesCache = [];
    let selectedDeviceId = null;
    let detailRefreshTimer = null;

    // init map
    const map = L.map('map', { center: [0,0], zoom: 2 });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      attribution: '© OpenStreetMap contributors'
    }).addTo(map);
    let marker = null;
    let accuracyCircle = null;

    async function fetchDevices(){
      const res = await fetch('/admin/devices');
      if (!res.ok) {
        if (res.status === 401) { window.location = '/admin/login'; return; }
        alert('Failed to fetch devices: ' + res.status);
        return;
      }
      const data = await res.json();
      devicesCache = data.devices || [];
      renderTable(devicesCache);
      const connected = devicesCache.filter(d => d.connected).length;
      document.getElementById('connectedCount').innerText = connected;
    }

    function renderTable(list){
      const tbody = document.querySelector('#devicesTbl tbody');
      tbody.innerHTML = '';
      const q = (document.getElementById('search').value || '').toLowerCase().trim();
      for (const d of list){
        // filter by search
        const owner = (d.owner || '').toLowerCase();
        const car = ((d.car_name||'') + ' ' + (d.car_model||'')).toLowerCase();
        const plate = (d.plate||'').toLowerCase();
        const id = (d.id||'').toLowerCase();
        if (q) {
          if (!(owner.includes(q) || car.includes(q) || plate.includes(q) || id.includes(q))) continue;
        }

        const tr = document.createElement('tr');
        const last = d.last_snapshot ? new Date(d.last_snapshot.ts).toLocaleString() : '—';
        const lat = d.last_snapshot ? d.last_snapshot.lat.toFixed(6) : null;
        const lon = d.last_snapshot ? d.last_snapshot.lon.toFixed(6) : null;
        const speed = d.last_snapshot ? ( (d.last_snapshot.speed_mps || 0) * 3.6 ).toFixed(1) + ' km/h' : '—';
        const socketStatus = d.connected ? 'yes' : 'no';

        tr.innerHTML = `
          <td>
            <div style="font-size:0.85em;"><code>${d.id}</code></div>
            <div class="muted">${d.owner || '—'}</div>
          </td>
          <td>${d.car_name || '—'}${d.car_model ? ' / ' + d.car_model : ''}</td>
          <td>${d.plate || '—'}</td>
          <td>${last}</td>
          <td>${lat && lon ? `<a href="https://www.openstreetmap.org/?mlat=${lat}&mlon=${lon}#map=18/${lat}/${lon}" target="_blank">view</a> <span class="muted">(${lat}, ${lon})</span>` : '—'}</td>
          <td>${speed}</td>
          <td>${socketStatus}</td>
          <td>
            <button class="btn" onclick="showDetails('${d.id}')">Details</button>
            <button class="btn" onclick="revokeDevice('${d.id}')">Revoke</button>
          </td>
        `;
        tbody.appendChild(tr);
      }
    }

    document.getElementById('search').addEventListener('input', () => renderTable(devicesCache));
    document.getElementById('btnRefresh').addEventListener('click', fetchDevices);
    document.getElementById('btnLogout').addEventListener('click', () => { window.location = '/admin/logout'; });

    async function showDetails(id){
      selectedDeviceId = id;
      // clear previous timer
      if (detailRefreshTimer) { clearInterval(detailRefreshTimer); detailRefreshTimer = null; }
      await loadDeviceDetailsOnce(id);
      // start live refresh every 3s for the detail panel & map
      detailRefreshTimer = setInterval(() => loadDeviceDetailsOnce(id), 3000);
    }

    async function loadDeviceDetailsOnce(id){
      const res = await fetch('/admin/device/' + id + '/json');
      if (!res.ok) { if (res.status === 401) window.location='/admin/login'; return; }
      const d = await res.json();
      renderDetailPanel(d);
      // update marker
      if (d.last_snapshot) {
        const lat = d.last_snapshot.lat;
        const lon = d.last_snapshot.lon;
        const speed = (d.last_snapshot.speed_mps || 0) * 3.6;
        placeMarker(lat, lon, speed, d.last_snapshot.ts);
      } else {
        clearMarker();
      }
    }

    function renderDetailPanel(d){
      const snapsHtml = (d.snapshots || []).map(s => `<li>${new Date(s.ts).toLocaleString()} — lat:${s.lat.toFixed(6)}, lon:${s.lon.toFixed(6)} — ${(s.speed_mps*3.6).toFixed(1)} km/h</li>`).join('');
      const el = document.getElementById('detail');
      el.innerHTML = `
        <h3>Device ${d.device.id} details</h3>
        <div><strong>Owner:</strong> ${d.device.owner || '—'}</div>
        <div><strong>Car:</strong> ${d.device.car_name || '—'} ${d.device.car_model ? (' / ' + d.device.car_model) : ''}</div>
        <div><strong>Plate:</strong> ${d.device.plate || '—'}</div>
        <div><strong>Extra JSON:</strong> <pre>${d.device.extra || '—'}</pre></div>
        <div><strong>Connected socket:</strong> ${d.connected ? 'yes' : 'no'}</div>
        <div><strong>Last snapshot:</strong> ${d.last_snapshot ? new Date(d.last_snapshot.ts).toLocaleString() : '—'}</div>
        <div style="margin-top:8px;"><strong>Recent snapshots:</strong><ul>${snapsHtml}</ul></div>
      `;
    }

    async function revokeDevice(id){
      if (!confirm('Revoke device token? This prevents app from authenticating with that token.')) return;
      const res = await fetch('/admin/device/' + id + '/revoke', { method: 'POST' });
      if (!res.ok) { alert('Revoke failed'); return; }
      alert('Revoked');
      fetchDevices();
      if (selectedDeviceId === id) {
        clearMarker();
        document.getElementById('detail').innerHTML = '<em>Device revoked.</em>';
      }
    }

    function placeMarker(lat, lon, speed, ts){
      if (!marker) {
        marker = L.marker([lat, lon]).addTo(map);
      } else {
        marker.setLatLng([lat, lon]);
      }
      if (!accuracyCircle) {
        accuracyCircle = L.circle([lat, lon], {radius: 5}).addTo(map);
      } else {
        accuracyCircle.setLatLng([lat, lon]);
      }
      // popup content
      const popup = `<div><strong>${selectedDeviceId || ''}</strong><br/>${new Date(ts).toLocaleString()}<br/>Speed: ${speed.toFixed(1)} km/h</div>`;
      marker.bindPopup(popup);
      // center map to marker (only pan when not already zoomed in too tightly)
      if (map.getZoom() < 15) {
        map.setView([lat, lon], 15);
      } else {
        map.panTo([lat, lon], {animate: true});
      }
    }

    function clearMarker(){
      if (marker) { map.removeLayer(marker); marker = null; }
      if (accuracyCircle) { map.removeLayer(accuracyCircle); accuracyCircle = null; }
    }

    // initial load
    fetchDevices();
    // global refresh every 5s so admin sees device list changes
    setInterval(fetchDevices, 5000);
  </script>
</body>
</html>
"""

# Admin web pages / endpoints
@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'GET':
        return render_template_string(ADMIN_LOGIN_HTML)
    # POST
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
    return redirect(url_for('friendly'))

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged', None)
    session.pop('admin_user', None)
    return redirect(url_for('admin_login'))

# -------------------------
# IMPORTANT: friendly + admin JSON endpoints are now public (no auth)
# -------------------------
@app.route('/friendly')
def friendly():
    # Public admin UI for now (temporarily disabled auth)
    return render_template_string(FRIENDLY_HTML)

@app.route('/admin/devices')
def admin_devices():
    # Temporarily public — no auth required
    devices = Device.query.all()
    out = []
    for d in devices:
        # last snapshot
        snap = Snapshot.query.filter_by(device_id=d.id).order_by(Snapshot.ts.desc()).first()
        last = None
        if snap:
            last = {
                "ts": snap.ts.isoformat(),
                "lat": snap.lat,
                "lon": snap.lon,
                "speed_mps": round(snap.speed_mps, 3),
                "bearing_deg": round(snap.bearing_deg, 1)
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
    # Temporarily public — no auth required
    d = Device.query.get_or_404(device_id)
    snaps = _recent_snapshots_for_device(device_id, limit=20)
    snaps_out = []
    for s in snaps:
        snaps_out.append({
            "ts": s.ts.isoformat(),
            "lat": s.lat,
            "lon": s.lon,
            "speed_mps": s.speed_mps,
            "bearing_deg": s.bearing_deg,
            "source": s.source
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
    # Temporarily public — no auth required (be careful; this is destructive)
    d = Device.query.get_or_404(device_id)
    d.revoked = True
    db.session.commit()
    # also drop any connected socket sids for this device
    sids = connected_sockets.pop(device_id, None)
    return jsonify({"ok": True, "revoked": True})

# -------------------------
# Socket.IO events (unchanged, preserved)
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
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=os.environ.get("FLASK_DEBUG", "0") == "1")    sid = request.sid
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

