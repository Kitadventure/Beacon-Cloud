
import os
import uuid
import threading
from math import radians, sin, cos, atan2, sqrt, degrees
from datetime import datetime, timedelta
import json
import time
from collections import defaultdict
from flask import current_app
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

# Accident detection tuning (server-side inference; no native changes required)
# thresholds in m/s^2 and m/s
ACCIDENT_DECEL_HIGH_MPS2 = float(os.environ.get("ACCIDENT_DECEL_HIGH_MPS2", "8.0"))   # strong deceleration ~ -8 m/s^2
ACCIDENT_DECEL_MED_MPS2 = float(os.environ.get("ACCIDENT_DECEL_MED_MPS2", "5.0"))     # medium ~ -5 m/s^2
ACCIDENT_SPEED_DROP_MPS = float(os.environ.get("ACCIDENT_SPEED_DROP_MPS", "10.0"))    # drop of 10 m/s (~36 km/h)
ACCIDENT_BEARING_JUMP_DEG = float(os.environ.get("ACCIDENT_BEARING_JUMP_DEG", "60.0"))# abrupt heading change
ACCIDENT_TIME_WINDOW_S = float(os.environ.get("ACCIDENT_TIME_WINDOW_S", "3.0"))       # examine last N seconds

# Jam detection tuning (new)
JAM_DETECT_INTERVAL_S = float(os.environ.get("JAM_DETECT_INTERVAL_S", "5.0"))
JAM_MIN_DEVICES = int(os.environ.get("JAM_MIN_DEVICES", "3"))
JAM_SPEED_THRESHOLD_MPS = float(os.environ.get("JAM_SPEED_THRESHOLD_MPS", "3.0"))  # < ~11 km/h
JAM_CLUSTER_RADIUS_M = float(os.environ.get("JAM_CLUSTER_RADIUS_M", "50.0"))
JAM_RETENTION_S = int(os.environ.get("JAM_RETENTION_S", "60"))  # keep jams recent for this many seconds

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
# New: Roads / speed rules model & helpers
# -------------------------
class Road(db.Model):
    id = db.Column(db.String(36), primary_key=True, default=lambda: uuid.uuid4().hex)
    name = db.Column(db.String(128))
    # stored as JSON list of [lon, lat] pairs (GeoJSON-like but only coords)
    coords_json = db.Column(db.Text)
    max_speed_kmh = db.Column(db.Float, default=50.0)
    buffer_m = db.Column(db.Float, default=20.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# -------------------------
# Simple in-memory rate tracking (per-device)
# -------------------------
_last_heartbeat_at = {}  # device_id -> timestamp (float)
# Note: for multi-process deployments, move this into Redis or DB.

# -------------------------
# In-memory active device cache (thread-safe)
# -------------------------
active_devices = {}
active_devices_lock = threading.Lock()

def update_active_device_from_snapshot(snap):
    """Given a Snapshot model instance, update in-memory active_devices."""
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
    """Remove entries older than CLEANUP_STALE_SECONDS."""
    try:
        cutoff = datetime.utcnow() - timedelta(seconds=CLEANUP_STALE_SECONDS)
        with active_devices_lock:
            to_del = [k for k,v in active_devices.items() if v.get("ts") < cutoff]
            for k in to_del:
                active_devices.pop(k, None)
    except Exception:
        pass

def _meters_xy_ref(lat_ref):
    # returns meters-per-radian constant at lat_ref
    return 6371000.0  # earth radius, used together with cos in projection below

def point_to_polyline_distance_m(lat, lon, poly_coords):
    """
    poly_coords: list of [lon, lat] points (GeoJSON order)
    returns minimum distance in meters from (lat, lon) to the polyline.
    Uses equirectangular approximation anchored at lat as reference.
    """
    if not poly_coords or len(poly_coords) < 2:
        return float('inf')
    R = _meters_xy_ref(lat)
    lat0 = radians(lat)
    cos_lat0 = cos(lat0)
    # convert lon,lat to x,y meters relative to (lon0, lat0)
    def to_xy(p_lon, p_lat):
        x = radians(p_lon - lon) * R * cos_lat0
        y = radians(p_lat - lat) * R
        return x, y
    px, py = to_xy(lon, lat)
    best = float('inf')
    for i in range(len(poly_coords) - 1):
        a_lon, a_lat = poly_coords[i][0], poly_coords[i][1]
        b_lon, b_lat = poly_coords[i+1][0], poly_coords[i+1][1]
        ax, ay = to_xy(a_lon, a_lat)
        bx, by = to_xy(b_lon, b_lat)
        # projection of p onto segment ab
        dx = bx - ax
        dy = by - ay
        if dx == 0 and dy == 0:
            d = sqrt((px - ax)**2 + (py - ay)**2)
        else:
            t = ((px - ax) * dx + (py - ay) * dy) / (dx*dx + dy*dy)
            t = max(0.0, min(1.0, t))
            projx = ax + t*dx
            projy = ay + t*dy
            d = sqrt((px - projx)**2 + (py - projy)**2)
        if d < best:
            best = d
    return best

# -------------------------
# In-memory alert & jam stores (new)
# -------------------------
alerts_store = []  # list of alert dicts (accident alerts)
alerts_lock = threading.Lock()

jams_store = []    # list of jam dicts
jams_lock = threading.Lock()

def push_alert(alert):
    try:
        with alerts_lock:
            alerts_store.insert(0, alert)  # newest first
            # trim to recent N (keep reasonable)
            if len(alerts_store) > 200:
                alerts_store[:] = alerts_store[:200]
    except Exception:
        pass

def list_alerts(limit=100):
    with alerts_lock:
        return alerts_store[:limit]

def clear_alerts():
    with alerts_lock:
        alerts_store.clear()

def push_jam(jam):
    try:
        with jams_lock:
            jams_store.insert(0, jam)
            # remove old
            cutoff = datetime.utcnow() - timedelta(seconds=JAM_RETENTION_S)
            jams_store[:] = [j for j in jams_store if datetime.fromisoformat(j["ts"]) >= cutoff]
            if len(jams_store) > 200:
                jams_store[:] = jams_store[:200]
    except Exception:
        pass

def list_jams(limit=100):
    with jams_lock:
        return jams_store[:limit]

def clear_jams():
    with jams_lock:
        jams_store.clear()

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
    """
    Returns decision in 'red' / 'orange' / 'green' (we use 'orange' instead of 'yellow' per user's colors).
    """
    if not self_snap or not other_snap:
        return {"decision": "orange", "confidence": 0.2, "reason": "missing_data"}

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
            return {"decision": "orange", "confidence": round(base_confidence * 0.6 + 0.2 * support_score, 2), "reason": "same_gap_low"}
        return {"decision": "green", "confidence": round(base_confidence * 0.8 + 0.1 * support_score, 2), "reason": "same_safe"}

    if direction == "cross":
        if ttc != float('inf') and ttc < (UNSAFE_TTC_SECONDS * 0.8):
            conf = min(1.0, base_confidence + 0.15 * support_score)
            return {"decision": "red", "confidence": round(conf, 2), "reason": f"cross_ttc_{round(ttc,1)}s"}
        else:
            return {"decision": "orange", "confidence": round(base_confidence * 0.6 + 0.1 * support_score, 2), "reason": "cross_caution"}

    return {"decision": "orange", "confidence": round(base_confidence, 2), "reason": "fallback_uncertain"}

# -------------------------
# Accident detection (server-side inference)
# -------------------------
def detect_accident_for_device(device_id):
    """
    Infer a possible accident from recent snapshots for device_id.
    Returns None or an alert dict:
      {
        "accident": True,
        "severity": "high"|"medium"|"low",
        "confidence": 0.0..1.0,
        "reason": "...",
        "ts": isoformat-of-latest-snap
      }
    Uses only existing heartbeat fields (speed_mps, bearing_deg, timestamps).
    """
    snaps = _recent_snapshots_for_device(device_id, limit=8)
    if not snaps or len(snaps) < 2:
        return None

    # order ascending (oldest -> newest) for deltas
    snaps = list(reversed(snaps))
    latest = snaps[-1]
    latest_ts = latest.ts
    # only consider recent window
    window_start = datetime.utcnow() - timedelta(seconds=ACCIDENT_TIME_WINDOW_S)
    relevant = [s for s in snaps if s.ts >= window_start]
    if not relevant or len(relevant) < 2:
        relevant = snaps  # fallback to whatever we have

    # compute decelerations and bearing jumps between successive pairs
    decel_events = []
    bearing_jumps = []
    speed_drops = []
    for i in range(1, len(relevant)):
        prev = relevant[i-1]
        cur = relevant[i]
        dt = (cur.ts - prev.ts).total_seconds()
        if dt <= 0:
            continue
        dv = cur.speed_mps - prev.speed_mps
        accel = dv / dt  # m/s^2 (negative = deceleration)
        decel_events.append(accel)
        # bearing jump
        bd = angle_diff(prev.bearing_deg or 0.0, cur.bearing_deg or 0.0)
        bearing_jumps.append(bd)
        # speed drop magnitude
        drop = max(0.0, prev.speed_mps - cur.speed_mps)
        speed_drops.append((drop, dt))

    max_decel = min(decel_events) if decel_events else 0.0  # most negative (largest decel)
    max_bearing_jump = max(bearing_jumps) if bearing_jumps else 0.0
    max_speed_drop = max([s for s,d in speed_drops]) if speed_drops else 0.0
    earliest_relevant = relevant[0]

    # base confidence from how extreme the metrics are
    conf = 0.0
    reason_parts = []
    if max_decel <= -ACCIDENT_DECEL_HIGH_MPS2:
        conf += 0.5
        reason_parts.append(f"high_decel_{abs(max_decel):.1f}m/s2")
    elif max_decel <= -ACCIDENT_DECEL_MED_MPS2:
        conf += 0.3
        reason_parts.append(f"med_decel_{abs(max_decel):.1f}m/s2")

    if max_speed_drop >= ACCIDENT_SPEED_DROP_MPS:
        conf += 0.25
        reason_parts.append(f"speed_drop_{max_speed_drop:.1f}mps")

    if max_bearing_jump >= ACCIDENT_BEARING_JUMP_DEG:
        conf += 0.2
        reason_parts.append(f"bearing_jump_{int(max_bearing_jump)}deg")

    # corroborate with nearby devices that show abrupt stops within CONFIRMATION_RADIUS_M
    mid_lat = latest.lat
    mid_lon = latest.lon
    supporters = _devices_near_point(mid_lat, mid_lon, CONFIRMATION_RADIUS_M)
    corroboration = 0
    for did, s in supporters.items():
        if did == device_id:
            continue
        # check if that support device has recent snapshots showing big drop
        other_snaps = _recent_snapshots_for_device(did, limit=4)
        if len(other_snaps) >= 2:
            o_prev = other_snaps[1]
            o_last = other_snaps[0]
            dt_o = (o_last.ts - o_prev.ts).total_seconds() if (o_last.ts and o_prev.ts) else 1.0
            if dt_o > 0:
                drop_o = max(0.0, o_prev.speed_mps - o_last.speed_mps)
                if drop_o >= (ACCIDENT_SPEED_DROP_MPS * 0.6):
                    corroboration += 1
    if corroboration > 0:
        conf += min(0.2, 0.05 * corroboration)
        reason_parts.append(f"corroboration_{corroboration}")

    conf = max(0.0, min(1.0, conf))
    if conf < 0.15:
        return None

    # severity heuristics
    if conf >= 0.7 or max_decel <= -ACCIDENT_DECEL_HIGH_MPS2 or max_speed_drop >= (ACCIDENT_SPEED_DROP_MPS * 1.5):
        severity = "high"
    elif conf >= 0.4:
        severity = "medium"
    else:
        severity = "low"

    return {
        "accident": True,
        "severity": severity,
        "confidence": round(conf, 2),
        "reason": ",".join(reason_parts) if reason_parts else "inferred",
        "ts": latest_ts.isoformat() if latest_ts else None,
        "lat": latest.lat,
        "lon": latest.lon,
        "device_id": device_id
    }

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
    """
    Traditional strict check: aborts if token missing/invalid.
    Keep for endpoints where recreation isn't wanted.
    """
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

def find_device_by_token(token):
    if not token:
        return None
    return Device.query.filter_by(token=token, revoked=False).first()

def restore_device_if_missing(device_id, token, payload=None):
    """
    If a Device row with (device_id, token) doesn't exist, recreate it using optional payload fields.
    Returns the Device instance (existing or newly created).
    """
    if not device_id or not token:
        return None
    existing = Device.query.filter_by(id=device_id).first()
    if existing:
        # if token mismatches, avoid clobbering — only create if token matches or no token present
        if existing.token != token:
            # token mismatch -> do not restore automatically
            return None
        return existing
    # create new Device row with any provided meta fields
    owner = None
    car_name = None
    car_model = None
    plate = None
    extra = None
    if payload:
        owner = payload.get("owner")
        car_name = payload.get("car_name")
        car_model = payload.get("car_model")
        plate = payload.get("plate")
        extra = payload.get("extra")
    try:
        d = Device(id=device_id, token=token, owner=owner, car_name=car_name, car_model=car_model, plate=plate, extra=(json.dumps(extra) if extra else None))
        db.session.add(d)
        db.session.commit()
        app.logger.info("Restored Device row for %s using token (auto-restore).", device_id)
        return d
    except Exception:
        db.session.rollback()
        return None

def find_or_restore_from_request(body=None):
    """
    Helper to extract token & device_id from headers or body and return device, possibly restoring the Device row.
    """
    body = body or (request.get_json(silent=True) or {})
    auth = request.headers.get("Authorization", "")
    token = None
    if auth and auth.lower().startswith("token "):
        token = auth.split(" ", 1)[1].strip()
    if not token:
        token = body.get("token") or request.args.get("token")
    device_id = body.get("device_id") or request.args.get("device_id")
    if not token:
        return None
    device = find_device_by_token(token)
    if device:
        return device
    # attempt restore if device_id present
    if device_id:
        return restore_device_if_missing(device_id, token, payload=body)
    return None

def require_admin_api():
    """
    API-level admin check: allow if logged in via session OR provide ADMIN_API_TOKEN in Bearer header.
    Use this for JSON admin endpoints. UI routes rely on session.
    """
    if session.get('admin_logged'):
        return True
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
    # prefer explicit vehicle_type/vehicle_category if provided (backwards compatible)
    car_name = payload.get("car_name") or payload.get("vehicle_type")
    car_model = payload.get("car_model") or payload.get("vehicle_category")
    plate = payload.get("plate")
    extra = payload.get("extra")
    device_id = uuid.uuid4().hex
    token = create_device_token()
    d = Device(
        id=device_id,
        token=token,
        owner=owner,
        car_name=car_name,
        car_model=car_model,
        plate=plate,
        extra=(json.dumps(extra) if extra else None)
    )
    db.session.add(d)
    db.session.commit()
    return jsonify({"device_id": device_id, "token": token})

@app.route("/reconnect", methods=["POST"])
def reconnect():
    """
    Optional explicit reconnect endpoint devices can call to re-create their Device row if the backend forgot it.
    Accepts device_id & token plus optional meta fields.
    """
    payload = request.get_json(force=True, silent=True) or {}
    device_id = payload.get("device_id")
    token = payload.get("token")
    if not device_id or not token:
        return jsonify({"error": "device_id and token required"}), 400
    device = find_device_by_token(token)
    if device:
        return jsonify({"ok": True, "note": "device already exists"})
    restored = restore_device_if_missing(device_id, token, payload=payload)
    if restored:
        return jsonify({"ok": True, "note": "device restored"})
    return jsonify({"error": "could not restore device"}), 500

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    # Attempt to find or restore device from Authorization header or body (so devices that kept tokens auto-recreate)
    body = request.get_json(force=True, silent=True) or {}
    device = find_or_restore_from_request(body)
    if not device:
        # fall back to strict check which will abort
        try:
            device = require_auth_token()
        except Exception:
            return jsonify({"error": "Missing or invalid token; cannot authenticate device"}), 401

    payload = body
    device_id = payload.get("device_id") or device.id

    # Rate-limiting (basic)
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

    # If the device was restored with minimal meta earlier, optionally update meta if provided
    try:
        update_meta = False
        if payload.get("owner") or payload.get("car_name") or payload.get("car_model") or payload.get("plate") or payload.get("extra") or payload.get("vehicle_type") or payload.get("vehicle_category"):
            update_meta = True
        if update_meta:
            try:
                drow = Device.query.get(device.id)
                if drow:
                    if payload.get("owner"): drow.owner = payload.get("owner")
                    # support both the older keys and newer vehicle_type/category keys
                    if payload.get("car_name"): drow.car_name = payload.get("car_name")
                    if payload.get("car_model"): drow.car_model = payload.get("car_model")
                    if payload.get("vehicle_type") and not payload.get("car_name"): drow.car_name = payload.get("vehicle_type")
                    if payload.get("vehicle_category") and not payload.get("car_model"): drow.car_model = payload.get("vehicle_category")
                    if payload.get("plate"): drow.plate = payload.get("plate")
                    if payload.get("extra"): drow.extra = json.dumps(payload.get("extra"))
                    db.session.commit()
            except Exception:
                db.session.rollback()
    except Exception:
        pass

    snap = Snapshot(
        device_id=device_id,
        ts=datetime.utcnow(),
        lat=lat,
        lon=lon,
        speed_mps=float(speed_mps),
        bearing_deg=(float(bearing) % 360.0),
        heading_deg=(float(heading) % 360.0),
        source=str(src),
        raw=json.dumps(payload)
    )
    db.session.add(snap)
    db.session.commit()

    # update in-memory cache for active devices and prune old entries
    try:
        update_active_device_from_snapshot(snap)
        prune_active_devices()
    except Exception:
        app.logger.exception("active_devices update error")

    cleanup_old_snapshots()

    # compute authoritative nearby + decisions and push via socket
    try:
        nearby_payload = compute_nearby_for_device(device_id, radius_m=NEARBY_DEFAULT_RADIUS_M)

        # Accident detection for the reporting device (server-side inferred)
        try:
            acc = detect_accident_for_device(device_id)
            if acc:
                # attach alerts into nearby payload
                nearby_payload.setdefault("alerts", []).append(acc)
                # push device-specific accident alert
                send_ws_to_device(device_id, 'accident_alert', acc)
                # record in global alerts store
                try:
                    push_alert(acc)
                except Exception:
                    pass
                # push a minimal "accident_nearby" to nearby connected devices so they can react
                for other in nearby_payload.get("nearby", []):
                    try:
                        send_ws_to_device(other["device_id"], 'accident_nearby', {
                            "accident": True,
                            "device_id": device_id,
                            "lat": acc.get("lat"),
                            "lon": acc.get("lon"),
                            "severity": acc.get("severity"),
                            "confidence": acc.get("confidence"),
                            "reason": acc.get("reason"),
                            "ts": acc.get("ts")
                        })
                    except Exception:
                        # continue best-effort
                        pass
        except Exception:
            app.logger.exception("accident detection error")

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
        # include server-side accident inference when requested via HTTP as well
        try:
            acc = detect_accident_for_device(device_id)
            if acc:
                payload.setdefault("alerts", []).append(acc)
        except Exception:
            pass
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def compute_nearby_for_device(device_id, radius_m=NEARBY_DEFAULT_RADIUS_M):
    # obtain the latest snapshot for the device (or fallback to active_devices cache)
    self_snap = Snapshot.query.filter_by(device_id=device_id).order_by(Snapshot.ts.desc()).first()
    if not self_snap:
        with active_devices_lock:
            entry = active_devices.get(device_id)
        if entry:
            class _Tmp:
                pass
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

    # gather other recent snapshots (DB + in-memory cache)
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
                class _Tmp2:
                    pass
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
        try:
            d = haversine_m(self_snap.lat, self_snap.lon, s.lat, s.lon)
        except Exception:
            continue
        if d > radius_m:
            continue
        direction = classify_direction(self_snap.bearing_deg, s.bearing_deg)
        closing = closing_speed_mps(self_snap.lat, self_snap.lon, self_snap.speed_mps, self_snap.bearing_deg,
                                    s.lat, s.lon, s.speed_mps, s.bearing_deg)
        guidance = compute_warning(self_snap.lat, self_snap.lon, self_snap.speed_mps, self_snap.bearing_deg,
                                   s.lat, s.lon, s.speed_mps, s.bearing_deg)
        risk = classify_risk(self_snap, s)

        # try to include device metadata from Device row (best-effort)
        meta_owner = None
        meta_car_name = None
        meta_car_model = None
        meta_plate = None
        try:
            drow = Device.query.get(s.device_id)
            if drow:
                meta_owner = drow.owner
                meta_car_name = drow.car_name
                meta_car_model = drow.car_model
                meta_plate = drow.plate
        except Exception:
            pass

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
            "reason": risk.get("reason"),
            # meta fields (for client convenience)
            "owner": meta_owner,
            "car_name": meta_car_name,
            "car_model": meta_car_model,
            "plate": meta_plate
        })
    # sort by distance so client shows closest first
    results.sort(key=lambda x: x["distance_m"])

    # self meta
    self_meta = {}
    try:
        dro = Device.query.get(device_id)
        if dro:
            self_meta = {
                "owner": dro.owner,
                "car_name": dro.car_name,
                "car_model": dro.car_model,
                "plate": dro.plate
            }
    except Exception:
        pass

    payload = {
        "self": {
            "device_id": self_snap.device_id,
            "lat": self_snap.lat,
            "lon": self_snap.lon,
            "speed_mps": round(self_snap.speed_mps, 2),
            "bearing_deg": round(self_snap.bearing_deg, 1),
            "ts": self_snap.ts.isoformat() if hasattr(self_snap, "ts") else None,
            "meta": self_meta
        },
        "nearby": results
    }

    # include server-inferred accidents for reporting + nearby devices (best-effort)
    alerts = []
    try:
        acc_self = detect_accident_for_device(device_id)
        if acc_self:
            alerts.append(acc_self)
        for r in results[:8]:
            try:
                a = detect_accident_for_device(r["device_id"])
                if a:
                    alerts.append(a)
            except Exception:
                pass
    except Exception:
        pass

    if alerts:
        payload["alerts"] = alerts

    return payload

DASHBOARD_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Beacon — Dashboard (Traffic + Road Speed)</title>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    :root{ --bg:#f7f9fb; --card:#ffffff; --muted:#6b7280; --accent:#0b84ff; }
    body{ font-family: Inter, system-ui, -apple-system, "Segoe UI", Roboto, Arial; margin:0; background:var(--bg); color:#111827;}
    header{ background: linear-gradient(90deg,#0b84ff 0%, #00c6ff 100%); color:white; padding:18px 20px; display:flex; align-items:center; gap:12px;}
    header h1{ font-size:18px; margin:0;}
    .wrap{ display:flex; gap:12px; padding:12px; height: calc(100vh - 72px); box-sizing:border-box;}
    .left{ width:360px; display:flex; flex-direction:column; gap:12px; }
    .card{ background:var(--card); border-radius:10px; padding:12px; box-shadow:0 6px 18px rgba(15,23,42,0.06); overflow:auto; }
    #devicesList{ list-style:none; margin:0; padding:0; }
    #devicesList li{ padding:10px; border-radius:8px; margin-bottom:8px; cursor:pointer; border:1px solid #eef2f7; display:flex; justify-content:space-between; align-items:center; gap:8px;}
    #devicesList li.selected{ background:#eef8ff; border-color:#cfe9ff; }
    .dev-meta{ font-size:13px; color:var(--muted); }
    .big{ font-weight:600; font-size:14px; }
    .muted{ color:var(--muted); font-size:13px; }
    #mapWrap{ flex:1; display:flex; flex-direction:column; gap:12px; }
    #map{ flex:1; border-radius:10px; overflow:hidden; }
    #detailCard{ height:220px; min-height:160px; transition: all 0.18s ease; }
    .row{ display:flex; justify-content:space-between; gap:8px; margin-top:6px; }
    .btn{ background:var(--accent); color:white; padding:8px 10px; border-radius:8px; border:none; cursor:pointer;}
    .btn.ghost{ background:transparent; color:var(--accent); border:1px solid #e6f2ff;}
    .small{ font-size:12px; padding:6px 8px; border-radius:6px; }
    a.osm{ color:var(--accent); text-decoration:none; font-weight:600; }
    footer { font-size:12px; color:var(--muted); padding:8px 16px; text-align:right; }
    #statusBar { font-size:13px; color:#08306B; margin-top:8px; }

    /* map control panel styles (enhanced for dragging + minimize/maximize) */
    .map-controls {
      position: absolute;
      left: 12px;
      top: 70px;
      z-index: 1100;
      width: 320px;
      touch-action: none;
      user-select: none;
    }
    .map-controls-header {
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:8px;
      padding:8px;
      background: linear-gradient(90deg,#ffffff, #fbfdff);
      border-radius:10px;
      box-shadow:0 6px 20px rgba(2,6,23,0.06);
      cursor:grab;
      margin-bottom:8px;
    }
    .map-controls-header:active { cursor:grabbing; }
    .map-controls-title { font-weight:700; font-size:13px; color:var(--accent); }
    .map-controls-actions { display:flex; gap:6px; align-items:center; }
    .map-controls-actions button {
      border: none;
      background: #f1f5f9;
      padding:6px 8px;
      border-radius:6px;
      cursor: pointer;
      font-size:12px;
    }
    .map-controls.minimized { width:140px; }
    .map-controls.minimized .map-panel { display:none; }
    .map-controls.minimized .map-controls-header { border-radius:10px; }
    .map-controls.maximized {
      left: 12px !important;
      right: 12px;
      top: 70px !important;
      width: auto !important;
      max-width: calc(100% - 24px);
    }
    .map-controls.maximized .map-panel { margin-bottom:10px; }

    .map-panel { background:white; border-radius:10px; padding:8px; box-shadow:0 8px 32px rgba(2,6,23,0.2); margin-bottom:8px; }
    .control-row { display:flex; gap:8px; align-items:center; }
    .control-row input[type="text"], .control-row input[type="number"] { flex:1; padding:6px 8px; border-radius:6px; border:1px solid #e6eef8; }
    .muted-small { font-size:12px; color:#6b7280; margin-top:6px; }
    .legend { display:flex; gap:8px; align-items:center; margin-top:6px; font-size:12px; color:#374151; }
    .legend .dot { width:12px; height:12px; border-radius:6px; display:inline-block; margin-right:6px; }

    /* small responsive tweak */
    @media (max-width:900px) {
      .left { width: 280px; }
      .map-controls { left: 8px; top: 80px; width: 280px; }
      .map-controls.minimized { width:120px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Beacon — Live Dashboard (Traffic + Roads)</h1>
    <div style="margin-left:auto; font-size:13px; opacity:0.95;">Auto-refresh every 5s — open this page on desktop or phone</div>
  </header>

  <div class="wrap">
    <div class="left">
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <div>
            <div class="muted">Devices</div>
            <div class="big" id="devicesCount">0 devices</div>
          </div>
          <div>
            <button id="btnRefresh" class="btn small">Refresh</button>
          </div>
        </div>

        <div id="statusBar" class="muted">Status: idle</div>

        <hr style="margin:10px 0; border:none; border-top:1px solid #f1f5f9;" />
        <ul id="devicesList"></ul>
      </div>

      <div id="detailCard" class="card">
        <div id="placeholderDetail"><em>Select a device to see details & map</em></div>
        <div id="deviceDetail" style="display:none;">
          <div style="display:flex; justify-content:space-between; align-items:center;">
            <div>
              <div id="detailName" class="big"></div>
              <div id="detailOwner" class="muted"></div>
            </div>
            <div>
              <button id="btnExpand" class="btn small" title="Expand details">Expand</button>
              <button id="btnRevoke" class="btn ghost small">Revoke</button>
            </div>
          </div>

          <div class="row">
            <div><div class="muted">Last seen</div><div id="detailTs" class="big"></div></div>
            <div><div class="muted">Speed</div><div id="detailSpeed" class="big">—</div></div>
          </div>

          <div class="row">
            <div><div class="muted">Location</div><div id="detailLoc" class="muted"></div></div>
            <div><a id="osmLink" class="osm" target="_blank">Open in OSM ↗</a></div>
          </div>

          <div style="margin-top:8px;">
            <div class="muted">Raw heartbeat (last)</div>
            <pre id="detailRaw" style="background:#fbfdff; padding:8px; border-radius:6px; max-height:120px; overflow:auto;">—</pre>
          </div>
        </div>
      </div>

    </div>

    <div id="mapWrap" class="card">
      <div id="map" style="position:relative"></div>

            <!-- map floating controls (REPLACED with draggable + minimize/maximize + show-all toggle) -->
      <div class="map-controls" id="mapControls" aria-label="Map controls">
        <div class="map-controls-header" id="mapControlsHeader" title="Drag me — double-click to minimize">
          <div class="map-controls-title">Beacon controls</div>
          <div class="map-controls-actions">
            <button id="mc-min" title="Minimize">▁</button>
            <button id="mc-toggleAll" title="Show all devices">All</button>
            <button id="mc-max" title="Maximize">▢</button>
          </div>
        </div>

        <div class="map-panel">
          <div style="font-weight:600; margin-bottom:6px;">Search</div>
          <div class="control-row">
            <input id="searchInput" type="text" placeholder="device id / name or place (press Enter)" />
            <button id="btnSearch" class="btn small">Go</button>
          </div>
          <div class="muted-small">Tip: type device name or paste coordinates, or type an address to pan map.</div>
        </div>

        <div class="map-panel">
          <div style="display:flex; justify-content:space-between; align-items:center;">
            <div style="font-weight:600;">Traffic view</div>
            <label style="margin-left:auto;">
              <input id="toggleHeat" type="checkbox" /> Heatmap
            </label>
          </div>
          <div class="legend">
            <span class="dot" style="background:#ff3333"></span> fast
            <span class="dot" style="background:#ffb43d; margin-left:8px;"></span> medium
            <span class="dot" style="background:#4ade80; margin-left:8px;"></span> slow
          </div>
        </div>

        <div class="map-panel">
          <div style="font-weight:600; margin-bottom:6px;">Draw road & speed rule</div>
          <div style="font-size:13px; margin-bottom:6px;">Click map to add points. Double-click to finish.</div>
          <div class="control-row" style="margin-bottom:6px;">
            <button id="btnDrawRoad" class="btn small">Draw road</button>
            <button id="btnClearRoad" class="btn ghost small">Clear</button>
          </div>
          <div class="control-row" style="margin-bottom:6px;">
            <input id="inputRoadName" type="text" placeholder="Road name" />
            <input id="inputMaxSpeed" type="number" placeholder="Max km/h" style="width:120px" />
          </div>
          <div style="display:flex; gap:8px;">
            <button id="btnSaveRoad" class="btn small">Save road</button>
            <button id="btnShowViolations" class="btn small">Show violations</button>
          </div>
          <div class="muted-small" id="roadStatus"></div>
        </div>
      </div>
      <!-- end map floating controls -->

    </div>
  </div>

  <footer>Beacon — simple live tracking dashboard</footer>

  <!-- Expanded modal -->
  <div id="modal" class="modal" role="dialog" aria-hidden="true" style="display:none;">
    <div class="modal-backdrop" onclick="closeModal()" style="position:fixed;inset:0;background: rgba(2,6,23,0.55);z-index:1200;"></div>
    <div class="modal-window" id="modalWindow" role="document" style="position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);width:90%;max-width:1000px;height:85%;background:white;border-radius:12px;padding:16px;overflow:auto;z-index:1210;">
      <button class="close" onclick="closeModal()" style="position:absolute;right:12px;top:12px;background:#eee;border:none;padding:6px 8px;border-radius:8px;cursor:pointer;">Close ✕</button>
      <h2 id="modalTitle">Device details</h2>
      <div id="modalContent">
        <div class="meta-row" id="modalMeta">Loading…</div>
        <hr/>
        <div><strong>Recent snapshots</strong></div>
        <pre id="modalSnapshots" style="white-space:pre-wrap; word-break:break-word; background:#fbfdff; padding:12px; border-radius:8px; max-height:60%; overflow:auto;">Loading…</pre>
      </div>
    </div>
  </div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.heat/dist/leaflet-heat.js"></script>
<script src="https://cdn.jsdelivr.net/npm/luxon@3/build/global/luxon.min.js"></script>
<script>
  /* Draggable / minimize / maximize behavior for map-controls.
     Self-contained and resilient: doesn't assume map exists yet.
  */
  (function(){
    const mapControls = document.getElementById('mapControls');
    if (!mapControls) return;
    const header = document.getElementById('mapControlsHeader');
    const btnMin = document.getElementById('mc-min');
    const btnMax = document.getElementById('mc-max');
    let dragging = false;
    let startX = 0, startY = 0, startLeft = 0, startTop = 0;

    function px(v){ return (typeof v === 'number') ? v + 'px' : v; }
    function getStyleNum(el, prop){
      const v = window.getComputedStyle(el)[prop];
      return v ? parseFloat(v.replace('px','')) : 0;
    }

    header.addEventListener('mousedown', onDragStart);
    header.addEventListener('touchstart', onDragStart, {passive:false});

    function onDragStart(e){
      if (e.target && (e.target.id === 'mc-min' || e.target.id === 'mc-max' || e.target.id === 'mc-toggleAll')) return;
      e.preventDefault();
      dragging = true;
      header.style.cursor = 'grabbing';
      startX = e.touches ? e.touches[0].clientX : e.clientX;
      startY = e.touches ? e.touches[0].clientY : e.clientY;
      startLeft = getStyleNum(mapControls, 'left');
      startTop = getStyleNum(mapControls, 'top');
      document.addEventListener('mousemove', onDragging);
      document.addEventListener('mouseup', onDragEnd);
      document.addEventListener('touchmove', onDragging, {passive:false});
      document.addEventListener('touchend', onDragEnd);
    }
    function onDragging(e){
      if (!dragging) return;
      e.preventDefault();
      const clientX = e.touches ? e.touches[0].clientX : e.clientX;
      const clientY = e.touches ? e.touches[0].clientY : e.clientY;
      const dx = clientX - startX;
      const dy = clientY - startY;
      mapControls.style.left = px(startLeft + dx);
      mapControls.style.top  = px(startTop + dy);
      if (mapControls.classList.contains('maximized')) mapControls.classList.remove('maximized');
    }
    function onDragEnd(){
      dragging = false;
      header.style.cursor = 'grab';
      document.removeEventListener('mousemove', onDragging);
      document.removeEventListener('mouseup', onDragEnd);
      document.removeEventListener('touchmove', onDragging);
      document.removeEventListener('touchend', onDragEnd);
      // keep panel inside viewport after drag
      const rect = mapControls.getBoundingClientRect();
      const vw = window.innerWidth, vh = window.innerHeight;
      let left = rect.left, top = rect.top;
      if (rect.right > vw) left = Math.max(8, vw - rect.width - 8);
      if (rect.left < 8) left = 8;
      if (rect.bottom > vh) top = Math.max(8, vh - rect.height - 8);
      if (rect.top < 8) top = 8;
      mapControls.style.left = px(left);
      mapControls.style.top = px(top);
      setTimeout(()=>{ if (window.map && typeof window.map.invalidateSize === 'function') window.map.invalidateSize(); }, 200);
    }

    header.addEventListener('dblclick', () => toggleMinimize());
    btnMin.addEventListener('click', (ev) => { ev.stopPropagation(); toggleMinimize(); });
    btnMax.addEventListener('click', (ev) => { ev.stopPropagation(); toggleMaximize(); });

    function toggleMinimize(){
      mapControls.classList.toggle('minimized');
      if (mapControls.classList.contains('maximized')) mapControls.classList.remove('maximized');
      setTimeout(()=>{ if (window.map && typeof window.map.invalidateSize === 'function') window.map.invalidateSize(); }, 300);
    }
    function toggleMaximize(){
      const isMax = mapControls.classList.toggle('maximized');
      if (isMax) {
        mapControls.style.left = '12px';
        mapControls.style.top  = '70px';
        mapControls.style.width = 'auto';
        mapControls.classList.remove('minimized');
      } else {
        mapControls.style.width = '320px';
      }
      setTimeout(()=>{ if (window.map && typeof window.map.invalidateSize === 'function') window.map.invalidateSize(); }, 300);
    }

    // Prevent clicks inside panel from propagating to the map (so draw mode isn't accidentally triggered)
    mapControls.addEventListener('mousedown', (e)=> e.stopPropagation());
    mapControls.addEventListener('touchstart', (e)=> e.stopPropagation());

    // reposition if window resized (keeps panel visible)
    window.addEventListener('resize', () => {
      const rect = mapControls.getBoundingClientRect();
      const vw = window.innerWidth, vh = window.innerHeight;
      let changed = false;
      if (rect.right > vw) { mapControls.style.left = px(Math.max(8, vw - rect.width - 8)); changed = true; }
      if (rect.bottom > vh) { mapControls.style.top = px(Math.max(8, vh - rect.height - 8)); changed = true; }
      if (changed) setTimeout(()=>{ if (window.map && window.map.invalidateSize) window.map.invalidateSize(); }, 200);
    });

  })();
</script>

<script>
  // show-all toggle flag + UI helper
  let showAllDevices = true;
  const mcToggleAll = document.getElementById('mc-toggleAll');
  function updateToggleAllUI() {
    if (!mcToggleAll) return;
    mcToggleAll.innerText = showAllDevices ? 'All' : 'Single';
    mcToggleAll.title = showAllDevices ? 'Showing all devices (click to focus selected)' : 'Showing only selected device (click to show all)';
  }
  if (mcToggleAll) {
    mcToggleAll.addEventListener('click', (e) => {
      e.stopPropagation();
      showAllDevices = !showAllDevices;
      updateToggleAllUI();
      renderTrafficView();
      if (showAllDevices) {
        // keep single marker but re-add markers/heat
        // renderTrafficView will add markers/heat back
      } else {
        // hide general layers when focusing single
        if (map.hasLayer(markersLayer)) map.removeLayer(markersLayer);
        if (map.hasLayer(heatLayer)) map.removeLayer(heatLayer);
      }
    });
  }
  updateToggleAllUI();

  const DateTime = luxon.DateTime;
  const devicesListEl = document.getElementById('devicesList');
  const devicesCountEl = document.getElementById('devicesCount');
  const btnRefresh = document.getElementById('btnRefresh');
  const statusBar = document.getElementById('statusBar');

  const btnExpand = document.getElementById('btnExpand');
  const modal = document.getElementById('modal');
  const modalTitle = document.getElementById('modalTitle');
  const modalMeta = document.getElementById('modalMeta');
  const modalSnapshots = document.getElementById('modalSnapshots');

  // control elements
  const searchInput = document.getElementById('searchInput');
  const btnSearch = document.getElementById('btnSearch');
  const toggleHeat = document.getElementById('toggleHeat');

  const btnDrawRoad = document.getElementById('btnDrawRoad');
  const btnClearRoad = document.getElementById('btnClearRoad');
  const btnSaveRoad = document.getElementById('btnSaveRoad');
  const btnShowViolations = document.getElementById('btnShowViolations');
  const inputRoadName = document.getElementById('inputRoadName');
  const inputMaxSpeed = document.getElementById('inputMaxSpeed');
  const roadStatus = document.getElementById('roadStatus');

  let devicesCache = [];
  let selectedId = null;
  let refreshTimer = null;

  // map init
  const map = L.map('map', { center: [-1.2921, 36.8219], zoom: 12, preferCanvas:true });
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19 }).addTo(map);

  // heat and markers layers
  let heatLayer = L.heatLayer([], { radius: 25, blur: 20, maxZoom: 15 });
  let markersLayer = L.layerGroup().addTo(map);
  let drawnRoadLayer = L.layerGroup().addTo(map);
  let violationLayer = L.layerGroup().addTo(map);

  // drawing state
  let drawing = false;
  let currentRoadPoints = []; // store [lat, lon] points
  let currentPolyline = null;
  let currentRoadId = null;

  btnRefresh.addEventListener('click', fetchDevices);
  btnExpand.addEventListener('click', () => { if (selectedId) openModal(selectedId); });
  btnSearch.addEventListener('click', handleSearch);
  searchInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') handleSearch(); });

  toggleHeat.addEventListener('change', () => {
    if (toggleHeat.checked) {
      heatLayer.addTo(map);
    } else {
      if (map.hasLayer(heatLayer)) map.removeLayer(heatLayer);
    }
  });

  btnDrawRoad.addEventListener('click', () => {
    drawing = !drawing;
    btnDrawRoad.innerText = drawing ? 'Drawing... (click to add, dblclick to finish)' : 'Draw road';
    if (!drawing) {
      map.off('click', _onMapClickForRoad);
      map.off('dblclick', _onMapDoubleClickFinish);
    } else {
      currentRoadPoints = [];
      if (currentPolyline) { drawnRoadLayer.removeLayer(currentPolyline); currentPolyline = null; }
      map.on('click', _onMapClickForRoad);
      map.on('dblclick', _onMapDoubleClickFinish);
    }
  });

  btnClearRoad.addEventListener('click', () => {
    currentRoadPoints = [];
    if (currentPolyline) { drawnRoadLayer.removeLayer(currentPolyline); currentPolyline = null; }
    roadStatus.innerText = 'Cleared drawing.';
  });

  btnSaveRoad.addEventListener('click', async () => {
    if (!currentRoadPoints || currentRoadPoints.length < 2) { alert('Draw at least two points first'); return; }
    const name = inputRoadName.value || ('road-' + Date.now());
    const maxSpeed = Number(inputMaxSpeed.value || 50);
    // backend expects coords as [ [lon,lat], ... ]
    const coords = currentRoadPoints.map(p => [p[1], p[0]]);
    try {
      const res = await fetch('/admin/roads', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ name: name, coords: coords, max_speed_kmh: maxSpeed })
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error('Save failed: ' + res.status + ' ' + txt);
      }
      const j = await res.json();
      roadStatus.innerText = 'Saved road: ' + (j.road && j.road.name ? j.road.name : j.road.id);
      // store road id to use for violations queries
      if (j.road && j.road.id) currentRoadId = j.road.id;
      // show the road persisted id as tooltip on the polyline
      if (currentPolyline) currentPolyline.bindPopup('Road: ' + name).openPopup();
    } catch (err) {
      console.error(err);
      alert('Error saving road. Make sure you are logged-in as admin or have ADMIN_API_TOKEN configured for server.');
    }
  });

  btnShowViolations.addEventListener('click', async () => {
    violationLayer.clearLayers();
    if (!currentRoadId) {
      alert('No saved road selected. Save a road first (or you can select an existing saved road by ID).');
      return;
    }
    try {
      const res = await fetch('/admin/roads/' + currentRoadId + '/violations');
      if (!res.ok) {
        const t = await res.text();
        throw new Error('Violations fetch failed: ' + res.status + ' ' + t);
      }
      const j = await res.json();
      const v = j.violations || [];
      roadStatus.innerText = 'Found ' + v.length + ' violation(s)';
      for (const viol of v) {
        const m = L.circleMarker([viol.lat, viol.lon], { radius: 7, color: '#ff3333', weight:1 }).addTo(violationLayer);
        m.bindPopup(`<strong>${viol.device_id}</strong><br/>${viol.speed_kmh} km/h — ${viol.distance_m} m from road`);
      }
      if (v.length) {
        map.fitBounds(violationLayer.getBounds(), { padding: [30,30] });
      }
    } catch (err) {
      console.error(err);
      alert('Error fetching violations: ' + err.message);
    }
  });

  function _onMapClickForRoad(ev) {
    currentRoadPoints.push([ev.latlng.lat, ev.latlng.lng]);
    if (currentPolyline) drawnRoadLayer.removeLayer(currentPolyline);
    currentPolyline = L.polyline(currentRoadPoints, { color: '#0b84ff', weight: 4 }).addTo(drawnRoadLayer);
    roadStatus.innerText = `Drawn ${currentRoadPoints.length} points`;
  }
  function _onMapDoubleClickFinish(ev) {
    drawing = false;
    btnDrawRoad.innerText = 'Draw road';
    map.off('click', _onMapClickForRoad);
    map.off('dblclick', _onMapDoubleClickFinish);
    roadStatus.innerText = `Drawing finished (${currentRoadPoints.length} points). Save the road with a name and max speed.`;
  }

  // initial marker / heat population
  function renderTrafficView() {
    // heat points: [lat, lon, intensity]
    const heatPoints = [];
    markersLayer.clearLayers();

    if (!showAllDevices) {
      // hide heat & markers entirely when focusing single device
      if (map.hasLayer(heatLayer)) map.removeLayer(heatLayer);
      if (map.hasLayer(markersLayer)) map.removeLayer(markersLayer);
      return;
    }

    for (const d of devicesCache) {
      const snap = d.last_snapshot;
      if (!snap || typeof snap.lat !== 'number' || typeof snap.lon !== 'number') continue;
      const spd = (snap.speed_mps || 0) * 3.6;
      const intensity = Math.min(1.0, spd / 120.0);
      heatPoints.push([snap.lat, snap.lon, intensity]);
      // speed color
      const color = spd > 80 ? '#ff3333' : (spd > 40 ? '#ffb43d' : '#4ade80');
      const c = L.circleMarker([snap.lat, snap.lon], { radius: 6, color: color, weight: 1, fillOpacity: 0.9 })
        .bindPopup(`<strong>${escapeHtml(d.car_name || d.id)}</strong><br/>${spd.toFixed(1)} km/h<br/>${snap.ts || ''}`)
        .addTo(markersLayer);
      c.on('click', () => selectDevice(d.id));
    }

    if (!map.hasLayer(markersLayer)) markersLayer.addTo(map);

    // heat layer handling
    heatLayer.setLatLngs(heatPoints);
    if (toggleHeat.checked) {
      if (!map.hasLayer(heatLayer)) heatLayer.addTo(map);
    } else {
      if (map.hasLayer(heatLayer)) map.removeLayer(heatLayer);
    }
  }

  async function fetchDevices(){
    statusBar.innerText = "Status: fetching /admin/devices...";
    try {
      const res = await fetch('/admin/devices', { cache: "no-store" });
      const txt = await res.text();
      let data;
      try {
        data = JSON.parse(txt);
      } catch (e) {
        const m = txt.match(/\\{\\s*"?devices"?:[\\s\\S]*\\}/);
        if (m) data = JSON.parse(m[0]);
      }
      devicesCache = data.devices || [];
      statusBar.innerText = `Status: fetched ${devicesCache.length} device(s)`;
      renderList();
      renderTrafficView();
    } catch (err) {
      console.error("fetchDevices error:", err);
      statusBar.innerText = "Status: fetch error — see console for details";
      devicesCache = [];
      renderList();
    }
  }

  function renderList(){
    devicesListEl.innerHTML = '';
    const count = devicesCache.length || 0;
    devicesCountEl.innerText = (count === 1) ? "1 device" : (count + " devices");
    devicesCache.sort((a,b) => {
      const ta = a.last_snapshot && a.last_snapshot.ts ? new Date(a.last_snapshot.ts).getTime() : 0;
      const tb = b.last_snapshot && b.last_snapshot.ts ? new Date(b.last_snapshot.ts).getTime() : 0;
      return tb - ta;
    });
    for (const d of devicesCache) {
      const li = document.createElement('li');
      li.dataset.id = d.id;
      const name = d.car_name || d.car_model || d.id;
      const last = d.last_snapshot ? (d.last_snapshot.ts ? new Date(d.last_snapshot.ts).toLocaleString() : 'seen') : 'never';
      const speed = d.last_snapshot ? (((d.last_snapshot.speed_mps || 0) * 3.6).toFixed(1) + ' km/h') : '—';
      li.innerHTML = `<div style="min-width:0;">
                        <div class="big">${escapeHtml(name)}</div>
                        <div class="dev-meta">${escapeHtml(d.owner || '')} — ${escapeHtml(d.plate || '')}</div>
                      </div>
                      <div style="text-align:right;">
                        <div class="muted">${escapeHtml(last)}</div>
                        <div class="muted">${escapeHtml(speed)}</div>
                      </div>`;
      li.addEventListener('click', () => selectDevice(d.id));
      if (d.id === selectedId) li.classList.add('selected');
      devicesListEl.appendChild(li);
    }
    if (!selectedId && devicesCache.length > 0) {
      selectDevice(devicesCache[0].id);
    }
  }

  function selectDevice(id) {
    selectedId = id;
    document.querySelectorAll('#devicesList li').forEach(li => {
      li.classList.toggle('selected', li.dataset.id === id);
    });
    const d = devicesCache.find(x => x.id === id);
    if (!d) return;
    showDetail(d);

    // focus single device by default when selecting
    showAllDevices = false;
    updateToggleAllUI();

    // hide general markers/heat to emphasize selected device
    if (map.hasLayer(markersLayer)) map.removeLayer(markersLayer);
    if (map.hasLayer(heatLayer)) map.removeLayer(heatLayer);

    if (d.last_snapshot && d.last_snapshot.lat && d.last_snapshot.lon) {
      placeMarker(d.last_snapshot.lat, d.last_snapshot.lon, d);
    } else {
      clearMarker();
    }
  }

  function showDetail(d) {
    document.getElementById('placeholderDetail').style.display = 'none';
    const panel = document.getElementById('deviceDetail');
    panel.style.display = 'block';
    document.getElementById('detailName').innerText = d.car_name || d.car_model || d.id;
    document.getElementById('detailOwner').innerText = d.owner || '';
    if (d.last_snapshot) {
      document.getElementById('detailTs').innerText = d.last_snapshot.ts ? new Date(d.last_snapshot.ts).toLocaleString() : 'seen';
      const spd = ((d.last_snapshot.speed_mps || 0) * 3.6).toFixed(1) + ' km/h';
      document.getElementById('detailSpeed').innerText = spd;
      document.getElementById('detailLoc').innerText = (typeof d.last_snapshot.lat === 'number' && typeof d.last_snapshot.lon === 'number')
        ? (d.last_snapshot.lat.toFixed(6) + ', ' + d.last_snapshot.lon.toFixed(6)) : '—';
      document.getElementById('osmLink').href = (d.last_snapshot && d.last_snapshot.lat && d.last_snapshot.lon)
        ? `https://www.openstreetmap.org/?mlat=${d.last_snapshot.lat}&mlon=${d.last_snapshot.lon}#map=18/${d.last_snapshot.lat}/${d.last_snapshot.lon}`
        : '#';
      document.getElementById('detailRaw').innerText = JSON.stringify(d.last_snapshot.raw || d.last_snapshot, null, 2);
      document.getElementById('btnRevoke').onclick = async () => {
        if (!confirm('Revoke device token? This prevents the app from authenticating with that token.')) return;
        try {
          const res = await fetch('/admin/device/' + d.id + '/revoke', { method: 'POST' });
          if (!res.ok) { alert('Revoke failed'); return; }
          alert('Device revoked');
          fetchDevices();
        } catch (e) { alert('Revoke error'); }
      };
    } else {
      document.getElementById('detailTs').innerText = '—';
      document.getElementById('detailSpeed').innerText = '—';
      document.getElementById('detailLoc').innerText = '—';
      document.getElementById('detailRaw').innerText = '—';
      document.getElementById('osmLink').href = '#';
    }
  }

  let singleMarker = null;
  let singleCircle = null;
  function placeMarker(lat, lon, d) {
    try {
      if (!singleMarker) {
        singleMarker = L.marker([lat, lon]).addTo(map);
      } else {
        singleMarker.setLatLng([lat, lon]);
      }
      if (!singleCircle) {
        const accuracyRadius = (d.last_snapshot && d.last_snapshot.raw && d.last_snapshot.raw.accuracy) ? d.last_snapshot.raw.accuracy : 20;
        singleCircle = L.circle([lat, lon], { radius: accuracyRadius }).addTo(map);
      } else {
        singleCircle.setLatLng([lat, lon]);
      }
      // center the map on the device
      map.setView([lat, lon], 15, { animate: true });

      // ensure single marker and circle are above other layers
      if (singleMarker && typeof singleMarker.bringToFront === 'function') singleMarker.bringToFront();
      if (singleCircle && typeof singleCircle.bringToFront === 'function') singleCircle.bringToFront();

      singleMarker.bindPopup(`<strong>${escapeHtml(d.car_name || d.id)}</strong><br/>${d.last_snapshot && d.last_snapshot.ts ? new Date(d.last_snapshot.ts).toLocaleString() : ''}`).openPopup();
    } catch (e) {
      console.error('placeMarker error', e);
    }
  }

  function clearMarker(){
    if (singleMarker) { try { map.removeLayer(singleMarker); } catch(e){} singleMarker = null; }
    if (singleCircle) { try { map.removeLayer(singleCircle); } catch(e){} singleCircle = null; }
  }

  function escapeHtml(s) {
    if (!s) return '';
    return s.replace(/[&<>"'`]/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;',"`":'&#96;'})[c]);
  }

  async function openModal(deviceId) {
    modal.style.display = 'block';
    modalTitle.innerText = 'Device details — ' + deviceId;
    modalMeta.innerText = 'Loading…';
    modalSnapshots.innerText = 'Loading…';
    try {
      const res = await fetch('/admin/device/' + deviceId + '/json', { cache: "no-store" });
      const j = await res.json();
      const dev = j.device || {};
      modalMeta.innerHTML = `
        <div><strong>Name:</strong> ${escapeHtml(dev.car_name || dev.car_model || dev.id || '')}</div>
        <div><strong>Owner:</strong> ${escapeHtml(dev.owner || '')}</div>
        <div><strong>Plate:</strong> ${escapeHtml(dev.plate || '')}</div>
        <div><strong>Created:</strong> ${escapeHtml(dev.created_at || '')}</div>
        <div><strong>Connected:</strong> ${j.connected ? 'yes' : 'no'}</div>
      `;
      modalSnapshots.innerText = JSON.stringify(j.snapshots || [], null, 2);
    } catch (err) {
      modalMeta.innerText = 'Error loading details';
      modalSnapshots.innerText = String(err);
    }
  }
  function closeModal() {
    modal.style.display = 'none';
  }

  // search: first try to match device name/id, otherwise try Nominatim place search
  async function handleSearch() {
    const q = searchInput.value && searchInput.value.trim();
    if (!q) return;
    // local device search
    const match = devicesCache.find(d => (d.car_name && d.car_name.toLowerCase() === q.toLowerCase()) || (d.id && d.id === q));
    if (match && match.last_snapshot && match.last_snapshot.lat && match.last_snapshot.lon) {
      // selecting device will focus it (selectDevice hides general markers)
      selectDevice(match.id);
      return;
    }
    // attempt Nominatim geocode
    try {
      const res = await fetch('https://nominatim.openstreetmap.org/search?format=json&q=' + encodeURIComponent(q));
      const results = await res.json();
      if (results && results.length > 0) {
        const r = results[0];
        // when searching a place (not a device) keep showAllDevices = true
        showAllDevices = true;
        updateToggleAllUI();
        map.setView([parseFloat(r.lat), parseFloat(r.lon)], 15);
        return;
      } else {
        alert('No place found and no device matched that string.');
      }
    } catch (err) {
      console.error(err);
      alert('Search failed: ' + err.message);
    }
  }

  // auto-refresh
  refreshTimer = setInterval(fetchDevices, 5000);
  // initial load
  fetchDevices();

</script>
</body>
</html>
"""

# Admin web pages / endpoints
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
    # friendly dashboard for non-technical users
    return render_template_string(DASHBOARD_HTML)

# keep /friendly for compatibility
@app.route('/friendly')
def friendly():
    return render_template_string(DASHBOARD_HTML)

@app.route('/admin/devices')
def admin_devices():
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
    d = Device.query.get_or_404(device_id)
    d.revoked = True
    db.session.commit()
    sids = connected_sockets.pop(device_id, None)
    with active_devices_lock:
        active_devices.pop(device_id, None)
    return jsonify({"ok": True, "revoked": True})

@app.route('/admin/roads', methods=['GET', 'POST'])
def admin_roads():
    # GET: list roads
    if request.method == 'GET':
        require_admin_api()
        roads = Road.query.order_by(Road.created_at.desc()).all()
        out = []
        for r in roads:
            out.append({
                "id": r.id,
                "name": r.name,
                "coords": json.loads(r.coords_json) if r.coords_json else [],
                "max_speed_kmh": r.max_speed_kmh,
                "buffer_m": r.buffer_m,
                "created_at": r.created_at.isoformat() if r.created_at else None
            })
        return jsonify({"roads": out})
    # POST: create a new road (admin API or session)
    require_admin_api()
    body = request.get_json(force=True, silent=True) or {}
    name = body.get("name") or "Unnamed road"
    coords = body.get("coords")  # expect list of [lon, lat]
    max_speed_kmh = float(body.get("max_speed_kmh", 50.0))
    buffer_m = float(body.get("buffer_m", 20.0))
    if not coords or not isinstance(coords, list) or len(coords) < 2:
        return jsonify({"error": "coords required (list of [lon,lat] points)"}), 400
    r = Road(name=name, coords_json=json.dumps(coords), max_speed_kmh=max_speed_kmh, buffer_m=buffer_m)
    db.session.add(r)
    db.session.commit()
    return jsonify({"ok": True, "road": {"id": r.id, "name": r.name}}), 201

@app.route('/admin/roads/<road_id>/violations', methods=['GET'])
def admin_road_violations(road_id):
    require_admin_api()
    r = Road.query.get_or_404(road_id)
    coords = json.loads(r.coords_json) if r.coords_json else []
    buffer_m = float(request.args.get("buffer_m", r.buffer_m or 20.0))
    # window: only recent snapshots (use CLEANUP_STALE_SECONDS)
    cutoff = datetime.utcnow() - timedelta(seconds=max(60, CLEANUP_STALE_SECONDS))
    snaps = Snapshot.query.filter(Snapshot.ts >= cutoff).all()
    violations = []
    for s in snaps:
        try:
            dist = point_to_polyline_distance_m(s.lat, s.lon, coords)
        except Exception:
            continue
        speed_kmh = (s.speed_mps or 0.0) * 3.6
        if dist <= buffer_m and speed_kmh > (r.max_speed_kmh or 0.0):
            violations.append({
                "device_id": s.device_id,
                "ts": s.ts.isoformat() if s.ts else None,
                "lat": s.lat,
                "lon": s.lon,
                "speed_kmh": round(speed_kmh, 1),
                "distance_m": round(dist, 1)
            })
    # sort by speed desc
    violations.sort(key=lambda x: -x["speed_kmh"])
    return jsonify({"road_id": r.id, "name": r.name, "violations": violations})

# -------------------------
# New admin endpoints for alerts & jams (requires admin session or ADMIN_API_TOKEN)
# -------------------------
@app.route('/admin/alerts', methods=['GET'])
def admin_list_alerts():
    require_admin_api()
    limit = int(request.args.get("limit", 100))
    return jsonify({"alerts": list_alerts(limit=limit)})

@app.route('/admin/alerts/clear', methods=['POST'])
def admin_clear_alerts():
    require_admin_api()
    clear_alerts()
    return jsonify({"ok": True})

@app.route('/admin/jams', methods=['GET'])
def admin_list_jams():
    require_admin_api()
    limit = int(request.args.get("limit", 100))
    return jsonify({"jams": list_jams(limit=limit)})

@app.route('/admin/jams/clear', methods=['POST'])
def admin_clear_jams():
    require_admin_api()
    clear_jams()
    return jsonify({"ok": True})

# -------------------------
# Socket.IO events (preserved + auto-restore on register)
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
        # Attempt to restore silently (device kept token)
        device = restore_device_if_missing(device_id, token, payload=data)
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
        # try restore (best-effort)
        device = restore_device_if_missing(device_id, token, payload=data)
    if not device:
        emit('error', {'error': 'invalid device/token'})
        return
    try:
        payload = compute_nearby_for_device(device_id, radius_m=NEARBY_DEFAULT_RADIUS_M)
        emit('nearby', payload)
    except Exception as e:
        emit('error', {'error': str(e)})

# -------------------------
# Jam detection background worker (new)
# -------------------------
def jam_detector_once():
    """
    Simple clustering: gather recent active snapshots and find clusters of devices
    within JAM_CLUSTER_RADIUS_M whose average speed is below JAM_SPEED_THRESHOLD_MPS.
    Produces a jam dict: { id, ts, lat, lon, count, avg_speed, device_ids }
    """
    try:
        cutoff = datetime.utcnow() - timedelta(seconds=CLEANUP_STALE_SECONDS)
        snaps = Snapshot.query.filter(Snapshot.ts >= cutoff).all()
        # add in-memory cache snapshot fallback for devices not in DB recent snaps
        with active_devices_lock:
            for did, entry in active_devices.items():
                if entry.get("ts") and entry.get("ts") >= cutoff:
                    # skip if DB already has a recent snapshot for same device
                    if any(s.device_id == did for s in snaps):
                        continue
                    class _T:
                        pass
                    t = _T()
                    t.device_id = did
                    t.lat = entry.get("lat")
                    t.lon = entry.get("lon")
                    t.speed_mps = entry.get("speed_mps") or 0.0
                    t.bearing_deg = entry.get("bearing_deg") or 0.0
                    t.ts = entry.get("ts")
                    snaps.append(t)
        if not snaps:
            return

        used = set()
        clusters = []
        for s in snaps:
            if s.device_id in used:
                continue
            # build cluster around s
            members = [s]
            for t in snaps:
                if t.device_id == s.device_id or t.device_id in used:
                    continue
                d = haversine_m(s.lat, s.lon, t.lat, t.lon)
                if d <= JAM_CLUSTER_RADIUS_M:
                    members.append(t)
            for m in members:
                used.add(m.device_id)
            # evaluate cluster
            count = len(members)
            avg_speed = sum([(m.speed_mps or 0.0) for m in members]) / max(1, count)
            if count >= JAM_MIN_DEVICES and avg_speed <= JAM_SPEED_THRESHOLD_MPS:
                # compute centroid
                avg_lat = sum([m.lat for m in members]) / count
                avg_lon = sum([m.lon for m in members]) / count
                cluster = {
                    "id": uuid.uuid4().hex,
                    "ts": datetime.utcnow().isoformat(),
                    "lat": round(avg_lat, 6),
                    "lon": round(avg_lon, 6),
                    "count": count,
                    "avg_speed_mps": round(avg_speed, 2),
                    "device_ids": [m.device_id for m in members]
                }
                clusters.append(cluster)

        # push clusters to jams_store and optionally emit to involved device sockets
        for jam in clusters:
            push_jam(jam)
            # emit to each device in jam that has sockets
            for did in jam["device_ids"]:
                try:
                    send_ws_to_device(did, "jam_alert", jam)
                except Exception:
                    pass
    except Exception:
        app.logger.exception("jam_detector_once error")

def jam_detector_loop():
    while True:
        try:
            jam_detector_once()
        except Exception:
            app.logger.exception("jam_detector_loop error")
        time.sleep(JAM_DETECT_INTERVAL_S)

# -------------------------
# CLI entry & startup hooks
# -------------------------
if __name__ == "__main__":
    init_db()
    # start jam detector background thread
    try:
        t = threading.Thread(target=jam_detector_loop, daemon=True)
        t.start()
        app.logger.info("Jam detector thread started.")
    except Exception:
        app.logger.exception("Failed to start jam detector thread.")

    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=os.environ.get("FLASK_DEBUG", "0") == "1")
