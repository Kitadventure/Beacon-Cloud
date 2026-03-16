#!/usr/bin/env python3
"""
app.py - Beacon Cloud minimal full server

Place this file next to index.html and Procfile. Requirements:
  pip install flask flask_sqlalchemy flask_socketio eventlet

Environment variables:
  DATABASE_URL     - SQLAlchemy DB URL (default: sqlite:///beacon.db)
  ADMIN_API_TOKEN  - token for API auth (optional but recommended)
  FLASK_SECRET     - flask secret key (used for session auth)
  PORT             - port to bind when running directly

Notes:
  - This is intentionally simple to be dropped into an existing project.
  - If you're integrating into a larger app, adapt model names/field names to match yours.
"""
import os
import math
import uuid
from datetime import datetime, timedelta

from flask import (
    Flask, request, jsonify, session, redirect, url_for,
    send_from_directory, abort
)
from flask_sqlalchemy import SQLAlchemy

# Optional Socket.IO - used if installed; otherwise fallback to no real-time
try:
    from flask_socketio import SocketIO, emit
    SOCKETIO_AVAILABLE = True
except Exception:
    SOCKETIO_AVAILABLE = False

# -----------------------
# Config & initialization
# -----------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///beacon.db")
ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN", "") or ""
FLASK_SECRET = os.environ.get("FLASK_SECRET", "change-me-in-prod")
DEDUPE_SECONDS = int(os.environ.get("OVERSPEED_DEDUPE_SECS", "30"))

app = Flask(__name__, static_folder=None)  # we'll serve index.html manually
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = FLASK_SECRET

db = SQLAlchemy(app)

socketio = SocketIO(app, cors_allowed_origins="*") if SOCKETIO_AVAILABLE else None

# -----------------------
# Utilities
# -----------------------
def now_utc():
    return datetime.utcnow()

def haversine_m(lat1, lon1, lat2, lon2):
    """
    Haversine distance in meters between two (lat, lon) points.
    """
    R = 6371000.0  # Earth radius meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2.0)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def kmh_from_mps(mps):
    return (mps or 0.0) * 3.6

# -----------------------
# Models
# -----------------------
class Device(db.Model):
    __tablename__ = "device"
    id = db.Column(db.String(64), primary_key=True)
    name = db.Column(db.String(128))
    plate = db.Column(db.String(64))
    model = db.Column(db.String(64))
    created_at = db.Column(db.DateTime, default=now_utc)

    # Latest known location/snapshot summary
    last_lat = db.Column(db.Float)
    last_lon = db.Column(db.Float)
    last_speed_mps = db.Column(db.Float)
    last_seen = db.Column(db.DateTime)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "plate": self.plate,
            "model": self.model,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "lat": self.last_lat,
            "lon": self.last_lon,
            "speed_mps": self.last_speed_mps,
            "speed_kmh": round(kmh_from_mps(self.last_speed_mps), 2) if self.last_speed_mps is not None else None,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None
        }

class Snapshot(db.Model):
    __tablename__ = "snapshot"
    id = db.Column(db.Integer, primary_key=True)
    device_id = db.Column(db.String(64), db.ForeignKey('device.id'), index=True)
    ts = db.Column(db.DateTime, default=now_utc, index=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    speed_mps = db.Column(db.Float)
    raw = db.Column(db.Text)

    def to_dict(self):
        return {
            "id": self.id,
            "device_id": self.device_id,
            "ts": self.ts.isoformat() if self.ts else None,
            "lat": self.lat,
            "lon": self.lon,
            "speed_mps": self.speed_mps,
            "speed_kmh": round(kmh_from_mps(self.speed_mps), 2) if self.speed_mps is not None else None,
            "raw": self.raw
        }

class Road(db.Model):
    __tablename__ = "road"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    center_lat = db.Column(db.Float, nullable=False)
    center_lon = db.Column(db.Float, nullable=False)
    radius_m = db.Column(db.Float, default=50.0)
    speed_limit_kmh = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=now_utc)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "center_lat": self.center_lat,
            "center_lon": self.center_lon,
            "radius_m": self.radius_m,
            "speed_limit_kmh": self.speed_limit_kmh,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

class Overspeed(db.Model):
    __tablename__ = "overspeed"
    id = db.Column(db.Integer, primary_key=True)
    device_id = db.Column(db.String(64), db.ForeignKey('device.id'), index=True)
    road_id = db.Column(db.Integer, db.ForeignKey('road.id'), index=True, nullable=True)
    ts = db.Column(db.DateTime, default=now_utc, index=True)
    speed_mps = db.Column(db.Float)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)

    def to_dict(self):
        return {
            "id": self.id,
            "device_id": self.device_id,
            "road_id": self.road_id,
            "ts": self.ts.isoformat() if self.ts else None,
            "speed_mps": self.speed_mps,
            "speed_kmh": round(kmh_from_mps(self.speed_mps), 2) if self.speed_mps is not None else None,
            "lat": self.lat,
            "lon": self.lon
        }

# -----------------------
# Auth helpers
# -----------------------
def require_admin_api():
    """
    Check admin access by either:
     - session['is_admin'] == True
     - Authorization: Bearer <ADMIN_API_TOKEN>
     - X-API-Token header
    """
    # session
    if session.get('is_admin'):
        return True
    # header token
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        token = auth.split(' ', 1)[1].strip()
        if ADMIN_API_TOKEN and token and token == ADMIN_API_TOKEN:
            return True
    # x-api-token
    xt = request.headers.get('X-API-Token', '')
    if ADMIN_API_TOKEN and xt and xt == ADMIN_API_TOKEN:
        return True
    abort(401, description="missing admin token or login")

# Simple admin login page (very small, for convenience)
@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        token = request.form.get('token') or request.form.get('password') or ''
        if ADMIN_API_TOKEN and token == ADMIN_API_TOKEN:
            session['is_admin'] = True
            return redirect(url_for('admin_dashboard'))
        return "invalid token", 401
    # simple form
    return """
    <html><body>
      <h3>Admin login</h3>
      <form method="post">
        <input name="token" placeholder="API Token" />
        <button type="submit">Login</button>
      </form>
    </body></html>
    """

@app.route('/admin/logout')
def admin_logout():
    session.pop('is_admin', None)
    return redirect('/')

# -----------------------
# Endpoints - Admin APIs
# -----------------------
@app.route('/admin/devices', methods=['GET'])
def admin_devices():
    # Public info endpoint: allow read without admin? we'll allow public reading
    # If you want to restrict, call require_admin_api()
    devices = Device.query.all()
    return jsonify([d.to_dict() for d in devices])

@app.route('/admin/device/<device_id>/json', methods=['GET'])
def admin_device_json(device_id):
    d = Device.query.get(device_id)
    if not d:
        return jsonify({"error": "not found"}), 404
    # include recent snapshots (last 50)
    snaps = Snapshot.query.filter_by(device_id=device_id).order_by(Snapshot.ts.desc()).limit(50).all()
    return jsonify({"device": d.to_dict(), "snapshots": [s.to_dict() for s in snaps]})

@app.route('/admin/roads', methods=['GET', 'POST'])
def admin_roads():
    if request.method == 'GET':
        # public read allowed
        roads = Road.query.order_by(Road.id.asc()).all()
        return jsonify([r.to_dict() for r in roads])
    # create road - require admin
    require_admin_api()
    data = request.get_json(force=True)
    if not data:
        return jsonify({"error": "json body required"}), 400
    # accept several possible field names for compat
    name = data.get('name') or data.get('road_name')
    lat = data.get('center_lat') or data.get('lat')
    lon = data.get('center_lon') or data.get('lon')
    radius_m = data.get('radius_m') or data.get('radius') or 50.0
    limit_kmh = data.get('speed_limit_kmh') or data.get('limit_kmh') or data.get('limit') or None
    if not (name and lat is not None and lon is not None and limit_kmh is not None):
        return jsonify({"error": "missing fields: name, lat, lon, speed_limit_kmh required"}), 400
    try:
        r = Road(
            name=str(name),
            center_lat=float(lat),
            center_lon=float(lon),
            radius_m=float(radius_m),
            speed_limit_kmh=float(limit_kmh)
        )
        db.session.add(r)
        db.session.commit()
        return jsonify({"ok": True, "id": r.id, "road": r.to_dict()})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@app.route('/admin/overspeeds', methods=['GET'])
def admin_overspeeds():
    # require admin to read overspeeds (optional - make public if you prefer)
    require_admin_api()
    road_id = request.args.get('road_id', type=int)
    q = Overspeed.query
    if road_id:
        q = q.filter_by(road_id=road_id)
    # optional time window
    since_secs = request.args.get('since_secs', type=int)
    if since_secs:
        cutoff = now_utc() - timedelta(seconds=since_secs)
        q = q.filter(Overspeed.ts >= cutoff)
    rows = q.order_by(Overspeed.ts.desc()).limit(1000).all()
    return jsonify({"overspeeds": [r.to_dict() for r in rows]})

# -----------------------
# Heartbeat / Telemetry ingestion
# -----------------------
@app.route('/api/heartbeat', methods=['POST'])
def api_heartbeat():
    """
    Receive JSON:
    {
      "device_id": "uuid-or-plate",
      "lat":  -1.2921,
      "lon": 36.8219,
      "speed_mps": 12.3,        # optional
      "ts": "2025-03-14T12:...Z",  # optional ISO8601
      "raw": {...}             # optional raw payload
    }
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "json required"}), 400

    device_id = str(data.get('device_id') or data.get('id') or data.get('device') or uuid.uuid4().hex)
    lat = data.get('lat') or data.get('latitude') or data.get('lat_deg')
    lon = data.get('lon') or data.get('longitude') or data.get('lon_deg')
    speed_mps = data.get('speed_mps') or data.get('speed') or None
    ts_raw = data.get('ts') or data.get('timestamp') or None
    raw = data.get('raw') or data

    # coerce
    try:
        lat = None if lat is None else float(lat)
        lon = None if lon is None else float(lon)
    except Exception:
        return jsonify({"error": "lat/lon must be numeric"}), 400

    try:
        speed_mps = None if speed_mps is None else float(speed_mps)
    except Exception:
        speed_mps = None

    # parse timestamp if present
    ts = None
    if ts_raw:
        try:
            # try isoformat parsing
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).astimezone(tz=None).replace(tzinfo=None)
        except Exception:
            try:
                # fallback, assume epoch seconds
                ts = datetime.utcfromtimestamp(float(ts_raw))
            except Exception:
                ts = now_utc()
    else:
        ts = now_utc()

    # find or create device
    device = Device.query.get(device_id)
    if not device:
        device = Device(id=device_id, name=data.get('name') or None, plate=data.get('plate') or None)
        db.session.add(device)
        db.session.commit()

    # create snapshot
    snap = Snapshot(device_id=device_id, ts=ts, lat=lat, lon=lon, speed_mps=speed_mps, raw=str(raw))
    db.session.add(snap)
    # update device quick fields
    if lat is not None and lon is not None:
        device.last_lat = lat
        device.last_lon = lon
    if speed_mps is not None:
        device.last_speed_mps = speed_mps
    device.last_seen = ts
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "db commit failed: " + str(e)}), 500

    # emit snapshot to websockets if available
    try:
        payload = {"device_id": device_id, "ts": ts.isoformat(), "lat": lat, "lon": lon, "speed_mps": speed_mps, "speed_kmh": kmh_from_mps(speed_mps)}
        if SOCKETIO_AVAILABLE:
            socketio.emit('snapshot', payload, broadcast=True)
    except Exception:
        app.logger.exception("socket emit failed for snapshot")

    # check overspeed against registered roads
    try:
        roads = Road.query.all()
        for r in roads:
            if lat is None or lon is None:
                continue
            dist_m = haversine_m(lat, lon, r.center_lat, r.center_lon)
            if dist_m <= (r.radius_m or 50.0):
                # if speed known and exceeds limit -> log overspeed
                if speed_mps is not None and kmh_from_mps(speed_mps) > float(r.speed_limit_kmh):
                    # dedupe: ensure we don't insert repeated overspeeds for same device+road within DEDUPE_SECONDS
                    cutoff = now_utc() - timedelta(seconds=DEDUPE_SECONDS)
                    last = Overspeed.query.filter_by(device_id=device_id, road_id=r.id).order_by(Overspeed.ts.desc()).first()
                    if not last or (last.ts < cutoff):
                        ov = Overspeed(device_id=device_id, road_id=r.id, ts=ts, speed_mps=speed_mps, lat=lat, lon=lon)
                        db.session.add(ov)
                        db.session.commit()
                        if SOCKETIO_AVAILABLE:
                            socketio.emit('overspeed', ov.to_dict(), broadcast=True)
    except Exception:
        app.logger.exception("overspeed check failed")

    return jsonify({"ok": True, "device_id": device_id, "ts": ts.isoformat()})

# -----------------------
# Serve index.html at /unlimited
# -----------------------
@app.route('/unlimited')
def unlimited_ui():
    """
    Serve index.html from the same directory as app.py
    """
    base = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(base, "index.html")
    if not os.path.exists(index_path):
        return "<h3>index.html not found. Place index.html next to app.py</h3>", 404
    return send_from_directory(base, "index.html")

# Root friendly message
@app.route('/')
def root():
    return """
    <h2>Beacon Cloud</h2>
    <div><a href="/unlimited">Open dashboard</a></div>
    <div><a href="/admin/devices">/admin/devices (json)</a></div>
    <div><a href="/admin/roads">/admin/roads (json)</a></div>
    """

# -----------------------
# CLI helpers
# -----------------------
@app.cli.command('initdb')
def initdb_command():
    """Initialize the database (flask initdb)."""
    db.create_all()
    print("Initialized the database.")

# allow simple startup to create DB if missing
@app.before_first_request
def ensure_db():
    db.create_all()

# -----------------------
# Main
# -----------------------
if __name__ == '__main__':
    # if using socketio and eventlet is installed, using socketio.run makes it realtime-friendly
    port = int(os.environ.get("PORT", 5000))
    if SOCKETIO_AVAILABLE:
        # prefer eventlet/uwsgi in production. Using eventlet for dev/realtime
        print("Starting with Socket.IO support")
        socketio.run(app, host='0.0.0.0', port=port)
    else:
        print("Starting without Socket.IO (flask only)")
        app.run(host='0.0.0.0', port=port, debug=True)
