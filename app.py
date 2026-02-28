# app.py — Beacon backend with WebSocket (Flask + Flask-SocketIO)
# Threading async_mode for Windows/dev. For production and multi-worker, use a message queue + eventlet/gevent.

import os
import uuid
from math import radians, sin, cos, atan2, sqrt, degrees
from datetime import datetime, timedelta
import json

from flask import (
    Flask, request, jsonify, render_template_string, abort,
    redirect, url_for, session, flash
)
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit, join_room
from werkzeug.security import generate_password_hash, check_password_hash

# -------------------------
# Configuration
# -------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///beacon.db")
CLEANUP_STALE_SECONDS = int(os.environ.get("CLEANUP_STALE_SECONDS", "12"))  # remove snapshots older than this
NEARBY_DEFAULT_RADIUS_M = float(os.environ.get("NEARBY_DEFAULT_RADIUS_M", "500"))
# Optional API token for scripted admin calls (keeps compatibility with env-based workflows)
ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN")

# SocketIO message queue not used for local dev with threading.
SOCKETIO_MESSAGE_QUEUE = os.environ.get("SOCKETIO_MESSAGE_QUEUE")

VEHICLE_LENGTH_M = float(os.environ.get("VEHICLE_LENGTH_M", "5.0"))
OVERTAKE_EXTRA_M = float(os.environ.get("OVERTAKE_EXTRA_M", "5.0"))
SAFETY_FACTOR = float(os.environ.get("SAFETY_FACTOR", "1.5"))

# -------------------------
# Flask + SQLAlchemy + SocketIO init
# -------------------------
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
# Use a secret for sessions. Set FLASK_SECRET in environment, otherwise a dev fallback is used.
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
    heading_deg = db.Column(db.Float) # optional separate heading if provided
    source = db.Column(db.String(32), default="app") # e.g., "app", "web"
    raw = db.Column(db.Text) # JSON dump of raw payload (optional)

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
            guidance['unsafe_if_overtaking'] = (ttc < 6.0)
    return guidance

# -------------------------
# DB helpers
# -------------------------
def init_db():
    with app.app_context():
        db.create_all()

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
    device = require_auth_token()
    payload = request.get_json(force=True, silent=True) or {}
    device_id = payload.get("device_id") or device.id
    lat = payload.get("lat")
    lon = payload.get("lon")
    if lat is None or lon is None:
        return jsonify({"error": "lat & lon required"}), 400
    speed_mps = payload.get("speed_mps")
    if speed_mps is None:
        speed_kmh = payload.get("speed_kmh")
        if speed_kmh is not None:
            speed_mps = float(speed_kmh) / 3.6
        else:
            speed_mps = 0.0
    bearing = float(payload.get("bearing", 0.0))
    heading = float(payload.get("heading", bearing))
    src = payload.get("source", "app")
    snap = Snapshot(
        device_id=device_id,
        ts=datetime.utcnow(),
        lat=float(lat),
        lon=float(lon),
        speed_mps=float(speed_mps),
        bearing_deg=float(bearing),
        heading_deg=float(heading),
        source=str(src),
        raw=json.dumps(payload)
    )
    db.session.add(snap)
    db.session.commit()

    cleanup_old_snapshots()

    try:
        nearby_payload = compute_nearby_for_device(device_id, radius_m=NEARBY_DEFAULT_RADIUS_M)
        send_ws_to_device(device_id, 'nearby_update', nearby_payload)
    except Exception:
        pass

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
    self_snap = Snapshot.query.filter_by(device_id=device_id).order_by(Snapshot.ts.desc()).first()
    if not self_snap:
        return jsonify({"error": "no snapshot for device"}), 404
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
            "guidance": guidance
        })
    results.sort(key=lambda x: x["distance_m"])
    return jsonify({
        "self": {
            "device_id": self_snap.device_id,
            "lat": self_snap.lat,
            "lon": self_snap.lon,
            "speed_mps": round(self_snap.speed_mps, 2),
            "bearing_deg": round(self_snap.bearing_deg, 1),
            "ts": self_snap.ts.isoformat()
        },
        "nearby": results
    })

def compute_nearby_for_device(device_id, radius_m=NEARBY_DEFAULT_RADIUS_M):
    self_snap = Snapshot.query.filter_by(device_id=device_id).order_by(Snapshot.ts.desc()).first()
    if not self_snap:
        raise RuntimeError('no snapshot')
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
            "guidance": guidance
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
# Admin/UI (friendly) templates
# -------------------------
FRIENDLY_HTML = """
<!doctype html>
<title>Beacon Admin — Friendly</title>
<style>
:root{--bg:#f7fafc;--card:#fff;--muted:#6b7280}
body{font-family:Inter,system-ui,Arial;background:var(--bg);padding:28px;color:#111}
.container{max-width:1100px;margin:0 auto}
.card{background:var(--card);padding:18px;border-radius:10px;box-shadow:0 6px 18px rgba(12,12,12,0.06);margin-bottom:12px}
h1{margin:0 0 6px 0}
.small{color:var(--muted);font-size:13px}
.table{width:100%;border-collapse:collapse;margin-top:12px}
.table th,.table td{padding:8px;border-bottom:1px solid #eee;text-align:left;font-size:13px}
.actions{display:flex;gap:8px}
.btn{padding:8px 10px;border-radius:6px;border:0;cursor:pointer}
.btn-danger{background:#ef4444;color:#fff}
.btn-primary{background:#2563eb;color:#fff}
.form-row{display:flex;gap:8px;margin-top:8px}
.input{padding:8px;border:1px solid #ddd;border-radius:6px;flex:1}
.note{font-size:12px;color:#666;margin-top:6px}
</style>
<div class="container">
  <div class="card">
    <h1>Beacon Admin — Friendly</h1>
    <div class="small">Manage devices, view last snapshots, revoke tokens, and push messages.</div>
  </div>

  {% if not logged_in and show_register %}
  <div class="card">
    <h2>Register admin (one-time)</h2>
    <form method="post" action="{{ url_for('friendly_register') }}">
      <div class="form-row">
        <input class="input" name="username" placeholder="admin username (e.g. admin)" required />
        <input class="input" name="password" placeholder="strong password" type="password" required />
        <button class="btn btn-primary" type="submit">Register</button>
      </div>
      <div class="note">This creates the single web admin account for this server. Register once.</div>
    </form>
  </div>
  {% elif not logged_in %}
  <div class="card">
    <h2>Admin login</h2>
    <form method="post" action="{{ url_for('friendly_login') }}">
      <div class="form-row">
        <input class="input" name="username" placeholder="username" required />
        <input class="input" name="password" placeholder="password" type="password" required />
        <button class="btn btn-primary" type="submit">Login</button>
      </div>
    </form>
  </div>
  {% endif %}

  {% if logged_in %}
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <div>
        <h2>Active devices (last {{stale_sec}}s)</h2>
        <div class="small">Nearby and online devices</div>
      </div>
      <div>
        <form method="post" action="{{ url_for('friendly_logout') }}">
          <button class="btn" type="submit">Logout</button>
        </form>
      </div>
    </div>

    <table class="table">
      <thead><tr><th>device_id</th><th>owner</th><th>car</th><th>last_seen</th><th>lat,lon</th><th>speed_kmh</th><th>revoked</th><th>actions</th></tr></thead>
      <tbody>
      {% for row in rows %}
        <tr>
          <td style="font-family:monospace">{{ row.device_id }}</td>
          <td>{{ row.owner }}</td>
          <td>{{ row.car }}</td>
          <td>{{ row.ts }}</td>
          <td>{{ row.lat }},{{ row.lon }}</td>
          <td>{{ row.speed }}</td>
          <td>{{ row.revoked }}</td>
          <td class="actions">
            <form method="post" action="{{ url_for('friendly_revoke') }}">
              <input type="hidden" name="device_id" value="{{ row.device_id }}" />
              <button class="btn btn-danger" type="submit">Revoke</button>
            </form>
            <form method="post" action="{{ url_for('friendly_push') }}">
              <input type="hidden" name="device_id" value="{{ row.device_id }}" />
              <input class="input" name="message" placeholder="message to device" />
              <button class="btn" type="submit">Send</button>
            </form>
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h3>All devices (admin)</h3>
    <table class="table">
      <thead><tr><th>device_id</th><th>owner</th><th>car_model</th><th>plate</th><th>created</th><th>revoked</th></tr></thead>
      <tbody>
      {% for d in all %}
        <tr>
          <td style="font-family:monospace">{{ d.id }}</td>
          <td>{{ d.owner }}</td>
          <td>{{ d.car_model }}</td>
          <td>{{ d.plate }}</td>
          <td>{{ d.created }}</td>
          <td>{{ d.revoked }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
  {% endif %}

</div>
"""

# -------------------------
# Admin/UI routes
# -------------------------
@app.route('/friendly', methods=['GET'])
def friendly():
    # decide what to show:
    admin_exists = Admin.query.first() is not None
    logged_in = session.get('admin_logged', False)
    # If admin doesn't exist, show register form (unless already logged in)
    show_register = not admin_exists
    if logged_in:
        # Build admin view
        cutoff = datetime.utcnow() - timedelta(seconds=CLEANUP_STALE_SECONDS)
        rows = []
        devices = Device.query.all()
        for d in devices:
            s = Snapshot.query.filter_by(device_id=d.id).order_by(Snapshot.ts.desc()).first()
            if s and s.ts >= cutoff:
                rows.append({
                    'device_id': d.id,
                    'owner': d.owner or '',
                    'car': d.car_name or d.car_model or '',
                    'ts': s.ts.isoformat(),
                    'lat': round(s.lat,5),
                    'lon': round(s.lon,5),
                    'speed': round(s.speed_mps * 3.6, 2),
                    'revoked': d.revoked
                })
        all_devices = Device.query.order_by(Device.created_at.desc()).all()
        all_view = []
        for d in all_devices:
            all_view.append({
                'id': d.id,
                'owner': d.owner,
                'car_model': d.car_model,
                'plate': d.plate,
                'created': d.created_at.isoformat(),
                'revoked': d.revoked
            })
        return render_template_string(FRIENDLY_HTML, logged_in=True, rows=rows, all=all_view, stale_sec=CLEANUP_STALE_SECONDS)
    else:
        return render_template_string(FRIENDLY_HTML, logged_in=False, show_register=show_register)

@app.route('/friendly/register', methods=['POST'])
def friendly_register():
    # allow registration only if no admin exists
    if Admin.query.first():
        flash("Admin account already exists. Please login.", "warning")
        return redirect(url_for('friendly'))
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')
    if not username or not password:
        flash("username and password required", "error")
        return redirect(url_for('friendly'))
    # create admin
    pwd_hash = generate_password_hash(password)
    admin = Admin(username=username, password_hash=pwd_hash)
    db.session.add(admin)
    db.session.commit()
    # auto-login after registration
    session['admin_logged'] = True
    session['admin_username'] = admin.username
    flash("Admin registered and logged in.", "success")
    return redirect(url_for('friendly'))

@app.route('/friendly/login', methods=['POST'])
def friendly_login():
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')
    admin = Admin.query.filter_by(username=username).first()
    if not admin or not check_password_hash(admin.password_hash, password):
        flash("Invalid credentials", "error")
        return redirect(url_for('friendly'))
    session['admin_logged'] = True
    session['admin_username'] = admin.username
    flash("Logged in", "success")
    return redirect(url_for('friendly'))

@app.route('/friendly/logout', methods=['POST'])
def friendly_logout():
    session.pop('admin_logged', None)
    session.pop('admin_username', None)
    flash("Logged out", "info")
    return redirect(url_for('friendly'))

@app.route('/friendly/revoke', methods=['POST'])
def friendly_revoke():
    if not session.get('admin_logged'):
        abort(401)
    device_id = request.form.get('device_id')
    d = Device.query.filter_by(id=device_id).first()
    if not d:
        abort(404)
    d.revoked = True
    connected_sockets.pop(device_id, None)
    db.session.commit()
    return redirect(url_for('friendly'))

@app.route('/friendly/push', methods=['POST'])
def friendly_push():
    if not session.get('admin_logged'):
        abort(401)
    device_id = request.form.get('device_id')
    message = request.form.get('message')
    payload = {'message': message, 'ts': datetime.utcnow().isoformat()}
    send_ws_to_device(device_id, 'admin_message', payload)
    return redirect(url_for('friendly'))

# -------------------------
# Admin JSON endpoints
# -------------------------
@app.route('/admin/devices')
def admin_devices():
    require_admin_api()
    devices = Device.query.all()
    out = []
    for d in devices:
        out.append({'id': d.id, 'owner': d.owner, 'car_model': d.car_model, 'plate': d.plate, 'created_at': d.created_at.isoformat(), 'revoked': d.revoked})
    return jsonify(out)

@app.route('/admin/revoke', methods=['POST'])
def admin_revoke():
    require_admin_api()
    device_id = request.json.get('device_id')
    d = Device.query.filter_by(id=device_id).first()
    if not d:
        return jsonify({'error': 'not found'}), 404
    d.revoked = True
    connected_sockets.pop(device_id, None)
    db.session.commit()
    return jsonify({'ok': True})

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