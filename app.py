#!/usr/bin/env python3
"""
Full-featured backend for Beacon Cloud (app.py)

Features:
 - Devices, Snapshots
 - Roads (center + radius geometry)
 - Overspeed detection with dedupe
 - Admin endpoints (CRUD + search + traffic overview)
 - Telemetry ingestion endpoint (/api/heartbeat)
 - Optional Socket.IO realtime events (if flask-socketio installed)
 - Token or session-based admin authentication
 - Pagination, validation, and CLI helpers

Configuration (env vars):
 - DATABASE_URL (default: sqlite:///beacon.db)
 - ADMIN_API_TOKEN (optional; if set allows Bearer auth)
 - FLASK_SECRET (default: change-me)
 - OVERSPEED_DEDUPE_SECS (default: 30)
 - DEFAULT_CENTER_LAT / DEFAULT_CENTER_LON (map centering defaults)
 - CORS_ORIGINS (optional, comma-separated)
 - DEBUG (0/1)

Usage:
  python app.py
  # or with socketio/eventlet support (if installed) the app will auto-detect
"""
from __future__ import annotations
import os
import math
import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from flask import (
    Flask, request, jsonify, abort, session, redirect, url_for,
    send_from_directory
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_

# optional real-time
try:
    from flask_socketio import SocketIO
    SOCKETIO_AVAILABLE = True
except Exception:
    SOCKETIO_AVAILABLE = False

# -----------------------
# Basic config & logging
# -----------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///beacon.db")
ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN", "") or ""
FLASK_SECRET = os.environ.get("FLASK_SECRET", "change-me-in-prod")
OVERSPEED_DEDUPE_SECS = int(os.environ.get("OVERSPEED_DEDUPE_SECS", "30"))
DEBUG = bool(int(os.environ.get("DEBUG", "0")))
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "")  # comma-separated
DEFAULT_CENTER_LAT = float(os.environ.get("DEFAULT_CENTER_LAT", "-1.2921"))
DEFAULT_CENTER_LON = float(os.environ.get("DEFAULT_CENTER_LON", "36.8219"))

logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("beacon")

app = Flask(__name__, static_folder=None)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.secret_key = FLASK_SECRET

db = SQLAlchemy(app)

socketio = SocketIO(app, cors_allowed_origins="*") if SOCKETIO_AVAILABLE else None

# -----------------------
# Utilities
# -----------------------
def now_utc() -> datetime:
    return datetime.utcnow()

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Haversine distance in meters.
    """
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def mps_to_kmh(mps: Optional[float]) -> Optional[float]:
    return None if mps is None else round(mps * 3.6, 2)

def safe_float(v, default=None):
    try:
        return None if v is None else float(v)
    except Exception:
        return default

def uuid_str():
    return uuid.uuid4().hex

# -----------------------
# Models
# -----------------------
class Device(db.Model):
    __tablename__ = "device"
    id = db.Column(db.String(64), primary_key=True)
    name = db.Column(db.String(128), nullable=True)
    plate = db.Column(db.String(64), nullable=True, index=True)
    model = db.Column(db.String(64), nullable=True)
    meta = db.Column(db.JSON, nullable=True)

    created_at = db.Column(db.DateTime, default=now_utc)
    last_lat = db.Column(db.Float, nullable=True)
    last_lon = db.Column(db.Float, nullable=True)
    last_speed_mps = db.Column(db.Float, nullable=True)
    last_seen = db.Column(db.DateTime, nullable=True)

    def to_dict(self, include_meta: bool = False) -> Dict[str, Any]:
        out = {
            "id": self.id,
            "name": self.name,
            "plate": self.plate,
            "model": self.model,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "lat": self.last_lat,
            "lon": self.last_lon,
            "speed_mps": self.last_speed_mps,
            "speed_kmh": mps_to_kmh(self.last_speed_mps),
            "last_seen": self.last_seen.isoformat() if self.last_seen else None
        }
        if include_meta:
            out["meta"] = self.meta
        return out

class Snapshot(db.Model):
    __tablename__ = "snapshot"
    id = db.Column(db.Integer, primary_key=True)
    device_id = db.Column(db.String(64), db.ForeignKey("device.id"), index=True)
    ts = db.Column(db.DateTime, default=now_utc, index=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    speed_mps = db.Column(db.Float, nullable=True)
    heading = db.Column(db.Float, nullable=True)
    battery = db.Column(db.Float, nullable=True)
    raw = db.Column(db.JSON, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "device_id": self.device_id,
            "ts": self.ts.isoformat() if self.ts else None,
            "lat": self.lat,
            "lon": self.lon,
            "speed_mps": self.speed_mps,
            "speed_kmh": mps_to_kmh(self.speed_mps),
            "heading": self.heading,
            "battery": self.battery,
            "raw": self.raw
        }

class Road(db.Model):
    """
    Simple geometry support: center_lat/center_lon + radius_m
    For more precise geometry, replace with PostGIS geometry and spatial queries.
    """
    __tablename__ = "road"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    center_lat = db.Column(db.Float, nullable=False)
    center_lon = db.Column(db.Float, nullable=False)
    radius_m = db.Column(db.Float, nullable=False, default=50.0)
    # speed limit expressed in km/h
    speed_limit_kmh = db.Column(db.Float, nullable=False)
    meta = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime, default=now_utc)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "center_lat": self.center_lat,
            "center_lon": self.center_lon,
            "radius_m": self.radius_m,
            "speed_limit_kmh": self.speed_limit_kmh,
            "meta": self.meta,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

class Overspeed(db.Model):
    __tablename__ = "overspeed"
    id = db.Column(db.Integer, primary_key=True)
    device_id = db.Column(db.String(64), db.ForeignKey("device.id"), index=True)
    road_id = db.Column(db.Integer, db.ForeignKey("road.id"), index=True, nullable=True)
    ts = db.Column(db.DateTime, default=now_utc, index=True)
    speed_mps = db.Column(db.Float, nullable=True)
    lat = db.Column(db.Float, nullable=True)
    lon = db.Column(db.Float, nullable=True)
    snapshot_id = db.Column(db.Integer, db.ForeignKey("snapshot.id"), nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "device_id": self.device_id,
            "road_id": self.road_id,
            "ts": self.ts.isoformat() if self.ts else None,
            "speed_mps": self.speed_mps,
            "speed_kmh": mps_to_kmh(self.speed_mps),
            "lat": self.lat,
            "lon": self.lon,
            "snapshot_id": self.snapshot_id
        }

# -----------------------
# DB helpers & CLI
# -----------------------
@app.cli.command("initdb")
def initdb_command():
    """Initialize the database tables."""
    db.create_all()
    logger.info("Database tables created.")

@app.cli.command("dropdb")
def dropdb_command():
    """Drop all tables (dangerous!)."""
    db.drop_all()
    logger.warning("Dropped all database tables.")

@app.cli.command("create-admin-token")
def create_admin_token_cmd():
    """Create an admin token to print out (not stored server-side by default)."""
    tok = uuid_str()
    print("Generated token (copy to ADMIN_API_TOKEN env):", tok)

# -----------------------
# Auth helpers
# -----------------------
def require_admin_api():
    """
    Allow access when:
      - session['is_admin'] == True (after /admin/login)
      - Authorization: Bearer <ADMIN_API_TOKEN>
      - X-API-Token: <ADMIN_API_TOKEN>
    """
    if session.get("is_admin"):
        return True
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth.split(" ", 1)[1].strip()
        if ADMIN_API_TOKEN and token and token == ADMIN_API_TOKEN:
            return True
    xt = request.headers.get("X-API-Token", "")
    if ADMIN_API_TOKEN and xt and xt == ADMIN_API_TOKEN:
        return True
    abort(401, description="admin auth required")

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    """
    Very small admin login for convenience (only sets session if ADMIN_API_TOKEN matches).
    Not intended as a hardened auth system.
    """
    if request.method == "POST":
        token = request.form.get("token") or request.form.get("password") or ""
        if ADMIN_API_TOKEN and token == ADMIN_API_TOKEN:
            session["is_admin"] = True
            return redirect(url_for("admin_ping"))
        return "invalid token", 401
    return """
    <html><body>
      <h3>Admin login</h3>
      <form method="post">
        <input name="token" placeholder="API Token" />
        <button type="submit">Login</button>
      </form>
    </body></html>
    """

@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    return redirect("/")

# -----------------------
# Utility endpoints
# -----------------------
@app.route("/health")
def health():
    return jsonify({"ok": True, "ts": now_utc().isoformat()})

@app.route("/admin/ping")
def admin_ping():
    require_admin_api()
    return jsonify({"ok": True, "ts": now_utc().isoformat()})

# -----------------------
# Device endpoints (CRUD + search)
# -----------------------
@app.route("/admin/devices", methods=["GET", "POST"])
def admin_devices():
    """
    GET: list devices (public)
      query params: page, per_page
    POST: create a device (admin)
    """
    if request.method == "GET":
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 100))
        q = Device.query.order_by(Device.created_at.desc())
        devices = q.paginate(page=page, per_page=per_page, error_out=False)
        return jsonify({
            "total": devices.total,
            "page": page,
            "per_page": per_page,
            "devices": [d.to_dict() for d in devices.items]
        })
    # POST - create device
    require_admin_api()
    data = request.get_json(force=True, silent=True) or {}
    dev_id = data.get("id") or uuid_str()
    if Device.query.get(dev_id):
        return jsonify({"error": "device already exists", "id": dev_id}), 400
    d = Device(id=str(dev_id), name=data.get("name"), plate=data.get("plate"), model=data.get("model"), meta=data.get("meta"))
    db.session.add(d)
    db.session.commit()
    return jsonify({"ok": True, "device": d.to_dict()}), 201

@app.route("/admin/device/<device_id>", methods=["GET", "PUT", "DELETE"])
def admin_device(device_id):
    d = Device.query.get(device_id)
    if not d:
        return jsonify({"error": "not found"}), 404
    if request.method == "GET":
        # include last 100 snapshots optionally
        include_snaps = bool(request.args.get("snapshots", "").lower() in ("1", "true", "yes"))
        out = d.to_dict(include_meta=True)
        if include_snaps:
            snaps = Snapshot.query.filter_by(device_id=device_id).order_by(Snapshot.ts.desc()).limit(100).all()
            out["snapshots"] = [s.to_dict() for s in snaps]
        return jsonify(out)
    if request.method == "PUT":
        require_admin_api()
        data = request.get_json(force=True, silent=True) or {}
        d.name = data.get("name", d.name)
        d.plate = data.get("plate", d.plate)
        d.model = data.get("model", d.model)
        if "meta" in data:
            d.meta = data.get("meta")
        db.session.commit()
        return jsonify({"ok": True, "device": d.to_dict()})
    if request.method == "DELETE":
        require_admin_api()
        # optionally cascade snapshots and overspeeds
        Snapshot.query.filter_by(device_id=device_id).delete()
        Overspeed.query.filter_by(device_id=device_id).delete()
        db.session.delete(d)
        db.session.commit()
        return jsonify({"ok": True})

@app.route("/admin/search")
def admin_search():
    """
    Search devices by id, name or plate.
    q parameter required.
    """
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "q parameter required"}), 400
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    qlike = f"%{q}%"
    devices = Device.query.filter(or_(Device.id.ilike(qlike), Device.name.ilike(qlike), Device.plate.ilike(qlike))).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({
        "total": devices.total,
        "page": page,
        "per_page": per_page,
        "devices": [d.to_dict() for d in devices.items]
    })

# -----------------------
# Road endpoints (CRUD)
# -----------------------
@app.route("/admin/roads", methods=["GET", "POST"])
def admin_roads():
    if request.method == "GET":
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 100))
        roads = Road.query.order_by(Road.id.asc()).paginate(page=page, per_page=per_page, error_out=False)
        return jsonify({
            "total": roads.total,
            "page": page,
            "per_page": per_page,
            "roads": [r.to_dict() for r in roads.items]
        })
    # POST -> create
    require_admin_api()
    data = request.get_json(force=True, silent=True) or {}
    name = data.get("name")
    center_lat = safe_float(data.get("center_lat") or data.get("lat"))
    center_lon = safe_float(data.get("center_lon") or data.get("lon"))
    radius_m = safe_float(data.get("radius_m") or data.get("radius") or 50.0, 50.0)
    limit_kmh = safe_float(data.get("speed_limit_kmh") or data.get("limit_kmh") or data.get("limit"))
    if not (name and center_lat is not None and center_lon is not None and limit_kmh is not None):
        return jsonify({"error": "missing name/center_lat/center_lon/speed_limit_kmh"}), 400
    r = Road(name=name, center_lat=center_lat, center_lon=center_lon, radius_m=radius_m, speed_limit_kmh=limit_kmh, meta=data.get("meta"))
    db.session.add(r)
    db.session.commit()
    return jsonify({"ok": True, "road": r.to_dict()}), 201

@app.route("/admin/roads/<int:road_id>", methods=["GET", "PUT", "DELETE"])
def admin_road(road_id):
    r = Road.query.get(road_id)
    if not r:
        return jsonify({"error": "not found"}), 404
    if request.method == "GET":
        return jsonify(r.to_dict())
    require_admin_api()
    if request.method == "PUT":
        data = request.get_json(force=True, silent=True) or {}
        r.name = data.get("name", r.name)
        r.center_lat = safe_float(data.get("center_lat"), r.center_lat)
        r.center_lon = safe_float(data.get("center_lon"), r.center_lon)
        r.radius_m = safe_float(data.get("radius_m"), r.radius_m)
        r.speed_limit_kmh = safe_float(data.get("speed_limit_kmh"), r.speed_limit_kmh)
        if "meta" in data:
            r.meta = data.get("meta")
        db.session.commit()
        return jsonify({"ok": True, "road": r.to_dict()})
    if request.method == "DELETE":
        # deleting road does not delete overspeeds (but could)
        db.session.delete(r)
        db.session.commit()
        return jsonify({"ok": True})

# -----------------------
# Overspeed endpoints
# -----------------------
@app.route("/admin/overspeeds", methods=["GET"])
def admin_overspeeds():
    """
    Query overspeeds. Admin-only.
    Query params:
      - road_id
      - device_id
      - min_speed (km/h)
      - since_secs (int)
      - page, per_page
    """
    require_admin_api()
    q = Overspeed.query
    road_id = request.args.get("road_id", type=int)
    device_id = request.args.get("device_id")
    min_speed = safe_float(request.args.get("min_speed"))
    since_secs = request.args.get("since_secs", type=int)
    if road_id:
        q = q.filter(Overspeed.road_id == road_id)
    if device_id:
        q = q.filter(Overspeed.device_id == device_id)
    if min_speed is not None:
        # convert km/h to mps for filter
        mpst = float(min_speed) / 3.6
        q = q.filter(Overspeed.speed_mps >= mpst)
    if since_secs:
        cutoff = now_utc() - timedelta(seconds=since_secs)
        q = q.filter(Overspeed.ts >= cutoff)
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 200))
    rows = q.order_by(Overspeed.ts.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({
        "total": rows.total,
        "page": page,
        "per_page": per_page,
        "overspeeds": [r.to_dict() for r in rows.items]
    })

# -----------------------
# Traffic overview
# -----------------------
@app.route("/admin/traffic", methods=["GET"])
def admin_traffic():
    """
    Return aggregated traffic per road or global:
     - if road_id provided -> detailed stats for that road
     - else -> list roads with vehicle_count, avg_speed, overspeed_count
    Query params: road_id, since_secs (default 3600)
    """
    require_admin_api()
    road_id = request.args.get("road_id", type=int)
    since_secs = request.args.get("since_secs", type=int) or 3600
    cutoff = now_utc() - timedelta(seconds=since_secs)

    if road_id:
        road = Road.query.get(road_id)
        if not road:
            return jsonify({"error": "road not found"}), 404
        # get snapshots within time window inside the road radius
        snaps = Snapshot.query.filter(Snapshot.ts >= cutoff).all()
        snaps_in = [s for s in snaps if (s.lat is not None and s.lon is not None and haversine_m(s.lat, s.lon, road.center_lat, road.center_lon) <= road.radius_m)]
        device_ids = set(s.device_id for s in snaps_in)
        avg_speed = None
        speeds = [s.speed_mps for s in snaps_in if s.speed_mps is not None]
        if speeds:
            avg_speed = sum(speeds) / len(speeds)
        overs = Overspeed.query.filter(Overspeed.road_id == road_id, Overspeed.ts >= cutoff).count()
        return jsonify({
            "road": road.to_dict(),
            "vehicle_count": len(device_ids),
            "avg_speed_mps": avg_speed,
            "avg_speed_kmh": mps_to_kmh(avg_speed),
            "overspeed_count": overs,
            "sample_snapshots": [s.to_dict() for s in snaps_in[:200]]
        })
    # global per-road aggregation
    roads = Road.query.all()
    result = []
    # perform in-python aggregation (simple but fine for moderate road counts)
    for r in roads:
        snaps = Snapshot.query.filter(Snapshot.ts >= cutoff).all()
        snaps_in = [s for s in snaps if (s.lat is not None and s.lon is not None and haversine_m(s.lat, s.lon, r.center_lat, r.center_lon) <= r.radius_m)]
        device_ids = set(s.device_id for s in snaps_in)
        speeds = [s.speed_mps for s in snaps_in if s.speed_mps is not None]
        avg_speed = (sum(speeds) / len(speeds)) if speeds else None
        overs = Overspeed.query.filter(Overspeed.road_id == r.id, Overspeed.ts >= cutoff).count()
        result.append({
            "road": r.to_dict(),
            "vehicle_count": len(device_ids),
            "avg_speed_kmh": mps_to_kmh(avg_speed),
            "overspeed_count": overs
        })
    return jsonify({"since_secs": since_secs, "roads": result})

# -----------------------
# Telemetry ingestion endpoint
# -----------------------
@app.route("/api/heartbeat", methods=["POST"])
def api_heartbeat():
    """
    Expected JSON payload (flexible):
    {
      "device_id": "abc",
      "lat": -1.29,
      "lon": 36.82,
      "speed_mps": 10.5,
      "heading": 180,
      "battery": 78,
      "ts": "2025-03-14T12:00:00Z",
      "raw": {...}    # optional
    }
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "json required"}), 400

    device_id = str(data.get("device_id") or data.get("id") or data.get("device") or uuid_str())
    lat = safe_float(data.get("lat") or data.get("latitude") or data.get("lat_deg"))
    lon = safe_float(data.get("lon") or data.get("longitude") or data.get("lon_deg"))
    speed_mps = safe_float(data.get("speed_mps") or data.get("speed") or None)
    heading = safe_float(data.get("heading") or data.get("hdg") or None)
    battery = safe_float(data.get("battery") or None)
    ts_raw = data.get("ts") or data.get("timestamp")
    raw = data.get("raw") or data

    # parse timestamp robustly
    ts = None
    if ts_raw:
        try:
            # handle 'Z'
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            try:
                ts = datetime.utcfromtimestamp(float(ts_raw))
            except Exception:
                ts = now_utc()
    else:
        ts = now_utc()

    # upsert device
    device = Device.query.get(device_id)
    if not device:
        device = Device(id=device_id, name=data.get("name"), plate=data.get("plate"), model=data.get("model"), meta=data.get("meta"))
        db.session.add(device)
        db.session.commit()

    # create snapshot
    snap = Snapshot(device_id=device_id, ts=ts, lat=lat, lon=lon, speed_mps=speed_mps, heading=heading, battery=battery, raw=raw)
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
        logger.exception("DB commit failed during heartbeat")
        return jsonify({"error": "db commit failed", "detail": str(e)}), 500

    # emit realtime
    snapshot_payload = {
        "device_id": device_id, "ts": ts.isoformat(), "lat": lat, "lon": lon,
        "speed_mps": speed_mps, "speed_kmh": mps_to_kmh(speed_mps),
        "heading": heading, "battery": battery
    }
    try:
        if socketio:
            socketio.emit("snapshot", snapshot_payload, broadcast=True)
        else:
            logger.debug("socketio unavailable; snapshot event not emitted")
    except Exception:
        logger.exception("socket emit snapshot failed")

    # overspeed check against registered roads
    try:
        if lat is not None and lon is not None and speed_mps is not None:
            roads = Road.query.all()
            speed_kmh = mps_to_kmh(speed_mps) or 0.0
            for r in roads:
                d = haversine_m(lat, lon, r.center_lat, r.center_lon)
                if d <= (r.radius_m or 50.0):
                    if speed_kmh > r.speed_limit_kmh:
                        # dedupe within OVERSPEED_DEDUPE_SECS for same device+road
                        cutoff = now_utc() - timedelta(seconds=OVERSPEED_DEDUPE_SECS)
                        last = Overspeed.query.filter_by(device_id=device_id, road_id=r.id).order_by(Overspeed.ts.desc()).first()
                        if not last or last.ts < cutoff:
                            ov = Overspeed(device_id=device_id, road_id=r.id, ts=ts, speed_mps=speed_mps, lat=lat, lon=lon, snapshot_id=snap.id)
                            db.session.add(ov)
                            db.session.commit()
                            logger.info("Overspeed recorded: device=%s road=%s speed_kmh=%.1f", device_id, r.id, speed_kmh)
                            if socketio:
                                socketio.emit("overspeed", ov.to_dict(), broadcast=True)
    except Exception:
        logger.exception("overspeed detection failed")

    # notify device update if realtime available
    try:
        if socketio:
            socketio.emit("device_update", device.to_dict(include_meta=True), broadcast=True)
    except Exception:
        logger.exception("socket emit device_update failed")

    return jsonify({"ok": True, "device_id": device_id, "ts": ts.isoformat()})

# -----------------------
# Optional: serve static UI (if you want later)
# -----------------------
@app.route("/unlimited")
def unlimited_placeholder():
    # No UI included by default. When index.html is added to the project root you can enable serving it here.
    base = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(base, "index.html")
    if os.path.exists(index_path):
        return send_from_directory(base, "index.html")
    return jsonify({"ok": True, "message": "No UI installed. Place index.html next to app.py to serve the dashboard at /unlimited."})

# -----------------------
# Root & helpful links
# -----------------------
@app.route("/")
def root():
    return jsonify({
        "service": "Beacon Cloud Backend",
        "endpoints": {
            "health": "/health",
            "devices": "/admin/devices",
            "roads": "/admin/roads",
            "overspeeds": "/admin/overspeeds",
            "heartbeat": "/api/heartbeat",
            "search": "/admin/search",
            "traffic": "/admin/traffic"
        }
    })

if __name__ == "__main__":
    try:
        with app.app_context():
            init_db()
    except Exception:
        app.logger.exception("Database initialization failed.")

    # start jam detector background thread
    try:
        t = threading.Thread(target=jam_detector_loop, daemon=True)
        t.start()
        app.logger.info("Jam detector thread started.")
    except Exception:
        app.logger.exception("Failed to start jam detector thread.")

    try:
        socketio.run(
            app,
            host="0.0.0.0",
            port=int(os.environ.get("PORT", 5000)),
            debug=os.environ.get("FLASK_DEBUG", "0") == "1"
        )
    except Exception:
        app.logger.exception("SocketIO server failed to start.")
