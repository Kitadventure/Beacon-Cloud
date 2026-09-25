from __future__ import annotations

import html
import json
import threading
import time
import uuid
from datetime import datetime, timedelta
from functools import wraps

from flask import jsonify, redirect, render_template_string, request, session, url_for

import app_legacy as legacy

app = legacy.app
db = legacy.db
Device = legacy.Device
Snapshot = legacy.Snapshot
Road = legacy.Road
TrafficZone = legacy.TrafficZone
BroadcastMessage = legacy.BroadcastMessage
BroadcastDelivery = legacy.BroadcastDelivery
PoliceUser = legacy.PoliceUser
GKUser = legacy.GKUser
Admin = legacy.Admin
SystemError = globals().get("SystemError")
LiveVehicleState = globals().get("LiveVehicleState")
AuthorityInboxMessage = globals().get("AuthorityInboxMessage")
OperationalIncident = globals().get("OperationalIncident")

# These globals are populated by install_modern_ui from app.py after all models/helpers exist.
_core = None
_status_cache = {"at": 0.0, "data": None}
_status_lock = threading.RLock()
_mobile_warning_last = {}
_mobile_warning_lock = threading.RLock()


MAROON = "#6f1025"
MAROON_DARK = "#4d0b1a"
BG = "#f4f6f9"
CARD = "#ffffff"
BORDER = "#e5e7eb"
TEXT = "#182230"
MUTED = "#667085"
GREEN = "#087443"
AMBER = "#9a6700"
RED = "#b42318"


SHELL = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light"><meta name="beacon-ui-version" content="2026.09.25-map2">
<title>{{ title }} · Beacon Cloud</title>
<style>
*{box-sizing:border-box}html,body{margin:0;padding:0;background:{{ bg }};color:{{ text }};font-family:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,Arial,sans-serif}a{color:inherit;text-decoration:none}
body{min-height:100vh}.topbar{height:66px;background:{{ maroon }};color:#fff;display:flex;align-items:center;padding:0 18px;position:sticky;top:0;z-index:50;box-shadow:0 2px 12px rgba(0,0,0,.14)}
.brand{font-weight:900;letter-spacing:.02em;display:flex;align-items:center;gap:10px}.brand-mark{width:34px;height:34px;border-radius:10px;background:rgba(255,255,255,.15);display:grid;place-items:center;font-size:16px}.top-actions{margin-left:auto;display:flex;gap:9px;align-items:center}.top-actions .btn{border-color:rgba(255,255,255,.2);background:rgba(255,255,255,.08);color:#fff}.layout{display:flex;min-height:calc(100vh - 66px)}
.sidebar{width:274px;background:#fff;border-right:1px solid {{ border }};padding:14px 10px;position:sticky;top:66px;height:calc(100vh - 66px);overflow:auto;flex:none}.side-toggle{width:100%;display:flex;align-items:center;justify-content:space-between;background:#fafafa;border:1px solid {{ border }};border-radius:11px;padding:10px 12px;font-weight:800;color:#344054;cursor:pointer;margin:7px 0}.group-items{display:block;padding:0 3px 5px}.group-items.collapsed{display:none}.nav-link{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:10px;color:#475467;font-weight:700;font-size:14px;margin:3px 0}.nav-link:hover{background:#f7f3f4}.nav-link.active{background:#f9e9ee;color:{{ maroon }};border-left:4px solid {{ maroon }};padding-left:8px}.nav-note{font-size:11px;color:#98a2b3;padding:5px 12px 10px}
.main{flex:1;min-width:0;padding:18px 20px 28px}.page-head{display:flex;gap:14px;align-items:flex-start;justify-content:space-between;margin-bottom:16px}.eyebrow{font-size:11px;text-transform:uppercase;letter-spacing:.14em;color:{{ maroon }};font-weight:900}.page-title{font-size:26px;line-height:1.15;margin:4px 0 5px}.page-sub{color:{{ muted }};margin:0;line-height:1.45}.grid{display:grid;gap:14px}.grid-4{grid-template-columns:repeat(4,minmax(0,1fr))}.grid-3{grid-template-columns:repeat(3,minmax(0,1fr))}.grid-2{grid-template-columns:repeat(2,minmax(0,1fr))}.card{background:{{ card }};border:1px solid {{ border }};border-radius:16px;padding:16px;box-shadow:0 4px 16px rgba(16,24,40,.035)}.card h2,.card h3{margin:0 0 10px}.metric{font-size:30px;font-weight:900;letter-spacing:-.02em}.metric-label{font-size:12px;color:{{ muted }};margin-top:3px}.tiny{font-size:12px;color:{{ muted }}}.badge{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:5px 9px;font-size:11px;font-weight:900}.badge-green{color:{{ green }};background:#ecfdf3}.badge-amber{color:{{ amber }};background:#fff7d6}.badge-red{color:{{ red }};background:#fef3f2}.badge-gray{color:#475467;background:#f2f4f7}.row{display:flex;gap:10px;align-items:center}.row-between{display:flex;gap:12px;align-items:center;justify-content:space-between}.stack{display:grid;gap:10px}.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;border:1px solid {{ border }};background:#fff;color:#344054;border-radius:10px;padding:9px 12px;font-weight:800;font-size:13px;cursor:pointer}.btn:hover{background:#fafafa}.btn-primary{background:{{ maroon }};border-color:{{ maroon }};color:#fff}.btn-danger{background:#fff4f3;border-color:#fecdca;color:{{ red }}}.btn-soft{background:#f8f2f4;border-color:#ead1d8;color:{{ maroon }}}
input,select,textarea{width:100%;border:1px solid #d0d5dd;background:#fff;border-radius:10px;padding:11px 12px;color:#101828;font:inherit;outline:none}input:focus,select:focus,textarea:focus{border-color:#b4234d;box-shadow:0 0 0 3px rgba(179,35,77,.10)}label{display:block;font-size:12px;font-weight:800;color:#344054;margin:2px 0 6px}.form-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.field-full{grid-column:1/-1}
.table-wrap{overflow:auto;border:1px solid {{ border }};border-radius:13px}.table{width:100%;border-collapse:collapse;min-width:760px}.table th,.table td{padding:11px 12px;border-bottom:1px solid {{ border }};text-align:left;vertical-align:top;font-size:13px}.table th{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:#667085;background:#fafafa;position:sticky;top:0}.table tr:last-child td{border-bottom:0}.empty{padding:28px;text-align:center;color:{{ muted }}}
.searchbox{position:relative}.searchbox input{padding-left:38px;font-size:16px;min-height:56px}.searchbox .icon{position:absolute;left:13px;top:13px;color:#98a2b3}.list{display:grid;gap:8px}.list-item{border:1px solid {{ border }};border-radius:12px;padding:11px 12px;background:#fff}.list-item:hover{border-color:#ceb7be}.small-muted{color:{{ muted }};font-size:12px}.hero{padding:20px;background:linear-gradient(135deg,#fff,#fbf2f5);border:1px solid #eed6dc;border-radius:17px}.footer{background:{{ maroon }};color:#fff;padding:18px 24px;text-align:center;font-size:12px;margin-top:24px}.footer strong{font-weight:900}.footer-sub{opacity:.85;margin-top:4px}
.notice{padding:11px 13px;border-radius:10px;margin-bottom:10px;border:1px solid #d0d5dd;background:#fff}.notice-danger{border-color:#fecdca;background:#fff6f5}.notice-ok{border-color:#b7ebcb;background:#f0fff6}.notice-warn{border-color:#f5d07b;background:#fffbeb}.code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;white-space:pre-wrap;background:#101828;color:#eaecf0;border-radius:10px;padding:12px;overflow:auto;max-height:340px}.split{display:grid;grid-template-columns:1.2fr .8fr;gap:14px}.pillbar{display:flex;flex-wrap:wrap;gap:8px}.pill{border:1px solid {{ border }};border-radius:999px;padding:7px 10px;background:#fff;font-size:12px;font-weight:800}.toolbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center}.toolbar .grow{flex:1;min-width:220px}
@media(max-width:1050px){.grid-4{grid-template-columns:repeat(2,minmax(0,1fr))}.grid-3{grid-template-columns:1fr 1fr}.split{grid-template-columns:1fr}}@media(max-width:780px){.sidebar{position:fixed;left:-286px;top:66px;transition:.2s;z-index:60;box-shadow:8px 0 28px rgba(0,0,0,.12)}.sidebar.open{left:0}.main{padding:15px 12px}.grid-2,.grid-3,.grid-4,.form-grid{grid-template-columns:1fr}.page-head{flex-direction:column}.mobile-menu{display:inline-flex!important}}
.mobile-menu{display:none}.hidden{display:none!important}
</style>
</head>
<body>
<header class="topbar"><button class="btn mobile-menu" onclick="document.getElementById('sidebar').classList.toggle('open')">☰</button><div class="brand"><div class="brand-mark">BC</div><div>Beacon Cloud</div></div><div class="top-actions"><span class="tiny" style="color:#f5dce3">{{ role|upper }}</span><a class="btn" href="{{ logout_url }}">Sign out</a></div></header>
<div class="layout">
<aside class="sidebar" id="sidebar">{{ nav|safe }}</aside>
<main class="main">{{ body|safe }}</main>
</div>
<footer class="footer"><strong>Toror Technology and Innovation’s Limited Company</strong><div class="footer-sub">All rights reserved.</div></footer>
<script>
(function(){
 const key='beacon-sidebar-groups';
 function setGroup(btn,open){const target=document.getElementById(btn.dataset.target);if(!target)return;target.classList.toggle('collapsed',!open);btn.querySelector('.sign').textContent=open?'−':'+';}
 document.querySelectorAll('.side-toggle').forEach(btn=>{btn.addEventListener('click',()=>{const target=document.getElementById(btn.dataset.target);const open=target.classList.contains('collapsed');setGroup(btn,open);localStorage.setItem(key+'-'+btn.dataset.target,open?'1':'0');});const open=localStorage.getItem(key+'-'+btn.dataset.target);if(open==='0')setGroup(btn,false);else setGroup(btn,true);});
 window.beaconReportError=function(error,context){try{fetch('{{ client_error_url }}',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:String(error&&error.message||error),stack:String(error&&error.stack||''),context:context||'authority-ui'})})}catch(e){}};
 window.addEventListener('error',function(e){beaconReportError(e.error||e.message,'window.error')});
 window.addEventListener('unhandledrejection',function(e){beaconReportError(e.reason,'unhandledrejection')});
})();
</script>
{{ extra_script|safe }}
</body></html>
"""


def _role():
    return legacy._current_role()


def _username():
    return session.get("username") or session.get("admin_user") or ""


def _require_roles(*roles):
    role = _role()
    if role not in set(roles):
        return None, redirect(url_for("admin_login", next=request.path))
    return role, None


def _esc(v):
    return html.escape("" if v is None else str(v), quote=True)


def _sidebar(role: str, active: str):
    groups = [
        ("Overview", [("dashboard", "Dashboard", "▦"), ("modern_vehicle_map", "Live vehicle map", "⌖")]),
        ("Operations", [
            ("modern_devices_page", "Vehicles & devices", "◉"),
            ("modern_messages_page", "Messages", "✉"),
            ("modern_inbox_page", "Authority inbox", "↩"),
            ("modern_incidents_page", "Incidents & alerts", "⚠"),
        ]),
        ("Intelligence", [
            ("modern_users_page", "Users & authority", "♙"),
            ("modern_traffic_hub", "Traffic operations", "⌁"),
        ]),
        ("Reports & data", [
            ("authority_reports", "Reports & downloads", "▤"),
            ("admin_backup_restore_page", "Backups & restore", "↻"),
        ]),
    ]
    if role == "admin":
        groups.append(("System", [("admin_errors", "System errors", "!"), ("modern_settings_page", "System health", "⚙")]))
    elif role in {"police", "gk"}:
        groups.append(("System", [("modern_settings_page", "System health", "⚙")]))

    chunks = []
    for idx, (name, links) in enumerate(groups):
        gid = f"nav-group-{idx}"
        chunks.append(f'<button class="side-toggle" data-target="{gid}"><span>{_esc(name)}</span><span class="sign">−</span></button>')
        chunks.append(f'<div class="group-items" id="{gid}">')
        for endpoint, label, icon in links:
            try:
                href = url_for(endpoint)
            except Exception:
                continue
            is_active = active == endpoint
            chunks.append(f'<a class="nav-link{" active" if is_active else ""}" href="{_esc(href)}"><span style="width:20px">{_esc(icon)}</span><span>{_esc(label)}</span></a>')
        chunks.append('</div>')
    chunks.append('<div class="nav-note">Live telemetry and authority tools</div>')
    return "".join(chunks)


def _render(title, body, active, extra_script="", role=None):
    role = role or _role() or "admin"
    return render_template_string(SHELL, title=title, bg=BG, text=TEXT, border=BORDER, maroon=MAROON, card=CARD, muted=MUTED,
                                  green=GREEN, amber=AMBER, red=RED, role=role, nav=_sidebar(role, active), body=body,
                                  extra_script=extra_script, logout_url=url_for("admin_logout"), client_error_url=url_for("authority_client_error"))


def _safe_q(q, default=""):
    return (q or default).strip()


def _latest_live_by_ids(ids):
    if not ids or not LiveVehicleState:
        return {}
    try:
        rows = LiveVehicleState.query.filter(LiveVehicleState.device_id.in_(list(ids))).all()
        return {r.device_id: r for r in rows}
    except Exception as exc:
        if _core:
            _core.record_system_error(exc, source="ui:live-state", severity="WARNING")
        return {}


def _state_for_device(did, ts):
    if _core:
        return _core._device_connection_state(did, ts)
    return False, bool(ts and (datetime.utcnow() - ts).total_seconds() < 30), "TELEMETRY ONLINE"


def _device_search_query(q):
    query = Device.query.filter_by(revoked=False)
    if q:
        like = f"%{q}%"
        query = query.filter(db.or_(
            Device.id.ilike(like), Device.owner.ilike(like), Device.car_name.ilike(like), Device.car_model.ilike(like),
            Device.plate.ilike(like), Device.phone_number.ilike(like), Device.extra.ilike(like)
        ))
    return query


def _device_rows(q="", limit=100, offset=0):
    q = q.strip()
    query = _device_search_query(q).order_by(Device.created_at.desc())
    total = query.count()
    rows = query.offset(max(0, offset)).limit(max(1, min(limit, 200))).all()
    states = _latest_live_by_ids([d.id for d in rows])
    out = []
    for d in rows:
        state = states.get(d.id)
        last_ts = state.ts if state else None
        socket_online, telemetry_online, state_name = _state_for_device(d.id, last_ts)
        speed = (float(state.speed_mps or 0) * 3.6) if state else None
        out.append({
            "id": d.id, "owner": d.owner or "", "car_name": d.car_name or "", "car_model": d.car_model or "",
            "plate": d.plate or "", "phone_number": legacy._device_phone_number(d) or d.phone_number or "",
            "created_at": d.created_at.isoformat() if d.created_at else None, "speed_kmh": round(speed, 1) if speed is not None else None,
            "lat": state.lat if state else None, "lon": state.lon if state else None,
            "ts": state.ts.isoformat() if state and state.ts else None,
            "socket_online": socket_online, "telemetry_online": telemetry_online, "connection_state": state_name,
            "place_name": state.place_name if state else None,
        })
    return out, total


def _status_data():
    now = time.time()
    with _status_lock:
        if _status_cache["data"] and now - _status_cache["at"] < 5:
            return _status_cache["data"]
    cutoff = datetime.utcnow() - timedelta(seconds=max(30, getattr(_core, "LIVE_CACHE_TTL_S", 15) * 2 if _core else 30))
    data = {"registered": 0, "telemetry_online": 0, "realtime": 0, "active_incidents": 0, "open_errors": 0, "unread_inbox": 0, "recent": []}
    try:
        data["registered"] = Device.query.filter_by(revoked=False).count()
        if LiveVehicleState:
            data["telemetry_online"] = LiveVehicleState.query.filter(LiveVehicleState.updated_at >= cutoff).count()
        if OperationalIncident:
            data["active_incidents"] = OperationalIncident.query.filter_by(status="active").count()
        if SystemError:
            data["open_errors"] = SystemError.query.filter_by(resolved=False).count()
        if AuthorityInboxMessage:
            data["unread_inbox"] = AuthorityInboxMessage.query.filter_by(status="unread").count()
        with getattr(_core, "_connected_lock", threading.RLock()):
            data["realtime"] = len(getattr(_core, "connected_sockets", {})) if _core else 0
        rec, _ = _device_rows("", 8, 0)
        data["recent"] = rec
    except Exception as exc:
        if _core:
            _core.record_system_error(exc, source="ui:status", severity="WARNING")
    with _status_lock:
        _status_cache.update({"at": now, "data": data})
    return data


DASHBOARD_BODY = """
<div class="page-head"><div><div class="eyebrow">Authority control</div><h1 class="page-title">Operational dashboard</h1><p class="page-sub">A fast overview of registered vehicles, live communications, incidents and system health.</p></div><div class="toolbar"><a class="btn btn-primary" href="__MSG__">Send message</a><a class="btn" href="__REP__">Reports & downloads</a></div></div>
<div class="grid grid-4" id="metrics"></div>
<div class="grid grid-2" style="margin-top:14px">
<div class="card"><div class="row-between"><h2>Recent vehicle activity</h2><a class="btn" href="__DEV__">Open all</a></div><div id="recent" class="list"><div class="small-muted">Loading…</div></div></div>
<div class="card"><div class="row-between"><h2>System health</h2><a class="btn" href="__HEALTH__">Details</a></div><div class="stack" id="health"></div></div>
</div>
<div class="hero" style="margin-top:14px"><div class="row-between"><div><h2 style="margin:0 0 4px">Live vehicle map</h2><p class="page-sub">Open the dedicated map when geographic visibility is needed; the dashboard stays fast.</p></div><a class="btn btn-soft" href="__MAP__">Open live map</a></div></div>
"""


def _dashboard_script():
    js = """
<script>
const urls={status:'__STATUS__'};
function esc(v){return String(v??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));}
function badge(state){if(state==='REAL-TIME')return '<span class="badge badge-green">● REAL-TIME</span>';if(state==='TELEMETRY ONLINE')return '<span class="badge badge-amber">● TELEMETRY ONLINE</span>';return '<span class="badge badge-gray">● OFFLINE</span>';}
async function refresh(){try{const r=await fetch(urls.status,{cache:'no-store'});const d=await r.json();document.getElementById('metrics').innerHTML=[['Registered vehicles',d.registered,''],['Telemetry online',d.telemetry_online,''],['Active incidents',d.active_incidents,''],['Open system errors',d.open_errors,'']].map(x=>`<div class="card"><div class="metric">${esc(x[1])}</div><div class="metric-label">${x[0]}</div></div>`).join('');document.getElementById('recent').innerHTML=(d.recent||[]).map(v=>`<div class="list-item"><div class="row-between"><strong>${esc(v.owner||v.id)}</strong>${badge(v.connection_state)}</div><div class="small-muted">${esc(v.car_name||'Vehicle')} · ${esc(v.plate||'No plate')} · ${esc(v.phone_number||'No phone captured')}</div><div class="small-muted">${esc(v.place_name||'Place locating…')}</div></div>`).join('')||'<div class="small-muted">No registered vehicles yet.</div>';document.getElementById('health').innerHTML=`<div class="row-between"><span>Real-time sockets</span><strong>${esc(d.realtime)}</strong></div><div class="row-between"><span>Unread authority inbox</span><strong>${esc(d.unread_inbox)}</strong></div><div class="row-between"><span>Service</span><span class="badge badge-green">● ONLINE</span></div>`;}catch(e){beaconReportError(e,'dashboard.refresh');document.getElementById('health').innerHTML='<div class="notice notice-danger">Health data could not be loaded.</div>';}}
refresh();setInterval(refresh,15000);
</script>
"""
    return js.replace("__STATUS__", url_for("modern_status_data"))


def modern_dashboard():
    role, resp = _require_roles("admin", "police", "gk")
    if resp:
        return resp
    body = DASHBOARD_BODY.replace("__MSG__", url_for("admin_messages")).replace("__REP__", url_for("authority_reports"))\
        .replace("__DEV__", url_for("admin_devices")).replace("__HEALTH__", url_for("modern_settings_page"))\
        .replace("__MAP__", url_for("modern_vehicle_map"))
    return _render("Dashboard", body, "dashboard", _dashboard_script(), role)


def modern_status_data():
    role, resp = _require_roles("admin", "police", "gk")
    if resp: return resp
    return jsonify(_status_data())


def modern_devices_page():
    role, resp = _require_roles("admin", "police", "gk")
    if resp: return resp
    body = """
<div class="page-head"><div><div class="eyebrow">Operations</div><h1 class="page-title">Vehicles & devices</h1><p class="page-sub">Registration, phone numbers, live telemetry and connection state without loading historical snapshots.</p></div><a class="btn btn-primary" href="__MSG__">Message a vehicle</a></div>
<div class="card"><div class="toolbar"><div class="grow searchbox"><span class="icon">⌕</span><input id="q" placeholder="Search owner, phone, plate, vehicle or device ID…" autocomplete="off"></div><button class="btn" onclick="loadDevices()">Refresh</button></div><div class="small-muted" id="count" style="margin:10px 0">Loading…</div><div class="table-wrap"><table class="table"><thead><tr><th>Owner</th><th>Phone</th><th>Vehicle</th><th>Plate</th><th>State</th><th>Speed</th><th>Last telemetry</th><th></th></tr></thead><tbody id="rows"></tbody></table></div></div>
""".replace("__MSG__", url_for("admin_messages"))
    js = """
<script>
const deviceUrl='__URL__', detailUrl='__DETAIL__';let t;
function esc(v){return String(v??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));}
function state(s){return s==='REAL-TIME'?'<span class="badge badge-green">REAL-TIME</span>':s==='TELEMETRY ONLINE'?'<span class="badge badge-amber">TELEMETRY ONLINE</span>':'<span class="badge badge-gray">OFFLINE</span>';}
async function loadDevices(){const q=document.getElementById('q').value.trim();const r=await fetch(deviceUrl+'?q='+encodeURIComponent(q)+'&limit=120',{cache:'no-store'});const d=await r.json();document.getElementById('count').textContent=(d.total||0)+' registered vehicle(s)';document.getElementById('rows').innerHTML=(d.items||[]).map(v=>`<tr><td><strong>${esc(v.owner||'—')}</strong><div class="small-muted">${esc(v.id)}</div></td><td>${esc(v.phone_number||'Not captured')}</td><td>${esc([v.car_name,v.car_model].filter(Boolean).join(' ')||'—')}</td><td>${esc(v.plate||'—')}</td><td>${state(v.connection_state)}</td><td>${v.speed_kmh==null?'—':esc(v.speed_kmh)+' km/h'}</td><td>${esc(v.ts||'—')}</td><td><a class="btn" href="${detailUrl.replace('__ID__',encodeURIComponent(v.id))}">Details</a></td></tr>`).join('')||'<tr><td colspan="8"><div class="empty">No matching vehicles.</div></td></tr>';}
document.getElementById('q').addEventListener('input',()=>{clearTimeout(t);t=setTimeout(loadDevices,250)});loadDevices();
</script>
""".replace("__URL__", url_for("modern_device_data")).replace("__DETAIL__", url_for("modern_device_detail", device_id="__ID__"))
    return _render("Vehicles & devices", body, "modern_devices_page", js, role)


def modern_device_data():
    role, resp = _require_roles("admin", "police", "gk")
    if resp: return resp
    q = _safe_q(request.args.get("q"))
    try:
        items, total = _device_rows(q, int(request.args.get("limit", 100)), int(request.args.get("offset", 0)))
        return jsonify({"ok": True, "items": items, "total": total})
    except Exception as exc:
        if _core: _core.record_system_error(exc, source="ui:devices", severity="ERROR")
        return jsonify({"ok": False, "items": [], "total": 0, "error": "Vehicle data is temporarily unavailable.", "request_id": _core._request_id() if _core else str(uuid.uuid4())}), 200


def modern_device_detail(device_id):
    role, resp = _require_roles("admin", "police", "gk")
    if resp: return resp
    d = Device.query.filter_by(id=device_id).first()
    if not d:
        return _render("Vehicle not found", '<div class="card"><h2>Vehicle not found</h2></div>', "modern_devices_page", role=role)
    state = LiveVehicleState.query.filter_by(device_id=d.id).first() if LiveVehicleState else None
    snaps = Snapshot.query.filter_by(device_id=d.id).order_by(Snapshot.ts.desc()).limit(20).all()
    _, tel, state_name = _state_for_device(d.id, state.ts if state else (snaps[0].ts if snaps else None))
    recent = []
    for s in snaps:
        recent.append({"ts":s.ts.isoformat() if s.ts else None,"lat":s.lat,"lon":s.lon,"speed_kmh":round(float(s.speed_mps or 0)*3.6,1),"source":s.source or "","bearing_deg":s.bearing_deg,"place_name":state.place_name if state else None})
    rows = ''.join(f'<tr><td>{_esc(x["ts"])}</td><td>{_esc(x.get("place_name") or "Place pending")}</td><td>{_esc(x["speed_kmh"])} km/h</td><td>{_esc(x["source"])}</td><td><details><summary>Open</summary><div class="small-muted">Latitude: {_esc(x["lat"])} · Longitude: {_esc(x["lon"])} · Bearing: {_esc(x.get("bearing_deg"))}</div></details></td></tr>' for x in recent) or '<tr><td colspan="5"><div class="empty">No snapshots.</div></td></tr>'
    body = f"""
<div class="page-head"><div><div class="eyebrow">Vehicle detail</div><h1 class="page-title">{_esc(d.owner or d.car_name or d.id)}</h1><p class="page-sub">Device ID: {_esc(d.id)}</p></div><div class="pillbar"><span class="pill">{_esc(state_name)}</span><span class="pill">Telemetry {'online' if tel else 'offline'}</span></div></div>
<div class="grid grid-4"><div class="card"><div class="metric-label">Owner</div><strong>{_esc(d.owner or '—')}</strong></div><div class="card"><div class="metric-label">Phone</div><strong>{_esc(legacy._device_phone_number(d) or d.phone_number or 'Not captured')}</strong></div><div class="card"><div class="metric-label">Vehicle / plate</div><strong>{_esc(' '.join([d.car_name or '', d.car_model or '']).strip() or '—')} · {_esc(d.plate or '—')}</strong></div><div class="card"><div class="metric-label">Current place</div><strong>{_esc(state.place_name if state else 'Place locating…')}</strong></div></div>
<div class="card" style="margin-top:14px"><div class="row-between"><h2>Recent telemetry</h2><a class="btn btn-primary" href="{url_for('admin_messages')}?device_id={_esc(d.id)}">Send message</a></div><div class="table-wrap"><table class="table"><thead><tr><th>Time</th><th>Place</th><th>Speed</th><th>Source</th><th>Technical details</th></tr></thead><tbody>{rows}</tbody></table></div></div>
"""
    return _render("Vehicle detail", body, "modern_devices_page", role=role)


def _fast_target_devices(body):
    target_type = (body.get("target_type") or "single").strip().lower()
    if target_type == "single":
        did = (body.get("target_device_id") or body.get("target_value") or "").strip()
        d = Device.query.filter_by(id=did, revoked=False).first() if did else None
        return [d] if d else []
    if target_type == "all":
        return Device.query.filter_by(revoked=False).all()
    if target_type == "search":
        q = (body.get("query") or body.get("target_value") or "").strip()
        return _device_search_query(q).limit(500).all()
    if target_type == "county":
        q = (body.get("county") or body.get("target_value") or "").strip()
        if not q: return []
        like = f"%{q}%"
        return Device.query.filter_by(revoked=False).filter(db.or_(Device.owner.ilike(like), Device.car_name.ilike(like), Device.car_model.ilike(like), Device.plate.ilike(like), Device.phone_number.ilike(like), Device.extra.ilike(like))).limit(500).all()
    if target_type == "overspeeders":
        threshold = float(body.get("min_kmh") or 80)
        cutoff = datetime.utcnow() - timedelta(seconds=90)
        if LiveVehicleState:
            states = LiveVehicleState.query.filter(LiveVehicleState.updated_at >= cutoff, LiveVehicleState.speed_mps >= threshold/3.6).all()
            ids = [s.device_id for s in states]
            return Device.query.filter(Device.id.in_(ids), Device.revoked.is_(False)).all() if ids else []
        return []
    # For road/zone, only evaluate live states; never scan historical snapshots.
    live_states = LiveVehicleState.query.filter(LiveVehicleState.updated_at >= datetime.utcnow()-timedelta(seconds=90)).all() if LiveVehicleState else []
    ids = []
    if target_type == "road":
        road_id = (body.get("road_id") or body.get("target_value") or "").strip()
        road = Road.query.get(road_id) if road_id else None
        if not road or road.center_lat is None or road.center_lon is None: return []
        radius = float(road.radius_m or 500)
        for st in live_states:
            try:
                if legacy.haversine_m(st.lat, st.lon, road.center_lat, road.center_lon) <= radius: ids.append(st.device_id)
            except Exception: pass
    elif target_type == "zone":
        zone_id = (body.get("zone_id") or body.get("target_value") or "").strip()
        zone = TrafficZone.query.get(zone_id) if zone_id else None
        if not zone: return []
        for st in live_states:
            try:
                if legacy._zone_matches(zone, st.lat, st.lon): ids.append(st.device_id)
            except Exception: pass
    else:
        return []
    return Device.query.filter(Device.id.in_(ids), Device.revoked.is_(False)).all() if ids else []


def _queue_device_alert(device_id, kind, severity, title, body, incident_id=None, unique_key=None):
    if not DeviceAlert:
        return None
    if unique_key:
        existing = DeviceAlert.query.filter_by(device_id=device_id, unique_key=unique_key).first()
        if existing:
            return existing
    row = DeviceAlert(device_id=device_id, kind=kind, severity=severity, title=title, body=body, incident_id=incident_id, unique_key=unique_key)
    db.session.add(row)
    return row


def _notify_device_and_queue(device_id, event, payload, *, kind, severity, title, body, incident_id=None, unique_key=None):
    ws_sent = False
    try:
        if _core:
            ws_sent = bool(_core._send_ws(device_id, event, payload))
    except Exception as exc:
        if _core: _core.record_system_error(exc, source="socket:device-notify", severity="WARNING")
    try:
        row = _queue_device_alert(device_id, kind, severity, title, body, incident_id, unique_key)
        if row is not None and ws_sent:
            row.delivered_at = datetime.utcnow()
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        if _core: _core.record_system_error(exc, source="database:device-alert", severity="WARNING")
    return ws_sent


def modern_message_users():
    role, resp = _require_roles("admin", "gk")
    if resp: return resp
    q = _safe_q(request.args.get("q"))
    try:
        items, total = _device_rows(q, int(request.args.get("limit", 100)), int(request.args.get("offset", 0)))
        return jsonify({"ok": True, "items": items, "total": total})
    except Exception as exc:
        if _core: _core.record_system_error(exc, source="ui:message-users", severity="ERROR")
        return jsonify({"ok": False, "items": [], "total": 0, "error": "User directory unavailable"}), 200


def modern_messages_page():
    role, resp = _require_roles("admin", "gk")
    if resp: return resp
    selected = _safe_q(request.args.get("device_id"))
    try:
        recent = BroadcastMessage.query.order_by(BroadcastMessage.created_at.desc()).limit(20).all()
    except Exception as exc:
        recent = []
        if _core: _core.record_system_error(exc, source="ui:messages-page", severity="ERROR")
    recent_html = ''.join(f'<div class="list-item"><div class="row-between"><strong>{_esc(m.title)}</strong><span class="badge badge-gray">{_esc(m.target_type)}</span></div><div class="small-muted">{_esc(m.created_at.isoformat() if m.created_at else "")} · {_esc(m.recipient_count or 0)} recipient(s)</div><div style="margin-top:5px">{_esc(m.body)}</div></div>' for m in recent) or '<div class="small-muted">No messages yet.</div>'
    body = f"""
<div class="page-head"><div><div class="eyebrow">Communications</div><h1 class="page-title">Messages</h1><p class="page-sub">Search every registered user, see the phone number captured at enrollment, then send a persistent message with live delivery fallback.</p></div></div>
<div class="split"><div class="card" style="min-width:0"><h2 style="font-size:20px">Find a recipient</h2><div class="searchbox"><span class="icon">⌕</span><input id="userq" value="{_esc(request.args.get('q') or '')}" placeholder="Search owner, phone, plate, vehicle or device ID…"></div><div id="results" class="list" style="margin-top:10px;max-height:500px;overflow:auto"></div></div>
<div class="card"><h2>Compose message</h2><form id="sendForm" class="stack"><div><label>Title</label><input name="title" required maxlength="255" placeholder="Road safety notice"></div><div><label>Message</label><textarea name="body" rows="8" required maxlength="5000" placeholder="Write the message…"></textarea></div><div><label>Audience</label><select name="target_type" id="targetType"><option value="single">Selected vehicle</option><option value="all">All registered vehicles</option><option value="overspeeders">Vehicles currently above speed threshold</option></select></div><div id="thresholdWrap" class="hidden"><label>Minimum speed (km/h)</label><input name="min_kmh" value="80"></div><input type="hidden" name="target_device_id" id="targetDevice" value="{_esc(selected)}"><div class="notice notice-warn" id="selectedInfo">{('Selected device: '+_esc(selected)) if selected else 'Select a user or choose a broader audience.'}</div><button class="btn btn-primary" type="submit">Send to phone</button><div id="sendResult"></div></form></div></div>
<div class="card" style="margin-top:14px"><div class="row-between"><h2>Recent authority messages</h2><a class="btn" href="{url_for('authority_reports')}">Reports</a></div><div class="list">{recent_html}</div></div>
"""
    js = """
<script>
const usersUrl='__USERS__',sendUrl='__SEND__';let timer;
function esc(v){return String(v??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));}
function choose(v){document.getElementById('targetType').value='single';document.getElementById('targetDevice').value=v.id;document.getElementById('selectedInfo').textContent=`Selected: ${v.owner||v.id} · ${v.phone_number||'No phone captured'} · ${v.plate||'No plate'}`;toggleAudience();}
function toggleAudience(){const t=document.getElementById('targetType').value;document.getElementById('targetDevice').disabled=(t!=='single');document.getElementById('thresholdWrap').classList.toggle('hidden',t!=='overspeeders');if(t==='all')document.getElementById('selectedInfo').textContent='Audience: all registered vehicles.';else if(t==='overspeeders')document.getElementById('selectedInfo').textContent='Audience: vehicles currently above the selected speed threshold.';else if(!document.getElementById('targetDevice').value)document.getElementById('selectedInfo').textContent='Select a user from the search results.';}
async function users(){const q=document.getElementById('userq').value.trim();const r=await fetch(usersUrl+'?q='+encodeURIComponent(q)+'&limit=120',{cache:'no-store'});const d=await r.json();document.getElementById('results').innerHTML=(d.items||[]).map(v=>`<button type="button" class="list-item" style="text-align:left;width:100%;cursor:pointer" onclick='choose(${JSON.stringify(v).replace(/'/g,"&#39;")})'><div class="row-between"><strong>${esc(v.owner||v.id)}</strong>${v.connection_state==='REAL-TIME'?'<span class="badge badge-green">REAL-TIME</span>':v.connection_state==='TELEMETRY ONLINE'?'<span class="badge badge-amber">TELEMETRY ONLINE</span>':'<span class="badge badge-gray">OFFLINE</span>'}</div><div class="small-muted">Phone: ${esc(v.phone_number||'Not captured')} · Plate: ${esc(v.plate||'—')}</div><div class="small-muted">${esc([v.car_name,v.car_model].filter(Boolean).join(' ')||'Vehicle')} · ${esc(v.id)}</div><div class="small-muted">${esc(v.place_name||'Place locating…')}</div></button>`).join('')||'<div class="empty">No registered users match that search.</div>';}
document.getElementById('userq').addEventListener('input',()=>{clearTimeout(timer);timer=setTimeout(users,250)});document.getElementById('targetType').addEventListener('change',toggleAudience);toggleAudience();users();
document.getElementById('sendForm').addEventListener('submit',async e=>{e.preventDefault();const d=Object.fromEntries(new FormData(e.target).entries());if(!d.target_device_id){document.getElementById('sendResult').innerHTML='<div class="notice notice-danger">Select a recipient first.</div>';return;}const r=await fetch(sendUrl,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});const x=await r.json();document.getElementById('sendResult').innerHTML=x.ok?`<div class="notice notice-ok">Message stored and queued for delivery to ${x.message.recipient_count||0} device(s).</div>`:`<div class="notice notice-danger">${esc(x.error||'Message failed')}</div>`;if(x.ok)e.target.reset();});
</script>
""".replace("__USERS__", url_for("modern_message_users")).replace("__SEND__", url_for("modern_message_send"))
    return _render("Messages", body, "modern_messages_page", js, role)


def modern_message_send():
    role, resp = _require_roles("admin", "gk")
    if resp: return resp
    body = request.get_json(silent=True) if request.is_json else request.form.to_dict(flat=True)
    body = body or {}
    try:
        recipients = _fast_target_devices(body)
        if not recipients:
            return jsonify({"ok": False, "error": "No registered recipient matched the selection."}), 400
        # Use the legacy model/serializer but with our optimized recipient set.
        title = (body.get("title") or "").strip()[:255]
        text = (body.get("body") or body.get("message") or "").strip()[:5000]
        if not title or not text:
            return jsonify({"ok": False, "error": "Title and message are required."}), 400
        msg = BroadcastMessage(title=title, body=text, target_type=(body.get("target_type") or "single").strip().lower(),
                               target_value=(body.get("target_device_id") or body.get("target_value") or "").strip(),
                               creator_role=role, creator_username=_username(), recipient_count=len(recipients))
        db.session.add(msg); db.session.flush()
        for d in recipients:
            db.session.add(BroadcastDelivery(message_id=msg.id, device_id=d.id))
        db.session.commit()
        payload = legacy._serialize_message(msg, recipient_count=len(recipients))
        payload["device_ids"] = [d.id for d in recipients]; payload["app_action"] = "popup"
        for d in recipients:
            try: _core._send_ws(d.id, "admin_message", payload)
            except Exception: pass
            try:
                _queue_device_alert(d.id, "authority_message", "info", title, text, unique_key=f"message:{msg.id}")
                db.session.commit()
            except Exception:
                db.session.rollback()
        return jsonify({"ok": True, "message": payload})
    except Exception as exc:
        db.session.rollback()
        if _core: _core.record_system_error(exc, source="messages:send", severity="ERROR")
        return jsonify({"ok": False, "error": "Message could not be stored.", "request_id": _core._request_id() if _core else str(uuid.uuid4())}), 500


def modern_inbox_page():
    role, resp = _require_roles("admin", "police", "gk")
    if resp: return resp
    q = _safe_q(request.args.get("q"))
    try:
        rows = AuthorityInboxMessage.query.order_by(AuthorityInboxMessage.created_at.desc()).limit(200).all() if AuthorityInboxMessage else []
    except Exception as exc:
        rows=[]
        if _core: _core.record_system_error(exc, source="ui:authority-inbox", severity="ERROR")
    data=[]
    for r in rows:
        blob=' '.join([r.owner or '',r.plate or '',r.phone_number or '',r.title or '',r.body or '']).lower()
        if q.lower() in blob: data.append(r)
    cards=''.join(f'<div class="list-item"><div class="row-between"><div><strong>{_esc(r.title)}</strong><div class="small-muted">{_esc(r.owner or "Unknown")} · {_esc(r.phone_number or "No phone")} · {_esc(r.plate or "No plate")}</div></div><span class="badge {"badge-amber" if r.status=="unread" else "badge-gray"}">{_esc(r.status or "unread")}</span></div><div style="margin-top:8px">{_esc(r.body)}</div><div class="small-muted" style="margin-top:7px">{_esc(r.created_at.isoformat() if r.created_at else "")}</div></div>' for r in data) or '<div class="empty">No driver messages.</div>'
    body=f'''<div class="page-head"><div><div class="eyebrow">Bidirectional communication</div><h1 class="page-title">Authority inbox</h1><p class="page-sub">Messages originating from enrolled phones, with owner, phone and vehicle context.</p></div></div><div class="card"><div class="searchbox"><span class="icon">⌕</span><input id="inboxQ" value="{_esc(q)}" placeholder="Search sender, phone, plate or message…"></div></div><div class="card" style="margin-top:14px"><div class="list" id="inboxList">{cards}</div></div>'''
    js="""<script>document.getElementById('inboxQ').addEventListener('input',e=>{clearTimeout(window._t);window._t=setTimeout(()=>{location.href='__URL__?q='+encodeURIComponent(e.target.value)},300)});</script>""".replace("__URL__",url_for("modern_inbox_page"))
    return _render("Authority inbox",body,"modern_inbox_page",js,role)


def modern_incidents_page():
    role, resp = _require_roles("admin", "police", "gk")
    if resp: return resp
    try:
        rows = OperationalIncident.query.order_by(OperationalIncident.created_at.desc()).limit(120).all() if OperationalIncident else []
    except Exception as exc:
        rows=[]
        if _core: _core.record_system_error(exc, source="ui:incidents", severity="ERROR")
    trs=''.join(f'<tr><td><strong>{_esc(r.type)}</strong><div class="small-muted">{_esc(r.id)}</div></td><td>{_esc(r.severity)}</td><td>{_esc(round(float(r.confidence or 0),2))}</td><td>{_esc(r.place_name or "Place pending")}</td><td>{_esc(r.device_id or "—")}</td><td>{_esc(r.status)}</td><td>{_esc(r.created_at.isoformat() if r.created_at else "")}</td></tr>' for r in rows) or '<tr><td colspan="6"><div class="empty">No incidents recorded.</div></td></tr>'
    body=f'''<div class="page-head"><div><div class="eyebrow">Safety operations</div><h1 class="page-title">Incidents & alerts</h1><p class="page-sub">Persistent operational incidents with status and confidence rather than transient popups only.</p></div></div><div class="card"><div class="table-wrap"><table class="table"><thead><tr><th>Type</th><th>Severity</th><th>Confidence</th><th>Place</th><th>Device</th><th>Status</th><th>Created</th></tr></thead><tbody>{trs}</tbody></table></div></div>'''
    return _render("Incidents",body,"modern_incidents_page",role=role)


def modern_users_page():
    role, resp = _require_roles("admin")
    if resp: return resp
    try:
        admins = Admin.query.order_by(Admin.username.asc()).all() if Admin else []
        police = PoliceUser.query.order_by(PoliceUser.username.asc()).all() if PoliceUser else []
        gks = GKUser.query.order_by(GKUser.username.asc()).all() if GKUser else []
    except Exception as exc:
        admins=[]; police=[]; gks=[]
        if _core: _core.record_system_error(exc, source="ui:authority-users", severity="ERROR")
    trs=''.join(f'<tr><td>{_esc(a.username)}</td><td><span class="badge badge-gray">ADMIN</span></td><td>Protected</td></tr>' for a in admins)
    trs += ''.join(f'<tr><td>{_esc(a.username)}</td><td><span class="badge badge-gray">POLICE</span></td><td>Authority</td></tr>' for a in police)
    trs += ''.join(f'<tr><td>{_esc(a.username)}</td><td><span class="badge badge-gray">GK</span></td><td>Authority</td></tr>' for a in gks)
    body=f'''<div class="page-head"><div><div class="eyebrow">Administration</div><h1 class="page-title">Users & authority</h1><p class="page-sub">Authority accounts are managed by the chief administrator. Public registration remains disabled.</p></div></div><div class="grid grid-2"><div class="card"><h2>Create authority account</h2><form method="post" action="{url_for('modern_create_authority')}" class="stack"><div><label>Username</label><input name="username" required></div><div><label>Temporary password</label><input name="password" type="password" required></div><div><label>Role</label><select name="role"><option value="police">Police</option><option value="gk">GK / Command</option><option value="admin">Administrator</option></select></div><button class="btn btn-primary" type="submit">Create account</button></form></div><div class="card"><h2>Current authority accounts</h2><div class="table-wrap"><table class="table"><thead><tr><th>Username</th><th>Role</th><th>Access</th></tr></thead><tbody>{trs or '<tr><td colspan="3"><div class="empty">No authority accounts.</div></td></tr>'}</tbody></table></div></div></div>'''
    return _render("Users & authority",body,"modern_users_page",role=role)


def modern_create_authority():
    role, resp = _require_roles("admin")
    if resp: return resp
    username=(request.form.get("username") or "").strip(); password=request.form.get("password") or ""; new_role=(request.form.get("role") or "police").strip().lower()
    try:
        if not username or not password or new_role not in {"admin","police","gk"}: return redirect(url_for("modern_users_page"))
        if new_role == "admin": obj=Admin(username=username,password_hash=legacy.generate_password_hash(password))
        elif new_role == "police": obj=PoliceUser(username=username,password_hash=legacy.generate_password_hash(password))
        else: obj=GKUser(username=username,password_hash=legacy.generate_password_hash(password))
        db.session.add(obj); db.session.commit()
    except Exception as exc:
        db.session.rollback()
        if _core: _core.record_system_error(exc,source="admin:create-authority",severity="ERROR")
    return redirect(url_for("modern_users_page"))



def modern_vehicle_map():
    role, resp = _require_roles("admin", "police", "gk")
    if resp:
        return resp
    data_url = url_for("modern_map_data")
    body = """
<div class="page-head"><div><div class="eyebrow">Geographic operations</div><h1 class="page-title">Live vehicle map</h1><p class="page-sub">Every live vehicle is represented. Dense areas are aggregated into counted dots/clusters so the map remains usable at very large fleet sizes.</p></div><div class="toolbar"><span class="pill" id="mapStatus">Loading map…</span><button class="btn" id="mapRefresh">Refresh</button></div></div>
<div class="card"><div id="vehicleMap" style="height:calc(100vh - 190px);min-height:520px;border-radius:14px;overflow:hidden;background:#e9eef3"></div><div class="row-between" style="margin-top:9px"><span class="small-muted" id="mapSummary">Preparing live fleet view…</span><span class="small-muted">Click a vehicle/cluster for details. Coordinates are hidden until a detail is opened.</span></div></div>
"""
    js = f"""
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
(function(){{
  const host=document.getElementById('vehicleMap');
  const status=document.getElementById('mapStatus');
  const summary=document.getElementById('mapSummary');
  const map=L.map(host,{{preferCanvas:true,worldCopyJump:true}}).setView([-1.286,36.817],6);
  L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',{{maxZoom:19,attribution:'© OpenStreetMap contributors'}}).addTo(map);
  const layer=L.layerGroup().addTo(map);
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
  function boundsParams(){{
    const b=map.getBounds();
    return new URLSearchParams({{min_lat:b.getSouth(),max_lat:b.getNorth(),min_lon:b.getWest(),max_lon:b.getEast(),zoom:map.getZoom()}}).toString();
  }}
  function dot(lat,lon,count,place,deviceId,detailsUrl,statusText){{
    const single=count===1;
    const size=single?7:Math.min(28,8+Math.log2(Math.max(2,count))*4);
    const m=L.circleMarker([lat,lon],{{radius:size,weight:2,fillOpacity:.78}}).addTo(layer);
    if(single){{
      m.bindPopup('<strong>Vehicle</strong><br>'+esc(place||'Place locating…')+'<br>'+esc(statusText||'Live')+'<br><a href="'+esc(detailsUrl)+'">Open details</a>');
    }} else {{
      m.bindPopup('<strong>'+esc(count.toLocaleString())+' vehicles</strong><br>'+esc(place||'Multiple vehicles in this area')+'<br><span>Zoom in to separate vehicles.</span>');
    }}
  }}
  async function load(){{
    try{{
      status.textContent='Updating…';
      const r=await fetch('{data_url}?'+boundsParams(),{{cache:'no-store'}});
      const d=await r.json();
      if(!d.ok) throw new Error(d.error||'Map data unavailable');
      layer.clearLayers();
      for(const c of (d.clusters||[])) dot(c.lat,c.lon,c.count,c.place_name,c.device_id,c.details_url,c.connection_state);
      for(const v of (d.vehicles||[])) dot(v.lat,v.lon,1,v.place_name,v.device_id,v.details_url,v.connection_state);
      summary.textContent=`${{Number(d.total||0).toLocaleString()}} live vehicle(s) represented · ${{Number(d.clusters||0).toLocaleString()}} clusters · ${{Number(d.vehicles?.length||0).toLocaleString()}} individual dots`;
      status.textContent=d.total?'LIVE MAP':'No live vehicles';
    }}catch(e){{
      status.textContent='Map unavailable'; summary.textContent=String(e.message||e); beaconReportError(e,'vehicle-map');
    }}
  }}
  map.on('moveend zoomend',()=>{{clearTimeout(window.__mapTimer);window.__mapTimer=setTimeout(load,180)}});
  document.getElementById('mapRefresh').addEventListener('click',load);
  load(); setInterval(load,15000);
}})();
</script>
"""
    return _render("Live vehicle map", body, "modern_vehicle_map", js, role)


def modern_map_data():
    role, resp = _require_roles("admin", "police", "gk")
    if resp:
        return resp
    try:
        min_lat=float(request.args.get("min_lat",-90)); max_lat=float(request.args.get("max_lat",90))
        min_lon=float(request.args.get("min_lon",-180)); max_lon=float(request.args.get("max_lon",180))
        zoom=max(0,min(19,int(request.args.get("zoom",6))))
        if min_lat>=max_lat or min_lon>=max_lon: raise ValueError("Invalid map bounds")
    except Exception as exc:
        return jsonify({"ok":False,"error":"Invalid map viewport","request_id":str(uuid.uuid4())}),400
    try:
        q=LiveVehicleState.query.filter(LiveVehicleState.lat>=min_lat,LiveVehicleState.lat<=max_lat,LiveVehicleState.lon>=min_lon,LiveVehicleState.lon<=max_lon)
        total=q.count()
        # Dense view: server-side grid aggregation. No million-point browser payloads.
        if total>2200 or zoom<11:
            span=max(max_lat-min_lat,max_lon-min_lon)
            cell=max(span/24.0,0.0002)
            sql=db.text("""
                SELECT CAST((lat-:min_lat)/:cell AS INTEGER) AS gx,
                       CAST((lon-:min_lon)/:cell AS INTEGER) AS gy,
                       COUNT(*) AS n, AVG(lat) AS lat, AVG(lon) AS lon,
                       MAX(updated_at) AS newest
                FROM live_vehicle_state
                WHERE lat BETWEEN :min_lat AND :max_lat
                  AND lon BETWEEN :min_lon AND :max_lon
                GROUP BY gx, gy
                ORDER BY n DESC
                LIMIT 5000
            """)
            rows=db.session.execute(sql,{{"min_lat":min_lat,"max_lat":max_lat,"min_lon":min_lon,"max_lon":max_lon,"cell":cell,"cutoff":cutoff}}).mappings().all()
            clusters=[{{"lat":float(r["lat"]),"lon":float(r["lon"]),"count":int(r["n"]),"place_name":None}} for r in rows]
            return jsonify({"ok":True,"total":total,"clusters":clusters,"vehicles":[],"clustered":True})
        rows=q.order_by(LiveVehicleState.updated_at.desc()).limit(5000).all()
        out=[]
        for st in rows:
            age=(datetime.utcnow()-st.updated_at).total_seconds() if st.updated_at else 10**9
            state_name="REAL-TIME" if age<30 else ("TELEMETRY ONLINE" if age<180 else "OFFLINE")
            out.append({"device_id":st.device_id,"lat":float(st.lat),"lon":float(st.lon),"place_name":st.place_name,"connection_state":state_name,"details_url":url_for("modern_device_detail",device_id=st.device_id)})
        return jsonify({"ok":True,"total":total,"clusters":[],"vehicles":out,"clustered":False})
    except Exception as exc:
        if _core: _core.record_system_error(exc,source="ui:vehicle-map",severity="ERROR")
        return jsonify({"ok":False,"error":"Vehicle map data unavailable","request_id":_core._request_id() if _core else str(uuid.uuid4())}),200


def modern_traffic_hub():
    role, resp = _require_roles("admin", "police", "gk")
    if resp:
        return resp
    data_url = url_for("modern_device_data")
    roads_url = url_for("admin_roads")
    speeders_url = url_for("admin_speeders")
    zones_url = url_for("admin_traffic")
    body = f"""
<div class="page-head"><div><div class="eyebrow">Intelligence</div><h1 class="page-title">Traffic operations</h1><p class="page-sub">Live traffic is plotted internally from telemetry, while operators see place names and operational state first.</p></div></div>
<div class="grid grid-2" style="margin-top:14px"><a class="list-item" href="{roads_url}"><strong>Roads</strong><div class="small-muted">Manage road safety profiles.</div></a><a class="list-item" href="{speeders_url}"><strong>Speeders</strong><div class="small-muted">Review speed events.</div></a><a class="list-item" href="{zones_url}"><strong>Traffic zones</strong><div class="small-muted">Manage operational zones.</div></a></div>
"""
    js = ""
    return _render("Traffic operations", body, "modern_traffic_hub", js, role)

def modern_reports_page():
    role, resp = _require_roles("admin", "police", "gk")
    if resp: return resp
    items=[
        ("Summary PDF", "/authority/report/summary.pdf", "Operational summary"),
        ("Incidents PDF", "/authority/report/incidents.pdf", "Incident history"),
        ("Vehicles PDF", "/authority/report/vehicles.pdf", "Registered vehicles"),
        ("Overspeed PDF", "/authority/report/overspeeds.pdf", "Speed events"),
        ("Backup snapshot PDF", "/authority/report/backup.pdf", "Backup inventory"),
    ]
    if role == "admin": items.append(("System errors PDF", "/authority/report/errors.pdf", "Error and diagnostic report"))
    links=''.join(f'<a class="list-item" href="{_esc(url)}"><div class="row-between"><strong>{_esc(t)}</strong><span>PDF ↗</span></div><div class="small-muted">{_esc(d)}</div></a>' for t,url,d in items)
    if role == "admin":
        data_links=f'''<div class="card"><h2>Data exports & backups</h2><div class="pillbar"><a class="btn btn-soft" href="{url_for('admin_backup_json')}">Download JSON</a><a class="btn btn-soft" href="{url_for('admin_backup_sqlite')}">Download SQLite</a><a class="btn btn-primary" href="{url_for('admin_backup_full')}">Full backup ZIP</a><a class="btn" href="{url_for('admin_backup_restore_page')}">Restore / manage backups</a></div><p class="small-muted" style="margin-top:10px">A full backup includes SQLite, JSON and a manifest. Restore creates a safety backup before replacement.</p></div>'''
    else:
        data_links=f'''<div class="card"><h2>Authority exports</h2><div class="pillbar"><a class="btn btn-soft" href="{url_for('authority_backup_json')}">Download authority JSON</a></div><p class="small-muted" style="margin-top:10px">Database replacement is restricted to the chief administrator.</p></div>'''
    body=f'''<div class="page-head"><div><div class="eyebrow">Reports & data</div><h1 class="page-title">Reports, downloads & backups</h1><p class="page-sub">PDF reports are role-aware; database backups and restore are restricted to the chief administrator.</p></div></div><div class="grid grid-3"><div class="card"><h2>PDF reports</h2><div class="list">{links}</div></div><div class="card"><h2>Operational use</h2><div class="stack"><div class="notice notice-ok">Reports are generated on demand so the dashboard remains fast.</div><div class="notice">Raw telemetry is not loaded into every report screen; detailed records are fetched when needed.</div></div></div><div>{data_links}</div></div>'''
    return _render("Reports & downloads",body,"authority_reports",role=role)


def modern_backup_page():
    role, resp = _require_roles("admin")
    if resp: return resp
    try:
        files=[]
        for p in sorted(getattr(_core,"BACKUP_DIR",[]).iterdir() if _core else [], key=lambda x:x.stat().st_mtime, reverse=True)[:30]:
            if p.is_file(): files.append(f'<tr><td>{_esc(p.name)}</td><td>{p.stat().st_size:,} bytes</td><td>{_esc(datetime.fromtimestamp(p.stat().st_mtime).isoformat())}</td></tr>')
    except Exception as exc:
        files=[]
        if _core: _core.record_system_error(exc,source="ui:backups",severity="WARNING")
    rows=''.join(files) or '<tr><td colspan="3"><div class="empty">No stored backups yet.</div></td></tr>'
    body=f'''<div class="page-head"><div><div class="eyebrow">Data protection</div><h1 class="page-title">Backups & restore</h1><p class="page-sub">Download SQLite/JSON/full archives and restore through the protected upload workflow.</p></div></div><div class="grid grid-2"><div class="card"><h2>Create backup</h2><div class="pillbar"><a class="btn btn-soft" href="{url_for('admin_backup_json')}">JSON</a><a class="btn btn-soft" href="{url_for('admin_backup_sqlite')}">SQLite</a><a class="btn btn-primary" href="{url_for('admin_backup_full')}">Full archive</a></div><div class="notice notice-warn" style="margin-top:14px">A restore should be used only with a validated backup. The system creates a pre-restore safety copy first.</div></div><div class="card"><h2>Restore</h2><form method="post" action="{url_for('admin_backup_restore')}" enctype="multipart/form-data" class="stack"><div><label>Backup file</label><input type="file" name="backup" accept=".zip,.db,.sqlite,.sqlite3,.json" required></div><label class="row" style="font-weight:700"><input type="checkbox" name="confirm_restore" value="1" required style="width:auto"> I understand this will replace the active database after validation.</label><button class="btn btn-primary" type="submit">Validate & restore</button></form></div></div><div class="card" style="margin-top:14px"><h2>Stored backups</h2><div class="table-wrap"><table class="table"><thead><tr><th>File</th><th>Size</th><th>Created</th></tr></thead><tbody>{rows}</tbody></table></div></div>'''
    return _render("Backups & restore",body,"admin_backup_restore_page",role=role)


def modern_errors_page():
    role, resp = _require_roles("admin")
    if resp: return resp
    rows=SystemError.query.order_by(SystemError.occurred_at.desc()).limit(160).all() if SystemError else []
    trs=''.join(f'<tr><td>{_esc(r.occurred_at.isoformat() if r.occurred_at else "")}</td><td><span class="badge {"badge-red" if r.severity=="ERROR" else "badge-amber"}">{_esc(r.severity)}</span></td><td>{_esc(r.source)}</td><td>{_esc(r.error_type)}</td><td>{_esc(r.message)}</td><td>{_esc(r.route)}</td><td>{"Resolved" if r.resolved else "Open"}</td><td><form method="post" action="{url_for('admin_error_resolve', error_id=r.id)}"><button class="btn" type="submit">{ "Reopen" if r.resolved else "Resolve"}</button></form></td></tr>' for r in rows)
    body=f'''<div class="page-head"><div><div class="eyebrow">Diagnostics</div><h1 class="page-title">System errors</h1><p class="page-sub">SQLite, JSON, socket, UI and request failures are recorded here with request IDs and source context.</p></div><a class="btn" href="{url_for('admin_errors_json')}">Export JSON</a></div><div class="card"><div class="table-wrap"><table class="table"><thead><tr><th>Time</th><th>Severity</th><th>Source</th><th>Type</th><th>Message</th><th>Route</th><th>Status</th><th></th></tr></thead><tbody>{trs or '<tr><td colspan="8"><div class="empty">No recorded errors.</div></td></tr>'}</tbody></table></div></div>'''
    return _render("System errors",body,"admin_errors",role=role)


def modern_error_resolve(error_id):
    role, resp = _require_roles("admin")
    if resp: return resp
    row=SystemError.query.get(error_id) if SystemError else None
    if row:
        row.resolved=not bool(row.resolved)
        db.session.commit()
    return redirect(url_for("admin_errors"))


def modern_settings_page():
    role, resp = _require_roles("admin", "police", "gk")
    if resp: return resp
    backend = str(db.engine.url.get_backend_name())
    data=_status_data()
    body=f'''<div class="page-head"><div><div class="eyebrow">Platform health</div><h1 class="page-title">System health</h1><p class="page-sub">Lightweight health checks; deep SQLite integrity checks are not run on every dashboard refresh.</p></div></div><div class="grid grid-3"><div class="card"><div class="metric">{_esc(backend.upper())}</div><div class="metric-label">Database backend</div></div><div class="card"><div class="metric">{_esc(data.get('registered',0))}</div><div class="metric-label">Registered vehicles</div></div><div class="card"><div class="metric">{_esc(data.get('open_errors',0))}</div><div class="metric-label">Open system errors</div></div></div><div class="card" style="margin-top:14px"><h2>Communication state</h2><div class="pillbar"><span class="pill">Real-time sockets: {_esc(data.get('realtime',0))}</span><span class="pill">Telemetry online: {_esc(data.get('telemetry_online',0))}</span><span class="pill">Inbox unread: {_esc(data.get('unread_inbox',0))}</span></div></div>'''
    return _render("System health",body,"modern_settings_page",role=role)


_client_error_last = {}
_client_error_lock = threading.RLock()
def modern_client_error():
    role, resp = _require_roles("admin", "police", "gk")
    if resp: return resp
    body=request.get_json(silent=True) or {}
    try:
        msg=(body.get("message") or "Authority client error")[:4000]
        source=str(body.get('context') or 'authority')[:100]
        key=f"{source}|{msg}"
        now=time.time()
        with _client_error_lock:
            if now-_client_error_last.get(key,0)<15:
                return jsonify({"ok":True,"deduplicated":True})
            _client_error_last[key]=now
        exc=RuntimeError(msg)
        if _core: _core.record_system_error(exc,source=f"ui:{source}",severity="WARNING")
    except Exception: pass
    return jsonify({"ok":True})


# ---------------- Warning persistence / phone delivery ----------------

def _warning_key(device_id, kind, title, ref=""):
    return f"{kind}:{device_id}:{ref or title}:{datetime.utcnow().strftime('%Y%m%d%H%M')}"


def _wrap_nearby(original):
    @wraps(original)
    def wrapped():
        response = original()
        try:
            data = response.get_json(silent=True) if hasattr(response, "get_json") else None
            if not isinstance(data, dict): return response
            device = _core._strict_device_from_request() if _core else None
            if not device: return response
            alerts = list(data.get("alerts") or [])
            now = datetime.utcnow()
            # Latest active accident for this device or nearby warning generated by distribution.
            if OperationalIncident:
                inc = (OperationalIncident.query.filter_by(device_id=device.id, status="active")
                       .order_by(OperationalIncident.created_at.desc()).first())
                if inc and inc.created_at and (now-inc.created_at).total_seconds() < 180:
                    item={"type":"accident","severity":"high","title":"Accident / hazard nearby","message":inc.reason or "Incident reported nearby","incident_id":inc.id}
                    if item not in alerts: alerts.append(item)
            # Durable warning fallback: anything not acknowledged as delivered over Socket.IO
            # is returned through the normal /nearby poll and marked delivered when handed to the device.
            pending = (DeviceAlert.query.filter_by(device_id=device.id, delivered_at=None)
                       .order_by(DeviceAlert.created_at.asc()).limit(10).all()) if DeviceAlert else []
            if pending:
                delivered_now = datetime.utcnow()
                for a in pending:
                    alerts.append({"type": a.kind, "severity": a.severity, "title": a.title, "message": a.body, "incident_id": a.incident_id, "alert_id": a.id})
                    a.delivered_at = delivered_now
                db.session.commit()
            # De-duplicate repeated identical payloads while retaining order.
            seen=set(); clean=[]
            for a in alerts:
                key=json.dumps({k:a.get(k) for k in ("type","severity","title","message","incident_id")},sort_keys=True,default=str) if isinstance(a,dict) else str(a)
                if key not in seen:
                    seen.add(key); clean.append(a)
            data["alerts"]=clean[-10:]
            from flask import jsonify as _jsonify
            return _jsonify(data), response.status_code
        except Exception as exc:
            if _core: _core.record_system_error(exc,source="nearby:warning-augmentation",severity="WARNING")
            return response
    return wrapped


def _wrap_heartbeat(original):
    @wraps(original)
    def wrapped(*args, **kwargs):
        result = original(*args, **kwargs)
        try:
            req = request
            body=req.get_json(silent=True) or {}
            did=str(body.get("device_id") or req.args.get("device_id") or "")
            device=Device.query.filter_by(id=did,revoked=False).first() if did else None
            if not device: return result
            # Re-read newest live state; cheap single-row query.
            state=LiveVehicleState.query.filter_by(device_id=device.id).first() if LiveVehicleState else None
            if not state: return result
            # Persist risk alerts conservatively from nearby result without making "no decision" into a warning.
            try:
                payload=result[0].get_json(silent=True) if isinstance(result,tuple) else result.get_json(silent=True)
                nearby=(payload or {}).get("nearby") or []
                reds=[x for x in nearby if x.get("decision")=="red"]
                cautions=[x for x in nearby if x.get("decision")=="caution"]
                candidate=reds[0] if reds else (cautions[0] if cautions else None)
                if candidate:
                    kind="overtaking_risk"; sev="high" if reds else "medium"; title="Overtaking risk detected" if reds else "Overtaking caution"
                    msg=candidate.get("reason") or "Vehicle interaction requires caution."
                    key=_warning_key(device.id,kind,title,candidate.get("device_id"))
                    _notify_device_and_queue(device.id,"server_warning",{"type":kind,"severity":sev,"title":title,"message":msg,"source_device_id":candidate.get("device_id")},kind=kind,severity=sev,title=title,body=msg,unique_key=key)

                # Overspeed events are persisted by the core risk engine. Queue only the latest
                # event for fallback delivery so a lost Socket.IO connection cannot lose the warning.
                if _core.OverspeedEvent:
                    evt=(_core.OverspeedEvent.query.filter_by(snapshot_id=state.snapshot_id).order_by(_core.OverspeedEvent.ts.desc()).first())
                    if evt:
                        data={"type":"overspeed","event_id":evt.id,"device_id":device.id,"road_id":evt.road_id,"speed_kmh":evt.speed_kmh,"lat":evt.lat,"lon":evt.lon,"ts":evt.ts.isoformat() if evt.ts else None}
                        key=f"overspeed:{device.id}:{evt.id}"
                        _notify_device_and_queue(device.id,"overspeed_alert",data,kind="overspeed",severity="high",title="Overspeed warning",body=f"Recorded speed {float(evt.speed_kmh or 0):.1f} km/h exceeds the configured threshold.",unique_key=key)

                # Accident observations are already deduplicated into OperationalIncident by the core.
                if OperationalIncident:
                    inc=(OperationalIncident.query.filter_by(device_id=device.id,type="possible_accident")
                         .order_by(OperationalIncident.created_at.desc()).first())
                    if inc and inc.created_at and (datetime.utcnow()-inc.created_at).total_seconds() <= 45:
                        payload2={"type":"accident","severity":inc.severity or "high","title":"Possible accident detected","message":inc.reason or "Impact-like telemetry detected.","incident_id":inc.id,"lat":inc.lat,"lon":inc.lon}
                        _notify_device_and_queue(device.id,"accident_alert",payload2,kind="accident",severity="high",title="Possible accident detected",body=inc.reason or "Impact-like telemetry detected.",incident_id=inc.id,unique_key=f"accident:{inc.id}")

                        # Warn only vehicles that are physically approaching the active incident zone.
                        try:
                            source_lat, source_lon = float(inc.lat), float(inc.lon)
                            for other_id, entry in core._latest_live_entries(exclude=device.id):
                                speed=float(entry.get("speed_mps") or 0.0)
                                if speed < 2.0: continue
                                dist=legacy.haversine_m(entry["lat"],entry["lon"],source_lat,source_lon)
                                if dist > 800: continue
                                desired=legacy.bearing_between(entry["lat"],entry["lon"],source_lat,source_lon)
                                heading=float(entry.get("bearing_deg") or entry.get("heading_deg") or 0.0)
                                if legacy.angle_diff(heading,desired) > 60: continue
                                eta=dist/speed
                                if eta > 150: continue
                                alert={"type":"accident_nearby","severity":"high","title":"Accident ahead","message":"A verified incident is ahead on your current approach.","incident_id":inc.id,"distance_m":round(dist,1),"eta_s":round(eta,1)}
                                _notify_device_and_queue(other_id,"accident_nearby",alert,kind="accident_nearby",severity="high",title="Accident ahead",body=alert["message"],incident_id=inc.id,unique_key=f"accident-nearby:{other_id}:{inc.id}")
                        except Exception as exc2:
                            if _core: _core.record_system_error(exc2,source="risk:accident-zone-distribution",severity="WARNING")
            except Exception:
                pass
            return result
        except Exception as exc:
            if _core: _core.record_system_error(exc,source="heartbeat:warning-queue",severity="WARNING")
            return result
    return wrapped


def install_modern_ui(core):
    global _core, SystemError, LiveVehicleState, AuthorityInboxMessage, OperationalIncident, DeviceAlert
    _core=core
    SystemError=core.SystemError; LiveVehicleState=core.LiveVehicleState; AuthorityInboxMessage=core.AuthorityInboxMessage; OperationalIncident=core.OperationalIncident
    DeviceAlert=core.DeviceAlert
    with app.app_context():
        db.create_all()

    # Optimized recipient selection is used by every future message dispatch.
    legacy._target_devices_for_message=_fast_target_devices

    # Modern pages / APIs.
    app.add_url_rule("/modern/api/status", endpoint="modern_status_data", view_func=modern_status_data, methods=["GET"])
    app.add_url_rule("/modern/api/devices", endpoint="modern_device_data", view_func=modern_device_data, methods=["GET"])
    app.add_url_rule("/admin/device/<device_id>/view", endpoint="modern_device_detail", view_func=modern_device_detail, methods=["GET"])
    app.add_url_rule("/modern/api/message-users", endpoint="modern_message_users", view_func=modern_message_users, methods=["GET"])
    app.add_url_rule("/admin/messages/send", endpoint="modern_message_send", view_func=modern_message_send, methods=["POST"])
    app.add_url_rule("/admin/users/create-authority", endpoint="modern_create_authority", view_func=modern_create_authority, methods=["POST"])
    app.add_url_rule("/admin/traffic-hub", endpoint="modern_traffic_hub", view_func=modern_traffic_hub, methods=["GET"])
    app.add_url_rule("/admin/map", endpoint="modern_vehicle_map", view_func=modern_vehicle_map, methods=["GET"])
    app.add_url_rule("/modern/api/map", endpoint="modern_map_data", view_func=modern_map_data, methods=["GET"])
    app.add_url_rule("/authority/inbox/view", endpoint="modern_inbox_page", view_func=modern_inbox_page, methods=["GET"])
    app.add_url_rule("/admin/incidents/view", endpoint="modern_incidents_page", view_func=modern_incidents_page, methods=["GET"])
    app.add_url_rule("/admin/users/view", endpoint="modern_users_page", view_func=modern_users_page, methods=["GET"])
    app.add_url_rule("/admin/reports/view", endpoint="authority_reports_modern_alias", view_func=modern_reports_page, methods=["GET"])

    # Replace legacy dashboard/page handlers without changing their URLs.
    for ep, fn in [("dashboard",modern_dashboard),("admin_devices",modern_devices_page),("admin_messages",modern_messages_page),
                   ("authority_reports",modern_reports_page),("admin_backup_restore_page",modern_backup_page),
                   ("admin_errors",modern_errors_page),("authority_inbox",modern_inbox_page),
                   ("admin_users",modern_users_page),("admin_admins",modern_users_page),
                   ("modern_settings_page",modern_settings_page)]:
        if ep in app.view_functions: app.view_functions[ep]=fn
    app.add_url_rule("/admin/health/view",endpoint="modern_settings_page",view_func=modern_settings_page,methods=["GET"])

    # Error resolve endpoint is separate so the new table never hits a legacy template.
    app.add_url_rule("/admin/errors/<int:error_id>/toggle", endpoint="admin_error_resolve", view_func=modern_error_resolve, methods=["POST"])

    # Add durable phone warning APIs.
    def device_alerts():
        d=_core._strict_device_from_request()
        if not d: return _core._api_error("Missing or invalid device token",401,"DEVICE_AUTH_REQUIRED")
        try: limit=max(1,min(50,int(request.args.get("limit",20))))
        except Exception: limit=20
        rows=DeviceAlert.query.filter_by(device_id=d.id).order_by(DeviceAlert.created_at.desc()).limit(limit).all()
        items=[]
        for r in reversed(rows):
            items.append({"id":r.id,"kind":r.kind,"severity":r.severity,"title":r.title,"message":r.body,"incident_id":r.incident_id,"created_at":r.created_at.isoformat() if r.created_at else None,"read":bool(r.read_at)})
        return jsonify({"ok":True,"alerts":items})
    def device_alerts_read(alert_id):
        d=_core._strict_device_from_request()
        if not d: return _core._api_error("Missing or invalid device token",401,"DEVICE_AUTH_REQUIRED")
        row=DeviceAlert.query.filter_by(id=alert_id,device_id=d.id).first()
        if not row: return _core._api_error("Alert not found",404,"ALERT_NOT_FOUND")
        row.read_at=datetime.utcnow(); db.session.commit(); return jsonify({"ok":True})
    app.add_url_rule("/device/alerts",endpoint="device_alerts",view_func=device_alerts,methods=["GET"])
    app.add_url_rule("/device/alerts/<int:alert_id>/read",endpoint="device_alerts_read",view_func=device_alerts_read,methods=["POST"])

    # Make /nearby/heartbeat responses durable for warnings, but keep the existing telemetry behavior.
    if "nearby" in app.view_functions:
        app.view_functions["nearby"]=_wrap_nearby(app.view_functions["nearby"])
    if "heartbeat" in app.view_functions:
        app.view_functions["heartbeat"]=_wrap_heartbeat(app.view_functions["heartbeat"])

    # Favicon noise-free endpoint.
    def favicon():
        return (b"",200,{"Content-Type":"image/x-icon","Cache-Control":"public,max-age=86400"})
    app.add_url_rule("/favicon.ico",endpoint="favicon_modern",view_func=favicon)

    # Ensure a consistent app version visible in status/diagnostics.
    core.BEACON_VERSION="2026.09.25-authority-platform-v5"
