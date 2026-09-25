#!/usr/bin/env python3
"""Small, explicit Beacon demo simulator.

Use only against an isolated demo server/database with ALLOW_SIMULATION=1.
Production/mobile builds do not use this module.
"""
import argparse
import json
import math
import time
import urllib.request
import uuid
from datetime import datetime, timezone


def post(url, token, payload):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url.rstrip('/') + '/heartbeat', data=body, method='POST',
                                 headers={'Content-Type': 'application/json', 'Authorization': 'Token ' + token})
    with urllib.request.urlopen(req, timeout=8) as response:
        return json.loads(response.read().decode())


def iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--server', required=True)
    p.add_argument('--device-id', required=True)
    p.add_argument('--token', required=True)
    p.add_argument('--scenario', choices=['safe', 'approach', 'unsafe'], default='unsafe')
    p.add_argument('--count', type=int, default=20)
    args = p.parse_args()

    if args.count < 1 or args.count > 200:
        raise SystemExit('--count must be 1..200')
    lat, lon = -1.286389, 36.817223
    for i in range(args.count):
        phase = i * 0.08
        if args.scenario == 'safe':
            speed = 11.0
        elif args.scenario == 'approach':
            speed = 19.0 + 1.5 * math.sin(phase)
        else:
            speed = 25.0 + 3.0 * math.sin(phase)
        payload = {
            'device_id': args.device_id,
            'lat': lat + 0.00001 * i,
            'lon': lon + 0.00001 * i,
            'speed_mps': speed,
            'bearing': 90.0,
            'heading': 90.0,
            'accuracy_m': 4.0,
            'speed_accuracy_mps': 0.5,
            'bearing_accuracy_deg': 2.0,
            'elapsed_realtime_ms': int(time.monotonic() * 1000),
            'timestamp': iso(),
            'sequence': int(time.time() * 1000) + i,
            'source': 'demo_simulator',
            'is_mock': True,
            'demo_id': uuid.uuid4().hex,
        }
        print(post(args.server, args.token, payload))
        time.sleep(1.0)


if __name__ == '__main__':
    main()
