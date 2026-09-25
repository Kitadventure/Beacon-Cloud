# Beacon demonstration simulator

This is deliberately separate from the production Android client. It sends `is_mock=true` telemetry and therefore requires `ALLOW_SIMULATION=1` on the demo server.

Use an isolated demo database and turn simulation back off before live deployment.

Example:

```bash
ALLOW_SIMULATION=1 python demo/simulator.py \
  --server http://127.0.0.1:5000 \
  --device-id YOUR_DEVICE_ID \
  --token YOUR_DEVICE_TOKEN \
  --scenario unsafe
```

The production Android client never uses this simulator and refuses to transmit Android mock locations.
