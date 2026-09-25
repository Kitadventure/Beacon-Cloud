# Integrated Release Validation

Validated before packaging:

- `app.py` and `app_legacy.py` Python AST/compile validation: PASS
- Beacon repair verifier: PASS
- Android project validator: PASS
- Android Overtaking Java sources in compile tree: 17
- Android XML resources: 47, XML parse validation: PASS
- Gradle wrapper JAR present: PASS
- Android SDK path configured for primary Windows build machine: `C:/Android`
- Legacy RealMart Java moved outside `app/src` so it cannot be compiled into the Overtaking Assistant APK
- Server/Android cross-side contract references for heartbeat, nearby, onboarding, messages, alerts and Socket.IO registration: PASS

Runtime qualification remains environment-dependent: Render/Python dependencies must install in Render, and the Android build machine must have JDK 17+ plus Android SDK Platform 36 at the configured path. The package does not contain compiled build outputs.
