BEACON CLOUD — CHIEF ADMIN ENVIRONMENT LOGIN FIX

Replace the deployed project's app.py with this file.

Supported complete Render credential pairs (checked in this order):
  ADMIN_NAME + ADMIN_PASSWORD
  ADMIN_USER + ADMIN_PASS
  ADMIN_USERNAME + ADMIN_PASSWORD
  BOOTSTRAP_ADMIN_USERNAME + BOOTSTRAP_ADMIN_PASSWORD

The variables are read as complete pairs so an old/stale username variable cannot be
combined with a password from another naming convention.

The chief-admin login is authoritative against the Render environment values and does
not depend on the existing SQLite admin password hash. The database hash is synchronized
when the environment credentials are used. Public registration remains disabled.

No other environment variables are required by this fix.
