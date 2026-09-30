"""Deterministic app identity for the test session.

This app's own name is read from the environment at import time so a deployment
can rename it. A test run that inherits the host's name (for example
``BOTTLE_APP_NAME=openchamber`` inside a Cloud in a Bottle app) would then
derive its own directories, exclusions and self-comparisons from that host,
which makes every test that assumes the default name fail for reasons that
have nothing to do with the code under test. Pin the name before any test
module imports the application; the configurable-name behavior is covered
explicitly by a test that overrides it.
"""

from __future__ import annotations

import os

os.environ["BOTTLE_APP_NAME"] = "backup"
# The two names are alternates, so a host value here would still win.
os.environ.pop("OPENHOST_APP_NAME", None)
