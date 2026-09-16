"""Test-only outer reaper: collect orphans when a test kills the real reaper.

The owner exit marker is not extinction. This wrapper exits only after ECHILD,
providing the supervisor-loss test with genuinely independent settlement.
"""
import ctypes
import json
import os
import subprocess
import sys
from pathlib import Path

assert ctypes.CDLL(None).prctl(36, 1, 0, 0, 0) == 0
root = Path(sys.argv[1])


def marker(name, text):
    (root / (name + ".tmp")).write_text(text)
    (root / (name + ".tmp")).replace(root / name)


owner = subprocess.Popen(sys.argv[2:])
marker("owner-pid", str(owner.pid))
code = owner.wait()
marker("owner-exit", json.dumps({"code": code if code >= 0 else None, "signal": "SIGKILL" if code == -9 else None}))
while True:
    try:
        os.waitpid(-1, 0)
    except ChildProcessError:
        break
