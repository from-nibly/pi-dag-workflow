"""Finite same-session fork/exit handoff; the direct command exits zero.

Markers live outside the candidate. No daemonization, session escape, or fork
bomb: there is one continuing descendant per generation, then a gated sleeper.
"""
import json
import os
import signal
import sys
import time
from pathlib import Path

assert Path("file").read_text() == "baseline\n"
root = Path(sys.argv[1])


def marker(name, text):
    (root / (name + ".tmp")).write_text(text)
    (root / (name + ".tmp")).replace(root / name)


if os.fork():
    os._exit(0)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
for fd in (0, 1, 2):
    os.close(fd)
marker("first", json.dumps({"pid": os.getpid(), "group": os.getpgrp()}))
for generation in range(60):
    time.sleep(0.02)
    if os.fork():
        os._exit(0)
    if generation == 29:
        marker("middle", str(os.getpid()))
marker("last", str(os.getpid()))
while not (root / "release").exists():
    time.sleep(0.01)
marker("done", str(os.getpid()))
os._exit(0)
