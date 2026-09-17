#!/usr/bin/env -S python3 -I -B
"""Operation-owned hook. The prepared tuple is checked under Git's ref locks.
No repository/user hook is chained. Any unsupported transaction fails closed.
"""
import json
import os
import re
import subprocess
import sys


def check(context, phase, text):
    if phase not in ("preparing", "prepared", "committed", "aborted"):
        raise ValueError("unknown phase")
    width = 40 if context["binding"]["objectFormat"] == "sha1" else 64
    oid = re.compile("[0-9a-f]{%d}" % width)
    old, new, target = context["expected"]["commit"], context["proposal"]["commit"], context["targetRef"]
    if not oid.fullmatch(old) or not oid.fullmatch(new) or old == "0" * width or new == "0" * width or not target.startswith("refs/heads/"):
        raise ValueError("invalid expected tuple")
    rows = [line.split(" ") for line in text.splitlines()]
    if not rows or any(len(row) != 3 or not oid.fullmatch(row[0]) or not oid.fullmatch(row[1]) for row in rows):
        raise ValueError("malformed transaction")
    if phase != "prepared":
        return
    for key in ("root", "common", "admin"):
        identity = context["binding"][key]
        st = os.stat(identity["path"], follow_symlinks=False)
        if str(st.st_dev) != identity["dev"] or str(st.st_ino) != identity["ino"]:
            raise ValueError("native identity drift")
    def git(*args):
        return subprocess.check_output(["git", "-c", "core.hooksPath=/dev/null", *args], cwd=context["binding"]["root"]["path"], stderr=subprocess.DEVNULL, text=True).strip()
    if git("symbolic-ref", "--no-recurse", "HEAD") != target:
        raise ValueError("bound HEAD changed")
    probe = subprocess.run(["git", "symbolic-ref", "--quiet", "--no-recurse", target], cwd=context["binding"]["root"]["path"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if probe.returncode != 1:
        raise ValueError("target is not direct")
    refs = [row[2] for row in rows]
    if len(set(refs)) != len(refs):
        raise ValueError("duplicate ref")
    for before, after, ref in rows:
        if ref in (target, "HEAD"):
            if before != old or after != new or target not in refs:
                raise ValueError("expected-old/new/direct-target mismatch")
        elif ref == "ORIG_HEAD":
            if len(rows) != 1 or after != old:
                raise ValueError("captured starting HEAD differs from expected old")
        elif ref == "AUTO_MERGE":
            if len(rows) != 1 or after != "0" * width:
                raise ValueError("unsupported AUTO_MERGE operation")
        else:
            raise ValueError("unexpected ref " + ref)


if __name__ == "__main__":
    try:
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "context.json")) as stream:
            context = json.load(stream)
        if len(sys.argv) != 2:
            raise ValueError("missing phase")
        check(context, sys.argv[1], sys.stdin.read(65537))
    except Exception as error:
        print("V2 reference guard rejected: " + str(error)[:1000], file=sys.stderr)
        sys.exit(1)
