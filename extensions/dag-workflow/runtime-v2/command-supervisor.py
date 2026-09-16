"""Trusted Linux command containment; no DAG authority and no sandbox.

Only waitpid(-1) == ECHILD after launch proves extinction. /proc enumeration is
used solely to deliver cancellation, never to prove absence of descendants.
"""
import ctypes
import json
import os
import select
import signal
import subprocess
import sys
import time


def durable_json(path, value):
    with open(path + ".tmp", "x", encoding="utf8") as file:
        json.dump(value, file)
        file.flush()
        os.fsync(file.fileno())
    os.replace(path + ".tmp", path)
    fd = os.open(os.path.dirname(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def main():
    config = json.loads(sys.argv[1])
    # Fail before ready/launch if the required kernel/Python capabilities fail.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
    enabled = ctypes.c_int()
    if libc.prctl(37, ctypes.byref(enabled), 0, 0, 0) != 0 or enabled.value != 1:
        raise RuntimeError("PR_GET_CHILD_SUBREAPER")
    probe = os.pidfd_open(os.getpid())
    try:
        signal.pidfd_send_signal(probe, 0)
    finally:
        os.close(probe)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as file:
        boot = file.read().strip()
    with open("/proc/self/stat", encoding="ascii") as file:
        start = file.read().rsplit(") ", 1)[1].split()[19]
    identity = {"pid": os.getpid(), "processStart": boot + ":" + start}
    if os.getsid(0) != os.getpid():
        raise RuntimeError("SUPERVISOR_SESSION_REQUIRED")
    os.write(3, (json.dumps({"ready": identity}) + "\n").encode())
    os.close(3)
    os.set_blocking(0, False)
    buffer = b""
    connected = True
    launched = False
    invoked = False
    command = None
    exit_code = None
    exit_signal = None
    diagnostic = ""
    abort_at = None
    deadline = time.monotonic() + config["timeoutMs"] / 1000 if config["timeoutMs"] is not None else float("inf")

    def cancel_descendants(sig):
        # Positive observations only. A missed fork will remain waitable and be
        # signaled on the next turn. pidfds prevent signaling a recycled PID.
        for name in os.listdir("/proc"):
            if not name.isdigit() or int(name) == os.getpid():
                continue
            fd = None
            try:
                fd = os.pidfd_open(int(name))
                with open("/proc/" + name + "/stat", encoding="ascii") as file:
                    fields = file.read().rsplit(") ", 1)[1].split()
                if int(fields[3]) == os.getpid():
                    signal.pidfd_send_signal(fd, sig)
            except (ProcessLookupError, FileNotFoundError):
                pass
            finally:
                if fd is not None:
                    os.close(fd)

    while True:
        now = time.monotonic()
        if now >= deadline or not connected:
            abort_at = abort_at if abort_at is not None else now
        if connected and select.select([0], [], [], 0)[0]:
            data = os.read(0, 65536)
            if not data:
                connected = False
                abort_at = abort_at if abort_at is not None else now
            buffer += data
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                message = json.loads(line)
                if message == {"action": "abort"}:
                    abort_at = abort_at if abort_at is not None else now
                elif message == {"action": "launch", "token": config["token"]} and not launched and abort_at is None:
                    launched = True
                    try:
                        # Popen reports exec failure through its CLOEXEC error
                        # pipe. Restore TERM (the supervisor itself ignores it).
                        def child_setup():
                            os.setpgrp()
                            signal.signal(signal.SIGTERM, signal.SIG_DFL)
                        command = subprocess.Popen(config["argv"], stdin=subprocess.DEVNULL, preexec_fn=child_setup)
                        invoked = True
                    except OSError as error:
                        diagnostic = str(error)
        extinct = False
        if launched or abort_at is not None:
            while True:
                try:
                    pid, status = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:  # ECHILD: kernel-backed extinction
                    extinct = True
                    break
                if pid == 0:
                    break
                if command is not None and pid == command.pid:
                    command.returncode = os.waitstatus_to_exitcode(status)
                    if os.WIFEXITED(status):
                        exit_code = os.WEXITSTATUS(status)
                    elif os.WIFSIGNALED(status):
                        number = os.WTERMSIG(status)
                        try:
                            exit_signal = signal.Signals(number).name
                        except ValueError:  # Unnamed Linux realtime signal.
                            exit_signal = "SIG" + str(number)
            if extinct:
                durable_json(config["outcome"], {
                    "version": 2, "token": config["token"], "identity": identity,
                    "cwd": os.getcwd(), "extinct": True, "invoked": invoked,
                    "exitCode": exit_code, "signal": exit_signal,
                    "interrupted": abort_at is not None, "diagnostic": diagnostic,
                })
                return
        if abort_at is not None:
            cancel_descendants(signal.SIGKILL if now - abort_at >= 1 else signal.SIGTERM)
        time.sleep(0.02)


if __name__ == "__main__":
    main()
