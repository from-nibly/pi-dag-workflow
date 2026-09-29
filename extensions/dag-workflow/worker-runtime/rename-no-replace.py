"""Atomically install a synced immutable file without a transient hard-link alias."""
import ctypes
import errno
import os
import sys

libc = ctypes.CDLL(None, use_errno=True)
rename = libc.renameat2
rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
rename.restype = ctypes.c_int
# Linux AT_FDCWD, RENAME_NOREPLACE. Unsupported kernels/filesystems fail closed;
# a check-then-rename fallback could overwrite another publisher's result.
if rename(-100, os.fsencode(sys.argv[1]), -100, os.fsencode(sys.argv[2]), 1) != 0:
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        sys.exit(17)
    raise OSError(error, os.strerror(error), sys.argv[2])
