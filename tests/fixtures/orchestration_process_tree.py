"""Controlled process tree used by F14 supervision tests."""

from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path


def _ignore_term(_signum, _frame) -> None:
    return None


def main() -> None:
    role = sys.argv[1]
    signal.signal(signal.SIGTERM, _ignore_term)
    print(f"{role}:{__import__('os').getpid()}", flush=True)
    if role == "parent":
        subprocess.Popen((sys.executable, str(Path(__file__).resolve()), "child"))
    elif role == "exit-with-silent-child":
        child = subprocess.Popen(
            (sys.executable, str(Path(__file__).resolve()), "silent-child"),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        print(f"silent-child-pid:{child.pid}", flush=True)
        return
    elif role == "child":
        subprocess.Popen(
            (sys.executable, str(Path(__file__).resolve()), "grandchild")
        )
    time.sleep(60)


if __name__ == "__main__":
    main()
