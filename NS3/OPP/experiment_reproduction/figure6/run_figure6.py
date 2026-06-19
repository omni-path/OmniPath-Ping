#!/usr/bin/env python3
"""Run all Figure 6 experiments serially."""

import pathlib
import subprocess
import sys


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
RUNNER_DIR = SCRIPT_DIR / "runner"

RUNNERS = (
    "run_fattree_coverage.py",
    "run_leaf_spine_coverage.py",
    "run_2d_torus_coverage.py",
    "run_3d_torus_coverage.py",
)


def main():
    for runner in RUNNERS:
        runner_path = RUNNER_DIR / runner
        cmd = [sys.executable, str(runner_path), "--opp-multicast-mode", "USM"]
        proc = subprocess.run(
            cmd,
            cwd=str(SCRIPT_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        rel_path = runner_path.relative_to(SCRIPT_DIR)
        if proc.returncode != 0:
            print("failed %s returncode=%d" % (rel_path, proc.returncode), flush=True)
            return proc.returncode
        print("finished %s" % rel_path, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
