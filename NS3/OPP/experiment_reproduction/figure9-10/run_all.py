#!/usr/bin/env python3
"""Run Figure 9 and Figure 10 reproduction runners in order."""

import argparse
import pathlib
import subprocess
import sys


FIGURE9_RUNNERS = (
    "run_figure9a.py",
    "run_figure9b.py",
    "run_figure9c.py",
    "run_figure9d.py",
)

FIGURE10_RUNNERS = (
    "run_figure10a.py",
    "run_figure10b.py",
    "run_figure10c.py",
    "run_figure10d.py",
)


def figure_dir_from_script():
    return pathlib.Path(__file__).resolve().parent


def run_child(script, args, jobs):
    cmd = [sys.executable, str(script)]
    if script.name.startswith("run_figure9"):
        cmd.extend(["--jobs", str(jobs)])
    proc = subprocess.run(cmd, cwd=str(args.figure_dir))
    return proc.returncode


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=24,
                        help="concurrency passed to Figure 9 runners")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.jobs <= 0:
        raise SystemExit("--jobs must be positive")

    args.figure_dir = figure_dir_from_script()
    runner_dir = args.figure_dir / "runner"
    scripts = [runner_dir / name for name in FIGURE9_RUNNERS + FIGURE10_RUNNERS]
    missing = [str(script) for script in scripts if not script.exists()]
    if missing:
        raise SystemExit("missing runner(s):\n" + "\n".join(missing))

    for script in scripts:
        rc = run_child(script, args, args.jobs)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
