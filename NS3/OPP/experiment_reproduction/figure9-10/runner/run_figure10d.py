#!/usr/bin/env python3
"""Generate Figure 10(d): 3D torus OPP token-id cache space vs m."""

import argparse
import csv
import pathlib
import sys


DEFAULT_K = (3, 4, 5, 6)
DEFAULT_M = tuple(range(1, 9))
DEFAULT_OUTPUT_FILE = "Figure10(d)_results.csv"


def figure_dir_from_script():
    return pathlib.Path(__file__).resolve().parents[1]


def entry_count(k, m):
    return k * k * k * m


def write_matrix(output_file, args):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["series"] + [str(m) for m in args.m])
        for k in args.k:
            writer.writerow(["%dx%dx%d" % (k, k, k)] +
                            [entry_count(k, m) for m in args.m])


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--k", type=int, nargs="+", default=list(DEFAULT_K),
                        help="3D torus dimensions; k means k x k x k")
    parser.add_argument("--m", type=int, nargs="+", default=list(DEFAULT_M),
                        help="OPP_TOKENS_PER_TOR values")
    parser.add_argument("--output-file", type=pathlib.Path,
                        default=pathlib.Path("results") / DEFAULT_OUTPUT_FILE)
    return parser.parse_args()


def main():
    args = parse_args()
    if any(k <= 0 for k in args.k):
        raise SystemExit("--k values must be positive")
    if any(m <= 0 for m in args.m):
        raise SystemExit("--m values must be positive")

    output_file = (figure_dir_from_script() / args.output_file).resolve()
    write_matrix(output_file, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
