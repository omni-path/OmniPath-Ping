#!/usr/bin/env python3
"""Generate Figure 11(b): leaf-spine OPP cache-entry bound vs multiplier."""

import argparse
import csv
import pathlib
import sys


DEFAULT_K = (32, 64)
DEFAULT_MULTIPLIERS = tuple(range(1, 9))
DEFAULT_OUTPUT_FILE = "Figure11(b)_results.csv"


def figure_dir_from_script():
    return pathlib.Path(__file__).resolve().parents[1]


def entry_count(k, multiplier, base_m1, base_m2):
    m1 = base_m1 * multiplier
    m2 = base_m2 * multiplier
    return m1 + 2 * m2 * k


def write_matrix(output_file, args):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["series"] + [str(x) for x in args.multiplier])
        for k in args.k:
            writer.writerow(
                ["k%d" % k] +
                [entry_count(k, x, args.base_m1, args.base_m2)
                 for x in args.multiplier]
            )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--k", type=int, nargs="+", default=list(DEFAULT_K))
    parser.add_argument("--multiplier", type=int, nargs="+",
                        default=list(DEFAULT_MULTIPLIERS))
    parser.add_argument("--base-m1", type=int, default=10,
                        help="base OPP_LEAFSPINE_M1, matching Figure 10(b)")
    parser.add_argument("--base-m2", type=int, default=10,
                        help="base OPP_LEAFSPINE_M2, matching Figure 10(b)")
    parser.add_argument("--output-file", type=pathlib.Path,
                        default=pathlib.Path("results") / DEFAULT_OUTPUT_FILE)
    return parser.parse_args()


def main():
    args = parse_args()
    if any(k <= 0 for k in args.k):
        raise SystemExit("--k values must be positive")
    if any(x <= 0 for x in args.multiplier):
        raise SystemExit("--multiplier values must be positive")
    if args.base_m1 <= 0 or args.base_m2 <= 0:
        raise SystemExit("--base-m1 and --base-m2 must be positive")

    output_file = (figure_dir_from_script() / args.output_file).resolve()
    write_matrix(output_file, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
