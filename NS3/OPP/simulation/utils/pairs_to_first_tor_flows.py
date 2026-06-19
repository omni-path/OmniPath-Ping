#!/usr/bin/env python3
"""Convert folded communication pairs into ns-3 flows from the first ToR only."""

import argparse
import csv
import pathlib
import sys


def read_summary(path):
    values = {}
    with path.open() as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) >= 2 and row[0]:
                values[row[0]] = row[1]
    return values


def convert(args):
    summary = read_summary(args.summary)
    hosts_per_tor = int(summary["hosts_per_tor"])
    first_tor_end = args.first_tor_base + hosts_per_tor

    rows = []
    with args.pairs.open() as f:
        reader = csv.DictReader(f)
        for pair in reader:
            src = int(pair["src"])
            if args.first_tor_base <= src < first_tor_end:
                rows.append((src, int(pair["dst"])))

    if args.base_dport + len(rows) - 1 > 65535:
        raise ValueError(
            "dport range exceeds 65535: base_dport=%d flow_count=%d" %
            (args.base_dport, len(rows))
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w") as f:
        f.write("%d\n" % len(rows))
        for idx, (src, dst) in enumerate(rows):
            f.write("%d %d %d %d %d %.9f\n" % (
                src,
                dst,
                args.pg,
                args.base_dport + idx,
                args.packet_count,
                args.start_time,
            ))

    return {
        "pairs": args.pairs,
        "summary": args.summary,
        "output": args.output,
        "hosts_per_tor": hosts_per_tor,
        "src_range": "%d-%d" % (args.first_tor_base, first_tor_end - 1),
        "flow_count": len(rows),
        "pg": args.pg,
        "packet_count": args.packet_count,
        "start_time": args.start_time,
        "base_dport": args.base_dport,
    }


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Write third.cc flow files from pair CSV rows whose src is in the first ToR."
    )
    parser.add_argument("--pairs", required=True, type=pathlib.Path)
    parser.add_argument("--summary", required=True, type=pathlib.Path)
    parser.add_argument("-o", "--output", required=True, type=pathlib.Path)
    parser.add_argument("--first-tor-base", type=int, default=0)
    parser.add_argument("--pg", type=int, default=7)
    parser.add_argument("--base-dport", type=int, default=10000)
    parser.add_argument("--packet-count", type=int, default=1)
    parser.add_argument("--start-time", type=float, default=2.0)
    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    try:
        result = convert(args)
    except (KeyError, ValueError) as error:
        print("error: %s" % error, file=sys.stderr)
        return 1
    for key in (
        "pairs",
        "summary",
        "output",
        "hosts_per_tor",
        "src_range",
        "flow_count",
        "pg",
        "packet_count",
        "start_time",
        "base_dport",
    ):
        print("%s: %s" % (key, result[key]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
