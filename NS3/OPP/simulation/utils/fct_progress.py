#!/usr/bin/env python3
import argparse
import csv
import pathlib
import time


def load_totals(metadata_path):
    totals = {}
    dport_to_packet_count = {}
    if not metadata_path.exists():
        return totals, dport_to_packet_count
    with metadata_path.open() as f:
        for row in csv.DictReader(f):
            dport = int(row["dport"])
            packet_count = int(row["packet_count"])
            totals[packet_count] = totals.get(packet_count, 0) + 1
            dport_to_packet_count[dport] = packet_count
    return totals, dport_to_packet_count


def load_completed(fct_path, dport_to_packet_count):
    completed = {}
    last_fct_ns = {}
    if not fct_path.exists():
        return completed, last_fct_ns
    with fct_path.open() as f:
        for line in f:
            parts = line.split()
            if len(parts) < 8:
                continue
            dport = int(parts[3])
            fct_ns = int(parts[6])
            packet_count = dport_to_packet_count.get(dport)
            if packet_count is None:
                continue
            completed[packet_count] = completed.get(packet_count, 0) + 1
            last_fct_ns[packet_count] = max(last_fct_ns.get(packet_count, 0), fct_ns)
    return completed, last_fct_ns


def print_progress(work_dir):
    totals, dport_to_packet_count = load_totals(work_dir / "flow_metadata.csv")
    completed, last_fct_ns = load_completed(work_dir / "fct.txt", dport_to_packet_count)
    print("work_dir=%s" % work_dir)
    print("packet_count,total_flows,completed_flows,remaining_flows,last_completed_fct_ms")
    for packet_count in sorted(totals):
        total = totals[packet_count]
        done = completed.get(packet_count, 0)
        fct_ms = last_fct_ns.get(packet_count, 0) / 1e6
        print("%d,%d,%d,%d,%.6f" % (packet_count, total, done, total - done, fct_ms))


def main():
    parser = argparse.ArgumentParser(description="Summarize FCT progress by packet count for a sweep work directory.")
    parser.add_argument("work_dir", type=pathlib.Path)
    parser.add_argument("--watch", type=float, default=0.0,
                        help="repeat every N seconds; default prints once")
    args = parser.parse_args()

    while True:
        print_progress(args.work_dir)
        if args.watch <= 0:
            break
        print("")
        time.sleep(args.watch)


if __name__ == "__main__":
    main()
