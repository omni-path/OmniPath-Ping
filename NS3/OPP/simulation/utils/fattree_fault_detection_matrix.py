#!/usr/bin/env python3
"""Run all fat-tree ping/fault diagnosis combinations in parallel.

This wrapper calls fattree_fault_detection_sweep.py once for each
mode/fault-mode pair, then merges the per-size diagnosis metrics into one CSV.
"""

import argparse
import csv
import datetime as _dt
import pathlib
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


PING_MODES = ("OPP", "RPINGMESH", "RDPROBE")
FAULT_MODES = (1, 2, 3, 4)


def read_summary(summary_file):
    rows = {}
    if not summary_file.exists():
        return rows
    with summary_file.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            packet_count = int(row["packet_count"])
            rows[packet_count] = {
                "total_flows": int(row["total_flows"]),
                "detected_flows": int(row["detected_flows"]),
            }
            for key in ("tp_flows", "fp_flows", "fn_flows", "tn_flows"):
                if key in row and row[key] != "":
                    rows[packet_count][key] = int(row[key])
            for key in ("recall", "precision", "f1_score"):
                if key in row and row[key] != "":
                    rows[packet_count][key] = float(row[key])
    return rows


def run_one(args, sweep_script, root_work_dir, mode, fault_mode):
    combo_name = "%s_fault%d" % (mode.lower(), fault_mode)
    work_dir = root_work_dir / combo_name
    stdout_file = work_dir / "sweep_stdout.log"
    work_dir.mkdir(parents=True, exist_ok=True)
    max_exp = args.opp_max_exp if mode == "OPP" and args.opp_max_exp is not None else args.max_exp

    cmd = [
        sys.executable,
        str(sweep_script),
        "--k", str(args.k),
        "--max-exp", str(max_exp),
        "--flows-per-size", str(args.flows_per_size),
        "--mode", mode,
        "--fault-mode", str(fault_mode),
        "--work-dir", str(work_dir),
        "--stop-time", "%.9f" % args.stop_time,
        "--start-time", "%.9f" % args.start_time,
        "--pg", str(args.pg),
        "--base-dport", str(args.base_dport),
        "--bandwidth", args.bandwidth,
        "--delay", args.delay,
        "--error-rate", "%.9f" % args.error_rate,
        "--fault-seed", str(args.fault_seed),
        "--opp-multicast-mode", args.opp_multicast_mode if mode == "OPP" else "STANDARD",
        "--opp-usm-sample-period", str(args.opp_usm_sample_period),
        "--opp-m", str(args.opp_m),
        "--opp-m1", str(args.opp_m1),
        "--opp-m2", str(args.opp_m2),
        "--opp-m3", str(args.opp_m3),
    ]

    started = time.time()
    with stdout_file.open("w") as stdout:
        proc = subprocess.run(cmd, stdout=stdout, stderr=subprocess.STDOUT)
    elapsed = time.time() - started

    summary_file = work_dir / "summary.csv"
    return {
        "mode": mode,
        "fault_mode": fault_mode,
        "status": "ok" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "elapsed_sec": elapsed,
        "work_dir": work_dir,
        "summary_file": summary_file,
        "diagnosis_result_file": work_dir / "diagnosis_result.txt",
        "run_log": work_dir / "run.log",
        "stdout_file": stdout_file,
        "max_exp": max_exp,
        "summary": read_summary(summary_file),
    }


def write_matrix(output_file, results, max_exp):
    packet_counts = [1 << exp for exp in range(max_exp + 1)]
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "mode",
            "fault_mode",
            "packet_count",
            "total_flows",
            "detected_flows",
            "tp_flows",
            "fp_flows",
            "fn_flows",
            "tn_flows",
            "recall",
            "precision",
            "f1_score",
            "status",
            "returncode",
            "elapsed_sec",
            "work_dir",
            "summary_file",
            "diagnosis_result_file",
            "run_log",
            "stdout_file",
            "combo_max_exp",
        ])
        for result in sorted(results, key=lambda r: (r["mode"], r["fault_mode"])):
            for packet_count in packet_counts:
                row = result["summary"].get(packet_count, {})
                writer.writerow([
                    result["mode"],
                    result["fault_mode"],
                    packet_count,
                    row.get("total_flows", ""),
                    row.get("detected_flows", ""),
                    row.get("tp_flows", ""),
                    row.get("fp_flows", ""),
                    row.get("fn_flows", ""),
                    row.get("tn_flows", ""),
                    "%.9f" % row["recall"] if "recall" in row else "",
                    "%.9f" % row["precision"] if "precision" in row else "",
                    "%.9f" % row["f1_score"] if "f1_score" in row else "",
                    result["status"],
                    result["returncode"],
                    "%.6f" % result["elapsed_sec"],
                    result["work_dir"],
                    result["summary_file"],
                    result["diagnosis_result_file"],
                    result["run_log"],
                    result["stdout_file"],
                    result.get("max_exp", max_exp),
                ])


def print_matrix(results, max_exp):
    packet_counts = [1 << exp for exp in range(max_exp + 1)]
    print("mode,fault_mode,packet_count,total_flows,detected_flows,tp_flows,fp_flows,fn_flows,tn_flows,recall,precision,f1_score,status")
    for result in sorted(results, key=lambda r: (r["mode"], r["fault_mode"])):
        for packet_count in packet_counts:
            row = result["summary"].get(packet_count, {})
            recall = "%.9f" % row["recall"] if "recall" in row else ""
            precision = "%.9f" % row["precision"] if "precision" in row else ""
            f1_score = "%.9f" % row["f1_score"] if "f1_score" in row else ""
            print("%s,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (
                result["mode"],
                result["fault_mode"],
                packet_count,
                row.get("total_flows", ""),
                row.get("detected_flows", ""),
                row.get("tp_flows", ""),
                row.get("fp_flows", ""),
                row.get("fn_flows", ""),
                row.get("tn_flows", ""),
                recall,
                precision,
                f1_score,
                result["status"]))


def main():
    script_path = pathlib.Path(__file__).resolve()
    repo_root = script_path.parents[2]
    sweep_script = script_path.with_name("fattree_fault_detection_sweep.py")

    parser = argparse.ArgumentParser(
        description="Parallel matrix runner for fat-tree fault-detection success rates.")
    parser.add_argument("--k", type=int, required=True, help="fat-tree radix")
    parser.add_argument("--max-exp", "-m", type=int, required=True,
                        help="evaluate packet counts 2^0..2^m")
    parser.add_argument("--opp-max-exp", type=int, default=None,
                        help="optional max exponent for OPP combinations only")
    parser.add_argument("--flows-per-size", "-n", type=int, required=True,
                        help="number of flows for each packet count")
    parser.add_argument("--work-dir", type=pathlib.Path, default=None,
                        help="root directory for all generated combination runs")
    parser.add_argument("--output-file", type=pathlib.Path, default=None,
                        help="merged CSV output path")
    parser.add_argument("--jobs", type=int, default=len(PING_MODES) * len(FAULT_MODES),
                        help="maximum number of mode/fault combinations to run concurrently")
    parser.add_argument("--stop-time", type=float, default=100.0)
    parser.add_argument("--start-time", type=float, default=0.000001)
    parser.add_argument("--pg", type=int, default=3)
    parser.add_argument("--base-dport", type=int, default=100)
    parser.add_argument("--bandwidth", default="100Gbps")
    parser.add_argument("--delay", default="1000ns")
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument("--fault-seed", type=int, default=7)
    parser.add_argument("--opp-multicast-mode", choices=["STANDARD", "USM", "SAMPLED"], default="STANDARD",
                        help="OPP probe multicast implementation mode for OPP combinations")
    parser.add_argument("--opp-usm-sample-period", type=int, default=1,
                        help="in SAMPLED mode, every p-th same-category OPP probe uses USM")
    parser.add_argument("--opp-m", type=int, default=1)
    parser.add_argument("--opp-m1", type=int, default=1)
    parser.add_argument("--opp-m2", type=int, default=1)
    parser.add_argument("--opp-m3", type=int, default=1)
    args = parser.parse_args()

    if not sweep_script.exists():
        parser.error("missing sweep script: %s" % sweep_script)
    if args.k <= 0 or args.k % 2 != 0:
        parser.error("--k must be a positive even number")
    if args.max_exp < 0:
        parser.error("--max-exp must be non-negative")
    if args.opp_max_exp is not None and args.opp_max_exp < 0:
        parser.error("--opp-max-exp must be non-negative")
    if args.flows_per_size <= 0:
        parser.error("--flows-per-size must be positive")
    if args.opp_usm_sample_period <= 0:
        parser.error("--opp-usm-sample-period must be positive")
    if args.jobs <= 0:
        parser.error("--jobs must be positive")

    if args.work_dir is None:
        stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        args.work_dir = repo_root / "simulation" / "mix" / "fattree_fault_detection_matrix" / (
            "k%d_m%d_n%d_%s" % (args.k, args.max_exp, args.flows_per_size, stamp))
    root_work_dir = args.work_dir.resolve()
    root_work_dir.mkdir(parents=True, exist_ok=True)

    if args.output_file is None:
        args.output_file = root_work_dir / "matrix_summary.csv"
    output_file = args.output_file.resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    combos = [(mode, fault_mode) for mode in PING_MODES for fault_mode in FAULT_MODES]
    print("work_dir=%s" % root_work_dir)
    print("output_file=%s" % output_file)
    print("combinations=%d jobs=%d" % (len(combos), min(args.jobs, len(combos))))

    results = []
    with ThreadPoolExecutor(max_workers=min(args.jobs, len(combos))) as executor:
        future_to_combo = {
            executor.submit(run_one, args, sweep_script, root_work_dir, mode, fault_mode): (mode, fault_mode)
            for mode, fault_mode in combos
        }
        for future in as_completed(future_to_combo):
            mode, fault_mode = future_to_combo[future]
            try:
                result = future.result()
            except Exception as exc:
                combo_dir = root_work_dir / ("%s_fault%d" % (mode.lower(), fault_mode))
                result = {
                    "mode": mode,
                    "fault_mode": fault_mode,
                    "status": "exception",
                    "returncode": -1,
                    "elapsed_sec": 0.0,
                    "work_dir": combo_dir,
                    "summary_file": combo_dir / "summary.csv",
                    "diagnosis_result_file": combo_dir / "diagnosis_result.txt",
                    "run_log": combo_dir / "run.log",
                    "stdout_file": combo_dir / "sweep_stdout.log",
                    "max_exp": args.opp_max_exp if mode == "OPP" and args.opp_max_exp is not None else args.max_exp,
                    "summary": {},
                }
                with (combo_dir / "wrapper_exception.txt").open("w") as f:
                    f.write("%s\n" % exc)
            results.append(result)
            print("finished mode=%s fault_mode=%d status=%s elapsed=%.3fs" % (
                result["mode"], result["fault_mode"], result["status"], result["elapsed_sec"]))

    write_matrix(output_file, results, args.max_exp)
    print_matrix(results, args.max_exp)
    print("matrix_summary_file=%s" % output_file)

    failed = [r for r in results if r["status"] != "ok"]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
