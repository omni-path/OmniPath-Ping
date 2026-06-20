#!/usr/bin/env python3
"""Run Figure 14: fat-tree k=64 sampled-USM experiments.

Part a varies the number of concurrent probe instances. Part b keeps the
previous per-rate m multiplier sweep. The result CSVs report only 700ns
loopback peak rate grouped by sample rate.
"""

import argparse
import csv
import math
import pathlib
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


_EXPERIMENT_REPRODUCTION_DIR = pathlib.Path(__file__).resolve()
while _EXPERIMENT_REPRODUCTION_DIR.name != "experiment_reproduction":
    if _EXPERIMENT_REPRODUCTION_DIR.parent == _EXPERIMENT_REPRODUCTION_DIR:
        raise RuntimeError("failed to locate experiment_reproduction directory")
    _EXPERIMENT_REPRODUCTION_DIR = _EXPERIMENT_REPRODUCTION_DIR.parent
sys.path.insert(0, str(_EXPERIMENT_REPRODUCTION_DIR))
from runner_path_utils import path_relative_to, relativize_config_lines


DEFAULT_SAMPLE_RATES = (0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100)
DEFAULT_MULTIPLIERS = (2, 3, 4, 6, 7, 8, 10, 11, 12, 13, 15)
DEFAULT_MULTIPLIER_DIVISORS = (1, 5, 10)
DEFAULT_INSTANCE_COUNTS = (1, 2, 3, 4)
DIVISOR_M1_OVERRIDES = {
    5: {
        0: 3,
        10: 6,
        20: 8,
        30: 10,
        40: 13,
        50: 15,
        60: 19,
        70: 19,
        80: 21,
        90: 23,
        100: 30,
    },
    10: {
        0: 2,
        10: 3,
        20: 4,
        30: 5,
        40: 6,
        50: 7,
        60: 10,
        70: 10,
        80: 11,
        90: 12,
        100: 13,
    },
}
DIVISOR_M2_OVERRIDES = {
    1: {
        0: 20,
        10: 30,
        20: 43,
        30: 60,
        40: 73,
        50: 84,
        60: 100,
        70: 111,
        80: 125,
        90: 139,
        100: 150,
    },
    5: {
        0: 3,
        10: 6,
        20: 8,
        30: 10,
        40: 13,
        50: 15,
        60: 19,
        70: 19,
        80: 21,
        90: 23,
        100: 30,
    },
    10: {
        0: 2,
        10: 3,
        20: 4,
        30: 5,
        40: 6,
        50: 8,
        60: 10,
        70: 10,
        80: 11,
        90: 12,
        100: 13,
    },
}
DIVISOR_M3_OVERRIDES = {
    1: {
        0: 2,
        10: 3,
        20: 4,
        30: 6,
        40: 8,
        50: 11,
        60: 13,
        70: 16,
        80: 20,
        90: 21,
        100: 26,
    },
    5: {
        0: 1,
        10: 1,
        20: 1,
        30: 2,
        40: 2,
        50: 2,
        60: 2,
        70: 3,
        80: 3,
        90: 3,
        100: 3,
    },
    10: {
        0: 1,
        10: 1,
        20: 1,
        30: 1,
        40: 1,
        50: 1,
        60: 1,
        70: 2,
        80: 2,
        90: 2,
        100: 2,
    },
}


def repo_root_from_script():
    return pathlib.Path(__file__).resolve().parents[2]


def figure_dir_from_script():
    return pathlib.Path(__file__).resolve().parent


def write_trace_file(path):
    path.write_text("0\n")


def read_declared_flow_count(flow_file):
    with flow_file.open() as f:
        return int(f.readline().strip())


def sample_rate_fraction(percent):
    if percent < 0 or percent > 100:
        raise ValueError("sample rate percent must be in [0, 100]")
    if percent == 0:
        return 0, 10
    if percent % 10 == 0:
        return percent // 10, 10
    g = math.gcd(percent, 100)
    return percent // g, 100 // g


def scaled_m(base, multiplier, divisor):
    return int(math.ceil(float(base * multiplier) / float(divisor)))


def scaled_m1_m2(args, sample_rate, multiplier, divisor):
    m1 = DIVISOR_M1_OVERRIDES.get(divisor, {}).get(
        sample_rate,
        scaled_m(args.base_m1, multiplier, divisor))
    m2 = DIVISOR_M2_OVERRIDES.get(divisor, {}).get(
        sample_rate,
        scaled_m(args.base_m2, multiplier, divisor))
    return m1, m2


def scaled_m3(args, sample_rate, multiplier, divisor):
    return DIVISOR_M3_OVERRIDES.get(divisor, {}).get(
        sample_rate,
        scaled_m(args.base_m3, multiplier, divisor))


def load_peak700_node_map(path, part):
    mapping = {}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                node = int(row["max_loopback_peak_tx_rate_700ns_node"])
                sample_rate = int(row["sample_rate_percent"])
                if part == "a":
                    key = (int(row["probe_instance_count"]), sample_rate)
                else:
                    key = (
                        int(row["multiplier_divisor"]),
                        sample_rate,
                        int(row["base_multiplier"]),
                    )
            except (KeyError, ValueError) as exc:
                raise SystemExit("invalid peak700 node map %s: %s" % (path, exc))
            mapping[key] = node
    return mapping


def qlen_trace_node_for_task(args, task):
    if args.qlen_trace_node is not None:
        return args.qlen_trace_node

    maps = getattr(args, "qlen_trace_node_maps", {})
    node_map = maps.get(task["part"])
    if node_map is None:
        return None

    if task["part"] == "a":
        key = (task["probe_instance_count"], task["sample_rate"])
    else:
        key = (task["multiplier_divisor"], task["sample_rate"], task["multiplier"])
    try:
        return node_map[key]
    except KeyError:
        raise SystemExit(
            "missing peak700 qlen node for part=%s key=%s" % (task["part"], key)
        )


def write_config(path, args, task, trace_file):
    work_dir = task["work_dir"]
    lines = [
        "ENABLE_QCN 1",
        "USE_DYNAMIC_PFC_THRESHOLD 1",
        "PACKET_PAYLOAD_SIZE 1000",
        "TOPOLOGY_FILE %s" % task["topology_file"],
        "TOPOLOGY_MODE FAT_TREE",
        "FAT_TREE_K 64",
        "FLOW_FILE %s" % task["flow_file"],
        "TRACE_FILE %s" % trace_file,
        "TRACE_OUTPUT_FILE %s" % (work_dir / "trace.tr"),
        "FCT_OUTPUT_FILE %s" % task["fct_file"],
        "PFC_OUTPUT_FILE %s" % (work_dir / "pfc.txt"),
        "FLOW_COVERAGE_OUTPUT_FILE %s" % (work_dir / "flow_coverage.txt"),
        "SIMULATOR_STOP_TIME %.9f" % args.stop_time,
        "CC_MODE 12",
        "ROUTING_MODE PACKET_SPRAY",
        "OPP_MULTICAST_MODE SAMPLED",
        "OPP_USM_SAMPLE_RATE %d %d" % (
            task["sample_numerator"], task["sample_denominator"]),
        "OPP_USM_SAMPLE_POLICY %s" % args.usm_sample_policy,
        "OPP_PROBE_INSTANCE_COUNT %d" % task["probe_instance_count"],
        "OPP_TOKENS_PER_TOR %d" % task["m3"],
        "OPP_FATTREE_M1 %d" % task["m1"],
        "OPP_FATTREE_M2 %d" % task["m2"],
        "OPP_FATTREE_M3 %d" % task["m3"],
        "OPP_INITIAL_INTERPOD_PROBE_SPREAD_NS %d" % (
            args.initial_interpod_probe_spread_us * 1000),
        "OPP_LEAFSPINE_M1 %d" % task["m1"],
        "OPP_LEAFSPINE_M2 %d" % task["m2"],
        "ALPHA_RESUME_INTERVAL 1",
        "RATE_DECREASE_INTERVAL 4",
        "CLAMP_TARGET_RATE 0",
        "RP_TIMER 900",
        "EWMA_GAIN 0.00390625",
        "FAST_RECOVERY_TIMES 1",
        "RATE_AI 50Mb/s",
        "RATE_HAI 50Mb/s",
        "MIN_RATE 100Mb/s",
        "DCTCP_RATE_AI 1000Mb/s",
        "ERROR_RATE_PER_LINK %.6f" % args.error_rate,
        "L2_CHUNK_SIZE 4000",
        "L2_ACK_INTERVAL 1",
        "L2_BACK_TO_ZERO 0",
        "HAS_WIN 1",
        "GLOBAL_T 1",
        "VAR_WIN 1",
        "FAST_REACT 1",
        "U_TARGET 0.95",
        "MI_THRESH 0",
        "INT_MULTI 1",
        "MULTI_RATE 0",
        "SAMPLE_FEEDBACK 0",
        "PINT_LOG_BASE 1.05",
        "PINT_PROB 1.0",
        "RATE_BOUND 1",
        "ACK_HIGH_PRIO 0",
        "LINK_DOWN 0 0 0",
        "ENABLE_TRACE 0",
        "KMAX_MAP 1 400000000000 6400",
        "KMIN_MAP 1 400000000000 1600",
        "PMAX_MAP 1 400000000000 0.2",
        "BUFFER_SIZE %d" % args.buffer_size,
        "QLEN_MON_FILE %s" % (work_dir / "qlen.txt"),
        "QLEN_MON_START 0",
        "QLEN_MON_END 0",
        "ENABLE_RDPROBE_DIAG 0",
        "RDPROBE_DIAG_OUTPUT_FILE %s" % (work_dir / "rdprobe_trace.txt"),
        "RDPROBE_DIAG_RESULT_OUTPUT_FILE %s" % (work_dir / "rdprobe_diag_result.txt"),
        "ENABLE_RPINGMESH_DIAG 0",
        "RPINGMESH_DIAG_RESULT_OUTPUT_FILE %s" % (work_dir / "rpingmesh_diag_result.txt"),
        "ENABLE_OPP_DIAG 0",
        "OPP_DIAG_RESULT_OUTPUT_FILE %s" % (work_dir / "opp_diag_result.txt"),
        "OPP_LOOPBACK_PEAK_BUCKET_OUTPUT_FILE %s" % task["loopback_peak_bucket_file"],
        "OPP_LOOPBACK_PEAK_BUCKET_INTERVAL_NS %d" % (
            args.loopback_peak_bucket_interval_us * 1000),
    ]
    qlen_trace_node = task.get("qlen_trace_node")
    if qlen_trace_node is not None:
        interval_ns = args.qlen_trace_interval_ns
        if args.qlen_trace_interval_us is not None:
            interval_ns = args.qlen_trace_interval_us * 1000
        lines.extend([
            "SWITCH_QLEN_TRACE_NODE %d" % qlen_trace_node,
            "SWITCH_QLEN_TRACE_OUTPUT_FILE %s" % task["switch_qlen_trace_file"],
            "SWITCH_QLEN_TRACE_DURATION_NS %d" % (args.qlen_trace_duration_us * 1000),
            "SWITCH_QLEN_TRACE_INTERVAL_NS %d" % interval_ns,
        ])
    path.write_text("\n".join(relativize_config_lines(lines, args.sim_dir)) + "\n")


def parse_max_fct_ns(fct_file):
    max_fct_ns = None
    if not fct_file.exists():
        return None
    with fct_file.open() as f:
        for line in f:
            fields = line.split()
            if len(fields) < 7:
                continue
            fct_ns = int(fields[6])
            if max_fct_ns is None or fct_ns > max_fct_ns:
                max_fct_ns = fct_ns
    return max_fct_ns


def parse_loopback_stats(run_log):
    stats = {
        "max_loopback_peak_tx_rate_1us_gbps": None,
        "max_loopback_peak_tx_rate_1us_node": None,
        "max_loopback_peak_tx_rate_700ns_gbps": None,
        "max_loopback_peak_tx_rate_700ns_node": None,
        "max_loopback_peak_tx_rate_700ns_after_200us_gbps": None,
        "max_loopback_peak_tx_rate_700ns_after_200us_node": None,
        "max_loopback_peak_tx_rate_100ns_gbps": None,
        "max_loopback_peak_tx_rate_100ns_node": None,
        "max_loopback_peak_queue_bytes": None,
        "max_loopback_peak_queue_node": None,
    }
    if not run_log.exists():
        return stats

    with run_log.open(errors="replace") as f:
        for line in f:
            fields = line.split()
            if not fields or fields[0] != "OPP_LOOPBACK_STATS":
                continue
            values = {}
            i = 1
            while i + 1 < len(fields):
                values[fields[i]] = fields[i + 1]
                i += 2
            try:
                node = int(values["node"])
            except (KeyError, ValueError):
                node = None

            for key, node_key, raw_key, convert in (
                    ("max_loopback_peak_tx_rate_1us_gbps",
                     "max_loopback_peak_tx_rate_1us_node",
                     "peak_tx_rate_1us_gbps", float),
                    ("max_loopback_peak_tx_rate_700ns_gbps",
                     "max_loopback_peak_tx_rate_700ns_node",
                     "peak_tx_rate_700ns_gbps", float),
                    ("max_loopback_peak_tx_rate_700ns_after_200us_gbps",
                     "max_loopback_peak_tx_rate_700ns_after_200us_node",
                     "peak_tx_rate_700ns_after_200us_gbps", float),
                    ("max_loopback_peak_tx_rate_100ns_gbps",
                     "max_loopback_peak_tx_rate_100ns_node",
                     "peak_tx_rate_100ns_gbps", float),
                    ("max_loopback_peak_queue_bytes",
                     "max_loopback_peak_queue_node",
                     "peak_queue_bytes", int)):
                try:
                    value = convert(values[raw_key])
                except (KeyError, ValueError):
                    continue
                if stats[key] is None or value > stats[key]:
                    stats[key] = value
                    stats[node_key] = node
    return stats


def run_task(sim_dir, task, no_run):
    if no_run:
        return {
            "status": "generated",
            "returncode": 0,
            "elapsed_sec": 0.0,
            "max_fct_ns": None,
            **parse_loopback_stats(task["run_log"]),
        }

    started = time.time()
    cmd = ["python2", "./waf", "--run", "scratch/third %s" % path_relative_to(task["config_file"], sim_dir)]
    with task["run_log"].open("w") as log:
        proc = subprocess.run(cmd, cwd=str(sim_dir), stdout=log, stderr=subprocess.STDOUT)
    elapsed = time.time() - started

    loopback_stats = parse_loopback_stats(task["run_log"])
    if proc.returncode != 0:
        return {
            "status": "failed",
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "max_fct_ns": None,
            **loopback_stats,
        }

    max_fct_ns = parse_max_fct_ns(task["fct_file"])
    if max_fct_ns is None:
        return {
            "status": "missing_fct",
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "max_fct_ns": None,
            **loopback_stats,
        }

    return {
        "status": "ok",
        "returncode": proc.returncode,
        "elapsed_sec": elapsed,
        "max_fct_ns": max_fct_ns,
        **loopback_stats,
    }


def build_tasks(args, repo_root, work_root, part):
    if part == "b" and len(args.sample_rate) != len(args.multiplier):
        raise SystemExit("--sample-rate and --multiplier must have the same length")

    topology_file = (
        repo_root / "experiment_reproduction" / "topologies" / "fattree" /
        "fattree_k64_topology.txt"
    )
    flow_file = (
        repo_root / "experiment_reproduction" / "figure9-10" /
        "traffic_load" / "fattree" / "k64" / "flow_2e0.txt"
    )
    if not topology_file.exists():
        raise FileNotFoundError("missing topology file: %s" % topology_file)
    if not flow_file.exists():
        raise FileNotFoundError("missing flow file: %s" % flow_file)

    declared_flows = read_declared_flow_count(flow_file)
    trace_file = work_root / "trace.txt"
    write_trace_file(trace_file)

    tasks = []
    if part == "a":
        for instance_count in args.instance_count:
            for sample_rate in args.sample_rate:
                numerator, denominator = sample_rate_fraction(sample_rate)
                work_dir = work_root / (
                    "instances_%d" % instance_count) / (
                        "sample_%03d" % sample_rate)
                work_dir.mkdir(parents=True, exist_ok=True)
                task = {
                    "part": "a",
                    "sample_rate": sample_rate,
                    "sample_numerator": numerator,
                    "sample_denominator": denominator,
                    "probe_instance_count": instance_count,
                    "multiplier": 1,
                    "multiplier_divisor": 1,
                    "m1": args.base_m1,
                    "m2": args.base_m2,
                    "m3": args.base_m3,
                    "declared_flows": declared_flows,
                    "work_dir": work_dir,
                    "topology_file": topology_file.resolve(),
                    "flow_file": flow_file.resolve(),
                    "config_file": work_dir / "config.txt",
                    "run_log": work_dir / "run.log",
                    "fct_file": work_dir / "fct.txt",
                    "loopback_peak_bucket_file": work_dir / (
                        "loopback_peak_%dus.csv" %
                        args.loopback_peak_bucket_interval_us),
                }
                qlen_trace_node = qlen_trace_node_for_task(args, task)
                task["qlen_trace_node"] = qlen_trace_node
                task["switch_qlen_trace_file"] = (
                    work_dir / ("switch_qlen_node_%d.csv" % qlen_trace_node)
                    if qlen_trace_node is not None else ""
                )
                write_config(task["config_file"], args, task, trace_file.resolve())
                tasks.append(task)
        return tasks

    for multiplier_divisor in args.multiplier_divisor:
        for sample_rate, multiplier in zip(args.sample_rate, args.multiplier):
            numerator, denominator = sample_rate_fraction(sample_rate)
            m1, m2 = scaled_m1_m2(args, sample_rate, multiplier, multiplier_divisor)
            work_dir = work_root / (
                "div_%d" % multiplier_divisor) / (
                    "sample_%03d_mul_%d" % (sample_rate, multiplier))
            work_dir.mkdir(parents=True, exist_ok=True)
            task = {
                "part": "b",
                "sample_rate": sample_rate,
                "sample_numerator": numerator,
                "sample_denominator": denominator,
                "probe_instance_count": 1,
                "multiplier": multiplier,
                "multiplier_divisor": multiplier_divisor,
                "m1": m1,
                "m2": m2,
                "m3": scaled_m3(args, sample_rate, multiplier, multiplier_divisor),
                "declared_flows": declared_flows,
                "work_dir": work_dir,
                "topology_file": topology_file.resolve(),
                "flow_file": flow_file.resolve(),
                "config_file": work_dir / "config.txt",
                "run_log": work_dir / "run.log",
                "fct_file": work_dir / "fct.txt",
                "loopback_peak_bucket_file": work_dir / (
                    "loopback_peak_%dus.csv" %
                    args.loopback_peak_bucket_interval_us),
            }
            qlen_trace_node = qlen_trace_node_for_task(args, task)
            task["qlen_trace_node"] = qlen_trace_node
            task["switch_qlen_trace_file"] = (
                work_dir / ("switch_qlen_node_%d.csv" % qlen_trace_node)
                if qlen_trace_node is not None else ""
            )
            write_config(task["config_file"], args, task, trace_file.resolve())
            tasks.append(task)
    return tasks


def row_from_task_result(task, result):
    max_fct_ns = result["max_fct_ns"]
    rate_1us = result["max_loopback_peak_tx_rate_1us_gbps"]
    rate_700ns = result["max_loopback_peak_tx_rate_700ns_gbps"]
    rate_700ns_after_200us = result[
        "max_loopback_peak_tx_rate_700ns_after_200us_gbps"]
    rate_100ns = result["max_loopback_peak_tx_rate_100ns_gbps"]
    peak_queue = result["max_loopback_peak_queue_bytes"]
    return {
        "part": task["part"],
        "sample_rate_percent": task["sample_rate"],
        "probe_instance_count": task["probe_instance_count"],
        "base_multiplier": task["multiplier"],
        "multiplier_divisor": task["multiplier_divisor"],
        "m1": task["m1"],
        "m2": task["m2"],
        "m3": task["m3"],
        "declared_flows": task["declared_flows"],
        "max_fct_ms": "" if max_fct_ns is None else "%.9f" % (max_fct_ns / 1e6),
        "max_loopback_peak_tx_rate_1us_gbps": (
            "" if rate_1us is None else "%.9f" % rate_1us
        ),
        "max_loopback_peak_tx_rate_1us_node": (
            "" if result["max_loopback_peak_tx_rate_1us_node"] is None else
            str(result["max_loopback_peak_tx_rate_1us_node"])
        ),
        "max_loopback_peak_tx_rate_700ns_gbps": (
            "" if rate_700ns is None else "%.9f" % rate_700ns
        ),
        "max_loopback_peak_tx_rate_700ns_node": (
            "" if result["max_loopback_peak_tx_rate_700ns_node"] is None else
            str(result["max_loopback_peak_tx_rate_700ns_node"])
        ),
        "max_loopback_peak_tx_rate_700ns_after_200us_gbps": (
            "" if rate_700ns_after_200us is None else
            "%.9f" % rate_700ns_after_200us
        ),
        "max_loopback_peak_tx_rate_700ns_after_200us_node": (
            "" if result[
                "max_loopback_peak_tx_rate_700ns_after_200us_node"] is None else
            str(result["max_loopback_peak_tx_rate_700ns_after_200us_node"])
        ),
        "max_loopback_peak_tx_rate_100ns_gbps": (
            "" if rate_100ns is None else "%.9f" % rate_100ns
        ),
        "max_loopback_peak_tx_rate_100ns_node": (
            "" if result["max_loopback_peak_tx_rate_100ns_node"] is None else
            str(result["max_loopback_peak_tx_rate_100ns_node"])
        ),
        "max_loopback_peak_queue_bytes": (
            "" if peak_queue is None else str(peak_queue)
        ),
        "max_loopback_peak_queue_node": (
            "" if result["max_loopback_peak_queue_node"] is None else
            str(result["max_loopback_peak_queue_node"])
        ),
        "status": result["status"],
        "returncode": result["returncode"],
        "elapsed_sec": "%.6f" % result["elapsed_sec"],
        "work_dir": task["work_dir"],
        "config_file": task["config_file"],
        "run_log": task["run_log"],
        "loopback_peak_bucket_file": task["loopback_peak_bucket_file"],
        "switch_qlen_trace_node": (
            "" if task["qlen_trace_node"] is None else str(task["qlen_trace_node"])
        ),
        "switch_qlen_trace_file": task["switch_qlen_trace_file"],
    }


def write_results(output_file, rows):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        if not rows:
            writer.writerow(["sample_rate_percent"])
            return

        part = rows[0]["part"]
        sample_rates = sorted({
            int(row["sample_rate_percent"]) for row in rows
        })
        if part == "a":
            series_values = sorted({
                int(row["probe_instance_count"]) for row in rows
            })
            series_labels = ["instances_%d" % value for value in series_values]

            def series_key(row):
                return int(row["probe_instance_count"])
        else:
            series_values = sorted({
                int(row["multiplier_divisor"]) for row in rows
            })
            series_labels = ["period_%d" % value for value in series_values]

            def series_key(row):
                return int(row["multiplier_divisor"])

        values = {}
        for row in rows:
            key = (int(row["sample_rate_percent"]), series_key(row))
            values[key] = row["max_loopback_peak_tx_rate_700ns_gbps"]

        writer.writerow(["sample_rate_percent"] + series_labels)
        for sample_rate in sample_rates:
            writer.writerow(
                [sample_rate] +
                [values.get((sample_rate, value), "")
                 for value in series_values])


def run_tasks(args, sim_dir, tasks, workers, output_files):
    rows = []
    statuses = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_task = {
            executor.submit(run_task, sim_dir, task, args.no_run): task
            for task in tasks
        }
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
            except Exception as exc:
                (task["work_dir"] / "wrapper_exception.txt").write_text("%s\n" % exc)
                result = {
                    "status": "exception",
                    "returncode": -1,
                    "elapsed_sec": 0.0,
                    "max_fct_ns": None,
                    "max_loopback_peak_tx_rate_1us_gbps": None,
                    "max_loopback_peak_tx_rate_1us_node": None,
                    "max_loopback_peak_tx_rate_700ns_gbps": None,
                    "max_loopback_peak_tx_rate_700ns_node": None,
                    "max_loopback_peak_tx_rate_700ns_after_200us_gbps": None,
                    "max_loopback_peak_tx_rate_700ns_after_200us_node": None,
                    "max_loopback_peak_tx_rate_100ns_gbps": None,
                    "max_loopback_peak_tx_rate_100ns_node": None,
                    "max_loopback_peak_queue_bytes": None,
                    "max_loopback_peak_queue_node": None,
                }
            statuses.append(result["status"])
            row = row_from_task_result(task, result)
            rows.append(row)

    for part, output_file in output_files.items():
        part_rows = [row for row in rows if row["part"] == part]
        write_results(output_file, part_rows)
    return statuses


def run_part(args, part, repo_root, figure_dir, sim_dir, work_root, output_file):
    tasks = build_tasks(args, repo_root, work_root, part)
    workers = min(args.jobs, len(tasks))
    return run_tasks(args, sim_dir, tasks, workers, {part: output_file})


def run_all_parts(args, repo_root, figure_dir, sim_dir, work_root):
    output_files = {}
    tasks = []
    for part in ("a", "b"):
        part_work_root = work_root / ("part_%s" % part)
        part_work_root.mkdir(parents=True, exist_ok=True)
        output_files[part] = (
            figure_dir / args.results_dir / ("Figure14(%s)_results.csv" % part)
        ).resolve()
        tasks.extend(build_tasks(args, repo_root, part_work_root, part))
    workers = min(args.jobs, len(tasks))
    return run_tasks(args, sim_dir, tasks, workers, output_files)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--part", choices=("a", "b", "all"), default="all",
                        help="which Figure 14 part to run; all runs a then b")
    parser.add_argument("--sample-rate", type=int, nargs="+",
                        default=list(DEFAULT_SAMPLE_RATES))
    parser.add_argument("--instance-count", type=int, nargs="+",
                        default=list(DEFAULT_INSTANCE_COUNTS))
    parser.add_argument("--multiplier", type=int, nargs="+",
                        default=list(DEFAULT_MULTIPLIERS))
    parser.add_argument("--multiplier-divisor", type=int, nargs="+",
                        default=list(DEFAULT_MULTIPLIER_DIVISORS),
                        help=("divisors applied to the base multiplier; "
                              "effective m values are ceil(base_m * "
                              "multiplier / divisor)"))
    parser.add_argument("--base-m1", type=int, default=10)
    parser.add_argument("--base-m2", type=int, default=10)
    parser.add_argument("--base-m3", type=int, default=1)
    parser.add_argument("--jobs", type=int, default=33)
    parser.add_argument("--work-dir", type=pathlib.Path, default=None,
                        help="optional directory for retaining generated configs/logs")
    parser.add_argument("--results-dir", type=pathlib.Path,
                        default=pathlib.Path("results"))
    parser.add_argument("--output-file", type=pathlib.Path, default=None)
    parser.add_argument("--stop-time", type=float, default=100.0)
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument("--buffer-size", type=int, default=128)
    parser.add_argument("--qlen-trace-node", type=int, default=None,
                        help=("switch node id to trace; when set, each task "
                              "writes switch qlen after that switch "
                              "receives its first packet"))
    parser.add_argument("--qlen-trace-from-peak700", action="store_true",
                        help=("use previous Figure14 peak700 node CSVs to choose "
                              "a qlen trace node per task"))
    parser.add_argument("--qlen-trace-peak-map-a", type=pathlib.Path, default=None,
                        help="part a CSV containing max_loopback_peak_tx_rate_700ns_node")
    parser.add_argument("--qlen-trace-peak-map-b", type=pathlib.Path, default=None,
                        help="part b CSV containing max_loopback_peak_tx_rate_700ns_node")
    parser.add_argument("--qlen-trace-duration-us", type=int, default=1000,
                        help="switch qlen trace duration after first packet")
    parser.add_argument("--qlen-trace-interval-ns", type=int, default=100,
                        help="switch qlen trace sampling interval in ns")
    parser.add_argument("--qlen-trace-interval-us", type=int, default=None,
                        help="deprecated; switch qlen trace sampling interval in us")
    parser.add_argument("--loopback-peak-bucket-interval-us", type=int, default=100,
                        help="bucket size for loopback peak-rate sidecar CSV")
    parser.add_argument("--initial-interpod-probe-spread-us", type=int, default=250,
                        help=("spread window for the initial cross-PoD OPP probe "
                              "wave; interval is window / 4 / m3, use 0 to disable"))
    parser.add_argument("--usm-sample-policy",
                        choices=("QP_ORDINAL", "TOR_GROUP_INTERVAL"),
                        default="TOR_GROUP_INTERVAL",
                        help=("QP_ORDINAL preserves the existing sampled-USM "
                              "marking; TOR_GROUP_INTERVAL samples every ToR "
                              "and traffic group with an interval accumulator"))
    parser.add_argument("--no-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.jobs <= 0:
        raise SystemExit("--jobs must be positive")
    if args.qlen_trace_node is not None and (
            args.qlen_trace_from_peak700 or
            args.qlen_trace_peak_map_a is not None or
            args.qlen_trace_peak_map_b is not None):
        raise SystemExit("--qlen-trace-node cannot be combined with peak700 node maps")
    if any(rate < 0 or rate > 100 for rate in args.sample_rate):
        raise SystemExit("--sample-rate values must be in [0, 100]")
    if any(instance_count <= 0 for instance_count in args.instance_count):
        raise SystemExit("--instance-count values must be positive")
    if any(multiplier <= 0 for multiplier in args.multiplier):
        raise SystemExit("--multiplier values must be positive")
    if any(divisor <= 0 for divisor in args.multiplier_divisor):
        raise SystemExit("--multiplier-divisor values must be positive")
    if args.base_m1 <= 0 or args.base_m2 <= 0 or args.base_m3 <= 0:
        raise SystemExit("--base-m1, --base-m2, and --base-m3 must be positive")
    if args.qlen_trace_node is not None and args.qlen_trace_node < 0:
        raise SystemExit("--qlen-trace-node must be non-negative")
    if args.qlen_trace_duration_us <= 0:
        raise SystemExit("--qlen-trace-duration-us must be positive")
    if args.qlen_trace_interval_ns <= 0:
        raise SystemExit("--qlen-trace-interval-ns must be positive")
    if args.qlen_trace_interval_us is not None and args.qlen_trace_interval_us <= 0:
        raise SystemExit("--qlen-trace-interval-us must be positive")
    if args.loopback_peak_bucket_interval_us <= 0:
        raise SystemExit("--loopback-peak-bucket-interval-us must be positive")
    if args.initial_interpod_probe_spread_us < 0:
        raise SystemExit("--initial-interpod-probe-spread-us must be non-negative")
    repo_root = repo_root_from_script()
    figure_dir = figure_dir_from_script()
    sim_dir = repo_root / "simulation"
    args.sim_dir = sim_dir

    if args.qlen_trace_from_peak700:
        if args.qlen_trace_peak_map_a is None:
            args.qlen_trace_peak_map_a = (
                figure_dir / "results" / "Figure14a_peak700_node_qlen.csv")
        if args.qlen_trace_peak_map_b is None:
            args.qlen_trace_peak_map_b = (
                figure_dir / "results" / "Figure14b_peak700_node_qlen.csv")

    args.qlen_trace_node_maps = {}
    if args.qlen_trace_peak_map_a is not None:
        if not args.qlen_trace_peak_map_a.exists():
            raise FileNotFoundError("missing qlen trace peak map: %s" %
                                    args.qlen_trace_peak_map_a)
        args.qlen_trace_node_maps["a"] = load_peak700_node_map(
            args.qlen_trace_peak_map_a, "a")
    if args.qlen_trace_peak_map_b is not None:
        if not args.qlen_trace_peak_map_b.exists():
            raise FileNotFoundError("missing qlen trace peak map: %s" %
                                    args.qlen_trace_peak_map_b)
        args.qlen_trace_node_maps["b"] = load_peak700_node_map(
            args.qlen_trace_peak_map_b, "b")

    if args.part == "all" and args.output_file is not None:
        raise SystemExit("--output-file can only be used with --part a or --part b")

    temp_work_dir = None
    if args.work_dir is None:
        temp_work_dir = tempfile.TemporaryDirectory(prefix="figure14_")
        work_root = pathlib.Path(temp_work_dir.name).resolve()
    else:
        work_root = args.work_dir.resolve()
    work_root.mkdir(parents=True, exist_ok=True)

    try:
        if args.part == "all":
            all_statuses = run_all_parts(
                args, repo_root, figure_dir, sim_dir, work_root)
        else:
            part_work_root = work_root
            part_work_root.mkdir(parents=True, exist_ok=True)
            output_file = (
                figure_dir / (
                    args.output_file if args.output_file is not None else
                    args.results_dir / ("Figure14(%s)_results.csv" % args.part)
                )
            ).resolve()
            all_statuses = run_part(
                args, args.part, repo_root, figure_dir, sim_dir,
                part_work_root, output_file)
        return 1 if any(status not in ("ok", "generated") for status in all_statuses) else 0
    finally:
        if temp_work_dir is not None:
            temp_work_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
