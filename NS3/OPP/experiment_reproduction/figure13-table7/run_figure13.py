#!/usr/bin/env python3
"""Run Figure 13 metrics and Table 7 under sampled-USM rates.

The runner reuses the Figure 9-10 single-packet traffic loads and evaluates:

    - Fat-tree k=64
    - Leaf-spine k=64
    - 2D torus 10x10
    - 3D torus 6x6x6

For each topology it runs OPP sampled USM at 0%, 10%, 20%, ..., 100%.
The runner writes one Figure 13(a) CSV for max FCT:

    sample_rate_percent,fattree_k64,leaf_spine_k64,2d_torus_10x10,3d_torus_6x6x6
    0,<max_fct_ms>,...
    ...

It also writes one Figure 13(b) CSV for the maximum per-switch loopback RX probe count:

    sample_rate_percent,fattree_k64,leaf_spine_k64,2d_torus_10x10,3d_torus_6x6x6

For the 100% sampled-USM cases, the runner enables detailed in-switch USM
copy-latency logging and writes Table 7:

    topology,count,avg_ns,p50_ns,p95_ns,p99_ns
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


DEFAULT_SAMPLE_RATES = tuple(range(0, 101, 10))
TOPOLOGY_ORDER = ("fattree_k64", "leaf_spine_k64", "2d_torus_10x10", "3d_torus_6x6x6")


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


def config_common_lines(args, task, trace_file):
    work_dir = task["work_dir"]
    return [
        "ENABLE_QCN 1",
        "USE_DYNAMIC_PFC_THRESHOLD 1",
        "PACKET_PAYLOAD_SIZE 1000",
        "TOPOLOGY_FILE %s" % task["topology_file"],
        "TOPOLOGY_MODE %s" % task["topology_mode"],
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
        "OPP_USM_COPY_LATENCY_DETAIL %d" % (
            1 if task["sample_rate"] == 100 else 0),
        "OPP_TOKENS_PER_TOR %d" % task["tokens_per_tor"],
        "OPP_FATTREE_M1 %d" % task["m1"],
        "OPP_FATTREE_M2 %d" % task["m2"],
        "OPP_FATTREE_M3 %d" % task["m3"],
        "OPP_LEAFSPINE_M1 %d" % task["leaf_m1"],
        "OPP_LEAFSPINE_M2 %d" % task["leaf_m2"],
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
    ]


def write_config(path, args, task, trace_file):
    lines = config_common_lines(args, task, trace_file)
    if task["topology_mode"] == "FAT_TREE":
        lines.insert(5, "FAT_TREE_K %d" % task["k"])
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
    max_rx_probe_count = None
    max_rx_probe_count_node = None
    max_peak_tx_rate_1us_gbps = None
    max_peak_tx_rate_1us_node = None
    max_peak_tx_rate_700ns_gbps = None
    max_peak_tx_rate_700ns_node = None
    max_peak_queue_bytes = None
    max_peak_queue_node = None

    if not run_log.exists():
        return {
            "max_loopback_rx_probe_count": None,
            "max_loopback_rx_probe_count_node": None,
            "max_loopback_peak_tx_rate_1us_gbps": None,
            "max_loopback_peak_tx_rate_1us_node": None,
            "max_loopback_peak_tx_rate_700ns_gbps": None,
            "max_loopback_peak_tx_rate_700ns_node": None,
            "max_loopback_peak_queue_bytes": None,
            "max_loopback_peak_queue_node": None,
        }

    with run_log.open() as f:
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

            try:
                rx_probe_count = int(values["rx_probe_count"])
            except (KeyError, ValueError):
                rx_probe_count = None
            if (rx_probe_count is not None and
                    (max_rx_probe_count is None or rx_probe_count > max_rx_probe_count)):
                max_rx_probe_count = rx_probe_count
                max_rx_probe_count_node = node

            try:
                peak_tx_rate_1us_gbps = float(values["peak_tx_rate_1us_gbps"])
            except (KeyError, ValueError):
                peak_tx_rate_1us_gbps = None
            if (peak_tx_rate_1us_gbps is not None and
                    (max_peak_tx_rate_1us_gbps is None or
                     peak_tx_rate_1us_gbps > max_peak_tx_rate_1us_gbps)):
                max_peak_tx_rate_1us_gbps = peak_tx_rate_1us_gbps
                max_peak_tx_rate_1us_node = node

            try:
                peak_tx_rate_700ns_gbps = float(values["peak_tx_rate_700ns_gbps"])
            except (KeyError, ValueError):
                peak_tx_rate_700ns_gbps = None
            if (peak_tx_rate_700ns_gbps is not None and
                    (max_peak_tx_rate_700ns_gbps is None or
                     peak_tx_rate_700ns_gbps > max_peak_tx_rate_700ns_gbps)):
                max_peak_tx_rate_700ns_gbps = peak_tx_rate_700ns_gbps
                max_peak_tx_rate_700ns_node = node

            try:
                peak_queue_bytes = int(values["peak_queue_bytes"])
            except (KeyError, ValueError):
                peak_queue_bytes = None
            if (peak_queue_bytes is not None and
                    (max_peak_queue_bytes is None or
                     peak_queue_bytes > max_peak_queue_bytes)):
                max_peak_queue_bytes = peak_queue_bytes
                max_peak_queue_node = node

    return {
        "max_loopback_rx_probe_count": max_rx_probe_count,
        "max_loopback_rx_probe_count_node": max_rx_probe_count_node,
        "max_loopback_peak_tx_rate_1us_gbps": max_peak_tx_rate_1us_gbps,
        "max_loopback_peak_tx_rate_1us_node": max_peak_tx_rate_1us_node,
        "max_loopback_peak_tx_rate_700ns_gbps": max_peak_tx_rate_700ns_gbps,
        "max_loopback_peak_tx_rate_700ns_node": max_peak_tx_rate_700ns_node,
        "max_loopback_peak_queue_bytes": max_peak_queue_bytes,
        "max_loopback_peak_queue_node": max_peak_queue_node,
    }


def parse_usm_copy_latency_detail(run_log):
    latencies_ns = []
    if not run_log.exists():
        return {
            "usm_copy_latency_count": 0,
            "usm_copy_latency_avg_ns": None,
            "usm_copy_latency_p50_ns": None,
            "usm_copy_latency_p95_ns": None,
            "usm_copy_latency_p99_ns": None,
        }

    with run_log.open() as f:
        for line in f:
            fields = line.split()
            if not fields or fields[0] != "OPP_USM_COPY_LATENCY":
                continue

            values = {}
            i = 1
            while i + 1 < len(fields):
                values[fields[i]] = fields[i + 1]
                i += 2

            try:
                latencies_ns.append(int(values["latency_ns"]))
            except (KeyError, ValueError):
                continue

    if not latencies_ns:
        return {
            "usm_copy_latency_count": 0,
            "usm_copy_latency_avg_ns": None,
            "usm_copy_latency_p50_ns": None,
            "usm_copy_latency_p95_ns": None,
            "usm_copy_latency_p99_ns": None,
        }

    latencies_ns.sort()
    count = len(latencies_ns)

    def nearest_rank(percentile):
        index = int(math.ceil(percentile * count)) - 1
        index = min(max(index, 0), count - 1)
        return latencies_ns[index]

    return {
        "usm_copy_latency_count": count,
        "usm_copy_latency_avg_ns": sum(latencies_ns) / float(count),
        "usm_copy_latency_p50_ns": nearest_rank(0.50),
        "usm_copy_latency_p95_ns": nearest_rank(0.95),
        "usm_copy_latency_p99_ns": nearest_rank(0.99),
    }


def run_task(sim_dir, task, no_run):
    if no_run:
        return {
            "status": "generated",
            "returncode": 0,
            "elapsed_sec": 0.0,
            "max_fct_ns": None,
            **parse_loopback_stats(task["run_log"]),
            **parse_usm_copy_latency_detail(task["run_log"]),
        }

    started = time.time()
    cmd = ["python2", "./waf", "--run", "scratch/third %s" % path_relative_to(task["config_file"], sim_dir)]
    with task["run_log"].open("w") as log:
        proc = subprocess.run(cmd, cwd=str(sim_dir), stdout=log, stderr=subprocess.STDOUT)
    elapsed = time.time() - started

    if proc.returncode != 0:
        return {
            "status": "failed",
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "max_fct_ns": None,
            **parse_loopback_stats(task["run_log"]),
            **parse_usm_copy_latency_detail(task["run_log"]),
        }

    max_fct_ns = parse_max_fct_ns(task["fct_file"])
    loopback_stats = parse_loopback_stats(task["run_log"])
    if max_fct_ns is None:
        return {
            "status": "missing_fct",
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "max_fct_ns": None,
            **loopback_stats,
            **parse_usm_copy_latency_detail(task["run_log"]),
        }

    return {
        "status": "ok",
        "returncode": proc.returncode,
        "elapsed_sec": elapsed,
        "max_fct_ns": max_fct_ns,
        **loopback_stats,
        **parse_usm_copy_latency_detail(task["run_log"]),
    }


def topology_specs(repo_root, traffic_root, args):
    return [
        {
            "topology": "fattree_k64",
            "topology_mode": "FAT_TREE",
            "k": 64,
            "topology_file": (
                repo_root / "experiment_reproduction" / "topologies" /
                "fattree" / "fattree_k64_topology.txt"
            ),
            "flow_file": traffic_root / "fattree" / "k64" / "flow_2e0.txt",
            "tokens_per_tor": args.fattree_base_m3,
            "m1": args.fattree_base_m1,
            "m2": args.fattree_base_m2,
            "m3": args.fattree_base_m3,
            "leaf_m1": args.fattree_base_m1,
            "leaf_m2": args.fattree_base_m2,
        },
        {
            "topology": "leaf_spine_k64",
            "topology_mode": "LEAF_SPINE",
            "k": 64,
            "topology_file": (
                repo_root / "experiment_reproduction" / "topologies" /
                "leaf_spine" / "leaf_spine_k64_topology.txt"
            ),
            "flow_file": traffic_root / "leaf_spine" / "k64" / "flow_2e0.txt",
            "tokens_per_tor": args.leaf_base_m2,
            "m1": args.leaf_base_m1,
            "m2": args.leaf_base_m2,
            "m3": args.leaf_base_m2,
            "leaf_m1": args.leaf_base_m1,
            "leaf_m2": args.leaf_base_m2,
        },
        {
            "topology": "2d_torus_10x10",
            "topology_mode": "2D_TORUS",
            "k": 10,
            "topology_file": (
                repo_root / "experiment_reproduction" / "topologies" /
                "2d_torus" / "torus_10x10_topology.txt"
            ),
            "flow_file": traffic_root / "2d_torus" / "10x10" / "flow_2e0.txt",
            "tokens_per_tor": args.torus_base_m,
            "m1": args.torus_base_m,
            "m2": args.torus_base_m,
            "m3": args.torus_base_m,
            "leaf_m1": args.torus_base_m,
            "leaf_m2": args.torus_base_m,
        },
        {
            "topology": "3d_torus_6x6x6",
            "topology_mode": "3D_TORUS",
            "k": 6,
            "topology_file": (
                repo_root / "experiment_reproduction" / "topologies" /
                "3d_torus" / "torus_6x6x6_topology.txt"
            ),
            "flow_file": traffic_root / "3d_torus" / "6x6x6" / "flow_2e0.txt",
            "tokens_per_tor": args.torus_base_m,
            "m1": args.torus_base_m,
            "m2": args.torus_base_m,
            "m3": args.torus_base_m,
            "leaf_m1": args.torus_base_m,
            "leaf_m2": args.torus_base_m,
        },
    ]


def build_tasks(args, repo_root, work_root):
    traffic_root = repo_root / "experiment_reproduction" / "figure9-10" / "traffic_load"
    trace_file = work_root / "trace.txt"
    write_trace_file(trace_file)

    tasks = []
    for spec in topology_specs(repo_root, traffic_root, args):
        if not spec["topology_file"].exists():
            raise FileNotFoundError("missing topology file: %s" % spec["topology_file"])
        if not spec["flow_file"].exists():
            raise FileNotFoundError("missing flow file: %s" % spec["flow_file"])
        declared_flows = read_declared_flow_count(spec["flow_file"])

        for sample_rate in args.sample_rate:
            numerator, denominator = sample_rate_fraction(sample_rate)
            task_name = "%s_sample_%03d" % (spec["topology"], sample_rate)
            task_work_dir = work_root / task_name
            task_work_dir.mkdir(parents=True, exist_ok=True)
            task = dict(spec)
            task.update({
                "sample_rate": sample_rate,
                "sample_numerator": numerator,
                "sample_denominator": denominator,
                "declared_flows": declared_flows,
                "work_dir": task_work_dir,
                "topology_file": spec["topology_file"].resolve(),
                "flow_file": spec["flow_file"].resolve(),
                "config_file": task_work_dir / "config.txt",
                "run_log": task_work_dir / "run.log",
                "fct_file": task_work_dir / "fct.txt",
            })
            write_config(task["config_file"], args, task, trace_file.resolve())
            tasks.append(task)
    return tasks


def row_from_task_result(task, result):
    max_fct_ns = result["max_fct_ns"]
    max_loopback_rx_probe_count = result["max_loopback_rx_probe_count"]
    max_loopback_peak_tx_rate_1us_gbps = result["max_loopback_peak_tx_rate_1us_gbps"]
    max_loopback_peak_tx_rate_700ns_gbps = result["max_loopback_peak_tx_rate_700ns_gbps"]
    max_loopback_peak_queue_bytes = result["max_loopback_peak_queue_bytes"]
    return {
        "topology": task["topology"],
        "sample_rate_percent": task["sample_rate"],
        "sample_rate": "%d/%d" % (task["sample_numerator"], task["sample_denominator"]),
        "declared_flows": task["declared_flows"],
        "max_fct_ms": "" if max_fct_ns is None else "%.9f" % (max_fct_ns / 1e6),
        "max_loopback_rx_probe_count": (
            "" if max_loopback_rx_probe_count is None else
            str(max_loopback_rx_probe_count)
        ),
        "max_loopback_rx_probe_count_node": (
            "" if result["max_loopback_rx_probe_count_node"] is None else
            str(result["max_loopback_rx_probe_count_node"])
        ),
        "max_loopback_peak_tx_rate_1us_gbps": (
            "" if max_loopback_peak_tx_rate_1us_gbps is None else
            "%.9f" % max_loopback_peak_tx_rate_1us_gbps
        ),
        "max_loopback_peak_tx_rate_1us_node": (
            "" if result["max_loopback_peak_tx_rate_1us_node"] is None else
            str(result["max_loopback_peak_tx_rate_1us_node"])
        ),
        "max_loopback_peak_tx_rate_700ns_gbps": (
            "" if max_loopback_peak_tx_rate_700ns_gbps is None else
            "%.9f" % max_loopback_peak_tx_rate_700ns_gbps
        ),
        "max_loopback_peak_tx_rate_700ns_node": (
            "" if result["max_loopback_peak_tx_rate_700ns_node"] is None else
            str(result["max_loopback_peak_tx_rate_700ns_node"])
        ),
        "max_loopback_peak_queue_bytes": (
            "" if max_loopback_peak_queue_bytes is None else
            str(max_loopback_peak_queue_bytes)
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
        "usm_copy_latency_count": result.get("usm_copy_latency_count", 0),
        "usm_copy_latency_avg_ns": result.get("usm_copy_latency_avg_ns"),
        "usm_copy_latency_p50_ns": result.get("usm_copy_latency_p50_ns"),
        "usm_copy_latency_p95_ns": result.get("usm_copy_latency_p95_ns"),
        "usm_copy_latency_p99_ns": result.get("usm_copy_latency_p99_ns"),
    }


def write_result_files(results_dir, rows):
    results_dir.mkdir(parents=True, exist_ok=True)
    values = {}
    sample_rates = set()
    for row in rows:
        topology = row["topology"]
        sample_rate = int(row["sample_rate_percent"])
        sample_rates.add(sample_rate)
        values[(topology, sample_rate)] = row

    output_files = []

    output_file = results_dir / "Figure13(a)_results.csv"
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sample_rate_percent"] + list(TOPOLOGY_ORDER))
        for sample_rate in sorted(sample_rates):
            writer.writerow([
                sample_rate,
                *[
                    values.get((topology, sample_rate), {}).get("max_fct_ms", "")
                    for topology in TOPOLOGY_ORDER
                ],
            ])
    output_files.append(output_file)

    output_file = results_dir / "Figure13(b)_results.csv"
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sample_rate_percent"] + list(TOPOLOGY_ORDER))
        for sample_rate in sorted(sample_rates):
            writer.writerow([
                sample_rate,
                *[
                    values.get((topology, sample_rate), {}).get(
                        "max_loopback_rx_probe_count", "")
                    for topology in TOPOLOGY_ORDER
                ],
            ])
    output_files.append(output_file)

    output_file = results_dir / "table7.csv"
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["topology", "count", "avg_ns", "p50_ns", "p95_ns", "p99_ns"])
        for topology in TOPOLOGY_ORDER:
            row = values.get((topology, 100), {})
            avg_ns = row.get("usm_copy_latency_avg_ns")
            p50_ns = row.get("usm_copy_latency_p50_ns")
            p95_ns = row.get("usm_copy_latency_p95_ns")
            p99_ns = row.get("usm_copy_latency_p99_ns")
            writer.writerow([
                topology,
                row.get("usm_copy_latency_count", ""),
                "" if avg_ns is None else "%.6f" % avg_ns,
                "" if p50_ns is None else str(p50_ns),
                "" if p95_ns is None else str(p95_ns),
                "" if p99_ns is None else str(p99_ns),
            ])
    output_files.append(output_file)

    return output_files


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=12)
    parser.add_argument("--work-dir", type=pathlib.Path, default=None,
                        help="optional directory for retaining generated configs/logs")
    parser.add_argument("--results-dir", type=pathlib.Path,
                        default=pathlib.Path("results"))
    parser.add_argument("--stop-time", type=float, default=100.0)
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument("--buffer-size", type=int, default=128)
    parser.add_argument("--sample-rate", type=int, nargs="+",
                        default=list(DEFAULT_SAMPLE_RATES),
                        help="sample-rate percentages to run")
    parser.add_argument("--fattree-base-m1", type=int, default=10)
    parser.add_argument("--fattree-base-m2", type=int, default=10)
    parser.add_argument("--fattree-base-m3", type=int, default=1)
    parser.add_argument("--leaf-base-m1", type=int, default=10)
    parser.add_argument("--leaf-base-m2", type=int, default=10)
    parser.add_argument("--torus-base-m", type=int, default=1)
    parser.add_argument("--no-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.jobs <= 0:
        raise SystemExit("--jobs must be positive")
    if any(rate < 0 or rate > 100 for rate in args.sample_rate):
        raise SystemExit("--sample-rate values must be in [0, 100]")
    args.sample_rate = sorted(set(args.sample_rate))
    for name in ("fattree_base_m1", "fattree_base_m2", "fattree_base_m3",
                 "leaf_base_m1", "leaf_base_m2", "torus_base_m"):
        if getattr(args, name) <= 0:
            raise SystemExit("--%s must be positive" % name.replace("_", "-"))

    repo_root = repo_root_from_script()
    figure_dir = figure_dir_from_script()
    sim_dir = repo_root / "simulation"
    args.sim_dir = sim_dir
    results_dir = (figure_dir / args.results_dir).resolve()

    temp_work_dir = None
    if args.work_dir is None:
        temp_work_dir = tempfile.TemporaryDirectory(prefix="figure13_")
        work_root = pathlib.Path(temp_work_dir.name).resolve()
    else:
        work_root = args.work_dir.resolve()
    work_root.mkdir(parents=True, exist_ok=True)

    try:
        tasks = build_tasks(args, repo_root, work_root)
        workers = min(args.jobs, len(tasks))

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
                        **parse_loopback_stats(task["run_log"]),
                        **parse_usm_copy_latency_detail(task["run_log"]),
                    }
                statuses.append(result["status"])
                row = row_from_task_result(task, result)
                rows.append(row)

        rows.sort(key=lambda row: (
            TOPOLOGY_ORDER.index(row["topology"]),
            int(row["sample_rate_percent"]),
        ))
        write_result_files(results_dir, rows)
        return 1 if any(status not in ("ok", "generated") for status in statuses) else 0
    finally:
        if temp_work_dir is not None:
            temp_work_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
