#!/usr/bin/env python3
"""Run Figure 10(d): 3D torus OPP sampled-USM completion time vs m.

The output CSV is a compact matrix:

    series,1,2,...,8
    3x3x3,<max_fct_ms for m=1>,...
    ...
"""

import argparse
import csv
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


DEFAULT_K = (3, 4, 5, 6)
DEFAULT_M = tuple(range(1, 9))
DEFAULT_OUTPUT_FILE = "Figure10(d)_results.csv"


def repo_root_from_script():
    return pathlib.Path(__file__).resolve().parents[3]


def figure_dir_from_script():
    return pathlib.Path(__file__).resolve().parents[1]


def write_trace_file(path):
    path.write_text("0\n")


def read_declared_flow_count(flow_file):
    with flow_file.open() as f:
        return int(f.readline().strip())


def write_config(path, args, task, trace_file):
    work_dir = task["work_dir"]
    lines = [
        "ENABLE_QCN 1",
        "USE_DYNAMIC_PFC_THRESHOLD 1",
        "PACKET_PAYLOAD_SIZE 1000",
        "TOPOLOGY_FILE %s" % task["topology_file"],
        "TOPOLOGY_MODE 3D_TORUS",
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
        "OPP_USM_SAMPLE_PERIOD 10",
        "OPP_TOKENS_PER_TOR %d" % task["m"],
        "OPP_FATTREE_M1 %d" % task["m"],
        "OPP_FATTREE_M2 %d" % task["m"],
        "OPP_FATTREE_M3 %d" % task["m"],
        "OPP_LEAFSPINE_M1 %d" % task["m"],
        "OPP_LEAFSPINE_M2 %d" % task["m"],
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


def run_task(sim_dir, task, no_run):
    if no_run:
        return {
            "status": "generated",
            "returncode": 0,
            "elapsed_sec": 0.0,
            "max_fct_ns": None,
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
        }

    max_fct_ns = parse_max_fct_ns(task["fct_file"])
    if max_fct_ns is None:
        return {
            "status": "missing_fct",
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "max_fct_ns": None,
        }

    return {
        "status": "ok",
        "returncode": proc.returncode,
        "elapsed_sec": elapsed,
        "max_fct_ns": max_fct_ns,
    }


def build_tasks(args, repo_root, figure_dir, work_root):
    trace_file = work_root / "trace.txt"
    write_trace_file(trace_file)

    tasks = []
    for k in args.k:
        topo_key = "%dx%dx%d" % (k, k, k)
        topology_file = (
            repo_root / "experiment_reproduction" / "topologies" / "3d_torus" /
            ("torus_%s_topology.txt" % topo_key)
        )
        flow_file = figure_dir / "traffic_load" / "3d_torus" / topo_key / "flow_2e0.txt"
        if not topology_file.exists():
            raise FileNotFoundError("missing topology file: %s" % topology_file)
        if not flow_file.exists():
            raise FileNotFoundError("missing flow file: %s" % flow_file)
        declared_flows = read_declared_flow_count(flow_file)

        for m in args.m:
            task_name = "%s_m%d" % (topo_key, m)
            task_work_dir = work_root / task_name
            task_work_dir.mkdir(parents=True, exist_ok=True)
            task = {
                "series": topo_key,
                "k": k,
                "m": m,
                "declared_flows": declared_flows,
                "work_dir": task_work_dir,
                "topology_file": topology_file.resolve(),
                "flow_file": flow_file.resolve(),
                "config_file": task_work_dir / "config.txt",
                "run_log": task_work_dir / "run.log",
                "fct_file": task_work_dir / "fct.txt",
            }
            write_config(task["config_file"], args, task, trace_file.resolve())
            tasks.append(task)
    return tasks


def row_from_task_result(task, result):
    max_fct_ns = result["max_fct_ns"]
    return {
        "series": task["series"],
        "k": task["k"],
        "m": task["m"],
        "declared_flows": task["declared_flows"],
        "max_fct_ms": "" if max_fct_ns is None else "%.9f" % (max_fct_ns / 1e6),
        "status": result["status"],
        "returncode": result["returncode"],
        "elapsed_sec": "%.6f" % result["elapsed_sec"],
        "work_dir": task["work_dir"],
        "config_file": task["config_file"],
        "run_log": task["run_log"],
    }


def write_matrix(output_file, rows):
    m_values = sorted({int(row["m"]) for row in rows})
    series_order = []
    by_series_m = {}
    for row in rows:
        series = row["series"]
        if series not in series_order:
            series_order.append(series)
        by_series_m[(series, int(row["m"]))] = row["max_fct_ms"]

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["series"] + [str(m) for m in m_values])
        for series in series_order:
            writer.writerow([series] + [by_series_m.get((series, m), "") for m in m_values])


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--k", type=int, nargs="+", default=list(DEFAULT_K),
                        help="3D torus dimensions; k means k x k x k")
    parser.add_argument("--m", type=int, nargs="+", default=list(DEFAULT_M),
                        help="OPP_TOKENS_PER_TOR values to sweep")
    parser.add_argument("--jobs", type=int, default=16)
    parser.add_argument("--work-dir", type=pathlib.Path, default=None,
                        help="optional directory for retaining generated configs/logs")
    parser.add_argument("--output-file", type=pathlib.Path,
                        default=pathlib.Path("results") / DEFAULT_OUTPUT_FILE)
    parser.add_argument("--stop-time", type=float, default=100.0)
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument("--buffer-size", type=int, default=128)
    parser.add_argument("--no-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.jobs <= 0:
        raise SystemExit("--jobs must be positive")
    if any(k <= 0 for k in args.k):
        raise SystemExit("--k values must be positive")
    if any(m <= 0 for m in args.m):
        raise SystemExit("--m values must be positive")

    repo_root = repo_root_from_script()
    figure_dir = figure_dir_from_script()
    sim_dir = repo_root / "simulation"
    args.sim_dir = sim_dir
    output_file = (figure_dir / args.output_file).resolve()

    temp_work_dir = None
    if args.work_dir is None:
        temp_work_dir = tempfile.TemporaryDirectory(prefix="figure10d_")
        work_root = pathlib.Path(temp_work_dir.name).resolve()
    else:
        work_root = args.work_dir.resolve()
    work_root.mkdir(parents=True, exist_ok=True)

    try:
        tasks = build_tasks(args, repo_root, figure_dir, work_root)
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
                    }
                statuses.append(result["status"])
                row = row_from_task_result(task, result)
                rows.append(row)

        rows.sort(key=lambda row: (int(row["k"]), int(row["m"])))
        write_matrix(output_file, rows)
        return 1 if any(status not in ("ok", "generated") for status in statuses) else 0
    finally:
        if temp_work_dir is not None:
            temp_work_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
