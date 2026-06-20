#!/usr/bin/env python3
"""Run Figure 5 3D-torus coverage sweeps.

The script evaluates RPINGMESH, RDPROBE, and OPP on 3D torus topologies using
the pre-generated `traffic_load/3d_torus/` source-limited flow files. It runs all required ns-3
jobs concurrently by default and writes one coverage-matrix CSV whose columns
are packet counts and whose rows are mode/topology series. For OPP, only the
single-packet run is executed; larger packet-count rows copy the single-packet
coverage result because OPP deterministically covers all legal ports in this
experiment.
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


MODE_TO_CC = {
    "RPINGMESH": 11,
    "OPP": 12,
    "RDPROBE": 13,
}

DEFAULT_MODES = ("RDPROBE", "RPINGMESH", "OPP")
OPP_SOURCE_PACKET_EXP = 0
DEFAULT_OUTPUT_FILE = "Figure5(d)_results.csv"


def repo_root_from_script():
    return pathlib.Path(__file__).resolve().parents[3]


def write_trace_file(path):
    path.write_text("0\n")


def write_config(path, args, task, topology_file, flow_file, trace_file):
    mode = task["mode"]
    cc_mode = MODE_TO_CC[mode]
    work_dir = task["work_dir"]
    opp_multicast_mode = args.opp_multicast_mode if mode == "OPP" else "STANDARD"

    lines = [
        "ENABLE_QCN 1",
        "USE_DYNAMIC_PFC_THRESHOLD 1",
        "PACKET_PAYLOAD_SIZE 1000",
        "TOPOLOGY_FILE %s" % topology_file,
        "TOPOLOGY_MODE 3D_TORUS",
        "FLOW_FILE %s" % flow_file,
        "TRACE_FILE %s" % trace_file,
        "TRACE_OUTPUT_FILE %s" % (work_dir / "trace.tr"),
        "FCT_OUTPUT_FILE %s" % (work_dir / "fct.txt"),
        "PFC_OUTPUT_FILE %s" % (work_dir / "pfc.txt"),
        "FLOW_COVERAGE_OUTPUT_FILE %s" % task["coverage_file"],
        "SIMULATOR_STOP_TIME %.9f" % args.stop_time,
        "CC_MODE %d" % cc_mode,
        "ROUTING_MODE PACKET_SPRAY",
        "OPP_MULTICAST_MODE %s" % opp_multicast_mode,
        "OPP_USM_SAMPLE_PERIOD %d" % args.opp_usm_sample_period,
        "OPP_TOKENS_PER_TOR %d" % args.opp_m,
        "OPP_FATTREE_M1 %d" % args.opp_m1,
        "OPP_FATTREE_M2 %d" % args.opp_m2,
        "OPP_FATTREE_M3 %d" % args.opp_m3,
        "OPP_LEAFSPINE_M1 %d" % args.opp_m1,
        "OPP_LEAFSPINE_M2 %d" % args.opp_m2,
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


def read_declared_flow_count(flow_file):
    with flow_file.open() as f:
        first = f.readline().strip()
    return int(first)


def parse_average_coverage(coverage_file):
    total = 0.0
    rows = 0
    observed_total = 0
    possible_total = 0
    with coverage_file.open() as f:
        for line in f:
            fields = line.split()
            if len(fields) < 8:
                continue
            observed_total += int(fields[5])
            possible_total += int(fields[6])
            total += float(fields[7])
            rows += 1
    if rows == 0:
        return {
            "flow_rows": 0,
            "avg_coverage": "",
            "observed_links_total": observed_total,
            "possible_links_total": possible_total,
        }
    return {
        "flow_rows": rows,
        "avg_coverage": total / rows,
        "observed_links_total": observed_total,
        "possible_links_total": possible_total,
    }


def run_task(sim_dir, task, no_run):
    started = time.time()
    if no_run:
        return {
            "status": "generated",
            "returncode": 0,
            "elapsed_sec": 0.0,
            "coverage": {
                "flow_rows": "",
                "avg_coverage": "",
                "observed_links_total": "",
                "possible_links_total": "",
            },
        }

    cmd = ["python2", "./waf", "--run", "scratch/third %s" % path_relative_to(task["config_file"], sim_dir)]
    with task["run_log"].open("w") as log:
        proc = subprocess.run(cmd, cwd=str(sim_dir), stdout=log, stderr=subprocess.STDOUT)
    elapsed = time.time() - started

    if proc.returncode != 0:
        return {
            "status": "failed",
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "coverage": {
                "flow_rows": "",
                "avg_coverage": "",
                "observed_links_total": "",
                "possible_links_total": "",
            },
        }

    coverage = parse_average_coverage(task["coverage_file"])
    return {
        "status": "ok",
        "returncode": proc.returncode,
        "elapsed_sec": elapsed,
        "coverage": coverage,
    }


def write_coverage_matrix(output_file, rows):
    packet_counts = sorted({int(row["packet_count"]) for row in rows})
    series_order = []
    by_series_packet = {}
    for row in rows:
        series = row["series"]
        if series not in series_order:
            series_order.append(series)
        by_series_packet[(series, int(row["packet_count"]))] = row["avg_coverage"]

    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["series"] + [str(packet_count) for packet_count in packet_counts])
        for series in series_order:
            writer.writerow(
                [series] + [by_series_packet.get((series, packet_count), "")
                            for packet_count in packet_counts])


def build_tasks(args, repo_root, figure5_dir, work_root):
    tasks = []
    trace_file = work_root / "trace.txt"
    write_trace_file(trace_file)

    for k in args.k:
        topology = "%dx%dx%d" % (k, k, k)
        topology_file = repo_root / "experiment_reproduction" / "topologies" / "3d_torus" / (
            "torus_%s_topology.txt" % topology)
        if not topology_file.exists():
            raise FileNotFoundError("missing topology file: %s" % topology_file)

        flow_dir = figure5_dir / "traffic_load" / "3d_torus" / topology
        if not flow_dir.exists():
            raise FileNotFoundError("missing flow directory: %s" % flow_dir)

        for mode in args.mode:
            for exp in range(args.min_exp, args.max_exp + 1):
                flow_file = flow_dir / ("flow_2e%d.txt" % exp)
                if not flow_file.exists():
                    raise FileNotFoundError("missing flow file: %s" % flow_file)

                task_name = "%s_%s_2e%d" % (mode.lower(), topology, exp)
                task_work_dir = work_root / task_name
                task_work_dir.mkdir(parents=True, exist_ok=True)
                task = {
                    "series": "%s_%s" % (mode, topology),
                    "topology": topology,
                    "k": k,
                    "mode": mode,
                    "packet_exp": exp,
                    "packet_count": 1 << exp,
                    "declared_flows": read_declared_flow_count(flow_file),
                    "work_dir": task_work_dir,
                    "topology_file": topology_file.resolve(),
                    "flow_file": flow_file.resolve(),
                    "config_file": task_work_dir / "config.txt",
                    "coverage_file": task_work_dir / "flow_coverage.txt",
                    "run_log": task_work_dir / "run.log",
                    "copy_from_packet_exp": (
                        OPP_SOURCE_PACKET_EXP
                        if mode == "OPP" and exp > OPP_SOURCE_PACKET_EXP
                        else None),
                }
                write_config(
                    task["config_file"], args, task,
                    task["topology_file"], task["flow_file"], trace_file.resolve())
                tasks.append(task)
    return tasks


def row_from_task_result(task, result, status_override=None, elapsed_override=None):
    coverage = result["coverage"]
    avg = coverage["avg_coverage"]
    return {
        "series": task["series"],
        "topology": task["topology"],
        "k": task["k"],
        "mode": task["mode"],
        "packet_exp": task["packet_exp"],
        "packet_count": task["packet_count"],
        "declared_flows": task["declared_flows"],
        "coverage_flow_rows": coverage["flow_rows"],
        "avg_coverage": "" if avg == "" else "%.9f" % avg,
        "observed_links_total": coverage["observed_links_total"],
        "possible_links_total": coverage["possible_links_total"],
        "status": status_override if status_override is not None else result["status"],
        "returncode": result["returncode"],
        "elapsed_sec": "%.6f" % (
            elapsed_override if elapsed_override is not None else result["elapsed_sec"]),
        "work_dir": task["work_dir"],
        "config_file": task["config_file"],
        "coverage_file": task["coverage_file"],
        "run_log": task["run_log"],
    }


def main():
    repo_root = repo_root_from_script()
    figure5_dir = pathlib.Path(__file__).resolve().parents[1]
    sim_dir = repo_root / "simulation"
    parser = argparse.ArgumentParser(
        description="Run 3D-torus average-coverage sweeps for Figure 5.")
    parser.add_argument("--k", type=int, nargs="+", default=[4, 6],
                        help="3D torus dimensions to run; k means k x k x k")
    parser.add_argument("--mode", type=lambda value: value.upper(), nargs="+",
                        choices=sorted(MODE_TO_CC.keys()), default=list(DEFAULT_MODES),
                        help="ping modes to evaluate")
    parser.add_argument("--min-exp", type=int, default=0,
                        help="minimum packet exponent; packet_count=2^exp")
    parser.add_argument("--max-exp", type=int, default=8,
                        help="maximum packet exponent; packet_count=2^exp")
    parser.add_argument("--jobs", type=int, default=0,
                        help="maximum concurrent ns-3 jobs; 0 means all runnable jobs")
    parser.add_argument("--work-dir", type=pathlib.Path, default=None,
                        help="optional directory for retaining generated configs/logs/coverage files")
    parser.add_argument("--output-file", type=pathlib.Path, default=None,
                        help="coverage-matrix CSV result file; default is results/Figure5(d)_results.csv")
    parser.add_argument("--stop-time", type=float, default=100.0,
                        help="SIMULATOR_STOP_TIME in seconds")
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument("--buffer-size", type=int, default=128)
    parser.add_argument("--opp-m", type=int, default=1,
                        help="torus OPP tokens per ToR/server")
    parser.add_argument("--opp-multicast-mode", choices=["STANDARD", "USM", "SAMPLED"],
                        default="STANDARD")
    parser.add_argument("--opp-usm-sample-period", type=int, default=1)
    parser.add_argument("--opp-m1", type=int, default=1,
                        help="unused for 3D torus; kept for config compatibility")
    parser.add_argument("--opp-m2", type=int, default=1,
                        help="unused for 3D torus; kept for config compatibility")
    parser.add_argument("--opp-m3", type=int, default=1,
                        help="unused for 3D torus; kept for config compatibility")
    parser.add_argument("--no-run", action="store_true",
                        help="generate configs only; do not launch ns-3")
    args = parser.parse_args()
    args.sim_dir = sim_dir

    if args.min_exp < 0 or args.max_exp < args.min_exp:
        parser.error("--max-exp must be greater than or equal to --min-exp >= 0")
    if args.jobs < 0:
        parser.error("--jobs must be non-negative")
    if "OPP" in args.mode and args.min_exp > OPP_SOURCE_PACKET_EXP:
        parser.error("OPP copy mode requires --min-exp 0 so packet_count=1 is available")
    if args.opp_m <= 0:
        parser.error("--opp-m must be positive")
    if args.opp_usm_sample_period <= 0:
        parser.error("--opp-usm-sample-period must be positive")

    if args.output_file is None:
        args.output_file = figure5_dir / "results" / DEFAULT_OUTPUT_FILE
    output_file = args.output_file.resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    temp_work_dir = None
    if args.work_dir is None:
        temp_work_dir = tempfile.TemporaryDirectory(prefix="figure5d_")
        work_root = pathlib.Path(temp_work_dir.name).resolve()
    else:
        work_root = args.work_dir.resolve()
    work_root.mkdir(parents=True, exist_ok=True)

    try:
        tasks = build_tasks(args, repo_root, figure5_dir, work_root)
        runnable_tasks = [task for task in tasks if task["copy_from_packet_exp"] is None]
        workers = len(runnable_tasks) if args.jobs == 0 else min(args.jobs, len(runnable_tasks))

        print("work_dir=%s" % work_root)
        print("output_file=%s" % output_file)
        print("tasks=%d ns3_tasks=%d jobs=%d no_run=%d" % (
            len(tasks), len(runnable_tasks), workers, 1 if args.no_run else 0))

        rows = []
        result_by_key = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_task = {
                executor.submit(run_task, sim_dir, task, args.no_run): task
                for task in runnable_tasks
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
                        "coverage": {
                            "flow_rows": "",
                            "avg_coverage": "",
                            "observed_links_total": "",
                            "possible_links_total": "",
                        },
                    }

                key = (task["k"], task["mode"], task["packet_exp"])
                result_by_key[key] = result
                row = row_from_task_result(task, result)
                rows.append(row)

                avg = result["coverage"]["avg_coverage"]
                print("finished series=%s packet_count=%d status=%s avg_coverage=%s elapsed=%.3fs" % (
                    task["series"], task["packet_count"], result["status"],
                    "" if avg == "" else "%.6f" % avg, result["elapsed_sec"]))

        task_by_key = {(task["k"], task["mode"], task["packet_exp"]): task for task in tasks}
        for task in tasks:
            if task["copy_from_packet_exp"] is None:
                continue
            source_key = (task["k"], task["mode"], task["copy_from_packet_exp"])
            source_result = result_by_key.get(source_key)
            if source_result is None:
                result = {
                    "status": "missing_source",
                    "returncode": -1,
                    "elapsed_sec": 0.0,
                    "coverage": {
                        "flow_rows": "",
                        "avg_coverage": "",
                        "observed_links_total": "",
                        "possible_links_total": "",
                    },
                }
                status = "missing_source"
            else:
                result = source_result
                status = (
                    "copied"
                    if source_result["status"] in ("ok", "generated")
                    else "copy_source_failed")
            row = row_from_task_result(task, result, status_override=status, elapsed_override=0.0)
            rows.append(row)
            source_task = task_by_key.get(source_key)
            print("copied series=%s packet_count=%d from_packet_count=%d status=%s avg_coverage=%s" % (
                task["series"], task["packet_count"],
                source_task["packet_count"] if source_task is not None else (1 << task["copy_from_packet_exp"]),
                status, row["avg_coverage"]))

        mode_order = {mode: idx for idx, mode in enumerate(DEFAULT_MODES)}
        rows.sort(key=lambda row: (
            mode_order.get(row["mode"], len(mode_order)),
            int(row["k"]),
            int(row["packet_exp"])))
        write_coverage_matrix(output_file, rows)
        print("summary_file=%s" % output_file)

        failed = [row for row in rows if row["status"] not in ("ok", "generated", "copied")]
        return 1 if failed else 0
    finally:
        if temp_work_dir is not None:
            temp_work_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
