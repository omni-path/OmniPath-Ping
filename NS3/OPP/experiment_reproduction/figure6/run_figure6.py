#!/usr/bin/env python3
"""Unified runner for Figure 6 workloads."""

import argparse
import csv
import pathlib
import re
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

MODE_ORDER = ("RDPROBE", "RPINGMESH", "OPP")


TOPOLOGY_CONFIGS = [
    ("fattree", "k32", {"RDPROBE": 8, "RPINGMESH": 8, "OPP": 0}),
    ("fattree", "k64", {"RDPROBE": 10, "RPINGMESH": 10, "OPP": 0}),
    ("leaf_spine", "k32", {"RDPROBE": 6, "RPINGMESH": 6, "OPP": 0}),
    ("leaf_spine", "k64", {"RDPROBE": 7, "RPINGMESH": 7, "OPP": 0}),
    ("2d_torus", "8x8", {"RDPROBE": 6, "RPINGMESH": 6, "OPP": 0}),
    ("2d_torus", "10x10", {"RDPROBE": 6, "RPINGMESH": 6, "OPP": 0}),
    ("3d_torus", "4x4x4", {"RDPROBE": 7, "RPINGMESH": 7, "OPP": 0}),
    ("3d_torus", "6x6x6", {"RDPROBE": 8, "RPINGMESH": 8, "OPP": 0}),
]

PROBE_ACK_TOTAL_RE = re.compile(r"^SWITCH_PROBE_ACK_RX_COUNT_TOTAL\s+(\d+)$")


def repo_root_from_script() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def topology_file_name(topology_type: str, topo_key: str) -> pathlib.Path:
    if topology_type == "fattree":
        return pathlib.Path("topologies/fattree") / f"fattree_{topo_key}_topology.txt"
    if topology_type == "leaf_spine":
        return pathlib.Path("topologies/leaf_spine") / f"leaf_spine_{topo_key}_topology.txt"
    if topology_type == "2d_torus":
        return pathlib.Path("topologies/2d_torus") / f"torus_{topo_key}_topology.txt"
    if topology_type == "3d_torus":
        return pathlib.Path("topologies/3d_torus") / f"torus_{topo_key}_topology.txt"
    raise ValueError(f"unknown topology type {topology_type}")


def norm_factor(topology_type: str, topo_key: str) -> int:
    if topology_type == "fattree":
        k = int(topo_key[1:])
        return k * k // 2
    if topology_type == "leaf_spine":
        k = int(topo_key[1:])
        return k
    if topology_type == "3d_torus":
        dim = int(topo_key.split("x", 1)[0])
        return dim * dim
    if topology_type == "2d_torus":
        return 1
    raise ValueError(f"unknown topology type {topology_type}")


def parse_header_switch_count(topology_file: pathlib.Path) -> int:
    with topology_file.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            fields = line.split()
            if len(fields) < 2:
                raise RuntimeError(f"invalid topology header in {topology_file}")
            return int(fields[1])
    raise RuntimeError(f"empty topology file {topology_file}")


def write_trace_file(trace_file: pathlib.Path):
    trace_file.write_text("0\n")


def write_config(path, args, task, topology_file, flow_file, trace_file):
    if task["topo_mode"] == "FAT_TREE":
        fat_k = int(task["topo_key"][1:])
    else:
        fat_k = None

    lines = [
        "ENABLE_QCN 1",
        "USE_DYNAMIC_PFC_THRESHOLD 1",
        "PACKET_PAYLOAD_SIZE 1000",
        f"TOPOLOGY_FILE {topology_file}",
        f"TOPOLOGY_MODE {task['topo_mode']}",
        *( [f"FAT_TREE_K {fat_k}"] if fat_k is not None else [] ),
        f"FLOW_FILE {flow_file}",
        f"TRACE_FILE {trace_file}",
        f"TRACE_OUTPUT_FILE {task['work_dir'] / 'trace.tr'}",
        f"FCT_OUTPUT_FILE {task['work_dir'] / 'fct.txt'}",
        f"PFC_OUTPUT_FILE {task['work_dir'] / 'pfc.txt'}",
        f"FLOW_COVERAGE_OUTPUT_FILE {task['work_dir'] / 'flow_coverage.txt'}",
        f"SIMULATOR_STOP_TIME {args.stop_time:.9f}",
        f"CC_MODE {MODE_TO_CC[task['mode']]}",
        "ROUTING_MODE PACKET_SPRAY",
        f"OPP_MULTICAST_MODE {args.opp_multicast_mode if task['mode'] == 'OPP' else 'STANDARD'}",
        f"OPP_USM_SAMPLE_PERIOD {args.opp_usm_sample_period}",
        f"OPP_TOKENS_PER_TOR {args.opp_m}",
        f"OPP_FATTREE_M1 {args.opp_m1}",
        f"OPP_FATTREE_M2 {args.opp_m2}",
        f"OPP_FATTREE_M3 {args.opp_m3}",
        f"OPP_LEAFSPINE_M1 {args.opp_m1}",
        f"OPP_LEAFSPINE_M2 {args.opp_m2}",
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
        f"ERROR_RATE_PER_LINK {args.error_rate:.6f}",
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
        f"BUFFER_SIZE {args.buffer_size}",
        f"QLEN_MON_FILE {task['work_dir'] / 'qlen.txt'}",
        "QLEN_MON_START 0",
        "QLEN_MON_END 0",
        f"ENABLE_RDPROBE_DIAG 0",
        f"RDPROBE_DIAG_OUTPUT_FILE {task['work_dir'] / 'rdprobe_trace.txt'}",
        f"RDPROBE_DIAG_RESULT_OUTPUT_FILE {task['work_dir'] / 'rdprobe_diag_result.txt'}",
        f"ENABLE_RPINGMESH_DIAG 0",
        f"RPINGMESH_DIAG_RESULT_OUTPUT_FILE {task['work_dir'] / 'rpingmesh_diag_result.txt'}",
        f"ENABLE_OPP_DIAG 0",
        f"OPP_DIAG_RESULT_OUTPUT_FILE {task['work_dir'] / 'opp_diag_result.txt'}",
    ]
    path.write_text("\n".join(relativize_config_lines(lines, args.sim_dir)) + "\n")


def parse_declared_flow_count(flow_file: pathlib.Path) -> int:
    with flow_file.open() as f:
        return int(f.readline().strip())


def parse_total_probe_ack_count(log_file: pathlib.Path):
    if not log_file.exists():
        return None
    total = None
    with log_file.open() as f:
        for line in f:
            m = PROBE_ACK_TOTAL_RE.match(line.strip())
            if m:
                total = int(m.group(1))
    return total


def run_task(sim_dir: pathlib.Path, task: dict, no_run: bool):
    if no_run:
        return {
            "status": "generated",
            "returncode": 0,
            "elapsed_sec": 0.0,
            "total_probe_ack": None,
        }

    started = time.time()
    cmd = ["python2", "./waf", "--run", "scratch/third %s" % path_relative_to(task["config_file"], sim_dir)]
    with task["run_log"].open("w") as f:
        proc = subprocess.run(cmd, cwd=str(sim_dir), stdout=f, stderr=subprocess.STDOUT)
    elapsed = time.time() - started

    total = parse_total_probe_ack_count(task["run_log"])
    if proc.returncode != 0:
        return {
            "status": "failed",
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "total_probe_ack": None,
        }
    if total is None:
        return {
            "status": "missing_metric",
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "total_probe_ack": None,
        }
    return {
        "status": "ok",
        "returncode": proc.returncode,
        "elapsed_sec": elapsed,
        "total_probe_ack": total,
    }


def build_tasks(repo_root: pathlib.Path, figure6_dir: pathlib.Path, work_root: pathlib.Path, args):
    topo_mode_map = {
        "fattree": "FAT_TREE",
        "leaf_spine": "LEAF_SPINE",
        "2d_torus": "2D_TORUS",
        "3d_torus": "3D_TORUS",
    }
    trace_file = work_root / "trace.txt"
    write_trace_file(trace_file)

    tasks = []
    for topology_type, topo_key, mode_exps in TOPOLOGY_CONFIGS:
        flow_dir = figure6_dir / "traffic_load" / topology_type / topo_key
        topo_file = (repo_root / topology_file_name(topology_type, topo_key)).resolve()
        if not topo_file.exists():
            raise FileNotFoundError(f"missing topology file: {topo_file}")
        switch_count = parse_header_switch_count(topo_file)

        if not flow_dir.exists():
            raise FileNotFoundError(f"missing traffic directory: {flow_dir}")

        for mode in MODE_ORDER:
            exp = mode_exps[mode]
            flow_file = flow_dir / f"flow_2e{exp}.txt"
            if not flow_file.exists():
                raise FileNotFoundError(f"missing flow file: {flow_file}")
            base = f"{topology_type}_{topo_key}_{mode}"
            task = {
                "topology_type": topology_type,
                "topo_key": topo_key,
                "topo_mode": topo_mode_map[topology_type],
                "norm_factor": norm_factor(topology_type, topo_key),
                "switch_count": switch_count,
                "mode": mode,
                "packet_count": 1 << exp,
                "flow_file": flow_file.resolve(),
                "topology_file": topo_file,
                "work_dir": work_root / base,
                "run_log": work_root / base / "run.log",
                "config_file": work_root / base / "config.txt",
                "coverage_file": work_root / base / "flow_coverage.txt",
                "declared_flows": parse_declared_flow_count(flow_file),
                "exp": exp,
            }
            task["work_dir"].mkdir(parents=True, exist_ok=True)
            task["run_log"] = task["work_dir"] / "run.log"
            task["config_file"] = task["work_dir"] / "config.txt"
            task["coverage_file"] = task["work_dir"] / "flow_coverage.txt"
            write_config(task["config_file"], args, task, task["topology_file"], task["flow_file"], trace_file)
            tasks.append(task)
    return tasks


def build_result_row(task: dict, result: dict):
    total = result["total_probe_ack"]
    status = result["status"]
    metric = ""
    if total is not None:
        metric = total / task["switch_count"] * task["norm_factor"]

    return {
        "topology": f"{task['topology_type']}_{task['topo_key']}",
        "mode": task["mode"],
        "avg_probe_ack_per_switch": "" if metric == "" else f"{metric:.9f}",
    }


def write_csv(output_file: pathlib.Path, rows):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "topology",
            "mode",
            "avg_probe_ack_per_switch",
        ])
        for row in rows:
            writer.writerow([
                row["topology"],
                row["mode"],
                row["avg_probe_ack_per_switch"],
            ])


def main():
    parser = argparse.ArgumentParser(description="Run Figure 6 full-load experiments.")
    parser.add_argument("--mode", type=lambda s: s.upper(), nargs="+", default=list(MODE_ORDER),
                        choices=sorted(MODE_TO_CC.keys()), help="modes to run")
    parser.add_argument("--stop-time", type=float, default=100.0)
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument("--buffer-size", type=int, default=128)
    parser.add_argument("--opp-m", type=int, default=1)
    parser.add_argument("--opp-m1", type=int, default=1)
    parser.add_argument("--opp-m2", type=int, default=8)
    parser.add_argument("--opp-m3", type=int, default=1)
    parser.add_argument("--opp-multicast-mode", choices=["STANDARD", "USM", "SAMPLED"], default="USM")
    parser.add_argument("--opp-usm-sample-period", type=int, default=1)
    parser.add_argument("--jobs", type=int, default=0, help="concurrency, 0 means all tasks")
    parser.add_argument("--no-run", action="store_true", help="generate configs only")
    parser.add_argument("--work-dir", type=pathlib.Path, default=None,
                        help="optional persistent working directory")
    parser.add_argument("--output-file", type=pathlib.Path, default=pathlib.Path("results/Figure6_results.csv"),
                        help="CSV output file")
    args = parser.parse_args()

    if args.jobs < 0:
        parser.error("--jobs must be non-negative")
    for n in (args.opp_m, args.opp_m1, args.opp_m2, args.opp_m3, args.opp_usm_sample_period):
        if n <= 0:
            parser.error("opp parameters must be positive")

    repo_root = repo_root_from_script()
    sim_dir = repo_root.parent / "simulation"
    args.sim_dir = sim_dir
    figure6_dir = pathlib.Path(__file__).resolve().parent
    output_file = (figure6_dir / args.output_file).resolve()

    if args.work_dir is None:
        work_dir = tempfile.TemporaryDirectory(prefix="figure6_")
        work_root = pathlib.Path(work_dir.name)
    else:
        work_root = args.work_dir
        work_root.mkdir(parents=True, exist_ok=True)

    try:
        tasks = build_tasks(repo_root, figure6_dir, work_root.resolve(), args)
        # Keep configured mode order only; default is RDPROBE/RPINGMESH/OPP
        tasks = [t for t in tasks if t["mode"] in set(args.mode)]
        if not tasks:
            raise RuntimeError("no task generated")

        workers = len(tasks) if args.jobs == 0 else min(args.jobs, len(tasks))
        rows = []
        statuses = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_task = {
                executor.submit(run_task, sim_dir, task, args.no_run): task
                for task in tasks
            }
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                result = future.result()
                statuses.append(result["status"])
                row = build_result_row(task, result)
                rows.append(row)

        rows.sort(key=lambda r: (MODE_ORDER.index(r["mode"]), r["topology"]))
        write_csv(output_file, rows)
        return 1 if any(status not in ("ok", "generated") for status in statuses) else 0
    finally:
        if args.work_dir is None:
            work_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
