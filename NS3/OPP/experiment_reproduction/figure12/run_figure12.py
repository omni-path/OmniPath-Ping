#!/usr/bin/env python3
"""Run Figure 12 OPP probe-delay CDFs."""

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


DEFAULT_SAMPLE_RATES = (0, 10, 40, 70, 100)
TOPOLOGY_ORDER = ("fattree_k64", "leaf_spine_k64", "2d_torus_10x10", "3d_torus_6x6x6")
FIGURE_OUTPUTS = (
    ("a", "fattree_k64", ("pod_cross_tor", "cross_pod")),
    ("b", "leaf_spine_k64", ("cross_tor",)),
    ("c", "2d_torus_10x10", ("all",)),
    ("d", "3d_torus_6x6x6", ("all",)),
)


def repo_root_from_script():
    return pathlib.Path(__file__).resolve().parents[2]


def figure_dir_from_script():
    return pathlib.Path(__file__).resolve().parent


def write_trace_file(path):
    path.write_text("0\n")


def sample_rate_fraction(percent):
    if percent < 0 or percent > 100:
        raise ValueError("sample rate percent must be in [0, 100]")
    if percent == 0:
        return 0, 10
    if percent % 10 == 0:
        return percent // 10, 10
    g = math.gcd(percent, 100)
    return percent // g, 100 // g


def topology_specs(repo_root, args):
    traffic_root = repo_root / "experiment_reproduction" / "figure9-10" / "traffic_load"
    return {
        "fattree_k64": {
            "topology": "fattree_k64",
            "topology_mode": "FAT_TREE",
            "k": 64,
            "topology_file": (
                repo_root / "experiment_reproduction" / "topologies" /
                "fattree" / "fattree_k64_topology.txt"
            ),
            "flow_file": traffic_root / "fattree" / "k64" / "flow_2e0.txt",
            "tokens_per_tor": args.m3,
        },
        "leaf_spine_k64": {
            "topology": "leaf_spine_k64",
            "topology_mode": "LEAF_SPINE",
            "k": 64,
            "topology_file": (
                repo_root / "experiment_reproduction" / "topologies" /
                "leaf_spine" / "leaf_spine_k64_topology.txt"
            ),
            "flow_file": traffic_root / "leaf_spine" / "k64" / "flow_2e0.txt",
            "tokens_per_tor": args.m2,
        },
        "2d_torus_10x10": {
            "topology": "2d_torus_10x10",
            "topology_mode": "2D_TORUS",
            "k": 10,
            "topology_file": (
                repo_root / "experiment_reproduction" / "topologies" /
                "2d_torus" / "torus_10x10_topology.txt"
            ),
            "flow_file": traffic_root / "2d_torus" / "10x10" / "flow_2e0.txt",
            "tokens_per_tor": args.m3,
        },
        "3d_torus_6x6x6": {
            "topology": "3d_torus_6x6x6",
            "topology_mode": "3D_TORUS",
            "k": 6,
            "topology_file": (
                repo_root / "experiment_reproduction" / "topologies" /
                "3d_torus" / "torus_6x6x6_topology.txt"
            ),
            "flow_file": traffic_root / "3d_torus" / "6x6x6" / "flow_2e0.txt",
            "tokens_per_tor": args.m3,
        },
    }


def write_config(path, args, task, trace_file):
    work_dir = task["work_dir"]
    lines = [
        "ENABLE_QCN 1",
        "USE_DYNAMIC_PFC_THRESHOLD 1",
        "PACKET_PAYLOAD_SIZE 1000",
        "TOPOLOGY_FILE %s" % task["topology_file"],
        "TOPOLOGY_MODE %s" % task["topology_mode"],
        "FLOW_FILE %s" % task["flow_file"],
        "TRACE_FILE %s" % trace_file,
        "TRACE_OUTPUT_FILE %s" % (work_dir / "trace.tr"),
        "FCT_OUTPUT_FILE %s" % task["fct_file"],
        "OPP_PROBE_DELAY_OUTPUT_FILE %s" % task["probe_delay_file"],
        "PFC_OUTPUT_FILE %s" % (work_dir / "pfc.txt"),
        "FLOW_COVERAGE_OUTPUT_FILE %s" % (work_dir / "flow_coverage.txt"),
        "SIMULATOR_STOP_TIME %.9f" % args.stop_time,
        "CC_MODE 12",
        "ROUTING_MODE PACKET_SPRAY",
        "OPP_MULTICAST_MODE SAMPLED",
        "OPP_USM_SAMPLE_RATE %d %d" % (
            task["sample_numerator"], task["sample_denominator"]),
        "OPP_TOKENS_PER_TOR %d" % task["tokens_per_tor"],
        "OPP_FATTREE_M1 %d" % args.m1,
        "OPP_FATTREE_M2 %d" % args.m2,
        "OPP_FATTREE_M3 %d" % args.m3,
        "OPP_LEAFSPINE_M1 %d" % args.m1,
        "OPP_LEAFSPINE_M2 %d" % args.m2,
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
    if task["topology_mode"] == "FAT_TREE":
        lines.insert(5, "FAT_TREE_K %d" % task["k"])
    path.write_text("\n".join(relativize_config_lines(lines, args.sim_dir)) + "\n")


def build_tasks(args, repo_root, work_root):
    trace_file = work_root / "trace.txt"
    write_trace_file(trace_file)
    specs = topology_specs(repo_root, args)
    tasks = []
    for topology in args.topology:
        spec = specs[topology]
        if not spec["topology_file"].exists():
            raise FileNotFoundError("missing topology file: %s" % spec["topology_file"])
        if not spec["flow_file"].exists():
            raise FileNotFoundError("missing flow file: %s" % spec["flow_file"])
        for sample_rate in args.sample_rate:
            numerator, denominator = sample_rate_fraction(sample_rate)
            task_work_dir = work_root / ("%s_sample_%03d" % (topology, sample_rate))
            task_work_dir.mkdir(parents=True, exist_ok=True)
            task = dict(spec)
            task.update({
                "sample_rate": sample_rate,
                "sample_numerator": numerator,
                "sample_denominator": denominator,
                "work_dir": task_work_dir,
                "topology_file": spec["topology_file"].resolve(),
                "flow_file": spec["flow_file"].resolve(),
                "config_file": task_work_dir / "config.txt",
                "run_log": task_work_dir / "run.log",
                "fct_file": task_work_dir / "fct.txt",
                "probe_delay_file": task_work_dir / "opp_probe_delay.csv",
            })
            write_config(task["config_file"], args, task, trace_file.resolve())
            tasks.append(task)
    return tasks


def run_task(sim_dir, task, no_run):
    if no_run:
        return {"status": "generated", "returncode": 0, "elapsed_sec": 0.0}
    started = time.time()
    cmd = ["python2", "./waf", "--run", "scratch/third %s" % path_relative_to(task["config_file"], sim_dir)]
    with task["run_log"].open("w") as log:
        proc = subprocess.run(cmd, cwd=str(sim_dir), stdout=log, stderr=subprocess.STDOUT)
    return {
        "status": "ok" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "elapsed_sec": time.time() - started,
    }


def node_id_from_ip_hex(ip_hex):
    return (int(ip_hex, 16) >> 8) & 0xffff


def classify_probe_delay_row(row, task):
    if task["topology"] == "fattree_k64":
        k = task["k"]
        hosts_per_tor = k // 2
        hosts_per_pod = (k // 2) * (k // 2)
        src = node_id_from_ip_hex(row["sip"])
        dst = node_id_from_ip_hex(row["dip"])
        src_pod = src // hosts_per_pod
        dst_pod = dst // hosts_per_pod
        src_tor = (src % hosts_per_pod) // hosts_per_tor
        dst_tor = (dst % hosts_per_pod) // hosts_per_tor
        if src_pod == dst_pod and src_tor == dst_tor:
            return None
        if src_pod == dst_pod:
            return "pod_cross_tor"
        return "cross_pod"

    if task["topology"] == "leaf_spine_k64":
        hosts_per_leaf = task["k"] // 2
        src = node_id_from_ip_hex(row["sip"])
        dst = node_id_from_ip_hex(row["dip"])
        if src // hosts_per_leaf == dst // hosts_per_leaf:
            return None
        return "cross_tor"

    return "all"


def domains_for_topology(topology):
    for _, output_topology, domains in FIGURE_OUTPUTS:
        if output_topology == topology:
            return domains
    return ()


def parse_probe_delay_file(path, task):
    values = {domain: [] for domain in domains_for_topology(task["topology"])}
    missing = {domain: 0 for domain in values}
    if not path.exists():
        return values, missing
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = classify_probe_delay_row(row, task)
            if domain not in values:
                continue
            if row.get("ack_observed") != "1" or not row.get("opp_probe_delay_ns"):
                missing[domain] += 1
                continue
            try:
                values[domain].append(int(row["opp_probe_delay_ns"]))
            except ValueError:
                missing[domain] += 1
    for domain_values in values.values():
        domain_values.sort()
    return values, missing


def nearest_rank(values, percentile):
    if not values:
        return None
    index = int(math.ceil(percentile * len(values))) - 1
    index = min(max(index, 0), len(values) - 1)
    return values[index]


def cdf_points(values, total_points):
    if not values:
        return []
    rows = []
    for point in range(1, total_points + 1):
        cdf = point / float(total_points)
        rows.append((point, cdf, nearest_rank(values, cdf)))
    return rows


def output_file_for(results_dir, figure_part, topology):
    return results_dir / ("Figure12(%s)_%s_100points.csv" % (figure_part, topology))


def write_cdf(path, topology, domains, sample_rates, values_by_key, total_points):
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "topology",
        "domain",
        "sample_rate_percent",
        "point",
        "cdf",
        "probe_delay_ns",
        "count",
    ]
    cdf_decimal_places = 0 if total_points == 1 else int(math.ceil(math.log10(total_points)))
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for domain in domains:
            for sample_rate in sample_rates:
                values = values_by_key.get((topology, domain, sample_rate), [])
                for point, cdf, delay_ns in cdf_points(values, total_points):
                    writer.writerow({
                        "topology": topology,
                        "domain": domain,
                        "sample_rate_percent": sample_rate,
                        "point": point,
                        "cdf": "%.*f" % (cdf_decimal_places, cdf),
                        "probe_delay_ns": delay_ns,
                        "count": len(values),
                    })


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=20)
    parser.add_argument("--work-dir", type=pathlib.Path, default=None,
                        help="optional directory for retaining generated configs/logs")
    parser.add_argument("--results-dir", type=pathlib.Path,
                        default=pathlib.Path("results"))
    parser.add_argument("--sample-rate", type=int, nargs="+",
                        default=list(DEFAULT_SAMPLE_RATES))
    parser.add_argument("--topology", nargs="+", choices=TOPOLOGY_ORDER,
                        default=list(TOPOLOGY_ORDER))
    parser.add_argument("--cdf-points", type=int, default=100)
    parser.add_argument("--m1", type=int, default=10)
    parser.add_argument("--m2", type=int, default=10)
    parser.add_argument("--m3", type=int, default=1)
    parser.add_argument("--stop-time", type=float, default=100.0)
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument("--buffer-size", type=int, default=128)
    parser.add_argument("--no-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.jobs <= 0:
        raise SystemExit("--jobs must be positive")
    if any(rate < 0 or rate > 100 for rate in args.sample_rate):
        raise SystemExit("--sample-rate values must be in [0, 100]")
    if args.m1 <= 0 or args.m2 <= 0 or args.m3 <= 0:
        raise SystemExit("--m1, --m2, and --m3 must be positive")
    if args.cdf_points <= 0:
        raise SystemExit("--cdf-points must be positive")
    args.sample_rate = sorted(set(args.sample_rate))
    args.topology = [topology for topology in TOPOLOGY_ORDER if topology in set(args.topology)]

    repo_root = repo_root_from_script()
    figure_dir = figure_dir_from_script()
    sim_dir = repo_root / "simulation"
    args.sim_dir = sim_dir
    results_dir = (figure_dir / args.results_dir).resolve()

    temp_work_dir = None
    if args.work_dir is None:
        temp_work_dir = tempfile.TemporaryDirectory(prefix="figure12_")
        work_root = pathlib.Path(temp_work_dir.name).resolve()
    else:
        work_root = args.work_dir.resolve()
    work_root.mkdir(parents=True, exist_ok=True)

    try:
        tasks = build_tasks(args, repo_root, work_root)
        workers = min(args.jobs, len(tasks))

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
                    result = {"status": "exception", "returncode": -1, "elapsed_sec": 0.0}
                statuses.append(result["status"])

        values_by_key = {}
        for task in tasks:
            values_by_domain, _ = parse_probe_delay_file(task["probe_delay_file"], task)
            for domain, values in values_by_domain.items():
                values_by_key[(task["topology"], domain, task["sample_rate"])] = values

        for figure_part, topology, domains in FIGURE_OUTPUTS:
            if topology not in args.topology:
                continue
            write_cdf(
                output_file_for(results_dir, figure_part, topology),
                topology,
                domains,
                args.sample_rate,
                values_by_key,
                args.cdf_points,
            )
        return 1 if any(status not in ("ok", "generated") for status in statuses) else 0
    finally:
        if temp_work_dir is not None:
            temp_work_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
