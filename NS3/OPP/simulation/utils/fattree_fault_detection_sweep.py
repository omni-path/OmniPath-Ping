#!/usr/bin/env python3
"""Generate and run fat-tree fault-detection sweeps.

The script creates a k-ary fat-tree, generates cross-pod flows whose possible
paths include the injected Agg(0,0)->Core(0,0) fault link, runs scratch/third,
and reports per-flow-size diagnosis precision/recall/F1 metrics.
"""

import argparse
import csv
import datetime as _dt
import pathlib
import subprocess
import sys


MODE_TO_CC = {
    "RPINGMESH": 11,
    "OPP": 12,
    "RDPROBE": 13,
}


def fat_tree_counts(k):
    half = k // 2
    host_count = k * k * k // 4
    edge_count = k * k // 2
    agg_count = k * k // 2
    core_count = k * k // 4
    switch_count = edge_count + agg_count + core_count
    node_count = host_count + switch_count
    link_count = host_count * 3
    return {
        "half": half,
        "hosts_per_edge": half,
        "hosts_per_pod": half * half,
        "host_count": host_count,
        "edge_count": edge_count,
        "agg_count": agg_count,
        "core_count": core_count,
        "switch_count": switch_count,
        "node_count": node_count,
        "link_count": link_count,
        "edge_base": host_count,
        "agg_base": host_count + edge_count,
        "core_base": host_count + edge_count + agg_count,
    }


def host_id(counts, pod, edge, host):
    return pod * counts["hosts_per_pod"] + edge * counts["hosts_per_edge"] + host


def edge_node(counts, pod, edge):
    return counts["edge_base"] + pod * counts["half"] + edge


def agg_node(counts, pod, agg):
    return counts["agg_base"] + pod * counts["half"] + agg


def core_node(counts, group, col):
    return counts["core_base"] + group * counts["half"] + col


def write_fat_tree_topology(path, k, bandwidth, delay, error_rate):
    counts = fat_tree_counts(k)
    half = counts["half"]
    with path.open("w") as f:
        f.write("%d %d %d\n" % (counts["node_count"], counts["switch_count"], counts["link_count"]))
        f.write(" ".join(str(i) for i in range(counts["edge_base"], counts["node_count"])))
        f.write("\n")

        for pod in range(k):
            for edge in range(half):
                edge_id = edge_node(counts, pod, edge)
                for h in range(half):
                    src = host_id(counts, pod, edge, h)
                    f.write("%d %d %s %s %.6f\n" % (src, edge_id, bandwidth, delay, error_rate))

        for pod in range(k):
            for edge in range(half):
                edge_id = edge_node(counts, pod, edge)
                for agg in range(half):
                    f.write("%d %d %s %s %.6f\n" % (
                        edge_id, agg_node(counts, pod, agg), bandwidth, delay, error_rate))

        for pod in range(k):
            for agg in range(half):
                agg_id = agg_node(counts, pod, agg)
                for col in range(half):
                    f.write("%d %d %s %s %.6f\n" % (
                        agg_id, core_node(counts, agg, col), bandwidth, delay, error_rate))


def generate_flows(path, metadata_path, k, max_exp, flows_per_size, pg, base_dport, start_time):
    counts = fat_tree_counts(k)
    half = counts["half"]
    total = (max_exp + 1) * flows_per_size
    if base_dport + total - 1 > 65535:
        raise ValueError("dport range exceeds 65535; reduce n/m or choose a smaller --base-dport")

    records = []
    for exp in range(max_exp + 1):
        packet_count = 1 << exp
        for idx in range(flows_per_size):
            global_idx = exp * flows_per_size + idx
            src_edge = global_idx % half
            src_host = (global_idx // half) % half
            dst_pod = 1 + (global_idx % (k - 1))
            dst_edge = (global_idx // (k - 1)) % half
            dst_host = (global_idx // ((k - 1) * half)) % half
            src = host_id(counts, 0, src_edge, src_host)
            dst = host_id(counts, dst_pod, dst_edge, dst_host)
            dport = base_dport + global_idx
            records.append({
                "exp": exp,
                "packet_count": packet_count,
                "src": src,
                "dst": dst,
                "pg": pg,
                "dport": dport,
                "start_time": start_time,
            })

    with path.open("w") as f:
        f.write("%d\n" % len(records))
        for rec in records:
            f.write("%d %d %d %d %d %.9f\n" % (
                rec["src"], rec["dst"], rec["pg"], rec["dport"],
                rec["packet_count"], rec["start_time"]))

    with metadata_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "exp", "packet_count", "src", "dst", "pg", "dport", "start_time"])
        writer.writeheader()
        writer.writerows(records)
    return records


def write_config(path, args, work_dir, topology_file, flow_file, trace_file, result_file):
    mode = args.mode.upper()
    cc_mode = MODE_TO_CC[mode]
    enable_rpingmesh = 1 if mode == "RPINGMESH" else 0
    enable_opp = 1 if mode == "OPP" else 0
    enable_rdprobe = 1 if mode == "RDPROBE" else 0

    lines = [
        "ENABLE_QCN 1",
        "USE_DYNAMIC_PFC_THRESHOLD 1",
        "PACKET_PAYLOAD_SIZE 1000",
        "TOPOLOGY_FILE %s" % topology_file,
        "TOPOLOGY_MODE FAT_TREE",
        "FAT_TREE_K %d" % args.k,
        "FLOW_FILE %s" % flow_file,
        "TRACE_FILE %s" % trace_file,
        "TRACE_OUTPUT_FILE %s" % (work_dir / "trace.tr"),
        "FCT_OUTPUT_FILE %s" % (work_dir / "fct.txt"),
        "PFC_OUTPUT_FILE %s" % (work_dir / "pfc.txt"),
        "FLOW_COVERAGE_OUTPUT_FILE %s" % (work_dir / "flow_coverage.txt"),
        "ENABLE_RDPROBE_DIAG %d" % enable_rdprobe,
        "RDPROBE_DIAG_OUTPUT_FILE %s" % (work_dir / "rdprobe_trace.txt"),
        "RDPROBE_DIAG_RESULT_OUTPUT_FILE %s" % result_file,
        "ENABLE_RPINGMESH_DIAG %d" % enable_rpingmesh,
        "RPINGMESH_DIAG_RESULT_OUTPUT_FILE %s" % result_file,
        "ENABLE_OPP_DIAG %d" % enable_opp,
        "OPP_DIAG_RESULT_OUTPUT_FILE %s" % result_file,
        "SIMULATOR_STOP_TIME %.9f" % args.stop_time,
        "CC_MODE %d" % cc_mode,
        "ROUTING_MODE PACKET_SPRAY",
        "OPP_MULTICAST_MODE %s" % args.opp_multicast_mode,
        "OPP_USM_SAMPLE_PERIOD %d" % args.opp_usm_sample_period,
        "OPP_TOKENS_PER_TOR %d" % args.opp_m,
        "OPP_FATTREE_M1 %d" % args.opp_m1,
        "OPP_FATTREE_M2 %d" % args.opp_m2,
        "OPP_FATTREE_M3 %d" % args.opp_m3,
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
        "KMAX_MAP 1 100000000000 1600",
        "KMIN_MAP 1 100000000000 400",
        "PMAX_MAP 1 100000000000 0.2",
        "BUFFER_SIZE 32",
        "QLEN_MON_FILE %s" % (work_dir / "qlen.txt"),
        "QLEN_MON_START 0",
        "QLEN_MON_END 0",
        "FAT_TREE_FAULT_MODE %d" % args.fault_mode,
        "FAT_TREE_FAULT_SEED %d" % args.fault_seed,
    ]
    path.write_text("\n".join(lines) + "\n")


def parse_diagnosis(result_file):
    results = {}
    if not result_file.exists():
        return results
    with result_file.open() as f:
        for line in f:
            fields = line.split()
            if len(fields) < 6:
                continue
            dport = int(fields[4])
            found = int(fields[5]) != 0
            result = {"found": found}
            if len(fields) >= 20:
                result["traceroute_result"] = fields[19]
            results[dport] = result
    return results


def summarize(records, results, summary_file, mode):
    by_size = {}
    for rec in records:
        size = rec["packet_count"]
        if size not in by_size:
            by_size[size] = {"total": 0, "detected": 0, "tp": 0, "fp": 0, "fn": 0, "tn": 0}
        by_size[size]["total"] += 1
        result = results.get(rec["dport"], {})
        if result.get("found", False):
            by_size[size]["detected"] += 1
        if mode == "RPINGMESH":
            traceroute_result = result.get("traceroute_result")
            if traceroute_result in ("tp", "fp", "fn"):
                by_size[size][traceroute_result] += 1
            elif result.get("found", False):
                by_size[size]["tp"] += 1
            else:
                by_size[size]["fn"] += 1
        else:
            if result.get("found", False):
                by_size[size]["tp"] += 1
            else:
                by_size[size]["fn"] += 1

    with summary_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "packet_count", "total_flows", "detected_flows",
            "tp_flows", "fp_flows", "fn_flows", "tn_flows",
            "recall", "precision", "f1_score"])
        for size in sorted(by_size):
            total = by_size[size]["total"]
            tp = by_size[size]["tp"]
            fp = by_size[size]["fp"]
            fn = by_size[size]["fn"]
            recall = float(tp) / (tp + fn) if (tp + fn) else 0.0
            precision = float(tp) / (tp + fp) if (tp + fp) else 0.0
            f1_score = (2.0 * precision * recall / (precision + recall)
                        if (precision + recall) else 0.0)
            writer.writerow([
                size,
                total,
                by_size[size]["detected"],
                tp,
                fp,
                fn,
                by_size[size]["tn"],
                "%.9f" % recall,
                "%.9f" % precision,
                "%.9f" % f1_score,
            ])
    return by_size


def print_summary(by_size):
    print("packet_count,total_flows,detected_flows,tp_flows,fp_flows,fn_flows,tn_flows,recall,precision,f1_score")
    for size in sorted(by_size):
        total = by_size[size]["total"]
        tp = by_size[size]["tp"]
        fp = by_size[size]["fp"]
        fn = by_size[size]["fn"]
        recall = float(tp) / (tp + fn) if (tp + fn) else 0.0
        precision = float(tp) / (tp + fp) if (tp + fp) else 0.0
        f1_score = (2.0 * precision * recall / (precision + recall)
                    if (precision + recall) else 0.0)
        print("%d,%d,%d,%d,%d,%d,%d,%.9f,%.9f,%.9f" % (
            size, total, by_size[size]["detected"], tp, fp, fn, by_size[size]["tn"],
            recall, precision, f1_score))


def run_simulation(sim_dir, config_file, log_file):
    cmd = ["python2", "./waf", "--run", "scratch/third %s" % config_file]
    with log_file.open("w") as log:
        proc = subprocess.run(cmd, cwd=str(sim_dir), stdout=log, stderr=subprocess.STDOUT)
    return proc.returncode


def main():
    script_path = pathlib.Path(__file__).resolve()
    sim_dir = script_path.parents[1]
    repo_root = script_path.parents[2]

    parser = argparse.ArgumentParser(
        description="Run k-ary fat-tree fault-detection sweeps for OPP/RPINGMESH/RDPROBE.")
    parser.add_argument("--k", type=int, required=True, help="fat-tree radix; must be even")
    parser.add_argument("--max-exp", "-m", type=int, required=True,
                        help="generate packet counts 2^0..2^m")
    parser.add_argument("--flows-per-size", "-n", type=int, required=True,
                        help="number of cross-pod flows for each packet count")
    parser.add_argument("--mode", type=lambda value: value.upper(),
                        choices=sorted(MODE_TO_CC.keys()), required=True,
                        help="diagnosis transport mode")
    parser.add_argument("--fault-mode", type=int, choices=[1, 2, 3, 4], required=True,
                        help="fat-tree fault mode: 1 ACL drop, 2 loop, 3 flapping, 4 congestion")
    parser.add_argument("--work-dir", type=pathlib.Path, default=None,
                        help="directory for generated inputs and outputs")
    parser.add_argument("--stop-time", type=float, default=100.0,
                        help="SIMULATOR_STOP_TIME in seconds")
    parser.add_argument("--start-time", type=float, default=0.000001,
                        help="flow start time in seconds")
    parser.add_argument("--pg", type=int, default=3, help="priority group for generated flows")
    parser.add_argument("--base-dport", type=int, default=100, help="first generated destination port")
    parser.add_argument("--bandwidth", default="100Gbps", help="topology link bandwidth")
    parser.add_argument("--delay", default="1000ns", help="topology link propagation delay")
    parser.add_argument("--error-rate", type=float, default=0.0, help="topology link error rate")
    parser.add_argument("--fault-seed", type=int, default=7, help="fault injection random seed")
    parser.add_argument("--opp-multicast-mode", choices=["STANDARD", "USM", "SAMPLED"], default="STANDARD",
                        help="OPP probe multicast implementation mode")
    parser.add_argument("--opp-usm-sample-period", type=int, default=1,
                        help="in SAMPLED mode, every p-th same-category OPP probe uses USM")
    parser.add_argument("--opp-m", type=int, default=1, help="legacy torus OPP token count")
    parser.add_argument("--opp-m1", type=int, default=1, help="fat-tree OPP same-ToR token count")
    parser.add_argument("--opp-m2", type=int, default=1, help="fat-tree OPP same-pod token count")
    parser.add_argument("--opp-m3", type=int, default=1, help="fat-tree OPP cross-pod token count")
    parser.add_argument("--no-run", action="store_true", help="only generate files, do not run ns-3")
    args = parser.parse_args()

    if args.k <= 0 or args.k % 2 != 0:
        parser.error("--k must be a positive even number")
    if args.k > 64:
        parser.error("--k > 64 would exceed the current 65536-host IP limit")
    if args.max_exp < 0:
        parser.error("--max-exp must be non-negative")
    if args.flows_per_size <= 0:
        parser.error("--flows-per-size must be positive")
    if args.opp_usm_sample_period <= 0:
        parser.error("--opp-usm-sample-period must be positive")

    if args.work_dir is None:
        stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        args.work_dir = repo_root / "simulation" / "mix" / "fattree_fault_detection_sweep" / (
            "k%d_%s_fault%d_m%d_n%d_%s" % (
                args.k, args.mode.lower(), args.fault_mode, args.max_exp,
                args.flows_per_size, stamp))
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    topology_file = work_dir / "topology.txt"
    flow_file = work_dir / "flow.txt"
    flow_meta_file = work_dir / "flow_metadata.csv"
    trace_file = work_dir / "trace.txt"
    config_file = work_dir / "config.txt"
    result_file = work_dir / "diagnosis_result.txt"
    summary_file = work_dir / "summary.csv"
    run_log = work_dir / "run.log"

    write_fat_tree_topology(topology_file, args.k, args.bandwidth, args.delay, args.error_rate)
    records = generate_flows(
        flow_file, flow_meta_file, args.k, args.max_exp, args.flows_per_size,
        args.pg, args.base_dport, args.start_time)
    trace_file.write_text("0\n")
    write_config(config_file, args, work_dir, topology_file, flow_file, trace_file, result_file)

    print("work_dir=%s" % work_dir)
    print("flows=%d" % len(records))

    if args.no_run:
        print("generated_only=1")
        return 0

    rc = run_simulation(sim_dir, config_file, run_log)
    if rc != 0:
        print("simulation_failed=1")
        print("run_log=%s" % run_log)
        return rc

    results = parse_diagnosis(result_file)
    by_size = summarize(records, results, summary_file, args.mode)
    print_summary(by_size)
    print("summary_file=%s" % summary_file)
    print("diagnosis_result_file=%s" % result_file)
    print("run_log=%s" % run_log)
    return 0


if __name__ == "__main__":
    sys.exit(main())
