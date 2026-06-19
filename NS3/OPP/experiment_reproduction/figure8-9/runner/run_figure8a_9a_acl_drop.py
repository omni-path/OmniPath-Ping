#!/usr/bin/env python3
"""Run Figure 8(a) ACL-drop recall sweeps on k=64 fat-tree."""

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

OPP_SOURCE_PACKET_EXP = 0
PACKET_EXPS = tuple(range(0, 15))
SPLIT_START_PACKET_EXP = 12
DEFAULT_OUTPUT_FILE = "Figure8(a)_results.csv"
DEFAULT_F1_OUTPUT_FILE = "Figure9(a)_results.csv"

SERIES = (
    {
        "series": "OPP_USM",
        "mode": "OPP",
        "opp_multicast_mode": "USM",
        "opp_usm_sample_period": 1,
        "run_exps": (OPP_SOURCE_PACKET_EXP,),
    },
    {
        "series": "OPP_STANDARD",
        "mode": "OPP",
        "opp_multicast_mode": "STANDARD",
        "opp_usm_sample_period": 1,
        "run_exps": (OPP_SOURCE_PACKET_EXP,),
    },
    {
        "series": "OPP_SAMPLED_USM_p10",
        "mode": "OPP",
        "opp_multicast_mode": "SAMPLED",
        "opp_usm_sample_period": 10,
        "run_exps": (OPP_SOURCE_PACKET_EXP,),
    },
    {
        "series": "RDPROBE",
        "mode": "RDPROBE",
        "opp_multicast_mode": "STANDARD",
        "opp_usm_sample_period": 1,
        "run_exps": PACKET_EXPS,
    },
    {
        "series": "RPINGMESH",
        "mode": "RPINGMESH",
        "opp_multicast_mode": "STANDARD",
        "opp_usm_sample_period": 1,
        "run_exps": PACKET_EXPS,
    },
)


def repo_root_from_script():
    return pathlib.Path(__file__).resolve().parents[3]


def figure_dir_from_script():
    return pathlib.Path(__file__).resolve().parents[1]


def write_trace_file(path):
    path.write_text("0\n")


def read_declared_flow_count(flow_file):
    with flow_file.open() as f:
        return int(f.readline().strip())


def split_count_for(series_cfg, exp):
    if series_cfg["mode"] == "OPP" or exp < SPLIT_START_PACKET_EXP:
        return 1
    return 1 << (exp - 11)


def write_split_flow_file(source_flow_file, split_flow_file, split_index, split_count):
    with source_flow_file.open() as f:
        declared = int(f.readline().strip())
        lines = f.readlines()
    if declared != len(lines):
        raise ValueError("flow count mismatch in %s: declared=%d actual=%d" % (
            source_flow_file, declared, len(lines)))

    start = declared * split_index // split_count
    end = declared * (split_index + 1) // split_count
    selected = lines[start:end]
    split_flow_file.parent.mkdir(parents=True, exist_ok=True)
    with split_flow_file.open("w") as f:
        f.write("%d\n" % len(selected))
        f.writelines(selected)


def diagnosis_flags_file(task):
    if task["mode"] == "OPP":
        return task["work_dir"] / "opp_diag_result.txt"
    if task["mode"] == "RDPROBE":
        return task["work_dir"] / "rdprobe_diag_result.txt"
    if task["mode"] == "RPINGMESH":
        return task["work_dir"] / "rpingmesh_diag_result.txt"
    raise ValueError("unknown mode %s" % task["mode"])


def empty_metrics():
    return {
        "total_flows": "",
        "found_fault_sum": "",
        "tp_flows": "",
        "fp_flows": "",
        "fn_flows": "",
        "tn_flows": "",
        "recall": "",
        "precision": "",
        "f1_score": "",
    }


def write_config(path, args, task, topology_file, flow_file, trace_file):
    mode = task["mode"]
    work_dir = task["work_dir"]
    enable_rdprobe = 1 if mode == "RDPROBE" else 0
    enable_rpingmesh = 1 if mode == "RPINGMESH" else 0
    enable_opp = 1 if mode == "OPP" else 0

    lines = [
        "ENABLE_QCN 1",
        "USE_DYNAMIC_PFC_THRESHOLD 1",
        "PACKET_PAYLOAD_SIZE 1000",
        "TOPOLOGY_FILE %s" % topology_file,
        "TOPOLOGY_MODE FAT_TREE",
        "FAT_TREE_K 64",
        "FLOW_FILE %s" % flow_file,
        "TRACE_FILE %s" % trace_file,
        "TRACE_OUTPUT_FILE %s" % (work_dir / "trace.tr"),
        "FCT_OUTPUT_FILE %s" % (work_dir / "fct.txt"),
        "PFC_OUTPUT_FILE %s" % (work_dir / "pfc.txt"),
        "FLOW_COVERAGE_OUTPUT_FILE %s" % (work_dir / "flow_coverage.txt"),
        "SIMULATOR_STOP_TIME %.9f" % args.stop_time,
        "CC_MODE %d" % MODE_TO_CC[mode],
        "ROUTING_MODE PACKET_SPRAY",
        "OPP_MULTICAST_MODE %s" % task["opp_multicast_mode"],
        "OPP_USM_SAMPLE_PERIOD %d" % task["opp_usm_sample_period"],
        "OPP_TOKENS_PER_TOR %d" % args.opp_m,
        "OPP_FATTREE_M1 %d" % args.opp_m1,
        "OPP_FATTREE_M2 %d" % args.opp_m2,
        "OPP_FATTREE_M3 %d" % args.opp_m3,
        "OPP_LEAFSPINE_M1 %d" % args.opp_m1,
        "OPP_LEAFSPINE_M2 %d" % args.opp_m2,
        "FAT_TREE_FAULT_MODE 1",
        "FAT_TREE_FAULT_SEED %d" % args.fault_seed,
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
        "ENABLE_RDPROBE_DIAG %d" % enable_rdprobe,
        "RDPROBE_DIAG_OUTPUT_FILE %s" % (work_dir / "rdprobe_trace.txt"),
        "RDPROBE_DIAG_RESULT_OUTPUT_FILE %s" % (work_dir / "rdprobe_diag_result.txt"),
        "ENABLE_RPINGMESH_DIAG %d" % enable_rpingmesh,
        "RPINGMESH_DIAG_RESULT_OUTPUT_FILE %s" % (work_dir / "rpingmesh_diag_result.txt"),
        "ENABLE_OPP_DIAG %d" % enable_opp,
        "OPP_DIAG_RESULT_OUTPUT_FILE %s" % (work_dir / "opp_diag_result.txt"),
    ]
    path.write_text("\n".join(relativize_config_lines(lines, args.sim_dir)) + "\n")


def parse_metrics(task):
    result_file = diagnosis_flags_file(task)
    found = 0
    rows = 0
    tp = 0
    fp = 0
    fn = 0
    tn = 0
    with result_file.open() as f:
        for line in f:
            fields = line.split()
            if len(fields) < 6:
                continue
            found_fault = int(fields[5]) != 0
            rows += 1
            if found_fault:
                found += 1

            if task["mode"] == "RPINGMESH":
                traceroute_result = fields[19] if len(fields) >= 20 else ""
                if traceroute_result == "tp":
                    tp += 1
                elif traceroute_result == "fp":
                    fp += 1
                else:
                    fn += 1
            else:
                if found_fault:
                    tp += 1
                else:
                    fn += 1
    if rows == 0:
        return {
            "total_flows": 0,
            "found_fault_sum": 0,
            "tp_flows": 0,
            "fp_flows": 0,
            "fn_flows": 0,
            "tn_flows": 0,
            "recall": "",
            "precision": "",
            "f1_score": "",
        }
    detection_recall = float(found) / float(rows) if rows else 0.0
    localization_recall = float(tp) / float(tp + fn) if (tp + fn) else 0.0
    precision = float(tp) / float(tp + fp) if (tp + fp) else 0.0
    f1_score = (
        2.0 * precision * localization_recall / (precision + localization_recall)
        if (precision + localization_recall)
        else 0.0
    )
    return {
        "total_flows": rows,
        "found_fault_sum": found,
        "tp_flows": tp,
        "fp_flows": fp,
        "fn_flows": fn,
        "tn_flows": tn,
        "recall": detection_recall,
        "precision": precision,
        "f1_score": f1_score,
    }


def aggregate_metrics(metrics_list):
    if not metrics_list or any(metric["total_flows"] == "" for metric in metrics_list):
        return empty_metrics()

    rows = sum(int(metric["total_flows"]) for metric in metrics_list)
    found = sum(int(metric["found_fault_sum"]) for metric in metrics_list)
    tp = sum(int(metric["tp_flows"]) for metric in metrics_list)
    fp = sum(int(metric["fp_flows"]) for metric in metrics_list)
    fn = sum(int(metric["fn_flows"]) for metric in metrics_list)
    tn = sum(int(metric["tn_flows"]) for metric in metrics_list)
    if rows == 0:
        return {
            "total_flows": 0,
            "found_fault_sum": 0,
            "tp_flows": 0,
            "fp_flows": 0,
            "fn_flows": 0,
            "tn_flows": 0,
            "recall": "",
            "precision": "",
            "f1_score": "",
        }

    detection_recall = float(found) / float(rows)
    localization_recall = float(tp) / float(tp + fn) if (tp + fn) else 0.0
    precision = float(tp) / float(tp + fp) if (tp + fp) else 0.0
    f1_score = (
        2.0 * precision * localization_recall / (precision + localization_recall)
        if (precision + localization_recall)
        else 0.0
    )
    return {
        "total_flows": rows,
        "found_fault_sum": found,
        "tp_flows": tp,
        "fp_flows": fp,
        "fn_flows": fn,
        "tn_flows": tn,
        "recall": detection_recall,
        "precision": precision,
        "f1_score": f1_score,
    }


def aggregate_results(child_results):
    if not child_results:
        return {
            "status": "missing_children",
            "returncode": -1,
            "elapsed_sec": 0.0,
            "metrics": empty_metrics(),
        }

    statuses = [result["status"] for result in child_results]
    returncodes = [result["returncode"] for result in child_results]
    if all(status == "generated" for status in statuses):
        status = "generated"
    elif all(status == "ok" for status in statuses):
        status = "ok"
    else:
        status = "partial"

    return {
        "status": status,
        "returncode": 0 if all(code == 0 for code in returncodes) else next(code for code in returncodes if code != 0),
        "elapsed_sec": max(float(result["elapsed_sec"]) for result in child_results),
        "metrics": aggregate_metrics([result["metrics"] for result in child_results]),
    }


def run_task(sim_dir, task, no_run):
    if no_run:
        return {
            "status": "generated",
            "returncode": 0,
            "elapsed_sec": 0.0,
            "metrics": empty_metrics(),
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
            "metrics": empty_metrics(),
        }

    try:
        metrics = parse_metrics(task)
    except Exception:
        (task["work_dir"] / "parse_error.txt").write_text("failed to parse diagnosis result\n")
        return {
            "status": "missing_metric",
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "metrics": empty_metrics(),
        }

    return {
        "status": "ok",
        "returncode": proc.returncode,
        "elapsed_sec": elapsed,
        "metrics": metrics,
    }


def write_metric_matrix(output_file, rows, metric_name):
    packet_counts = sorted({int(row["packet_count"]) for row in rows})
    series_order = []
    by_series_packet = {}
    for row in rows:
        series = row["series"]
        if series not in series_order:
            series_order.append(series)
        by_series_packet[(series, int(row["packet_count"]))] = row[metric_name]

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["series"] + [str(packet_count) for packet_count in packet_counts])
        for series in series_order:
            writer.writerow(
                [series] + [by_series_packet.get((series, packet_count), "")
                            for packet_count in packet_counts])


def write_recall_matrix(output_file, rows):
    write_metric_matrix(output_file, rows, "recall")


def write_f1_matrix(output_file, rows):
    write_metric_matrix(output_file, rows, "f1_score")


def build_tasks(args, repo_root, figure_dir, work_root):
    logical_tasks = []
    runnable_tasks = []
    trace_file = work_root / "trace.txt"
    write_trace_file(trace_file)

    topology_file = repo_root / "experiment_reproduction" / "topologies" / "fattree" / "fattree_k64_topology.txt"
    if not topology_file.exists():
        raise FileNotFoundError("missing topology file: %s" % topology_file)

    flow_dir = figure_dir / "traffic_load" / "fattree" / "k64"
    if not flow_dir.exists():
        raise FileNotFoundError("missing flow directory: %s" % flow_dir)

    for series_cfg in SERIES:
        for exp in PACKET_EXPS:
            flow_file = flow_dir / ("flow_2e%d.txt" % exp)
            if not flow_file.exists():
                raise FileNotFoundError("missing flow file: %s" % flow_file)

            copy_from_exp = None
            if series_cfg["mode"] == "OPP" and exp != OPP_SOURCE_PACKET_EXP:
                copy_from_exp = OPP_SOURCE_PACKET_EXP

            task_name = "%s_2e%d" % (series_cfg["series"].lower(), exp)
            work_dir = work_root / task_name
            work_dir.mkdir(parents=True, exist_ok=True)
            logical_task = {
                "series": series_cfg["series"],
                "mode": series_cfg["mode"],
                "packet_exp": exp,
                "packet_count": 1 << exp,
                "declared_flows": read_declared_flow_count(flow_file),
                "copy_from_packet_exp": copy_from_exp,
                "child_task_ids": [],
                "opp_multicast_mode": series_cfg["opp_multicast_mode"],
                "opp_usm_sample_period": series_cfg["opp_usm_sample_period"],
                "work_dir": work_dir,
                "topology_file": topology_file.resolve(),
                "flow_file": flow_file.resolve(),
                "config_file": "",
                "run_log": "",
                "diagnosis_file": "",
            }

            if copy_from_exp is None:
                split_count = split_count_for(series_cfg, exp)
                for split_index in range(split_count):
                    if split_count == 1:
                        child_name = task_name
                        child_work_dir = work_dir
                        child_flow_file = flow_file.resolve()
                    else:
                        child_name = "%s_part%dof%d" % (task_name, split_index + 1, split_count)
                        child_work_dir = work_root / child_name
                        child_work_dir.mkdir(parents=True, exist_ok=True)
                        child_flow_file = child_work_dir / "flow.txt"
                        write_split_flow_file(flow_file, child_flow_file, split_index, split_count)

                    child_task = dict(logical_task)
                    child_task["task_id"] = child_name
                    child_task["split_index"] = split_index
                    child_task["split_count"] = split_count
                    child_task["declared_flows"] = read_declared_flow_count(child_flow_file)
                    child_task["work_dir"] = child_work_dir
                    child_task["flow_file"] = child_flow_file.resolve()
                    child_task["config_file"] = child_work_dir / "config.txt"
                    child_task["run_log"] = child_work_dir / "run.log"
                    child_task["diagnosis_file"] = diagnosis_flags_file(child_task)
                    write_config(child_task["config_file"], args, child_task,
                                 child_task["topology_file"], child_task["flow_file"], trace_file.resolve())
                    logical_task["child_task_ids"].append(child_task["task_id"])
                    runnable_tasks.append(child_task)

            logical_tasks.append(logical_task)
    return logical_tasks, runnable_tasks


def row_from_task_result(task, result, status_override=None, elapsed_override=None):
    metrics = result["metrics"]
    recall = metrics["recall"]
    precision = metrics["precision"]
    f1_score = metrics["f1_score"]
    return {
        "series": task["series"],
        "packet_exp": task["packet_exp"],
        "packet_count": task["packet_count"],
        "declared_flows": task["declared_flows"],
        "found_fault_sum": metrics["found_fault_sum"],
        "total_flows": metrics["total_flows"],
        "tp_flows": metrics["tp_flows"],
        "fp_flows": metrics["fp_flows"],
        "fn_flows": metrics["fn_flows"],
        "tn_flows": metrics["tn_flows"],
        "recall": "" if recall == "" else "%.9f" % recall,
        "precision": "" if precision == "" else "%.9f" % precision,
        "f1_score": "" if f1_score == "" else "%.9f" % f1_score,
        "status": status_override if status_override is not None else result["status"],
        "returncode": result["returncode"],
        "elapsed_sec": "%.6f" % (elapsed_override if elapsed_override is not None else result["elapsed_sec"]),
        "work_dir": task.get("work_dir", ""),
        "config_file": task.get("config_file", ""),
        "run_log": task.get("run_log", ""),
        "diagnosis_file": task.get("diagnosis_file", ""),
    }


def run_runnable_tasks(sim_dir, tasks, workers, no_run, result_by_task_id):
    if not tasks:
        return

    workers = min(workers, len(tasks))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_task = {
            executor.submit(run_task, sim_dir, task, no_run): task
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
                    "metrics": empty_metrics(),
                }
            result_by_task_id[task["task_id"]] = result


def main():
    repo_root = repo_root_from_script()
    figure_dir = figure_dir_from_script()
    sim_dir = repo_root / "simulation"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=0,
                        help="maximum concurrent ns-3 jobs; 0 means all runnable jobs")
    parser.add_argument("--work-dir", type=pathlib.Path, default=None,
                        help="optional directory for retaining generated configs/logs")
    parser.add_argument("--output-file", type=pathlib.Path, default=None,
                        help="result CSV; default is results/Figure8(a)_results.csv")
    parser.add_argument("--f1-output-file", type=pathlib.Path, default=None,
                        help="F1 CSV; default is results/Figure9(a)_results.csv")
    parser.add_argument("--stop-time", type=float, default=100.0)
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument("--buffer-size", type=int, default=128)
    parser.add_argument("--fault-seed", type=int, default=1)
    parser.add_argument("--opp-m", type=int, default=1)
    parser.add_argument("--opp-m1", type=int, default=1)
    parser.add_argument("--opp-m2", type=int, default=8)
    parser.add_argument("--opp-m3", type=int, default=1)
    parser.add_argument("--no-run", action="store_true")
    args = parser.parse_args()
    args.sim_dir = sim_dir

    if args.jobs < 0:
        parser.error("--jobs must be non-negative")
    for n in (args.opp_m, args.opp_m1, args.opp_m2, args.opp_m3):
        if n <= 0:
            parser.error("OPP token parameters must be positive")

    if args.output_file is None:
        args.output_file = figure_dir / "results" / DEFAULT_OUTPUT_FILE
    output_file = args.output_file.resolve()
    if args.f1_output_file is None:
        args.f1_output_file = figure_dir / "results" / DEFAULT_F1_OUTPUT_FILE
    f1_output_file = args.f1_output_file.resolve()

    temp_work_dir = None
    if args.work_dir is None:
        temp_work_dir = tempfile.TemporaryDirectory(prefix="figure8a_acl_")
        work_root = pathlib.Path(temp_work_dir.name).resolve()
    else:
        work_root = args.work_dir.resolve()
    work_root.mkdir(parents=True, exist_ok=True)

    try:
        logical_tasks, runnable_tasks = build_tasks(args, repo_root, figure_dir, work_root)

        workers = len(runnable_tasks) if args.jobs == 0 else min(args.jobs, len(runnable_tasks))
        rows = []
        result_by_key = {}
        result_by_task_id = {}
        run_runnable_tasks(sim_dir, runnable_tasks, workers, args.no_run, result_by_task_id)

        task_by_key = {(task["series"], task["packet_exp"]): task for task in logical_tasks}
        for task in logical_tasks:
            key = (task["series"], task["packet_exp"])
            if task["copy_from_packet_exp"] is None:
                child_results = [
                    result_by_task_id[task_id]
                    for task_id in task["child_task_ids"]
                    if task_id in result_by_task_id
                ]
                result = aggregate_results(child_results)
                result_by_key[key] = result
                row = row_from_task_result(task, result)
                rows.append(row)
                continue
            source_key = (task["series"], task["copy_from_packet_exp"])
            source_result = result_by_key.get(source_key)
            if source_result is None:
                result = {
                    "status": "missing_source",
                    "returncode": -1,
                    "elapsed_sec": 0.0,
                    "metrics": empty_metrics(),
                }
                status = "missing_source"
            else:
                result = source_result
                status = "copied" if source_result["status"] in ("ok", "generated") else "copy_source_failed"
            row = row_from_task_result(task, result, status_override=status, elapsed_override=0.0)
            rows.append(row)

        series_order = {cfg["series"]: idx for idx, cfg in enumerate(SERIES)}
        rows.sort(key=lambda row: (series_order.get(row["series"], len(series_order)), int(row["packet_exp"])))
        write_recall_matrix(output_file, rows)
        write_f1_matrix(f1_output_file, rows)

        failed = [row for row in rows if row["status"] not in ("ok", "generated", "copied")]
        return 1 if failed else 0
    finally:
        if temp_work_dir is not None:
            temp_work_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
