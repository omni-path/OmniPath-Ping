#!/usr/bin/env python3
"""Run Figure 7/8 subfigures a, b, d, c with one global concurrency limit.

Subfigures are released in a -> b -> d -> c order.  The next subfigure may
start only after all ns-3 child tasks of the previous subfigure have been
started; already-running tasks from earlier subfigures can overlap with later
subfigures, subject to the global --jobs limit.
"""

import argparse
import importlib.util
import pathlib
import sys
import tempfile
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from types import SimpleNamespace


GROUP_SPECS = (
    ("a", "run_figure7a_8a_acl_drop.py"),
    ("b", "run_figure7b_8b_loop.py"),
    ("d", "run_figure7d_8d_congestion.py"),
    ("c", "run_figure7c_8c_link_flapping.py"),
)


def repo_root_from_script():
    return pathlib.Path(__file__).resolve().parents[2]


def figure_dir_from_script():
    return pathlib.Path(__file__).resolve().parent


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_runner_args(args):
    return SimpleNamespace(
        jobs=args.jobs,
        work_dir=None,
        output_file=None,
        f1_output_file=None,
        stop_time=args.stop_time,
        error_rate=args.error_rate,
        buffer_size=args.buffer_size,
        fault_seed=args.fault_seed,
        opp_m=args.opp_m,
        opp_m1=args.opp_m1,
        opp_m2=args.opp_m2,
        opp_m3=args.opp_m3,
        no_run=args.no_run,
        sim_dir=args.sim_dir,
    )


def build_groups(args, repo_root, figure_dir, work_root):
    groups = []
    runner_dir = figure_dir / "runner"
    runner_args = build_runner_args(args)
    for label, script_name in GROUP_SPECS:
        if label not in args.groups:
            continue
        module = load_module("figure7_8_%s" % label, runner_dir / script_name)
        group_work_dir = work_root / label
        group_work_dir.mkdir(parents=True, exist_ok=True)
        logical_tasks, runnable_tasks = module.build_tasks(
            runner_args, repo_root, figure_dir, group_work_dir)
        for idx, task in enumerate(runnable_tasks):
            task["_global_task_id"] = "%s:%s:%d" % (label, task["task_id"], idx)
        output_dir = (work_root / "results") if args.no_run else (figure_dir / "results")
        groups.append({
            "label": label,
            "module": module,
            "logical_tasks": logical_tasks,
            "runnable_tasks": runnable_tasks,
            "next_to_start": 0,
            "results_by_task_id": {},
            "output_file": output_dir / module.DEFAULT_OUTPUT_FILE,
            "f1_output_file": output_dir / module.DEFAULT_F1_OUTPUT_FILE,
        })
    return groups


def submit_next_task(executor, groups, unlocked_group, sim_dir, no_run, futures):
    for group_index in range(unlocked_group + 1):
        group = groups[group_index]
        if group["next_to_start"] >= len(group["runnable_tasks"]):
            continue
        task = group["runnable_tasks"][group["next_to_start"]]
        group["next_to_start"] += 1
        future = executor.submit(group["module"].run_task, sim_dir, task, no_run)
        futures[future] = (group_index, task)
        return True
    return False


def update_unlocked_group(groups, unlocked_group):
    while (unlocked_group + 1 < len(groups) and
           groups[unlocked_group]["next_to_start"] >= len(groups[unlocked_group]["runnable_tasks"])):
        unlocked_group += 1
    return unlocked_group


def aggregate_group(group):
    module = group["module"]
    rows = []
    result_by_key = {}

    for task in group["logical_tasks"]:
        key = (task["series"], task["packet_exp"])
        if task["copy_from_packet_exp"] is None:
            child_results = [
                group["results_by_task_id"][task_id]
                for task_id in task["child_task_ids"]
                if task_id in group["results_by_task_id"]
            ]
            result = module.aggregate_results(child_results)
            result_by_key[key] = result
            row = module.row_from_task_result(task, result)
            rows.append(row)
            continue

        source_key = (task["series"], task["copy_from_packet_exp"])
        source_result = result_by_key.get(source_key)
        if source_result is None:
            result = {
                "status": "missing_source",
                "returncode": -1,
                "elapsed_sec": 0.0,
                "metrics": module.empty_metrics(),
            }
            status = "missing_source"
        else:
            result = source_result
            status = "copied" if source_result["status"] in ("ok", "generated") else "copy_source_failed"
        row = module.row_from_task_result(task, result, status_override=status, elapsed_override=0.0)
        rows.append(row)

    series_order = {cfg["series"]: idx for idx, cfg in enumerate(module.SERIES)}
    rows.sort(key=lambda row: (series_order.get(row["series"], len(series_order)), int(row["packet_exp"])))
    module.write_recall_matrix(group["output_file"], rows)
    module.write_f1_matrix(group["f1_output_file"], rows)

    failed = [row for row in rows if row["status"] not in ("ok", "generated", "copied")]
    return failed


def run_global(args):
    repo_root = repo_root_from_script()
    figure_dir = figure_dir_from_script()
    sim_dir = repo_root / "simulation"
    args.sim_dir = sim_dir

    temp_work_dir = None
    if args.work_root is None:
        temp_work_dir = tempfile.TemporaryDirectory(prefix="figure7_8_abdc_")
        work_root = pathlib.Path(temp_work_dir.name).resolve()
    else:
        work_root = args.work_root.resolve()
    work_root.mkdir(parents=True, exist_ok=True)

    try:
        groups = build_groups(args, repo_root, figure_dir, work_root)

        unlocked_group = 0
        futures = {}
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            while True:
                unlocked_group = update_unlocked_group(groups, unlocked_group)
                while len(futures) < args.jobs:
                    if not submit_next_task(executor, groups, unlocked_group, sim_dir, args.no_run, futures):
                        break
                    unlocked_group = update_unlocked_group(groups, unlocked_group)

                if not futures:
                    all_started = all(
                        group["next_to_start"] >= len(group["runnable_tasks"])
                        for group in groups)
                    if all_started:
                        break
                    continue

                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    group_index, task = futures.pop(future)
                    group = groups[group_index]
                    try:
                        result = future.result()
                    except Exception as exc:
                        (task["work_dir"] / "wrapper_exception.txt").write_text("%s\n" % exc)
                        result = {
                            "status": "exception",
                            "returncode": -1,
                            "elapsed_sec": 0.0,
                            "metrics": group["module"].empty_metrics(),
                        }
                    group["results_by_task_id"][task["task_id"]] = result

        failures = []
        for group in groups:
            failures.extend(aggregate_group(group))
        return 1 if failures else 0
    finally:
        if temp_work_dir is not None:
            temp_work_dir.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=24,
                        help="global maximum concurrent ns-3 child tasks")
    parser.add_argument("--groups", nargs="+", choices=[label for label, _ in GROUP_SPECS],
                        default=[label for label, _ in GROUP_SPECS],
                        help="subfigures to run, in the requested order")
    parser.add_argument("--work-root", type=pathlib.Path, default=None,
                        help="optional directory for retaining generated configs/logs")
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

    if args.jobs <= 0:
        parser.error("--jobs must be positive")
    seen = set()
    args.groups = [group for group in args.groups
                   if not (group in seen or seen.add(group))]
    for n in (args.opp_m, args.opp_m1, args.opp_m2, args.opp_m3):
        if n <= 0:
            parser.error("OPP token parameters must be positive")
    return run_global(args)


if __name__ == "__main__":
    sys.exit(main())
