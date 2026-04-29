use std::env;

use rust_unit_merge::{
    build_clos, build_dragonfly, clos_caps, dragonfly_natural_usage, print_result,
    print_topology_summary, run_unit_merge, RunConfig,
};

fn parse_usize(args: &[String], name: &str, default: usize) -> usize {
    value(args, name)
        .map(|v| {
            v.parse::<usize>()
                .unwrap_or_else(|_| panic!("invalid value for {}", name))
        })
        .unwrap_or(default)
}

fn parse_i64_opt(args: &[String], name: &str) -> Option<i64> {
    value(args, name).map(|v| {
        v.parse::<i64>()
            .unwrap_or_else(|_| panic!("invalid value for {}", name))
    })
}

fn parse_usize_opt(args: &[String], name: &str) -> Option<usize> {
    value(args, name).map(|v| {
        v.parse::<usize>()
            .unwrap_or_else(|_| panic!("invalid value for {}", name))
    })
}

fn parse_f64(args: &[String], name: &str, default: f64) -> f64 {
    value(args, name)
        .map(|v| {
            v.parse::<f64>()
                .unwrap_or_else(|_| panic!("invalid value for {}", name))
        })
        .unwrap_or(default)
}

fn value(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find_map(|w| {
        if w[0] == name {
            Some(w[1].clone())
        } else {
            None
        }
    })
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|arg| arg == name)
}

fn common_config(args: &[String], default_l: usize) -> RunConfig {
    let group_size_limit = parse_usize(args, "--group-size-limit", default_l);
    let raw_candidate_limit = parse_usize(args, "--candidate-limit", 50_000);
    RunConfig {
        group_size_limit,
        candidate_limit: if raw_candidate_limit == 0 {
            None
        } else {
            Some(raw_candidate_limit)
        },
        groups_per_switch: parse_usize(args, "--groups-per-switch", 1024),
        cycle_representative_search: has_flag(args, "--cycle-representative-search"),
        representative_switch_group: parse_usize(args, "--representative-switch-group", 0),
        symmetry_max_batch: parse_usize(args, "--symmetry-max-batch", 0),
        symmetry_trials_per_group: parse_usize(args, "--symmetry-trials-per-group", 16),
        symmetry_min_gain_pct: parse_usize(args, "--symmetry-min-gain-pct", 100) as i64,
        shift_symmetry_max_batch: parse_usize(args, "--shift-symmetry-max-batch", 0),
        shift_symmetry_min_gain_pct: parse_usize(args, "--shift-symmetry-min-gain-pct", 0) as i64,
        shift_orbit_alternatives: parse_usize(args, "--shift-orbit-alternatives", 512),
        max_steps: parse_usize_opt(args, "--max-steps"),
        verbose_every: parse_usize(args, "--verbose-every", 200),
        rebuild_every: parse_usize(args, "--rebuild-every", 1000),
        output_groups: value(args, "--output-groups"),
    }
}

fn print_usage() {
    eprintln!(
        "Usage:
  cargo run --release -- clos [--k 8] [--m3 1] [--group-size-limit 4]
  cargo run --release -- dragonfly [--groups 9] [--routers-per-group 4] [--global-links-per-router 2] --cap 78 --group-size-limit 2

Common options:
  --candidate-limit N      candidates considered per iteration, 0 means unlimited
  --groups-per-switch N    active groups sampled from each overloaded switch index
  --cycle-representative-search
                            Dragonfly/Clos: only scan overloaded switches in one representative group/POD
  --representative-switch-group N
                            representative switch group/POD for representative search, default 0
  --symmetry-max-batch N   after one representative merge, apply up to N same-signature merges
  --symmetry-trials-per-group N
                            candidate trials per same-signature group during symmetry batching
  --symmetry-min-gain-pct N require batch candidate gain to be at least N percent of representative gain
  --shift-symmetry-max-batch N
                            Dragonfly/Clos: apply up to N cyclic group/POD-shift counterpart merges
  --shift-symmetry-min-gain-pct N
                            require shifted merge gain to be at least N percent of representative gain
  --shift-orbit-alternatives N
                            search this many top candidates for a complete cyclic orbit
  --max-steps N            stop after N merges
  --verbose-every N        print progress every N merges
  --rebuild-every N        rebuild switch->group index every N merges, 0 disables
  --build-log-every N      print topology-build progress every N BFS roots / pairs
  --output-groups PATH     write final active groups as CSV

Clos options:
  --tor-cap N --agg-cap N  override paper-like capacities

Dragonfly options:
  --cap N                  uniform switch capacity
  --capacity-scale X       if --cap is absent, scale natural-group max usage"
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || has_flag(&args, "--help") || has_flag(&args, "-h") {
        print_usage();
        return;
    }

    match args[1].as_str() {
        "clos" => {
            let k = parse_usize(&args, "--k", 8);
            let m3 = parse_usize(&args, "--m3", 1);
            let topo = build_clos(k);
            let default_l = (k / 2 / m3).max(1);
            let cfg = common_config(&args, default_l);
            let caps = clos_caps(
                &topo,
                m3,
                parse_i64_opt(&args, "--tor-cap"),
                parse_i64_opt(&args, "--agg-cap"),
            );
            print_topology_summary(&topo);
            println!(
                "mode=unit c=1 L={} candidate_limit={:?} ToR_cap={} Agg_cap={} representative_search={} representative_pod={}",
                cfg.group_size_limit,
                cfg.candidate_limit,
                caps[0],
                caps.last().copied().unwrap_or(0),
                cfg.cycle_representative_search,
                cfg.representative_switch_group
            );
            let result = run_unit_merge(&topo, &caps, &cfg);
            print_result(&topo, &caps, &cfg, &result);
        }
        "dragonfly" => {
            let groups = parse_usize(&args, "--groups", 9);
            let routers_per_group = parse_usize(&args, "--routers-per-group", 4);
            let global_links_per_router = parse_usize(&args, "--global-links-per-router", 2);
            let build_log_every = parse_usize(&args, "--build-log-every", 0);
            let topo = build_dragonfly(
                groups,
                routers_per_group,
                global_links_per_router,
                build_log_every,
            );
            let cfg = common_config(&args, routers_per_group);
            let caps = if let Some(cap) = parse_i64_opt(&args, "--cap") {
                vec![cap; topo.num_switches()]
            } else {
                let natural_usage = dragonfly_natural_usage(&topo, cfg.group_size_limit);
                let max_nat = natural_usage.iter().copied().max().unwrap_or(0);
                let scale = parse_f64(&args, "--capacity-scale", 1.0);
                let cap = ((max_nat as f64) * scale).ceil() as i64;
                println!(
                    "natural_usage_max={} capacity_scale={} uniform_cap={}",
                    max_nat, scale, cap
                );
                vec![cap; topo.num_switches()]
            };
            print_topology_summary(&topo);
            println!(
                "mode=unit c=1 L={} candidate_limit={:?} uniform_cap={} cycle_representative_search={} representative_switch_group={}",
                cfg.group_size_limit,
                cfg.candidate_limit,
                caps[0],
                cfg.cycle_representative_search,
                cfg.representative_switch_group
            );
            let result = run_unit_merge(&topo, &caps, &cfg);
            print_result(&topo, &caps, &cfg, &result);
        }
        _ => {
            print_usage();
            panic!("unknown subcommand {}", args[1]);
        }
    }
}
