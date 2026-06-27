use std::env;

use rust_unit_merge::solver::run_solver;
use rust_unit_merge::{
    build_clos, build_dragonfly, build_dragonfly_plus, build_leaf_spine, build_torus2d,
    build_torus3d, print_result, print_topology_summary, ClosSourceGroupScope,
    DragonflyPlusSourceGroupScope, DragonflySourceGroupScope, SolverConfig, SolverProgress,
    TopologyCase, TopologyMeta, Torus2DSourceGroupScope, Torus3DSourceGroupScope,
};

fn parse_usize(args: &[String], name: &str, default: usize) -> usize {
    value(args, name)
        .map(|v| {
            v.parse::<usize>()
                .unwrap_or_else(|_| panic!("invalid value for {}", name))
        })
        .unwrap_or(default)
}

fn parse_usize_any(args: &[String], names: &[&str], default: usize) -> usize {
    names
        .iter()
        .find_map(|name| value(args, name).map(|v| (*name, v)))
        .map(|(name, v)| {
            v.parse::<usize>()
                .unwrap_or_else(|_| panic!("invalid value for {}", name))
        })
        .unwrap_or(default)
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

fn parse_clos_source_group_scope(args: &[String]) -> ClosSourceGroupScope {
    match value(args, "--source-group-scope")
        .unwrap_or_else(|| "pod".to_string())
        .as_str()
    {
        "pod" => ClosSourceGroupScope::Pod,
        "tor" => ClosSourceGroupScope::Tor,
        other => panic!("invalid value for --source-group-scope: {}", other),
    }
}

fn parse_dragonfly_plus_source_group_scope(args: &[String]) -> DragonflyPlusSourceGroupScope {
    match value(args, "--source-group-scope")
        .unwrap_or_else(|| "group".to_string())
        .as_str()
    {
        "group" => DragonflyPlusSourceGroupScope::Group,
        "tor" => DragonflyPlusSourceGroupScope::Tor,
        other => panic!("invalid value for --source-group-scope: {}", other),
    }
}

fn parse_dragonfly_source_group_scope(args: &[String]) -> DragonflySourceGroupScope {
    match value(args, "--source-group-scope")
        .unwrap_or_else(|| "group".to_string())
        .as_str()
    {
        "group" => DragonflySourceGroupScope::Group,
        "router" | "tor" => DragonflySourceGroupScope::Router,
        "source-target-group" | "router-group" | "tor-group" => {
            DragonflySourceGroupScope::SourceTargetGroup
        }
        other => panic!("invalid value for --source-group-scope: {}", other),
    }
}

fn parse_torus3d_source_group_scope(args: &[String]) -> Torus3DSourceGroupScope {
    match value(args, "--source-group-scope")
        .unwrap_or_else(|| "line".to_string())
        .as_str()
    {
        "line" => Torus3DSourceGroupScope::Line,
        "host" => Torus3DSourceGroupScope::Host,
        other => panic!("invalid value for --source-group-scope: {}", other),
    }
}

fn parse_torus2d_source_group_scope(args: &[String]) -> Torus2DSourceGroupScope {
    match value(args, "--source-group-scope")
        .unwrap_or_else(|| "row".to_string())
        .as_str()
    {
        "row" | "line" => Torus2DSourceGroupScope::Row,
        "host" => Torus2DSourceGroupScope::Host,
        other => panic!("invalid value for --source-group-scope: {}", other),
    }
}

fn common_config(args: &[String], default_l: usize) -> SolverConfig {
    let raw_candidate_limit = parse_usize(args, "--candidate-limit", 50_000);
    SolverConfig {
        group_size_limit: parse_usize(args, "--group-size-limit", default_l),
        candidate_limit: if raw_candidate_limit == 0 {
            None
        } else {
            Some(raw_candidate_limit)
        },
        groups_per_node: parse_usize_any(args, &["--groups-per-node", "--groups-per-switch"], 1024),
        representative_search: !has_flag(args, "--no-representative-search"),
        validate_symmetry: !has_flag(args, "--skip-symmetry-validation"),
        orbit_alternative_limit: parse_usize_any(
            args,
            &["--orbit-alternative-limit", "--shift-orbit-alternatives"],
            512,
        ),
        max_steps: parse_usize_opt(args, "--max-steps"),
        progress_every: parse_usize_any(args, &["--progress-every", "--verbose-every"], 200),
        rebuild_every: parse_usize(args, "--rebuild-every", 1000),
    }
}

fn print_usage() {
    eprintln!(
        "Usage:
  cargo run --release -- clos [--k 8] [--m3 1] [--group-size-limit 4]
  cargo run --release -- dragonfly [--groups 9] [--routers-per-group 4] [--global-links-per-router 2] --cap 78 --group-size-limit 2
  cargo run --release -- dragonfly-plus [--groups 65] [--leaves-per-group 8] [--spines-per-group 8] [--global-links-per-spine 8] [--dense-global] --cap 4096 --group-size-limit 6
  cargo run --release -- leaf-spine [--leaves 128] [--spines 64] [--leaf-cap 1024] [--group-size-limit 1]
  cargo run --release -- torus2d [--n 32] [--cap 4096] [--group-size-limit 8]
  cargo run --release -- torus3d [--n 8] [--cap 4096] [--group-size-limit 8]

Common options:
  --candidate-limit N        candidates considered per iteration, 0 means unlimited
  --groups-per-node N        active groups sampled from each overloaded node
  --no-representative-search search all overloaded nodes instead of first symmetry group + fixed nodes
  --skip-symmetry-validation skip expensive demand/footprint closure checks
  --orbit-alternative-limit N
                              search this many top candidates for a complete symmetry orbit
  --max-steps N              stop after N merges
  --progress-every N         print progress every N merges
  --rebuild-every N          rebuild node->group index every N merges, 0 disables
  --no-symmetry              disable topology-provided orbit symmetry
  --build-log-every N        print Dragonfly build progress every N BFS roots / demands
  --output-groups PATH       write final active groups as CSV

Compatibility aliases:
  --groups-per-switch, --shift-orbit-alternatives, --verbose-every

Clos options:
  --tor-cap N --agg-cap N    override paper-like capacities
  --include-intra-pod        include same-POD ToR-ToR demands in CLOS
  --source-group-scope pod|tor
                              CLOS source merge control domain, default pod

Dragonfly options:
  --cap N                    uniform node capacity
  --capacity-scale X         if --cap is absent, scale natural-group max usage
  --source-group-scope group|router|source-target-group
                              Dragonfly source merge control domain, default group

Dragonfly+ options:
  --groups N
  --leaves-per-group N
  --spines-per-group N
  --global-links-per-spine N
  --dense-global            connect every same-index spine pair between every group pair
  --cap N                    uniform switch capacity
  --source-group-scope group|tor
                              Dragonfly+ source merge control domain, default group

Leaf-spine options:
  --leaves N --spines N      leaf-spine topology size
  --leaf-cap N               table capacity for leaf nodes; spines are not in footprints

Torus3D options:
  --n N                      3D torus side length
  --cap N                    capacity for every torus node
  --source-group-scope line|host
                              Torus3D source merge control domain, default line

Torus2D options:
  --n N                      2D torus side length
  --cap N                    capacity for every torus node
  --source-group-scope row|host
                              Torus2D source merge control domain, default row"
    );
}

fn solve_and_print(case: &TopologyCase, cfg: &SolverConfig, output_groups: Option<&str>) {
    let mut progress = |event: SolverProgress| {
        println!(
            "step={} reps={} groups={} overflow={} max_overflow={} elapsed_s={:.1} steps/s={:.1} overflow_drop/s={:.1} overloaded={}/{} candidates={} orbit_batch={} orbit_tested={}",
            event.steps,
            event.representative_steps,
            event.active_groups,
            event.overflow,
            event.max_overflow,
            event.elapsed_ms as f64 / 1000.0,
            event.steps_per_s,
            event.overflow_drop_per_s,
            event.searched_overloaded_nodes,
            event.overloaded_nodes,
            event.candidates_considered,
            event.merged_in_batch,
            event.orbit_tested,
        );
    };
    let result = if cfg.progress_every > 0 {
        run_solver(&case.problem, cfg, Some(&mut progress))
    } else {
        run_solver(&case.problem, cfg, None)
    }
    .expect("solver failed");
    print_result(case, cfg, &result, output_groups);
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
            let default_l = (k / 2 / m3).max(1);
            let cfg = common_config(&args, default_l);
            let source_group_scope = parse_clos_source_group_scope(&args);
            let mut case = build_clos(
                k,
                m3,
                parse_usize_opt(&args, "--tor-cap"),
                parse_usize_opt(&args, "--agg-cap"),
                has_flag(&args, "--include-intra-pod"),
                source_group_scope,
            );
            if has_flag(&args, "--no-symmetry") {
                case.problem.symmetry = None;
            }
            print_topology_summary(&case);
            if let TopologyMeta::Clos {
                tor_cap, agg_cap, ..
            } = &case.meta
            {
                println!(
                    "mode=unit c=1 L={} candidate_limit={:?} ToR_cap={} Agg_cap={} source_group_scope={} representative_search={} strict_orbit={}",
                    cfg.group_size_limit,
                    cfg.candidate_limit,
                    tor_cap,
                    agg_cap,
                    source_group_scope.as_str(),
                    cfg.representative_search,
                    case.problem.symmetry.is_some(),
                );
            }
            solve_and_print(&case, &cfg, value(&args, "--output-groups").as_deref());
        }
        "dragonfly" => {
            let groups = parse_usize(&args, "--groups", 9);
            let routers_per_group = parse_usize(&args, "--routers-per-group", 4);
            let global_links_per_router = parse_usize(&args, "--global-links-per-router", 2);
            let build_log_every = parse_usize(&args, "--build-log-every", 0);
            let cfg = common_config(&args, routers_per_group);
            let source_group_scope = parse_dragonfly_source_group_scope(&args);
            let mut case = build_dragonfly(
                groups,
                routers_per_group,
                global_links_per_router,
                parse_usize_opt(&args, "--cap"),
                parse_f64(&args, "--capacity-scale", 1.0),
                cfg.group_size_limit,
                build_log_every,
                source_group_scope,
            );
            if has_flag(&args, "--no-symmetry") {
                case.problem.symmetry = None;
            }
            if let TopologyMeta::Dragonfly {
                natural_usage_max,
                capacity_scale,
                uniform_cap,
                ..
            } = &case.meta
            {
                if let (Some(max_nat), Some(scale)) = (natural_usage_max, capacity_scale) {
                    println!(
                        "natural_usage_max={} capacity_scale={} uniform_cap={}",
                        max_nat, scale, uniform_cap
                    );
                }
            }
            print_topology_summary(&case);
            if let TopologyMeta::Dragonfly { uniform_cap, .. } = &case.meta {
                println!(
                    "mode=unit c=1 L={} candidate_limit={:?} uniform_cap={} source_group_scope={} representative_search={} strict_orbit={}",
                    cfg.group_size_limit,
                    cfg.candidate_limit,
                    uniform_cap,
                    source_group_scope.as_str(),
                    cfg.representative_search,
                    case.problem.symmetry.is_some(),
                );
            }
            solve_and_print(&case, &cfg, value(&args, "--output-groups").as_deref());
        }
        "dragonfly-plus" | "dragonfly_plus" => {
            let groups = parse_usize(&args, "--groups", 65);
            let leaves_per_group = parse_usize(&args, "--leaves-per-group", 8);
            let spines_per_group = parse_usize(&args, "--spines-per-group", 8);
            let global_links_per_spine = parse_usize(&args, "--global-links-per-spine", 8);
            let dense_global = has_flag(&args, "--dense-global");
            let cap = parse_usize(&args, "--cap", 4096);
            let build_log_every = parse_usize(&args, "--build-log-every", 0);
            let cfg = common_config(&args, leaves_per_group);
            let source_group_scope = parse_dragonfly_plus_source_group_scope(&args);
            let mut case = build_dragonfly_plus(
                groups,
                leaves_per_group,
                spines_per_group,
                global_links_per_spine,
                dense_global,
                cap,
                build_log_every,
                source_group_scope,
            );
            if has_flag(&args, "--no-symmetry") {
                case.problem.symmetry = None;
            }
            print_topology_summary(&case);
            if let TopologyMeta::DragonflyPlus {
                cap, dense_global, ..
            } = &case.meta
            {
                println!(
                    "mode=unit c=1 L={} candidate_limit={:?} uniform_cap={} dense_global={} source_group_scope={} representative_search={} strict_orbit={}",
                    cfg.group_size_limit,
                    cfg.candidate_limit,
                    cap,
                    dense_global,
                    source_group_scope.as_str(),
                    cfg.representative_search,
                    case.problem.symmetry.is_some(),
                );
            }
            solve_and_print(&case, &cfg, value(&args, "--output-groups").as_deref());
        }
        "leaf-spine" | "leaf_spine" => {
            let leaves = parse_usize(&args, "--leaves", 128);
            let spines = parse_usize(&args, "--spines", 64);
            let leaf_cap = parse_usize(&args, "--leaf-cap", 1024);
            let cfg = common_config(&args, 1);
            let mut case = build_leaf_spine(leaves, spines, leaf_cap);
            if has_flag(&args, "--no-symmetry") {
                case.problem.symmetry = None;
            }
            print_topology_summary(&case);
            if let TopologyMeta::LeafSpine { leaf_cap, .. } = &case.meta {
                println!(
                    "mode=unit c=1 L={} candidate_limit={:?} leaf_cap={} representative_search={} strict_orbit={}",
                    cfg.group_size_limit,
                    cfg.candidate_limit,
                    leaf_cap,
                    cfg.representative_search,
                    case.problem.symmetry.is_some(),
                );
            }
            solve_and_print(&case, &cfg, value(&args, "--output-groups").as_deref());
        }
        "torus3d" | "torus-3d" => {
            let n = parse_usize(&args, "--n", 8);
            let cap = parse_usize(&args, "--cap", 4096);
            let cfg = common_config(&args, n);
            let source_group_scope = parse_torus3d_source_group_scope(&args);
            let mut case = build_torus3d(n, cap, source_group_scope);
            if has_flag(&args, "--no-symmetry") {
                case.problem.symmetry = None;
            }
            print_topology_summary(&case);
            if let TopologyMeta::Torus3D { cap, .. } = &case.meta {
                println!(
                    "mode=unit c=1 L={} candidate_limit={:?} cap={} source_group_scope={} representative_search={} strict_orbit={}",
                    cfg.group_size_limit,
                    cfg.candidate_limit,
                    cap,
                    source_group_scope.as_str(),
                    cfg.representative_search,
                    case.problem.symmetry.is_some(),
                );
            }
            solve_and_print(&case, &cfg, value(&args, "--output-groups").as_deref());
        }
        "torus2d" | "torus-2d" => {
            let n = parse_usize(&args, "--n", 32);
            let cap = parse_usize(&args, "--cap", 4096);
            let cfg = common_config(&args, n);
            let source_group_scope = parse_torus2d_source_group_scope(&args);
            let mut case = build_torus2d(n, cap, source_group_scope);
            if has_flag(&args, "--no-symmetry") {
                case.problem.symmetry = None;
            }
            print_topology_summary(&case);
            if let TopologyMeta::Torus2D { cap, .. } = &case.meta {
                println!(
                    "mode=unit c=1 L={} candidate_limit={:?} cap={} source_group_scope={} representative_search={} strict_orbit={}",
                    cfg.group_size_limit,
                    cfg.candidate_limit,
                    cap,
                    source_group_scope.as_str(),
                    cfg.representative_search,
                    case.problem.symmetry.is_some(),
                );
            }
            solve_and_print(&case, &cfg, value(&args, "--output-groups").as_deref());
        }
        _ => {
            print_usage();
            panic!("unknown subcommand {}", args[1]);
        }
    }
}
