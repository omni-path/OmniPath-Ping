use std::time::Instant;

use rust_unit_merge::{
    build_dragonfly, print_result, print_topology_summary, run_unit_merge, RunConfig,
};

fn experiment_config() -> RunConfig {
    RunConfig {
        group_size_limit: 8,
        candidate_limit: Some(1_000),
        groups_per_switch: 256,
        cycle_representative_search: true,
        representative_switch_group: 0,
        symmetry_max_batch: 0,
        symmetry_trials_per_group: 16,
        symmetry_min_gain_pct: 100,
        shift_symmetry_max_batch: 128,
        shift_symmetry_min_gain_pct: 0,
        shift_orbit_alternatives: 1_000,
        max_steps: Some(4_500_000),
        verbose_every: 1_000_000,
        rebuild_every: 1000,
        output_groups: None,
    }
}

fn main() {
    let total_start = Instant::now();
    let build_start = Instant::now();
    let topo = build_dragonfly(129, 16, 8, 0);
    let build_ms = build_start.elapsed().as_millis();
    let cfg = experiment_config();
    let caps = vec![5212; topo.num_switches()];

    println!("== Dragonfly A=16 H=8 experiment case ==");
    print_topology_summary(&topo);
    println!(
        "mode=unit c=1 L={} candidate_limit={:?} uniform_cap={} representative_search={} representative_group={} shift_batch={}",
        cfg.group_size_limit,
        cfg.candidate_limit,
        caps[0],
        cfg.cycle_representative_search,
        cfg.representative_switch_group,
        cfg.shift_symmetry_max_batch
    );

    let result = run_unit_merge(&topo, &caps, &cfg);
    print_result(&topo, &caps, &cfg, &result);
    println!(
        "case_time: build_ms={} solve_ms={} total_ms={}",
        build_ms,
        result.elapsed_ms(),
        total_start.elapsed().as_millis()
    );
}
