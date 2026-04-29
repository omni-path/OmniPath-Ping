use std::time::Instant;

use rust_unit_merge::{
    build_clos, clos_caps, print_result, print_topology_summary, run_unit_merge, RunConfig,
};

fn experiment_config() -> RunConfig {
    RunConfig {
        group_size_limit: 16,
        candidate_limit: Some(1_000),
        groups_per_switch: 256,
        cycle_representative_search: true,
        representative_switch_group: 0,
        symmetry_max_batch: 0,
        symmetry_trials_per_group: 16,
        symmetry_min_gain_pct: 100,
        shift_symmetry_max_batch: 31,
        shift_symmetry_min_gain_pct: 0,
        shift_orbit_alternatives: 1_000,
        max_steps: Some(300_000),
        verbose_every: 100_000,
        rebuild_every: 1000,
        output_groups: None,
    }
}

fn main() {
    let total_start = Instant::now();
    let build_start = Instant::now();
    let topo = build_clos(32);
    let build_ms = build_start.elapsed().as_millis();
    let cfg = experiment_config();
    let caps = clos_caps(&topo, 1, None, None);

    println!("== Clos k=32 experiment case ==");
    print_topology_summary(&topo);
    println!(
        "mode=unit c=1 L={} candidate_limit={:?} ToR_cap={} Agg_cap={} representative_search={} representative_pod={} shift_batch={}",
        cfg.group_size_limit,
        cfg.candidate_limit,
        caps[0],
        caps.last().copied().unwrap_or(0),
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
