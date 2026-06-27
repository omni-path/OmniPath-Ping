pub mod clos;
pub mod dragonfly;
pub mod dragonfly_plus;
pub mod leaf_spine;
pub mod torus2d;
pub mod torus3d;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};

use crate::solver::{
    group_size_counts, hottest, max_overflow, total_overflow, usage_counts, usage_stats, Demand,
    SolverConfig, SolverProblem, SolverResult,
};

pub use clos::{build_clos, ClosSourceGroupScope};
pub use dragonfly::{build_dragonfly, DragonflySourceGroupScope};
pub use dragonfly_plus::{build_dragonfly_plus, DragonflyPlusSourceGroupScope};
pub use leaf_spine::build_leaf_spine;
pub use torus2d::{build_torus2d, Torus2DSourceGroupScope};
pub use torus3d::{build_torus3d, Torus3DSourceGroupScope};

pub struct TopologyCase {
    pub name: String,
    pub problem: SolverProblem,
    pub meta: TopologyMeta,
}

pub enum TopologyMeta {
    Clos {
        k: usize,
        num_tors: usize,
        num_aggs: usize,
        tors_per_pod: usize,
        tor_cap: usize,
        agg_cap: usize,
        include_intra_pod: bool,
        source_group_scope: ClosSourceGroupScope,
    },
    Dragonfly {
        groups: usize,
        routers_per_group: usize,
        global_links_per_router: usize,
        path_lengths: BTreeMap<usize, usize>,
        footprint_sizes: BTreeMap<usize, usize>,
        uniform_cap: usize,
        natural_usage_max: Option<usize>,
        capacity_scale: Option<f64>,
        source_group_scope: DragonflySourceGroupScope,
    },
    DragonflyPlus {
        groups: usize,
        leaves_per_group: usize,
        spines_per_group: usize,
        global_links_per_spine: usize,
        dense_global: bool,
        leaves: usize,
        spines: usize,
        cap: usize,
        source_group_scope: DragonflyPlusSourceGroupScope,
        path_lengths: BTreeMap<usize, usize>,
        footprint_sizes: BTreeMap<usize, usize>,
    },
    LeafSpine {
        leaves: usize,
        spines: usize,
        leaf_cap: usize,
    },
    Torus3D {
        n: usize,
        cap: usize,
        source_group_scope: Torus3DSourceGroupScope,
        path_lengths: BTreeMap<usize, usize>,
        footprint_sizes: BTreeMap<usize, usize>,
    },
    Torus2D {
        n: usize,
        cap: usize,
        source_group_scope: Torus2DSourceGroupScope,
        path_lengths: BTreeMap<usize, usize>,
        footprint_sizes: BTreeMap<usize, usize>,
    },
}

impl TopologyCase {
    pub fn num_nodes(&self) -> usize {
        self.problem.num_nodes
    }

    pub fn capacities(&self) -> &[usize] {
        &self.problem.capacities
    }

    pub fn demands(&self) -> &[Demand] {
        &self.problem.demands
    }
}

pub fn print_topology_summary(case: &TopologyCase) {
    match &case.meta {
        TopologyMeta::Clos {
            k,
            num_tors,
            num_aggs,
            tors_per_pod,
            include_intra_pod,
            source_group_scope,
            ..
        } => {
            println!(
                "topology={} k={} tors_per_pod={} include_intra_pod={} source_group_scope={} ToRs={} Aggs={} nodes={} directed_demands={}",
                case.name,
                k,
                tors_per_pod,
                include_intra_pod,
                source_group_scope.as_str(),
                num_tors,
                num_aggs,
                case.problem.num_nodes,
                case.problem.demands.len()
            );
        }
        TopologyMeta::Dragonfly {
            groups,
            routers_per_group,
            global_links_per_router,
            source_group_scope,
            path_lengths,
            footprint_sizes,
            ..
        } => {
            println!(
                "topology={} groups={} routers/group={} global/router={} source_group_scope={} nodes={} directed_demands={}",
                case.name,
                groups,
                routers_per_group,
                global_links_per_router,
                source_group_scope.as_str(),
                case.problem.num_nodes,
                case.problem.demands.len()
            );
            println!("path_lengths={:?}", path_lengths);
            println!("footprint_sizes={:?}", footprint_sizes);
        }
        TopologyMeta::DragonflyPlus {
            groups,
            leaves_per_group,
            spines_per_group,
            global_links_per_spine,
            dense_global,
            leaves,
            spines,
            cap,
            source_group_scope,
            path_lengths,
            footprint_sizes,
        } => {
            println!(
                "topology={} groups={} leaves/group={} spines/group={} global/spine={} dense_global={} source_group_scope={} leaves={} spines={} cap={} nodes={} directed_demands={}",
                case.name,
                groups,
                leaves_per_group,
                spines_per_group,
                global_links_per_spine,
                dense_global,
                source_group_scope.as_str(),
                leaves,
                spines,
                cap,
                case.problem.num_nodes,
                case.problem.demands.len()
            );
            println!("path_lengths={:?}", path_lengths);
            println!("footprint_sizes={:?}", footprint_sizes);
        }
        TopologyMeta::LeafSpine {
            leaves,
            spines,
            leaf_cap,
        } => {
            println!(
                "topology={} leaves={} spines={} leaf_cap={} nodes={} directed_demands={}",
                case.name,
                leaves,
                spines,
                leaf_cap,
                case.problem.num_nodes,
                case.problem.demands.len()
            );
        }
        TopologyMeta::Torus3D {
            n,
            cap,
            source_group_scope,
            path_lengths,
            footprint_sizes,
        } => {
            println!(
                "topology={} n={} cap={} source_group_scope={} nodes={} directed_demands={}",
                case.name,
                n,
                cap,
                source_group_scope.as_str(),
                case.problem.num_nodes,
                case.problem.demands.len()
            );
            println!("path_lengths={:?}", path_lengths);
            println!("footprint_sizes={:?}", footprint_sizes);
        }
        TopologyMeta::Torus2D {
            n,
            cap,
            source_group_scope,
            path_lengths,
            footprint_sizes,
        } => {
            println!(
                "topology={} n={} cap={} source_group_scope={} nodes={} directed_demands={}",
                case.name,
                n,
                cap,
                source_group_scope.as_str(),
                case.problem.num_nodes,
                case.problem.demands.len()
            );
            println!("path_lengths={:?}", path_lengths);
            println!("footprint_sizes={:?}", footprint_sizes);
        }
    }
}

pub fn print_result(
    case: &TopologyCase,
    cfg: &SolverConfig,
    result: &SolverResult,
    output_groups: Option<&str>,
) {
    println!(
        "done: steps={} groups={} L={} overflow={} max_overflow={} elapsed_ms={} stop={:?}",
        result.steps,
        result.active_groups,
        cfg.group_size_limit,
        total_overflow(&result.usage, &case.problem.capacities),
        max_overflow(&result.usage, &case.problem.capacities),
        result.elapsed_ms,
        result.stop_reason
    );
    println!("group_sizes={:?}", group_size_counts(&result.groups));
    let counts = usage_counts(&result.usage);
    if counts.len() <= 80 {
        println!("usage_counts={:?}", counts);
    } else {
        let (min_u, p50, p90, p99, max_u) = usage_stats(&result.usage);
        println!(
            "usage_stats=min:{} p50:{} p90:{} p99:{} max:{} distinct={}",
            min_u,
            p50,
            p90,
            p99,
            max_u,
            counts.len()
        );
    }
    println!(
        "hot={:?}",
        hottest(&result.usage, &case.problem.capacities, 10)
    );

    if let Some(path) = output_groups {
        write_groups(path, result, case).expect("failed to write groups");
        println!("wrote groups to {}", path);
    }
}

fn write_groups(path: &str, result: &SolverResult, case: &TopologyCase) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);
    writeln!(w, "group_id,size,demand_ids,pairs")?;
    for (group_id, group) in result.groups.iter().enumerate() {
        let demand_ids = group
            .demand_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        let pairs = group
            .demand_ids
            .iter()
            .map(|&demand_id| {
                let demand = &case.problem.demands[demand_id];
                format!("{}->{}", demand.src, demand.dst)
            })
            .collect::<Vec<_>>()
            .join(" ");
        writeln!(
            w,
            "{},{},{},{}",
            group_id,
            group.demand_ids.len(),
            demand_ids,
            pairs
        )?;
    }
    Ok(())
}
