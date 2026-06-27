use crate::solver::{Demand, SolverProblem, SymmetrySpec};

use super::{TopologyCase, TopologyMeta};

pub fn build_leaf_spine(leaves: usize, spines: usize, leaf_cap: usize) -> TopologyCase {
    if leaves < 2 {
        panic!("--leaves must be at least 2");
    }
    if spines == 0 {
        panic!("--spines must be positive");
    }

    let num_nodes = leaves + spines;
    let mut capacities = vec![leaf_cap; leaves];
    capacities.extend(std::iter::repeat(0).take(spines));

    let mut demands = Vec::with_capacity(leaves * (leaves - 1));
    for src in 0..leaves {
        for dst in 0..leaves {
            if src == dst {
                continue;
            }
            demands.push(Demand {
                src,
                dst,
                footprint: vec![src, dst],
            });
        }
    }

    let cyclic_groups = (0..leaves).map(|leaf| vec![leaf]).collect::<Vec<_>>();
    let source_merge_groups = cyclic_groups.clone();

    TopologyCase {
        name: format!("leaf-spine-l{}-s{}", leaves, spines),
        problem: SolverProblem {
            num_nodes,
            capacities,
            demands,
            source_merge_groups,
            demand_merge_classes: None,
            symmetry: Some(SymmetrySpec { cyclic_groups }),
        },
        meta: TopologyMeta::LeafSpine {
            leaves,
            spines,
            leaf_cap,
        },
    }
}
