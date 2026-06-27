use crate::solver::{Demand, SolverProblem, SymmetrySpec};

use super::{TopologyCase, TopologyMeta};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClosSourceGroupScope {
    Pod,
    Tor,
}

impl ClosSourceGroupScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pod => "pod",
            Self::Tor => "tor",
        }
    }
}

pub fn build_clos(
    k: usize,
    m3: usize,
    tor_cap_override: Option<usize>,
    agg_cap_override: Option<usize>,
    include_intra_pod: bool,
    source_group_scope: ClosSourceGroupScope,
) -> TopologyCase {
    if k < 4 || k % 2 != 0 {
        panic!("--k must be an even integer >= 4");
    }
    let tors_per_pod = k / 2;
    let num_pods = k;
    let num_tors = num_pods * tors_per_pod;
    let num_aggs = num_pods * tors_per_pod;
    let num_nodes = num_tors + num_aggs;
    let tor_cap = tor_cap_override.unwrap_or(m3 * (k * k / 2 + k));
    let agg_cap = agg_cap_override.unwrap_or(m3 * k * k);
    let mut capacities = vec![tor_cap; num_tors];
    capacities.extend(std::iter::repeat(agg_cap).take(num_aggs));

    let tor_pod: Vec<usize> = (0..num_tors).map(|id| id / tors_per_pod).collect();
    let source_merge_groups = match source_group_scope {
        ClosSourceGroupScope::Pod => {
            let mut groups = Vec::with_capacity(num_pods);
            for pod in 0..num_pods {
                let mut group = Vec::with_capacity(tors_per_pod);
                for tor in 0..tors_per_pod {
                    group.push(pod * tors_per_pod + tor);
                }
                groups.push(group);
            }
            groups
        }
        ClosSourceGroupScope::Tor => {
            let mut groups = Vec::with_capacity(num_tors);
            for tor in 0..num_tors {
                groups.push(vec![tor]);
            }
            groups
        }
    };

    let mut demands = Vec::new();
    for src in 0..num_tors {
        let src_pod = tor_pod[src];
        for dst in 0..num_tors {
            let dst_pod = tor_pod[dst];
            if src == dst || (!include_intra_pod && src_pod == dst_pod) {
                continue;
            }
            let footprint_capacity = if src_pod == dst_pod {
                2 + tors_per_pod
            } else {
                2 + 2 * tors_per_pod
            };
            let mut footprint = Vec::with_capacity(footprint_capacity);
            footprint.push(src);
            footprint.push(dst);
            if src_pod == dst_pod {
                for agg in 0..tors_per_pod {
                    footprint.push(num_tors + src_pod * tors_per_pod + agg);
                }
            } else {
                for agg in 0..tors_per_pod {
                    footprint.push(num_tors + src_pod * tors_per_pod + agg);
                    footprint.push(num_tors + dst_pod * tors_per_pod + agg);
                }
            }
            demands.push(Demand {
                src,
                dst,
                footprint,
            });
        }
    }

    let mut cyclic_groups = Vec::with_capacity(num_pods);
    for pod in 0..num_pods {
        let mut group = Vec::with_capacity(2 * tors_per_pod);
        for tor in 0..tors_per_pod {
            group.push(pod * tors_per_pod + tor);
        }
        for agg in 0..tors_per_pod {
            group.push(num_tors + pod * tors_per_pod + agg);
        }
        cyclic_groups.push(group);
    }

    TopologyCase {
        name: format!("clos-k{}", k),
        problem: SolverProblem {
            num_nodes,
            capacities,
            demands,
            source_merge_groups,
            demand_merge_classes: None,
            symmetry: Some(SymmetrySpec { cyclic_groups }),
        },
        meta: TopologyMeta::Clos {
            k,
            num_tors,
            num_aggs,
            tors_per_pod,
            tor_cap,
            agg_cap,
            include_intra_pod,
            source_group_scope,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clos_pod_scope_groups_sources_by_pod() {
        let case = build_clos(8, 1, None, None, false, ClosSourceGroupScope::Pod);

        assert_eq!(case.problem.source_merge_groups.len(), 8);
        assert_eq!(case.problem.source_merge_groups[0], vec![0, 1, 2, 3]);
    }

    #[test]
    fn clos_tor_scope_uses_singleton_source_groups() {
        let case = build_clos(8, 1, None, None, false, ClosSourceGroupScope::Tor);

        assert_eq!(case.problem.source_merge_groups.len(), 32);
        assert!(case
            .problem
            .source_merge_groups
            .iter()
            .all(|group| group.len() == 1));
    }
}
