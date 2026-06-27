use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

use crate::solver::{Demand, SolverProblem, SymmetrySpec};

use super::{TopologyCase, TopologyMeta};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DragonflySourceGroupScope {
    Group,
    Router,
    SourceTargetGroup,
}

impl DragonflySourceGroupScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Group => "group",
            Self::Router => "router",
            Self::SourceTargetGroup => "source-target-group",
        }
    }
}

pub fn build_dragonfly(
    groups: usize,
    routers_per_group: usize,
    global_links_per_router: usize,
    cap: Option<usize>,
    capacity_scale: f64,
    group_size_limit: usize,
    build_log_every: usize,
    source_group_scope: DragonflySourceGroupScope,
) -> TopologyCase {
    if groups - 1 != routers_per_group * global_links_per_router {
        panic!(
            "canonical dragonfly requires groups - 1 == routers_per_group * global_links_per_router"
        );
    }
    let num_nodes = groups * routers_per_group;
    let router_group: Vec<usize> = (0..num_nodes).map(|id| id / routers_per_group).collect();
    let adj = build_adjacency(groups, routers_per_group, global_links_per_router);
    let build_start = Instant::now();

    let mut all_dist = Vec::with_capacity(num_nodes);
    for src in 0..num_nodes {
        all_dist.push(bfs(&adj, src));
        if build_log_every > 0 && (src + 1) % build_log_every == 0 {
            println!(
                "build: bfs {}/{} elapsed_s={:.1}",
                src + 1,
                num_nodes,
                build_start.elapsed().as_secs_f64()
            );
        }
    }

    let mut demands = Vec::new();
    let mut path_lengths = BTreeMap::<usize, usize>::new();
    let mut footprint_sizes = BTreeMap::<usize, usize>::new();
    for src in 0..num_nodes {
        for dst in 0..num_nodes {
            if src == dst {
                continue;
            }
            let shortest = all_dist[src][dst];
            let mut footprint = Vec::new();
            for node in 0..num_nodes {
                if all_dist[src][node] >= 0
                    && all_dist[dst][node] >= 0
                    && all_dist[src][node] + all_dist[dst][node] == shortest
                {
                    footprint.push(node);
                }
            }
            *path_lengths.entry(shortest as usize).or_insert(0) += 1;
            *footprint_sizes.entry(footprint.len()).or_insert(0) += 1;
            demands.push(Demand {
                src,
                dst,
                footprint,
            });
            if build_log_every > 0 && demands.len() % build_log_every == 0 {
                println!(
                    "build: footprints {}/{} elapsed_s={:.1}",
                    demands.len(),
                    num_nodes * (num_nodes - 1),
                    build_start.elapsed().as_secs_f64()
                );
            }
        }
    }

    let natural_usage_max;
    let uniform_cap = if let Some(cap) = cap {
        natural_usage_max = None;
        cap
    } else {
        let natural_usage =
            dragonfly_natural_usage(num_nodes, &router_group, &demands, group_size_limit);
        let max_nat = natural_usage.into_iter().max().unwrap_or(0);
        natural_usage_max = Some(max_nat);
        ((max_nat as f64) * capacity_scale).ceil() as usize
    };
    let capacities = vec![uniform_cap; num_nodes];

    let mut cyclic_groups = Vec::with_capacity(groups);
    for group_id in 0..groups {
        let mut group = Vec::with_capacity(routers_per_group);
        for router in 0..routers_per_group {
            group.push(group_id * routers_per_group + router);
        }
        cyclic_groups.push(group);
    }
    let source_merge_groups = match source_group_scope {
        DragonflySourceGroupScope::Group => cyclic_groups.clone(),
        DragonflySourceGroupScope::Router | DragonflySourceGroupScope::SourceTargetGroup => {
            (0..num_nodes).map(|node| vec![node]).collect()
        }
    };
    let demand_merge_classes = match source_group_scope {
        DragonflySourceGroupScope::Group | DragonflySourceGroupScope::Router => None,
        DragonflySourceGroupScope::SourceTargetGroup => Some(
            demands
                .iter()
                .map(|demand| demand.src * groups + router_group[demand.dst])
                .collect(),
        ),
    };

    TopologyCase {
        name: format!(
            "dragonfly-g{}-a{}-h{}",
            groups, routers_per_group, global_links_per_router
        ),
        problem: SolverProblem {
            num_nodes,
            capacities,
            demands,
            source_merge_groups,
            demand_merge_classes,
            symmetry: Some(SymmetrySpec { cyclic_groups }),
        },
        meta: TopologyMeta::Dragonfly {
            groups,
            routers_per_group,
            global_links_per_router,
            path_lengths,
            footprint_sizes,
            uniform_cap,
            natural_usage_max,
            capacity_scale: cap.is_none().then_some(capacity_scale),
            source_group_scope,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dragonfly_group_scope_groups_sources_by_group() {
        let case = build_dragonfly(
            5,
            2,
            2,
            Some(1024),
            1.0,
            2,
            0,
            DragonflySourceGroupScope::Group,
        );

        assert_eq!(case.problem.source_merge_groups.len(), 5);
        assert_eq!(case.problem.source_merge_groups[0], vec![0, 1]);
    }

    #[test]
    fn dragonfly_router_scope_uses_singleton_source_groups() {
        let case = build_dragonfly(
            5,
            2,
            2,
            Some(1024),
            1.0,
            2,
            0,
            DragonflySourceGroupScope::Router,
        );

        assert_eq!(case.problem.source_merge_groups.len(), 10);
        assert!(case
            .problem
            .source_merge_groups
            .iter()
            .all(|group| group.len() == 1));
    }

    #[test]
    fn dragonfly_source_target_group_scope_sets_demand_classes() {
        let case = build_dragonfly(
            5,
            2,
            2,
            Some(1024),
            1.0,
            2,
            0,
            DragonflySourceGroupScope::SourceTargetGroup,
        );

        assert_eq!(case.problem.source_merge_groups.len(), 10);
        let classes = case.problem.demand_merge_classes.as_ref().unwrap();
        let src0_to_group1 = case
            .problem
            .demands
            .iter()
            .position(|demand| demand.src == 0 && demand.dst == 2)
            .unwrap();
        let src0_to_group2 = case
            .problem
            .demands
            .iter()
            .position(|demand| demand.src == 0 && demand.dst == 4)
            .unwrap();
        assert_ne!(classes[src0_to_group1], classes[src0_to_group2]);
    }
}

fn build_adjacency(
    groups: usize,
    routers_per_group: usize,
    global_links_per_router: usize,
) -> Vec<Vec<usize>> {
    let num_nodes = groups * routers_per_group;
    let mut adj = vec![Vec::<usize>::new(); num_nodes];
    let rid = |group: usize, idx: usize| -> usize { group * routers_per_group + idx };
    let endpoint_idx = |delta: usize| -> usize { (delta - 1) / global_links_per_router };

    for group in 0..groups {
        for i in 0..routers_per_group {
            for j in (i + 1)..routers_per_group {
                let u = rid(group, i);
                let v = rid(group, j);
                adj[u].push(v);
                adj[v].push(u);
            }
        }
    }

    for g1 in 0..groups {
        for g2 in (g1 + 1)..groups {
            let delta = (g2 + groups - g1) % groups;
            let reverse_delta = groups - delta;
            let i1 = endpoint_idx(delta);
            let i2 = endpoint_idx(reverse_delta);
            let u = rid(g1, i1);
            let v = rid(g2, i2);
            adj[u].push(v);
            adj[v].push(u);
        }
    }

    for neigh in &mut adj {
        neigh.sort_unstable();
        neigh.dedup();
    }
    adj
}

fn bfs(adj: &[Vec<usize>], src: usize) -> Vec<i32> {
    let mut dist = vec![-1i32; adj.len()];
    let mut q = VecDeque::new();
    dist[src] = 0;
    q.push_back(src);
    while let Some(node) = q.pop_front() {
        for &next in &adj[node] {
            if dist[next] < 0 {
                dist[next] = dist[node] + 1;
                q.push_back(next);
            }
        }
    }
    dist
}

fn dragonfly_natural_usage(
    num_nodes: usize,
    router_group: &[usize],
    demands: &[Demand],
    group_size_limit: usize,
) -> Vec<usize> {
    let mut by_key = HashMap::<(usize, usize), Vec<usize>>::new();
    for (demand_id, demand) in demands.iter().enumerate() {
        by_key
            .entry((demand.src, router_group[demand.dst]))
            .or_default()
            .push(demand_id);
    }

    let mut usage = vec![0usize; num_nodes];
    for demand_ids in by_key.values() {
        let c = ceil_div(demand_ids.len(), group_size_limit);
        let mut footprint = vec![false; num_nodes];
        for &demand_id in demand_ids {
            for &node in &demands[demand_id].footprint {
                footprint[node] = true;
            }
        }
        for (node, used) in footprint.into_iter().enumerate() {
            if used {
                usage[node] += c;
            }
        }
    }
    usage
}

fn ceil_div(n: usize, d: usize) -> usize {
    ((n + d - 1) / d).max(1)
}
