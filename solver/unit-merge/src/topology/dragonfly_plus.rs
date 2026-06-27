use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use crate::solver::{Demand, SolverProblem, SymmetrySpec};

use super::{TopologyCase, TopologyMeta};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DragonflyPlusSourceGroupScope {
    Group,
    Tor,
}

impl DragonflyPlusSourceGroupScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Group => "group",
            Self::Tor => "tor",
        }
    }
}

pub fn build_dragonfly_plus(
    groups: usize,
    leaves_per_group: usize,
    spines_per_group: usize,
    global_links_per_spine: usize,
    dense_global: bool,
    cap: usize,
    build_log_every: usize,
    source_group_scope: DragonflyPlusSourceGroupScope,
) -> TopologyCase {
    if groups < 2 {
        panic!("--groups must be at least 2");
    }
    if leaves_per_group == 0 {
        panic!("--leaves-per-group must be positive");
    }
    if spines_per_group == 0 {
        panic!("--spines-per-group must be positive");
    }
    let global_links_per_spine = if dense_global {
        groups - 1
    } else {
        global_links_per_spine
    };
    if global_links_per_spine == 0 {
        panic!("--global-links-per-spine must be positive");
    }
    if !dense_global && groups - 1 != spines_per_group * global_links_per_spine {
        panic!(
            "canonical dragonfly+ requires groups - 1 == spines_per_group * global_links_per_spine"
        );
    }

    let switches_per_group = leaves_per_group + spines_per_group;
    let leaves = groups * leaves_per_group;
    let spines = groups * spines_per_group;
    let num_nodes = groups * switches_per_group;
    let adj = build_adjacency(
        groups,
        leaves_per_group,
        spines_per_group,
        global_links_per_spine,
        dense_global,
    );
    let build_start = Instant::now();

    let mut leaf_nodes = Vec::with_capacity(leaves);
    for group in 0..groups {
        for leaf in 0..leaves_per_group {
            leaf_nodes.push(leaf_id(group, leaf, switches_per_group));
        }
    }

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

    let mut demands = Vec::with_capacity(leaves * (leaves - 1));
    let mut path_lengths = BTreeMap::<usize, usize>::new();
    let mut footprint_sizes = BTreeMap::<usize, usize>::new();
    for &src in &leaf_nodes {
        for &dst in &leaf_nodes {
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
                    leaves * (leaves - 1),
                    build_start.elapsed().as_secs_f64()
                );
            }
        }
    }

    let mut cyclic_groups = Vec::with_capacity(groups);
    let mut source_merge_groups = match source_group_scope {
        DragonflyPlusSourceGroupScope::Group => Vec::with_capacity(groups),
        DragonflyPlusSourceGroupScope::Tor => Vec::with_capacity(leaves),
    };
    for group in 0..groups {
        let group_start = group * switches_per_group;
        cyclic_groups.push((group_start..group_start + switches_per_group).collect());
        match source_group_scope {
            DragonflyPlusSourceGroupScope::Group => {
                source_merge_groups.push(
                    (0..leaves_per_group)
                        .map(|leaf| leaf_id(group, leaf, switches_per_group))
                        .collect(),
                );
            }
            DragonflyPlusSourceGroupScope::Tor => {
                for leaf in 0..leaves_per_group {
                    source_merge_groups.push(vec![leaf_id(group, leaf, switches_per_group)]);
                }
            }
        }
    }

    TopologyCase {
        name: format!(
            "dragonfly-plus{}-g{}-l{}-s{}-h{}",
            if dense_global { "-dense" } else { "" },
            groups,
            leaves_per_group,
            spines_per_group,
            global_links_per_spine
        ),
        problem: SolverProblem {
            num_nodes,
            capacities: vec![cap; num_nodes],
            demands,
            source_merge_groups,
            demand_merge_classes: None,
            symmetry: Some(SymmetrySpec { cyclic_groups }),
        },
        meta: TopologyMeta::DragonflyPlus {
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
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dragonfly_plus_group_scope_groups_sources_by_group() {
        let case = build_dragonfly_plus(
            3,
            2,
            2,
            1,
            false,
            1024,
            0,
            DragonflyPlusSourceGroupScope::Group,
        );

        assert_eq!(case.problem.source_merge_groups.len(), 3);
        assert_eq!(case.problem.source_merge_groups[0], vec![0, 1]);
    }

    #[test]
    fn dragonfly_plus_tor_scope_uses_singleton_source_groups() {
        let case = build_dragonfly_plus(
            3,
            2,
            2,
            1,
            false,
            1024,
            0,
            DragonflyPlusSourceGroupScope::Tor,
        );

        assert_eq!(case.problem.source_merge_groups.len(), 6);
        assert!(case
            .problem
            .source_merge_groups
            .iter()
            .all(|group| group.len() == 1));
    }
}

fn build_adjacency(
    groups: usize,
    leaves_per_group: usize,
    spines_per_group: usize,
    global_links_per_spine: usize,
    dense_global: bool,
) -> Vec<Vec<usize>> {
    let switches_per_group = leaves_per_group + spines_per_group;
    let num_nodes = groups * switches_per_group;
    let mut adj = vec![Vec::<usize>::new(); num_nodes];

    for group in 0..groups {
        for leaf in 0..leaves_per_group {
            for spine in 0..spines_per_group {
                add_edge(
                    &mut adj,
                    leaf_id(group, leaf, switches_per_group),
                    spine_id(group, spine, leaves_per_group, switches_per_group),
                );
            }
        }
    }

    if dense_global {
        for g1 in 0..groups {
            for g2 in (g1 + 1)..groups {
                for spine in 0..spines_per_group {
                    add_edge(
                        &mut adj,
                        spine_id(g1, spine, leaves_per_group, switches_per_group),
                        spine_id(g2, spine, leaves_per_group, switches_per_group),
                    );
                }
            }
        }
    } else {
        for g1 in 0..groups {
            for g2 in (g1 + 1)..groups {
                let delta = (g2 + groups - g1) % groups;
                let reverse_delta = groups - delta;
                let s1 = endpoint_spine(delta, global_links_per_spine);
                let s2 = endpoint_spine(reverse_delta, global_links_per_spine);
                add_edge(
                    &mut adj,
                    spine_id(g1, s1, leaves_per_group, switches_per_group),
                    spine_id(g2, s2, leaves_per_group, switches_per_group),
                );
            }
        }
    }

    for neigh in &mut adj {
        neigh.sort_unstable();
        neigh.dedup();
    }
    adj
}

fn leaf_id(group: usize, leaf: usize, switches_per_group: usize) -> usize {
    group * switches_per_group + leaf
}

fn spine_id(
    group: usize,
    spine: usize,
    leaves_per_group: usize,
    switches_per_group: usize,
) -> usize {
    group * switches_per_group + leaves_per_group + spine
}

fn endpoint_spine(delta: usize, global_links_per_spine: usize) -> usize {
    (delta - 1) / global_links_per_spine
}

fn add_edge(adj: &mut [Vec<usize>], u: usize, v: usize) {
    adj[u].push(v);
    adj[v].push(u);
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
