use std::collections::{BTreeMap, VecDeque};

use crate::solver::{Demand, SolverProblem, SymmetrySpec};

use super::{TopologyCase, TopologyMeta};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Torus3DSourceGroupScope {
    Line,
    Host,
}

impl Torus3DSourceGroupScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Line => "line",
            Self::Host => "host",
        }
    }
}

pub fn build_torus3d(
    n: usize,
    cap: usize,
    source_group_scope: Torus3DSourceGroupScope,
) -> TopologyCase {
    if n < 3 {
        panic!("--n must be at least 3");
    }

    let num_nodes = n * n * n;
    let adj = build_adjacency(n);
    let mut all_dist = Vec::with_capacity(num_nodes);
    for src in 0..num_nodes {
        all_dist.push(bfs(&adj, src));
    }

    let mut demands = Vec::with_capacity(num_nodes * (num_nodes - 1));
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
        }
    }

    let source_merge_groups = match source_group_scope {
        Torus3DSourceGroupScope::Line => {
            let mut groups = Vec::with_capacity(n * n);
            for z in 0..n {
                for y in 0..n {
                    let mut group = Vec::with_capacity(n);
                    for x in 0..n {
                        group.push(node_id(x, y, z, n));
                    }
                    groups.push(group);
                }
            }
            groups
        }
        Torus3DSourceGroupScope::Host => (0..num_nodes).map(|node| vec![node]).collect(),
    };

    let mut cyclic_groups = Vec::with_capacity(n);
    for z in 0..n {
        let mut plane = Vec::with_capacity(n * n);
        for y in 0..n {
            for x in 0..n {
                plane.push(node_id(x, y, z, n));
            }
        }
        cyclic_groups.push(plane);
    }

    TopologyCase {
        name: format!("torus3d-n{}", n),
        problem: SolverProblem {
            num_nodes,
            capacities: vec![cap; num_nodes],
            demands,
            source_merge_groups,
            demand_merge_classes: None,
            symmetry: Some(SymmetrySpec { cyclic_groups }),
        },
        meta: TopologyMeta::Torus3D {
            n,
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
    fn torus3d_line_scope_groups_sources_by_x_line() {
        let case = build_torus3d(4, 1024, Torus3DSourceGroupScope::Line);

        assert_eq!(case.problem.source_merge_groups.len(), 16);
        assert_eq!(case.problem.source_merge_groups[0], vec![0, 1, 2, 3]);
    }

    #[test]
    fn torus3d_host_scope_uses_singleton_source_groups() {
        let case = build_torus3d(4, 1024, Torus3DSourceGroupScope::Host);

        assert_eq!(case.problem.source_merge_groups.len(), 64);
        assert!(case
            .problem
            .source_merge_groups
            .iter()
            .all(|group| group.len() == 1));
    }
}

fn build_adjacency(n: usize) -> Vec<Vec<usize>> {
    let num_nodes = n * n * n;
    let mut adj = vec![Vec::<usize>::new(); num_nodes];
    for z in 0..n {
        for y in 0..n {
            for x in 0..n {
                let u = node_id(x, y, z, n);
                for (dx, dy, dz) in [(1, 0, 0), (0, 1, 0), (0, 0, 1)] {
                    let v = node_id((x + dx) % n, (y + dy) % n, (z + dz) % n, n);
                    adj[u].push(v);
                    adj[v].push(u);
                }
            }
        }
    }
    for neigh in &mut adj {
        neigh.sort_unstable();
        neigh.dedup();
    }
    adj
}

fn node_id(x: usize, y: usize, z: usize, n: usize) -> usize {
    (z * n + y) * n + x
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
