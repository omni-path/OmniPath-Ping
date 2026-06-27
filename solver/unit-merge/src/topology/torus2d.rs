use std::collections::BTreeMap;

use crate::solver::{Demand, SolverProblem, SymmetrySpec};

use super::{TopologyCase, TopologyMeta};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Torus2DSourceGroupScope {
    Row,
    Host,
}

impl Torus2DSourceGroupScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Row => "row",
            Self::Host => "host",
        }
    }
}

pub fn build_torus2d(
    n: usize,
    cap: usize,
    source_group_scope: Torus2DSourceGroupScope,
) -> TopologyCase {
    if n < 3 {
        panic!("--n must be at least 3");
    }

    let num_nodes = n * n;
    let mut demands = Vec::with_capacity(num_nodes * (num_nodes - 1));
    let mut path_lengths = BTreeMap::<usize, usize>::new();
    let mut footprint_sizes = BTreeMap::<usize, usize>::new();

    for src in 0..num_nodes {
        let (sx, sy) = node_xy(src, n);
        for dst in 0..num_nodes {
            if src == dst {
                continue;
            }
            let (dx, dy) = node_xy(dst, n);
            let x_paths = axis_shortest_paths(sx, dx, n);
            let y_paths = axis_shortest_paths(sy, dy, n);
            let shortest = x_paths[0].len() + y_paths[0].len() - 2;

            let mut in_footprint = vec![false; num_nodes];
            let mut footprint = Vec::new();
            for x_path in &x_paths {
                for y_path in &y_paths {
                    for &y in y_path {
                        for &x in x_path {
                            let node = node_id(x, y, n);
                            if !in_footprint[node] {
                                in_footprint[node] = true;
                                footprint.push(node);
                            }
                        }
                    }
                }
            }
            footprint.sort_unstable();

            *path_lengths.entry(shortest).or_insert(0) += 1;
            *footprint_sizes.entry(footprint.len()).or_insert(0) += 1;
            demands.push(Demand {
                src,
                dst,
                footprint,
            });
        }
    }

    let source_merge_groups = match source_group_scope {
        Torus2DSourceGroupScope::Row => {
            let mut groups = Vec::with_capacity(n);
            for y in 0..n {
                let mut group = Vec::with_capacity(n);
                for x in 0..n {
                    group.push(node_id(x, y, n));
                }
                groups.push(group);
            }
            groups
        }
        Torus2DSourceGroupScope::Host => (0..num_nodes).map(|node| vec![node]).collect(),
    };

    let mut cyclic_groups = Vec::with_capacity(n);
    for y in 0..n {
        let mut row = Vec::with_capacity(n);
        for x in 0..n {
            row.push(node_id(x, y, n));
        }
        cyclic_groups.push(row);
    }

    TopologyCase {
        name: format!("torus2d-n{}", n),
        problem: SolverProblem {
            num_nodes,
            capacities: vec![cap; num_nodes],
            demands,
            source_merge_groups,
            demand_merge_classes: None,
            symmetry: Some(SymmetrySpec { cyclic_groups }),
        },
        meta: TopologyMeta::Torus2D {
            n,
            cap,
            source_group_scope,
            path_lengths,
            footprint_sizes,
        },
    }
}

fn axis_shortest_paths(src: usize, dst: usize, n: usize) -> Vec<Vec<usize>> {
    let forward = (dst + n - src) % n;
    let backward = (src + n - dst) % n;

    if forward == 0 {
        return vec![vec![src]];
    }
    if forward < backward {
        return vec![axis_path(src, forward, 1, n)];
    }
    if backward < forward {
        return vec![axis_path(src, backward, -1, n)];
    }

    vec![
        axis_path(src, forward, 1, n),
        axis_path(src, backward, -1, n),
    ]
}

fn axis_path(src: usize, steps: usize, dir: isize, n: usize) -> Vec<usize> {
    let mut path = Vec::with_capacity(steps + 1);
    for step in 0..=steps {
        let offset = step as isize * dir;
        path.push(((src as isize + offset).rem_euclid(n as isize)) as usize);
    }
    path
}

fn node_id(x: usize, y: usize, n: usize) -> usize {
    y * n + x
}

fn node_xy(node: usize, n: usize) -> (usize, usize) {
    (node % n, node / n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn torus2d_host_scope_uses_singleton_source_groups() {
        let case = build_torus2d(4, 1024, Torus2DSourceGroupScope::Host);

        assert_eq!(case.problem.source_merge_groups.len(), 16);
        assert!(case
            .problem
            .source_merge_groups
            .iter()
            .all(|group| group.len() == 1));
    }

    #[test]
    fn torus2d_wrap_tie_footprint_includes_both_shortest_directions() {
        let case = build_torus2d(4, 1024, Torus2DSourceGroupScope::Host);
        let demand = case
            .problem
            .demands
            .iter()
            .find(|demand| demand.src == 0 && demand.dst == 2)
            .expect("missing demand");

        assert_eq!(demand.footprint, vec![0, 1, 2, 3]);
    }
}
