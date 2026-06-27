use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct SolverProblem {
    pub num_nodes: usize,
    pub capacities: Vec<usize>,
    pub demands: Vec<Demand>,
    pub source_merge_groups: Vec<Vec<usize>>,
    pub demand_merge_classes: Option<Vec<usize>>,
    pub symmetry: Option<SymmetrySpec>,
}

#[derive(Clone, Debug)]
pub struct Demand {
    pub src: usize,
    pub dst: usize,
    pub footprint: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct SymmetrySpec {
    pub cyclic_groups: Vec<Vec<usize>>,
}

#[derive(Clone, Debug)]
pub struct SolverConfig {
    pub group_size_limit: usize,
    pub candidate_limit: Option<usize>,
    pub groups_per_node: usize,
    pub representative_search: bool,
    pub validate_symmetry: bool,
    pub orbit_alternative_limit: usize,
    pub max_steps: Option<usize>,
    pub rebuild_every: usize,
    pub progress_every: usize,
}

#[derive(Clone, Debug)]
pub struct SolverGroup {
    pub demand_ids: Vec<usize>,
    pub footprint: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct SolverResult {
    pub groups: Vec<SolverGroup>,
    pub usage: Vec<usize>,
    pub steps: usize,
    pub active_groups: usize,
    pub elapsed_ms: u128,
    pub stop_reason: StopReason,
}

impl SolverResult {
    pub fn elapsed_ms(&self) -> u128 {
        self.elapsed_ms
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    NoOverflow,
    NoCandidate,
    NoValidOrbit,
    MaxSteps,
}

#[derive(Clone, Debug)]
pub struct SolverProgress {
    pub steps: usize,
    pub representative_steps: usize,
    pub active_groups: usize,
    pub overflow: usize,
    pub max_overflow: usize,
    pub elapsed_ms: u128,
    pub steps_per_s: f64,
    pub overflow_drop_per_s: f64,
    pub searched_overloaded_nodes: usize,
    pub overloaded_nodes: usize,
    pub candidates_considered: usize,
    pub merged_in_batch: usize,
    pub orbit_tested: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolverError {
    message: String,
}

impl SolverError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SolverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for SolverError {}

#[derive(Clone)]
struct Group {
    src_group: usize,
    size: usize,
    fp_count: usize,
    footprint: Vec<u64>,
    members: Vec<usize>,
    active: bool,
}

#[derive(Clone)]
struct NormalizedDemand {
    src: usize,
    dst: usize,
    src_group: usize,
    footprint: Vec<usize>,
    footprint_words: Vec<u64>,
}

#[derive(Clone)]
struct PreparedDemand {
    src_group: usize,
    fp_count: usize,
    footprint_words: Vec<u64>,
}

struct PreparedProblem {
    capacities: Vec<i64>,
    demands: Vec<PreparedDemand>,
    source_group_count: usize,
    symmetry: Option<SymmetryRuntime>,
}

#[derive(Clone)]
struct SymmetryRuntime {
    permutations: Vec<Vec<usize>>,
    demand_lookup: DemandLookup,
    demand_endpoints: Vec<(usize, usize)>,
    representative_nodes: Vec<bool>,
}

#[derive(Clone)]
enum DemandLookup {
    Dense { num_nodes: usize, ids: Vec<usize> },
    Sparse(HashMap<(usize, usize), usize>),
}

impl DemandLookup {
    fn from_map(num_nodes: usize, demand_map: &HashMap<(usize, usize), usize>) -> Self {
        const MAX_DENSE_LOOKUP_BYTES: usize = 96 * 1024 * 1024;
        let max_dense_entries = MAX_DENSE_LOOKUP_BYTES / std::mem::size_of::<usize>();
        let density_limit = demand_map.len().saturating_mul(8).max(1_000_000);

        if let Some(lookup_len) = num_nodes.checked_mul(num_nodes) {
            if lookup_len <= max_dense_entries && lookup_len <= density_limit {
                let mut ids = vec![usize::MAX; lookup_len];
                for (&(src, dst), &demand_id) in demand_map {
                    ids[src * num_nodes + dst] = demand_id;
                }
                return Self::Dense { num_nodes, ids };
            }
        }

        Self::Sparse(demand_map.clone())
    }

    fn get(&self, src: usize, dst: usize) -> Option<usize> {
        match self {
            Self::Dense { num_nodes, ids } => {
                let demand_id = ids[src * *num_nodes + dst];
                (demand_id != usize::MAX).then_some(demand_id)
            }
            Self::Sparse(map) => map.get(&(src, dst)).copied(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ScoreKey {
    squared_overflow_gain: i64,
    overflow_reduction: usize,
    new_size: usize,
    shared: usize,
    neg_union: isize,
}

struct CandidateResult {
    best: Option<(ScoreKey, usize, usize)>,
    alternatives: Vec<(ScoreKey, usize, usize)>,
    considered: usize,
    overloaded_count: usize,
    searched_overloaded_count: usize,
}

struct CandidateScratch {
    by_source: Vec<Vec<usize>>,
    touched_sources: Vec<usize>,
}

impl CandidateScratch {
    fn new(source_group_count: usize) -> Self {
        Self {
            by_source: vec![Vec::new(); source_group_count],
            touched_sources: Vec::new(),
        }
    }

    fn clear_touched_sources(&mut self) {
        for &source in &self.touched_sources {
            self.by_source[source].clear();
        }
        self.touched_sources.clear();
    }
}

struct Orbit {
    pairs: Vec<(usize, usize)>,
    tested: usize,
}

struct MergeBatch {
    merged: usize,
    tested: usize,
}

const NO_SOURCE_GROUP: usize = usize::MAX;
type ByNodeIndex = Vec<Vec<usize>>;

pub fn run_solver(
    problem: &SolverProblem,
    config: &SolverConfig,
    mut progress: Option<&mut dyn FnMut(SolverProgress)>,
) -> Result<SolverResult, SolverError> {
    let start = Instant::now();
    let PreparedProblem {
        capacities,
        demands,
        source_group_count,
        symmetry,
    } = prepare_problem(problem, config)?;
    let use_symmetry = symmetry.is_some();
    let (mut groups, mut usage, mut by_node, mut members_index) =
        init_singletons(&demands, capacities.len(), use_symmetry);
    drop(demands);
    let mut active_groups = groups.len();
    let mut steps = 0usize;
    let mut representative_steps = 0usize;
    let mut last_log_step = 0usize;
    let mut last_log_ms = 0u128;
    let mut last_log_overflow = total_overflow_i64(&usage, &capacities);
    let mut candidate_scratch = CandidateScratch::new(source_group_count);

    let stop_reason = loop {
        let overflow = total_overflow_i64(&usage, &capacities);
        if overflow == 0 {
            break StopReason::NoOverflow;
        }
        if config.max_steps.map(|max| steps >= max).unwrap_or(false) {
            break StopReason::MaxSteps;
        }
        if config.rebuild_every > 0 && steps > 0 && steps % config.rebuild_every == 0 {
            by_node = rebuild_index(&groups, problem.num_nodes);
        }

        let representative_nodes = symmetry
            .as_ref()
            .filter(|_| config.representative_search)
            .map(|sym| sym.representative_nodes.as_slice());
        let candidates = best_candidate(
            &groups,
            &by_node,
            &usage,
            &capacities,
            config.group_size_limit,
            config.candidate_limit,
            config.groups_per_node,
            source_group_count,
            if use_symmetry {
                config.orbit_alternative_limit
            } else {
                0
            },
            representative_nodes,
            &mut candidate_scratch,
        );
        let Some((best_score, best_i, best_j)) = candidates.best else {
            break StopReason::NoCandidate;
        };

        representative_steps += 1;
        let batch =
            if let (Some(symmetry), Some(index)) = (symmetry.as_ref(), members_index.as_mut()) {
                let candidate_list = if candidates.alternatives.is_empty() {
                    vec![(best_score, best_i, best_j)]
                } else {
                    candidates.alternatives.clone()
                };
                let mut selected = None;
                let mut tested = 0usize;
                for (_score, i, j) in candidate_list {
                    let orbit = collect_full_orbit(
                        &groups,
                        index,
                        symmetry,
                        &usage,
                        &capacities,
                        config.group_size_limit,
                        i,
                        j,
                    );
                    tested += orbit.tested;
                    if !orbit.pairs.is_empty() {
                        selected = Some(orbit);
                        break;
                    }
                }
                let Some(orbit) = selected else {
                    break StopReason::NoValidOrbit;
                };
                let mut batch = apply_orbit(&mut groups, &mut usage, &mut by_node, index, orbit);
                batch.tested += tested;
                batch
            } else {
                let new_gid = apply_merge(&mut groups, &mut usage, &mut by_node, best_i, best_j);
                if let Some(index) = members_index.as_mut() {
                    index
                        .entry(groups[new_gid].members.clone())
                        .or_default()
                        .push(new_gid);
                }
                MergeBatch {
                    merged: 1,
                    tested: 0,
                }
            };

        active_groups -= batch.merged;
        steps += batch.merged;

        if config.progress_every > 0
            && steps / config.progress_every > last_log_step / config.progress_every
        {
            let now_ms = start.elapsed().as_millis();
            let overflow = total_overflow_i64(&usage, &capacities);
            let step_delta = steps - last_log_step;
            let ms_delta = (now_ms - last_log_ms).max(1);
            let steps_per_s = step_delta as f64 * 1000.0 / ms_delta as f64;
            let overflow_drop_per_s =
                (last_log_overflow - overflow) as f64 * 1000.0 / ms_delta as f64;
            if let Some(callback) = progress.as_mut() {
                (*callback)(SolverProgress {
                    steps,
                    representative_steps,
                    active_groups,
                    overflow: overflow as usize,
                    max_overflow: max_overflow_i64(&usage, &capacities) as usize,
                    elapsed_ms: now_ms,
                    steps_per_s,
                    overflow_drop_per_s,
                    searched_overloaded_nodes: candidates.searched_overloaded_count,
                    overloaded_nodes: candidates.overloaded_count,
                    candidates_considered: candidates.considered,
                    merged_in_batch: batch.merged,
                    orbit_tested: batch.tested,
                });
            }
            last_log_step = steps;
            last_log_ms = now_ms;
            last_log_overflow = overflow;
        }
    };

    let checked = validate_usage(&groups, problem.num_nodes);
    let final_usage = if checked == usage { usage } else { checked };
    let final_groups = groups
        .iter()
        .filter(|group| group.active)
        .map(|group| SolverGroup {
            demand_ids: group.members.clone(),
            footprint: bit_iter(&group.footprint)
                .filter(|&node| node < problem.num_nodes)
                .collect(),
        })
        .collect::<Vec<_>>();

    Ok(SolverResult {
        active_groups,
        groups: final_groups,
        usage: final_usage
            .into_iter()
            .map(|value| value as usize)
            .collect(),
        steps,
        elapsed_ms: start.elapsed().as_millis(),
        stop_reason,
    })
}

fn prepare_problem(
    problem: &SolverProblem,
    config: &SolverConfig,
) -> Result<PreparedProblem, SolverError> {
    validate_config(config)?;
    if problem.capacities.len() != problem.num_nodes {
        return Err(SolverError::new(format!(
            "capacities length {} does not match num_nodes {}",
            problem.capacities.len(),
            problem.num_nodes
        )));
    }
    let mut capacities = Vec::with_capacity(problem.capacities.len());
    for (node, &cap) in problem.capacities.iter().enumerate() {
        capacities.push(i64::try_from(cap).map_err(|_| {
            SolverError::new(format!("capacity for node {} does not fit in i64", node))
        })?);
    }
    let (source_group_by_node, source_merge_groups) =
        prepare_source_merge_groups(problem.num_nodes, &problem.source_merge_groups)?;
    if let Some(classes) = &problem.demand_merge_classes {
        if classes.len() != problem.demands.len() {
            return Err(SolverError::new(format!(
                "demand_merge_classes length {} must match demands length {}",
                classes.len(),
                problem.demands.len()
            )));
        }
    }

    let mut demand_map = HashMap::<(usize, usize), usize>::new();
    let mut normalized_demands = Vec::with_capacity(problem.demands.len());
    for (demand_id, demand) in problem.demands.iter().enumerate() {
        if demand.src >= problem.num_nodes || demand.dst >= problem.num_nodes {
            return Err(SolverError::new(format!(
                "demand {} endpoint out of range: {} -> {} with num_nodes {}",
                demand_id, demand.src, demand.dst, problem.num_nodes
            )));
        }
        if demand.src == demand.dst {
            return Err(SolverError::new(format!(
                "demand {} has identical src and dst {}",
                demand_id, demand.src
            )));
        }
        if let Some(previous) = demand_map.insert((demand.src, demand.dst), demand_id) {
            return Err(SolverError::new(format!(
                "duplicate directed demand {} -> {} at indices {} and {}",
                demand.src, demand.dst, previous, demand_id
            )));
        }
        let source_group = source_group_by_node[demand.src];
        if source_group == NO_SOURCE_GROUP {
            return Err(SolverError::new(format!(
                "demand {} src {} is not covered by source_merge_groups",
                demand_id, demand.src
            )));
        }
        let src_group = problem
            .demand_merge_classes
            .as_ref()
            .map(|classes| classes[demand_id])
            .unwrap_or(source_group);
        let mut footprint = demand.footprint.clone();
        footprint.sort_unstable();
        footprint.dedup();
        if footprint.is_empty() {
            return Err(SolverError::new(format!(
                "demand {} has an empty footprint",
                demand_id
            )));
        }
        if let Some(&node) = footprint.iter().find(|&&node| node >= problem.num_nodes) {
            return Err(SolverError::new(format!(
                "demand {} footprint node {} out of range for num_nodes {}",
                demand_id, node, problem.num_nodes
            )));
        }
        let mut footprint_words = vec![0u64; words_for_bits(problem.num_nodes)];
        for &node in &footprint {
            set_bit(&mut footprint_words, node);
        }
        normalized_demands.push(NormalizedDemand {
            src: demand.src,
            dst: demand.dst,
            src_group,
            footprint,
            footprint_words,
        });
    }

    let symmetry = match problem.symmetry.as_ref() {
        Some(spec) => Some(prepare_symmetry(
            problem.num_nodes,
            &capacities,
            &normalized_demands,
            &demand_map,
            &source_merge_groups,
            spec,
            config.validate_symmetry,
        )?),
        None => None,
    };

    let demands = normalized_demands
        .into_iter()
        .map(|demand| PreparedDemand {
            src_group: demand.src_group,
            fp_count: demand.footprint.len(),
            footprint_words: demand.footprint_words,
        })
        .collect();

    Ok(PreparedProblem {
        capacities,
        demands,
        source_group_count: problem
            .demand_merge_classes
            .as_ref()
            .and_then(|classes| classes.iter().copied().max().map(|max_class| max_class + 1))
            .unwrap_or(source_merge_groups.len()),
        symmetry,
    })
}

fn validate_config(config: &SolverConfig) -> Result<(), SolverError> {
    if config.group_size_limit == 0 {
        return Err(SolverError::new("group_size_limit must be positive"));
    }
    if config.groups_per_node == 0 {
        return Err(SolverError::new("groups_per_node must be positive"));
    }
    Ok(())
}

fn prepare_source_merge_groups(
    num_nodes: usize,
    source_merge_groups: &[Vec<usize>],
) -> Result<(Vec<usize>, Vec<Vec<usize>>), SolverError> {
    let mut source_group_by_node = vec![NO_SOURCE_GROUP; num_nodes];
    let mut normalized_groups = Vec::with_capacity(source_merge_groups.len());

    for (group_id, group) in source_merge_groups.iter().enumerate() {
        if group.is_empty() {
            return Err(SolverError::new(format!(
                "source_merge_groups[{}] must not be empty",
                group_id
            )));
        }
        for &node in group {
            if node >= num_nodes {
                return Err(SolverError::new(format!(
                    "source_merge_groups[{}] node {} out of range for num_nodes {}",
                    group_id, node, num_nodes
                )));
            }
            if source_group_by_node[node] != NO_SOURCE_GROUP {
                return Err(SolverError::new(format!(
                    "source_merge_groups node {} appears more than once",
                    node
                )));
            }
            source_group_by_node[node] = group_id;
        }

        let mut normalized = group.clone();
        normalized.sort_unstable();
        normalized_groups.push(normalized);
    }

    Ok((source_group_by_node, normalized_groups))
}

fn prepare_symmetry(
    num_nodes: usize,
    capacities: &[i64],
    demands: &[NormalizedDemand],
    demand_map: &HashMap<(usize, usize), usize>,
    source_merge_groups: &[Vec<usize>],
    spec: &SymmetrySpec,
    validate_closure: bool,
) -> Result<SymmetryRuntime, SolverError> {
    let demand_lookup = DemandLookup::from_map(num_nodes, demand_map);
    let demand_endpoints = demands
        .iter()
        .map(|demand| (demand.src, demand.dst))
        .collect::<Vec<_>>();

    if spec.cyclic_groups.is_empty() {
        return Ok(SymmetryRuntime {
            permutations: vec![(0..num_nodes).collect()],
            demand_lookup,
            demand_endpoints,
            representative_nodes: vec![true; num_nodes],
        });
    }
    if spec.cyclic_groups.len() < 2 {
        return Err(SolverError::new(
            "symmetry cyclic_groups must contain at least two groups",
        ));
    }
    let width = spec.cyclic_groups[0].len();
    if width == 0 {
        return Err(SolverError::new("symmetry groups must not be empty"));
    }
    let mut seen_nodes = vec![false; num_nodes];
    for (row, group) in spec.cyclic_groups.iter().enumerate() {
        if group.len() != width {
            return Err(SolverError::new(format!(
                "symmetry group {} has length {}, expected {}",
                row,
                group.len(),
                width
            )));
        }
        for &node in group {
            if node >= num_nodes {
                return Err(SolverError::new(format!(
                    "symmetry node {} out of range for num_nodes {}",
                    node, num_nodes
                )));
            }
            if seen_nodes[node] {
                return Err(SolverError::new(format!(
                    "symmetry node {} appears more than once",
                    node
                )));
            }
            seen_nodes[node] = true;
        }
    }

    for col in 0..width {
        let first = spec.cyclic_groups[0][col];
        for row in 1..spec.cyclic_groups.len() {
            let node = spec.cyclic_groups[row][col];
            if capacities[node] != capacities[first] {
                return Err(SolverError::new(format!(
                    "capacity is not symmetric for nodes {} and {}",
                    first, node
                )));
            }
        }
    }

    let shift_count = spec.cyclic_groups.len();
    let mut permutations = Vec::with_capacity(shift_count);
    for shift in 0..shift_count {
        let mut perm = (0..num_nodes).collect::<Vec<_>>();
        for row in 0..shift_count {
            let shifted_row = (row + shift) % shift_count;
            for col in 0..width {
                perm[spec.cyclic_groups[row][col]] = spec.cyclic_groups[shifted_row][col];
            }
        }
        permutations.push(perm);
    }

    let mut representative_nodes = vec![true; num_nodes];
    for row in 1..shift_count {
        for &node in &spec.cyclic_groups[row] {
            representative_nodes[node] = false;
        }
    }

    if validate_closure {
        let generator_shift = 1;
        let perm = &permutations[generator_shift];
        validate_source_merge_group_closure(source_merge_groups, perm)?;
        for (demand_id, demand) in demands.iter().enumerate() {
            let shifted_src = perm[demand.src];
            let shifted_dst = perm[demand.dst];
            let Some(shifted_id) = demand_lookup.get(shifted_src, shifted_dst) else {
                return Err(SolverError::new(format!(
                    "symmetry generator maps demand {} ({} -> {}) to missing demand {} -> {}",
                    demand_id, demand.src, demand.dst, shifted_src, shifted_dst
                )));
            };
            let mut shifted_footprint = demand
                .footprint
                .iter()
                .map(|&node| perm[node])
                .collect::<Vec<_>>();
            shifted_footprint.sort_unstable();
            shifted_footprint.dedup();
            if shifted_footprint != demands[shifted_id].footprint {
                return Err(SolverError::new(format!(
                    "symmetry generator maps demand {} footprint to a different footprint for demand {}",
                    demand_id, shifted_id
                )));
            }
        }
    }

    Ok(SymmetryRuntime {
        permutations,
        demand_lookup,
        demand_endpoints,
        representative_nodes,
    })
}

fn validate_source_merge_group_closure(
    source_merge_groups: &[Vec<usize>],
    perm: &[usize],
) -> Result<(), SolverError> {
    let mut group_index = HashMap::<Vec<usize>, usize>::with_capacity(source_merge_groups.len());
    for (group_id, group) in source_merge_groups.iter().enumerate() {
        group_index.insert(group.clone(), group_id);
    }

    let mut shifted = Vec::new();
    for (group_id, group) in source_merge_groups.iter().enumerate() {
        shifted.clear();
        shifted.extend(group.iter().map(|&node| perm[node]));
        shifted.sort_unstable();
        if !group_index.contains_key(&shifted) {
            return Err(SolverError::new(format!(
                "symmetry generator maps source_merge_groups[{}] to a missing source merge group",
                group_id
            )));
        }
    }
    Ok(())
}

fn init_singletons(
    demands: &[PreparedDemand],
    num_nodes: usize,
    build_members_index: bool,
) -> (
    Vec<Group>,
    Vec<i64>,
    ByNodeIndex,
    Option<HashMap<Vec<usize>, Vec<usize>>>,
) {
    let mut groups = Vec::with_capacity(demands.len());
    let mut usage = vec![0i64; num_nodes];
    let mut by_node = vec![Vec::new(); num_nodes];
    let mut members_index = if build_members_index {
        Some(HashMap::<Vec<usize>, Vec<usize>>::with_capacity(
            demands.len(),
        ))
    } else {
        None
    };

    for (demand_id, demand) in demands.iter().enumerate() {
        let gid = groups.len();
        for node in bit_iter(&demand.footprint_words).filter(|&node| node < num_nodes) {
            usage[node] += 1;
            by_node[node].push(gid);
        }
        let members = vec![demand_id];
        if let Some(index) = members_index.as_mut() {
            index.entry(members.clone()).or_default().push(gid);
        }
        groups.push(Group {
            src_group: demand.src_group,
            size: 1,
            fp_count: demand.fp_count,
            footprint: demand.footprint_words.clone(),
            members,
            active: true,
        });
    }

    (groups, usage, by_node, members_index)
}

fn rebuild_index(groups: &[Group], num_nodes: usize) -> ByNodeIndex {
    let mut by_node = vec![Vec::new(); num_nodes];
    for (gid, group) in groups.iter().enumerate() {
        if !group.active {
            continue;
        }
        for node in bit_iter(&group.footprint).filter(|&node| node < num_nodes) {
            by_node[node].push(gid);
        }
    }
    by_node
}

fn best_candidate(
    groups: &[Group],
    by_node: &[Vec<usize>],
    usage: &[i64],
    caps: &[i64],
    group_size_limit: usize,
    candidate_limit: Option<usize>,
    groups_per_node: usize,
    source_group_count: usize,
    alternative_limit: usize,
    representative_nodes: Option<&[bool]>,
    scratch: &mut CandidateScratch,
) -> CandidateResult {
    debug_assert_eq!(scratch.by_source.len(), source_group_count);
    let mut overloaded: Vec<usize> = usage
        .iter()
        .zip(caps.iter())
        .enumerate()
        .filter_map(|(node, (&u, &c))| if u > c { Some(node) } else { None })
        .collect();
    let overloaded_count = overloaded.len();
    if overloaded.is_empty() {
        return CandidateResult {
            best: None,
            alternatives: Vec::new(),
            considered: 0,
            overloaded_count,
            searched_overloaded_count: 0,
        };
    }
    if let Some(representative_nodes) = representative_nodes {
        overloaded.retain(|&node| representative_nodes[node]);
    }
    let searched_overloaded_count = overloaded.len();
    if overloaded.is_empty() {
        return CandidateResult {
            best: None,
            alternatives: Vec::new(),
            considered: 0,
            overloaded_count,
            searched_overloaded_count,
        };
    }
    overloaded.sort_unstable_by_key(|&node| std::cmp::Reverse(usage[node] - caps[node]));

    let per_node_limit = candidate_limit.map(|limit| {
        let base = limit / overloaded.len().max(1) + 1;
        base.max(64)
    });

    let seen_capacity = candidate_limit
        .unwrap_or_else(|| overloaded.len().saturating_mul(groups_per_node))
        .min(1_000_000);
    let mut seen = HashSet::<(usize, usize)>::with_capacity(seen_capacity);
    let mut considered = 0usize;
    let mut best: Option<(ScoreKey, usize, usize)> = None;
    let alternatives_capacity = if alternative_limit == 0 {
        0
    } else {
        alternative_limit
            .min(candidate_limit.unwrap_or(alternative_limit))
            .min(1_000_000)
    };
    let mut alternatives = Vec::<(ScoreKey, usize, usize)>::with_capacity(alternatives_capacity);

    'node: for node in overloaded {
        for gid in by_node[node].iter().rev().copied().filter(|&gid| {
            let group = &groups[gid];
            group.active && group.size < group_size_limit
        }) {
            let source = groups[gid].src_group;
            if scratch.by_source[source].is_empty() {
                scratch.touched_sources.push(source);
            }
            if scratch.by_source[source].len() < groups_per_node {
                scratch.by_source[source].push(gid);
            }
        }
        if scratch.touched_sources.is_empty() {
            continue;
        }

        let mut added_for_node = 0usize;
        scratch
            .touched_sources
            .sort_unstable_by_key(|&source| std::cmp::Reverse(scratch.by_source[source].len()));
        for touched_pos in 0..scratch.touched_sources.len() {
            let source = scratch.touched_sources[touched_pos];
            if scratch.by_source[source].len() < 2 {
                continue;
            }
            scratch.by_source[source].sort_unstable_by(|&a, &b| {
                groups[b]
                    .size
                    .cmp(&groups[a].size)
                    .then_with(|| groups[b].fp_count.cmp(&groups[a].fp_count))
                    .then_with(|| b.cmp(&a))
            });
            for a_pos in 0..scratch.by_source[source].len() {
                let i = scratch.by_source[source][a_pos];
                for b_pos in (a_pos + 1)..scratch.by_source[source].len() {
                    let j = scratch.by_source[source][b_pos];
                    if groups[i].size + groups[j].size > group_size_limit {
                        continue;
                    }
                    let key = if i < j { (i, j) } else { (j, i) };
                    if !seen.insert(key) {
                        continue;
                    }
                    considered += 1;
                    added_for_node += 1;
                    if let Some(score) =
                        score_pair(&groups[i], &groups[j], usage, caps, group_size_limit)
                    {
                        if alternative_limit > 0 {
                            alternatives.push((score, i, j));
                        }
                        if best
                            .as_ref()
                            .map(|(best_score, _, _)| score > *best_score)
                            .unwrap_or(true)
                        {
                            best = Some((score, i, j));
                        }
                    }
                    if candidate_limit
                        .map(|limit| considered >= limit)
                        .unwrap_or(false)
                    {
                        scratch.clear_touched_sources();
                        return CandidateResult {
                            best,
                            alternatives: finish_alternatives(alternatives, alternative_limit),
                            considered,
                            overloaded_count,
                            searched_overloaded_count,
                        };
                    }
                    if per_node_limit
                        .map(|limit| added_for_node >= limit)
                        .unwrap_or(false)
                    {
                        scratch.clear_touched_sources();
                        continue 'node;
                    }
                }
            }
        }
        scratch.clear_touched_sources();
    }

    CandidateResult {
        best,
        alternatives: finish_alternatives(alternatives, alternative_limit),
        considered,
        overloaded_count,
        searched_overloaded_count,
    }
}

fn finish_alternatives(
    mut alternatives: Vec<(ScoreKey, usize, usize)>,
    limit: usize,
) -> Vec<(ScoreKey, usize, usize)> {
    if limit == 0 {
        return Vec::new();
    }
    alternatives.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    alternatives.truncate(limit);
    alternatives
}

fn score_pair(
    g1: &Group,
    g2: &Group,
    usage: &[i64],
    caps: &[i64],
    group_size_limit: usize,
) -> Option<ScoreKey> {
    let new_size = g1.size + g2.size;
    if g1.src_group != g2.src_group || new_size > group_size_limit {
        return None;
    }

    let mut gain = 0i64;
    let mut overflow_reduction = 0usize;
    let mut shared = 0usize;

    for (word_idx, (&a, &b)) in g1.footprint.iter().zip(g2.footprint.iter()).enumerate() {
        let mut word = a & b;
        shared += word.count_ones() as usize;
        while word != 0 {
            let bit = word.trailing_zeros() as usize;
            let node = word_idx * 64 + bit;
            word &= word - 1;
            if node >= usage.len() {
                continue;
            }
            let over = usage[node] - caps[node];
            if over > 0 {
                gain += 2 * over - 1;
                overflow_reduction += 1;
            }
        }
    }

    if gain <= 0 {
        return None;
    }

    let union_count = g1.fp_count + g2.fp_count - shared;
    Some(ScoreKey {
        squared_overflow_gain: gain,
        overflow_reduction,
        new_size,
        shared,
        neg_union: -(union_count as isize),
    })
}

fn collect_full_orbit(
    groups: &[Group],
    members_index: &HashMap<Vec<usize>, Vec<usize>>,
    symmetry: &SymmetryRuntime,
    usage: &[i64],
    caps: &[i64],
    group_size_limit: usize,
    i: usize,
    j: usize,
) -> Orbit {
    let shift_count = symmetry.permutations.len();
    let mut pairs = Vec::with_capacity(shift_count);
    let mut seen_pairs = Vec::<(usize, usize)>::with_capacity(shift_count);
    let mut used_groups = Vec::<usize>::with_capacity(shift_count.saturating_mul(2));
    let mut shifted_a = Vec::with_capacity(groups[i].members.len());
    let mut shifted_b = Vec::with_capacity(groups[j].members.len());
    let mut tested = 0usize;

    for shift in 0..shift_count {
        if !shifted_members_into(&groups[i].members, symmetry, shift, &mut shifted_a) {
            return Orbit {
                pairs: Vec::new(),
                tested,
            };
        }
        if !shifted_members_into(&groups[j].members, symmetry, shift, &mut shifted_b) {
            return Orbit {
                pairs: Vec::new(),
                tested,
            };
        }
        if shifted_a == groups[j].members || shifted_b == groups[i].members {
            return Orbit {
                pairs: Vec::new(),
                tested,
            };
        }
        let Some(a) = find_active_group_by_members(members_index, groups, &shifted_a) else {
            return Orbit {
                pairs: Vec::new(),
                tested,
            };
        };
        let Some(b) = find_active_group_by_members(members_index, groups, &shifted_b) else {
            return Orbit {
                pairs: Vec::new(),
                tested,
            };
        };
        if a == b {
            return Orbit {
                pairs: Vec::new(),
                tested,
            };
        }
        let key = if a < b { (a, b) } else { (b, a) };
        if seen_pairs.contains(&key) {
            continue;
        }
        seen_pairs.push(key);
        tested += 1;
        if used_groups.contains(&a) || used_groups.contains(&b) {
            return Orbit {
                pairs: Vec::new(),
                tested,
            };
        }
        if score_pair(&groups[a], &groups[b], usage, caps, group_size_limit).is_none() {
            return Orbit {
                pairs: Vec::new(),
                tested,
            };
        }
        used_groups.push(a);
        used_groups.push(b);
        pairs.push((a, b));
    }

    Orbit { pairs, tested }
}

fn apply_orbit(
    groups: &mut Vec<Group>,
    usage: &mut [i64],
    by_node: &mut [Vec<usize>],
    members_index: &mut HashMap<Vec<usize>, Vec<usize>>,
    orbit: Orbit,
) -> MergeBatch {
    let mut merged = 0usize;
    for (i, j) in orbit.pairs {
        if !groups[i].active || !groups[j].active || i == j {
            continue;
        }
        let new_gid = apply_merge(groups, usage, by_node, i, j);
        members_index
            .entry(groups[new_gid].members.clone())
            .or_default()
            .push(new_gid);
        merged += 1;
    }
    MergeBatch {
        merged,
        tested: orbit.tested,
    }
}

fn apply_merge(
    groups: &mut Vec<Group>,
    usage: &mut [i64],
    by_node: &mut [Vec<usize>],
    i: usize,
    j: usize,
) -> usize {
    let num_nodes = usage.len();
    let (src_group, new_footprint, members) = {
        let group_i = &groups[i];
        let group_j = &groups[j];
        debug_assert_eq!(group_i.src_group, group_j.src_group);
        let src_group = group_i.src_group;
        for node in bit_iter(&group_i.footprint).filter(|&node| node < num_nodes) {
            usage[node] -= 1;
        }
        for node in bit_iter(&group_j.footprint).filter(|&node| node < num_nodes) {
            usage[node] -= 1;
        }

        let new_footprint = union_words(&group_i.footprint, &group_j.footprint);
        let mut members = Vec::with_capacity(group_i.members.len() + group_j.members.len());
        members.extend_from_slice(&group_i.members);
        members.extend_from_slice(&group_j.members);
        members.sort_unstable();
        (src_group, new_footprint, members)
    };

    groups[i].active = false;
    groups[j].active = false;

    let new_gid = groups.len();
    for node in bit_iter(&new_footprint).filter(|&node| node < num_nodes) {
        usage[node] += 1;
        by_node[node].push(new_gid);
    }
    groups.push(Group {
        src_group,
        size: members.len(),
        fp_count: count_bits(&new_footprint),
        footprint: new_footprint,
        members,
        active: true,
    });
    new_gid
}

fn find_active_group_by_members(
    members_index: &HashMap<Vec<usize>, Vec<usize>>,
    groups: &[Group],
    members: &[usize],
) -> Option<usize> {
    members_index.get(members).and_then(|list| {
        list.iter()
            .rev()
            .copied()
            .find(|&gid| groups[gid].active && groups[gid].members == members)
    })
}

fn shifted_members_into(
    members: &[usize],
    symmetry: &SymmetryRuntime,
    shift: usize,
    out: &mut Vec<usize>,
) -> bool {
    let perm = &symmetry.permutations[shift];
    out.clear();
    for &demand_id in members {
        let (src, dst) = symmetry.demand_endpoints[demand_id];
        let shifted_src = perm[src];
        let shifted_dst = perm[dst];
        let Some(shifted_id) = symmetry.demand_lookup.get(shifted_src, shifted_dst) else {
            return false;
        };
        out.push(shifted_id);
    }
    out.sort_unstable();
    true
}

fn validate_usage(groups: &[Group], num_nodes: usize) -> Vec<i64> {
    let mut usage = vec![0i64; num_nodes];
    for group in groups.iter().filter(|group| group.active) {
        for node in bit_iter(&group.footprint).filter(|&node| node < num_nodes) {
            usage[node] += 1;
        }
    }
    usage
}

fn words_for_bits(bits: usize) -> usize {
    (bits + 63) / 64
}

fn set_bit(words: &mut [u64], bit: usize) {
    words[bit / 64] |= 1u64 << (bit % 64);
}

fn count_bits(words: &[u64]) -> usize {
    words.iter().map(|word| word.count_ones() as usize).sum()
}

fn union_words(a: &[u64], b: &[u64]) -> Vec<u64> {
    a.iter().zip(b.iter()).map(|(x, y)| x | y).collect()
}

fn bit_iter(words: &[u64]) -> BitIter<'_> {
    BitIter {
        words,
        word_idx: 0,
        word: words.first().copied().unwrap_or(0),
    }
}

struct BitIter<'a> {
    words: &'a [u64],
    word_idx: usize,
    word: u64,
}

impl Iterator for BitIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.word != 0 {
                let bit = self.word.trailing_zeros() as usize;
                self.word &= self.word - 1;
                return Some(self.word_idx * 64 + bit);
            }
            self.word_idx += 1;
            if self.word_idx >= self.words.len() {
                return None;
            }
            self.word = self.words[self.word_idx];
        }
    }
}

fn total_overflow_i64(usage: &[i64], caps: &[i64]) -> i64 {
    usage
        .iter()
        .zip(caps.iter())
        .map(|(u, c)| (u - c).max(0))
        .sum()
}

fn max_overflow_i64(usage: &[i64], caps: &[i64]) -> i64 {
    usage
        .iter()
        .zip(caps.iter())
        .map(|(u, c)| (u - c).max(0))
        .max()
        .unwrap_or(0)
}

pub fn total_overflow(usage: &[usize], caps: &[usize]) -> usize {
    usage
        .iter()
        .zip(caps.iter())
        .map(|(u, c)| u.saturating_sub(*c))
        .sum()
}

pub fn max_overflow(usage: &[usize], caps: &[usize]) -> usize {
    usage
        .iter()
        .zip(caps.iter())
        .map(|(u, c)| u.saturating_sub(*c))
        .max()
        .unwrap_or(0)
}

pub fn usage_counts(usage: &[usize]) -> BTreeMap<usize, usize> {
    let mut counts = BTreeMap::new();
    for &value in usage {
        *counts.entry(value).or_insert(0) += 1;
    }
    counts
}

pub fn group_size_counts(groups: &[SolverGroup]) -> BTreeMap<usize, usize> {
    let mut counts = BTreeMap::new();
    for group in groups {
        *counts.entry(group.demand_ids.len()).or_insert(0) += 1;
    }
    counts
}

pub fn hottest(usage: &[usize], caps: &[usize], limit: usize) -> Vec<(isize, usize, usize, usize)> {
    let mut items: Vec<_> = usage
        .iter()
        .zip(caps.iter())
        .enumerate()
        .map(|(node, (&u, &c))| (u as isize - c as isize, node, u, c))
        .collect();
    items.sort_unstable_by(|a, b| b.cmp(a));
    items.truncate(limit);
    items
}

pub fn usage_stats(usage: &[usize]) -> (usize, usize, usize, usize, usize) {
    let mut sorted = usage.to_vec();
    sorted.sort_unstable();
    let last = sorted.len().saturating_sub(1);
    let at = |pct: usize| -> usize { sorted[last * pct / 100] };
    (sorted[0], at(50), at(90), at(99), sorted[last])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> SolverConfig {
        SolverConfig {
            group_size_limit: 2,
            candidate_limit: None,
            groups_per_node: 16,
            representative_search: true,
            validate_symmetry: true,
            orbit_alternative_limit: 16,
            max_steps: None,
            rebuild_every: 0,
            progress_every: 0,
        }
    }

    #[test]
    fn duplicate_directed_demands_are_rejected() {
        let problem = SolverProblem {
            num_nodes: 2,
            capacities: vec![1, 1],
            demands: vec![
                Demand {
                    src: 0,
                    dst: 1,
                    footprint: vec![0],
                },
                Demand {
                    src: 0,
                    dst: 1,
                    footprint: vec![1],
                },
            ],
            source_merge_groups: vec![vec![0]],
            demand_merge_classes: None,
            symmetry: None,
        };
        let err = run_solver(&problem, &base_config(), None).unwrap_err();
        assert!(err.to_string().contains("duplicate directed demand"));
    }

    #[test]
    fn symmetry_requires_capacity_closure() {
        let problem = SolverProblem {
            num_nodes: 4,
            capacities: vec![1, 2, 1, 1],
            demands: vec![
                Demand {
                    src: 0,
                    dst: 2,
                    footprint: vec![0, 2],
                },
                Demand {
                    src: 1,
                    dst: 3,
                    footprint: vec![1, 3],
                },
            ],
            source_merge_groups: vec![vec![0, 2], vec![1, 3]],
            demand_merge_classes: None,
            symmetry: Some(SymmetrySpec {
                cyclic_groups: vec![vec![0, 2], vec![1, 3]],
            }),
        };
        let err = run_solver(&problem, &base_config(), None).unwrap_err();
        assert!(err.to_string().contains("capacity is not symmetric"));
    }

    #[test]
    fn symmetry_requires_footprint_closure() {
        let problem = SolverProblem {
            num_nodes: 4,
            capacities: vec![1, 1, 1, 1],
            demands: vec![
                Demand {
                    src: 0,
                    dst: 2,
                    footprint: vec![0, 2],
                },
                Demand {
                    src: 1,
                    dst: 3,
                    footprint: vec![1],
                },
            ],
            source_merge_groups: vec![vec![0, 2], vec![1, 3]],
            demand_merge_classes: None,
            symmetry: Some(SymmetrySpec {
                cyclic_groups: vec![vec![0, 2], vec![1, 3]],
            }),
        };
        let err = run_solver(&problem, &base_config(), None).unwrap_err();
        assert!(err.to_string().contains("different footprint"));
    }

    #[test]
    fn basic_merge_reduces_overflow_without_symmetry() {
        let problem = SolverProblem {
            num_nodes: 3,
            capacities: vec![1, 1, 1],
            demands: vec![
                Demand {
                    src: 0,
                    dst: 1,
                    footprint: vec![1],
                },
                Demand {
                    src: 2,
                    dst: 1,
                    footprint: vec![1],
                },
            ],
            source_merge_groups: vec![vec![0, 2]],
            demand_merge_classes: None,
            symmetry: None,
        };
        let result = run_solver(&problem, &base_config(), None).unwrap();
        assert_eq!(result.stop_reason, StopReason::NoOverflow);
        assert_eq!(result.groups.len(), 1);
        assert_eq!(result.steps, 1);
    }

    #[test]
    fn merge_requires_same_source_group() {
        let problem = SolverProblem {
            num_nodes: 3,
            capacities: vec![1, 1, 1],
            demands: vec![
                Demand {
                    src: 0,
                    dst: 1,
                    footprint: vec![1],
                },
                Demand {
                    src: 2,
                    dst: 1,
                    footprint: vec![1],
                },
            ],
            source_merge_groups: vec![vec![0], vec![2]],
            demand_merge_classes: None,
            symmetry: None,
        };
        let result = run_solver(&problem, &base_config(), None).unwrap();
        assert_eq!(result.stop_reason, StopReason::NoCandidate);
        assert_eq!(result.groups.len(), 2);
        assert_eq!(total_overflow(&result.usage, &problem.capacities), 1);
    }

    #[test]
    fn demand_merge_classes_override_source_group() {
        let problem = SolverProblem {
            num_nodes: 3,
            capacities: vec![1, 1, 1],
            demands: vec![
                Demand {
                    src: 0,
                    dst: 1,
                    footprint: vec![2],
                },
                Demand {
                    src: 0,
                    dst: 2,
                    footprint: vec![2],
                },
            ],
            source_merge_groups: vec![vec![0]],
            demand_merge_classes: Some(vec![0, 1]),
            symmetry: None,
        };
        let result = run_solver(&problem, &base_config(), None).unwrap();
        assert_eq!(result.stop_reason, StopReason::NoCandidate);
        assert_eq!(result.groups.len(), 2);
        assert_eq!(total_overflow(&result.usage, &problem.capacities), 1);
    }

    #[test]
    fn source_merge_groups_must_cover_demand_sources() {
        let problem = SolverProblem {
            num_nodes: 3,
            capacities: vec![1, 1, 1],
            demands: vec![Demand {
                src: 2,
                dst: 1,
                footprint: vec![1],
            }],
            source_merge_groups: vec![vec![0]],
            demand_merge_classes: None,
            symmetry: None,
        };
        let err = run_solver(&problem, &base_config(), None).unwrap_err();
        assert!(err.to_string().contains("not covered"));
    }

    #[test]
    fn source_merge_groups_reject_invalid_nodes() {
        let duplicate = SolverProblem {
            num_nodes: 3,
            capacities: vec![1, 1, 1],
            demands: Vec::new(),
            source_merge_groups: vec![vec![0], vec![0]],
            demand_merge_classes: None,
            symmetry: None,
        };
        let err = run_solver(&duplicate, &base_config(), None).unwrap_err();
        assert!(err.to_string().contains("appears more than once"));

        let out_of_range = SolverProblem {
            num_nodes: 3,
            capacities: vec![1, 1, 1],
            demands: Vec::new(),
            source_merge_groups: vec![vec![3]],
            demand_merge_classes: None,
            symmetry: None,
        };
        let err = run_solver(&out_of_range, &base_config(), None).unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn symmetry_requires_source_merge_group_closure() {
        let problem = SolverProblem {
            num_nodes: 4,
            capacities: vec![1, 1, 1, 1],
            demands: vec![
                Demand {
                    src: 0,
                    dst: 2,
                    footprint: vec![0, 2],
                },
                Demand {
                    src: 1,
                    dst: 3,
                    footprint: vec![1, 3],
                },
            ],
            source_merge_groups: vec![vec![0, 2], vec![1]],
            demand_merge_classes: None,
            symmetry: Some(SymmetrySpec {
                cyclic_groups: vec![vec![0, 2], vec![1, 3]],
            }),
        };
        let err = run_solver(&problem, &base_config(), None).unwrap_err();
        assert!(err.to_string().contains("source_merge_groups[0]"));
    }

    #[test]
    fn strict_orbit_merge_preserves_symmetric_usage() {
        let problem = SolverProblem {
            num_nodes: 4,
            capacities: vec![1, 1, 1, 1],
            demands: vec![
                Demand {
                    src: 0,
                    dst: 2,
                    footprint: vec![0, 2],
                },
                Demand {
                    src: 1,
                    dst: 3,
                    footprint: vec![1, 3],
                },
                Demand {
                    src: 0,
                    dst: 3,
                    footprint: vec![0, 3],
                },
                Demand {
                    src: 1,
                    dst: 2,
                    footprint: vec![1, 2],
                },
            ],
            source_merge_groups: vec![vec![0, 2], vec![1, 3]],
            demand_merge_classes: None,
            symmetry: Some(SymmetrySpec {
                cyclic_groups: vec![vec![0, 2], vec![1, 3]],
            }),
        };
        let mut config = base_config();
        config.group_size_limit = 2;
        let result = run_solver(&problem, &config, None).unwrap();
        assert_eq!(result.usage[0], result.usage[1]);
        assert_eq!(result.usage[2], result.usage[3]);
    }
}
