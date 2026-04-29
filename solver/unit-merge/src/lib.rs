use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::time::Instant;

#[derive(Clone)]
struct Group {
    size: usize,
    fp_count: usize,
    footprint: Vec<u64>,
    members: Vec<usize>,
    key: GroupKey,
    active: bool,
}

pub struct Topology {
    name: String,
    num_switches: usize,
    pairs: Vec<(usize, usize)>,
    footprints: Vec<Vec<u64>>,
    pair_keys: Vec<GroupKey>,
    meta: TopologyMeta,
}

enum TopologyMeta {
    Clos {
        k: usize,
        num_tors: usize,
        num_aggs: usize,
        tors_per_pod: usize,
        switch_group: Vec<usize>,
    },
    Dragonfly {
        groups: usize,
        routers_per_group: usize,
        global_links_per_router: usize,
        path_lengths: BTreeMap<usize, usize>,
        footprint_sizes: BTreeMap<usize, usize>,
        router_group: Vec<usize>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ScoreKey {
    squared_overflow_gain: i64,
    overflow_reduction: usize,
    new_size: usize,
    shared: usize,
    neg_union: isize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GroupKey {
    size: usize,
    h1: u64,
    h2: u64,
}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.size.hash(state);
        self.h1.hash(state);
        self.h2.hash(state);
    }
}

pub struct RunConfig {
    pub group_size_limit: usize,
    pub candidate_limit: Option<usize>,
    pub groups_per_switch: usize,
    pub cycle_representative_search: bool,
    pub representative_switch_group: usize,
    pub symmetry_max_batch: usize,
    pub symmetry_trials_per_group: usize,
    pub symmetry_min_gain_pct: i64,
    pub shift_symmetry_max_batch: usize,
    pub shift_symmetry_min_gain_pct: i64,
    pub shift_orbit_alternatives: usize,
    pub max_steps: Option<usize>,
    pub verbose_every: usize,
    pub rebuild_every: usize,
    pub output_groups: Option<String>,
}

pub struct RunResult {
    groups: Vec<Group>,
    usage: Vec<i64>,
    steps: usize,
    active_groups: usize,
    elapsed_ms: u128,
}

impl Topology {
    pub fn num_switches(&self) -> usize {
        self.num_switches
    }
}

impl RunResult {
    pub fn elapsed_ms(&self) -> u128 {
        self.elapsed_ms
    }
}

struct CandidateResult {
    best: Option<(ScoreKey, usize, usize)>,
    alternatives: Vec<(ScoreKey, usize, usize)>,
    considered: usize,
    overloaded_count: usize,
    searched_overloaded_count: usize,
}

fn words_for_bits(bits: usize) -> usize {
    (bits + 63) / 64
}

fn set_bit(words: &mut [u64], bit: usize) {
    words[bit / 64] |= 1u64 << (bit % 64);
}

fn count_bits(words: &[u64]) -> usize {
    words.iter().map(|w| w.count_ones() as usize).sum()
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

fn ceil_div(n: usize, d: usize) -> i64 {
    ((n + d - 1) / d).max(1) as i64
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn hash_fields(fields: &[usize], seed: u64) -> u64 {
    let mut h = seed;
    for &field in fields {
        h = mix64(h ^ (field as u64).wrapping_add(0x9e3779b97f4a7c15));
    }
    h
}

fn singleton_key(fields: &[usize]) -> GroupKey {
    GroupKey {
        size: 1,
        h1: hash_fields(fields, 0x243f6a8885a308d3),
        h2: hash_fields(fields, 0x13198a2e03707344),
    }
}

fn merge_key(a: GroupKey, b: GroupKey) -> GroupKey {
    GroupKey {
        size: a.size + b.size,
        h1: a.h1.wrapping_add(b.h1),
        h2: a.h2.wrapping_add(b.h2),
    }
}

fn total_overflow(usage: &[i64], caps: &[i64]) -> i64 {
    usage
        .iter()
        .zip(caps.iter())
        .map(|(u, c)| (u - c).max(0))
        .sum()
}

fn max_overflow(usage: &[i64], caps: &[i64]) -> i64 {
    usage
        .iter()
        .zip(caps.iter())
        .map(|(u, c)| (u - c).max(0))
        .max()
        .unwrap_or(0)
}

fn usage_counts(usage: &[i64]) -> BTreeMap<i64, usize> {
    let mut counts = BTreeMap::new();
    for &u in usage {
        *counts.entry(u).or_insert(0) += 1;
    }
    counts
}

fn size_counts(groups: &[Group]) -> BTreeMap<usize, usize> {
    let mut counts = BTreeMap::new();
    for g in groups.iter().filter(|g| g.active) {
        *counts.entry(g.size).or_insert(0) += 1;
    }
    counts
}

fn hottest(usage: &[i64], caps: &[i64], limit: usize) -> Vec<(i64, usize, i64, i64)> {
    let mut items: Vec<_> = usage
        .iter()
        .zip(caps.iter())
        .enumerate()
        .map(|(v, (&u, &c))| (u - c, v, u, c))
        .collect();
    items.sort_unstable_by(|a, b| b.cmp(a));
    items.truncate(limit);
    items
}

fn usage_stats(usage: &[i64]) -> (i64, i64, i64, i64, i64) {
    let mut sorted = usage.to_vec();
    sorted.sort_unstable();
    let last = sorted.len().saturating_sub(1);
    let at = |pct: usize| -> i64 { sorted[last * pct / 100] };
    (sorted[0], at(50), at(90), at(99), sorted[last])
}

fn init_singletons(
    topo: &Topology,
    build_members_index: bool,
) -> (
    Vec<Group>,
    Vec<i64>,
    Vec<Vec<usize>>,
    HashMap<GroupKey, Vec<usize>>,
    Option<HashMap<Vec<usize>, Vec<usize>>>,
) {
    let mut groups = Vec::with_capacity(topo.footprints.len());
    let mut usage = vec![0i64; topo.num_switches];
    let mut by_switch = vec![Vec::new(); topo.num_switches];
    let mut symmetry_index = HashMap::<GroupKey, Vec<usize>>::new();
    let mut members_index = if build_members_index {
        Some(HashMap::<Vec<usize>, Vec<usize>>::new())
    } else {
        None
    };

    for (pid, fp) in topo.footprints.iter().enumerate() {
        let gid = groups.len();
        let key = topo.pair_keys[pid];
        for v in bit_iter(fp).filter(|&v| v < topo.num_switches) {
            usage[v] += 1;
            by_switch[v].push(gid);
        }
        symmetry_index.entry(key).or_default().push(gid);
        if let Some(index) = members_index.as_mut() {
            index.entry(vec![pid]).or_default().push(gid);
        }
        groups.push(Group {
            size: 1,
            fp_count: count_bits(fp),
            footprint: fp.clone(),
            members: vec![pid],
            key,
            active: true,
        });
    }

    (groups, usage, by_switch, symmetry_index, members_index)
}

fn rebuild_index(groups: &[Group], num_switches: usize) -> Vec<Vec<usize>> {
    let mut by_switch = vec![Vec::new(); num_switches];
    for (gid, group) in groups.iter().enumerate() {
        if !group.active {
            continue;
        }
        for v in bit_iter(&group.footprint).filter(|&v| v < num_switches) {
            by_switch[v].push(gid);
        }
    }
    by_switch
}

fn score_pair(
    g1: &Group,
    g2: &Group,
    usage: &[i64],
    caps: &[i64],
    group_size_limit: usize,
) -> Option<ScoreKey> {
    let new_size = g1.size + g2.size;
    if new_size > group_size_limit {
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
            let v = word_idx * 64 + bit;
            word &= word - 1;
            if v >= usage.len() {
                continue;
            }
            let over = usage[v] - caps[v];
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

fn best_candidate(
    groups: &[Group],
    by_switch: &[Vec<usize>],
    usage: &[i64],
    caps: &[i64],
    group_size_limit: usize,
    candidate_limit: Option<usize>,
    groups_per_switch: usize,
    alternative_limit: usize,
    representative_switch_group: Option<usize>,
    router_groups: Option<&[usize]>,
) -> CandidateResult {
    let mut overloaded: Vec<usize> = usage
        .iter()
        .zip(caps.iter())
        .enumerate()
        .filter_map(|(v, (&u, &c))| if u > c { Some(v) } else { None })
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
    if let (Some(group), Some(router_groups)) = (representative_switch_group, router_groups) {
        overloaded.retain(|&v| router_groups.get(v).copied() == Some(group));
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
    overloaded.sort_unstable_by_key(|&v| std::cmp::Reverse(usage[v] - caps[v]));

    let per_switch_limit = candidate_limit.map(|limit| {
        let base = limit / overloaded.len().max(1) + 1;
        base.max(64)
    });

    let mut seen = HashSet::<(usize, usize)>::new();
    let mut considered = 0usize;
    let mut best: Option<(ScoreKey, usize, usize)> = None;
    let mut alternatives = Vec::<(ScoreKey, usize, usize)>::new();

    for v in overloaded {
        let mut idxs = Vec::with_capacity(groups_per_switch.min(by_switch[v].len()));
        for gid in by_switch[v].iter().rev().copied().filter(|&gid| {
            let g = &groups[gid];
            g.active && g.size < group_size_limit
        }) {
            idxs.push(gid);
            if idxs.len() >= groups_per_switch {
                break;
            }
        }
        idxs.sort_unstable_by(|&a, &b| {
            groups[b]
                .size
                .cmp(&groups[a].size)
                .then_with(|| groups[b].fp_count.cmp(&groups[a].fp_count))
                .then_with(|| b.cmp(&a))
        });

        let mut added_for_switch = 0usize;
        'outer: for a_pos in 0..idxs.len() {
            let i = idxs[a_pos];
            for &j in idxs.iter().skip(a_pos + 1) {
                if groups[i].size + groups[j].size > group_size_limit {
                    continue;
                }
                let key = if i < j { (i, j) } else { (j, i) };
                if !seen.insert(key) {
                    continue;
                }
                considered += 1;
                added_for_switch += 1;
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
                    return CandidateResult {
                        best,
                        alternatives: finish_alternatives(alternatives, alternative_limit),
                        considered,
                        overloaded_count,
                        searched_overloaded_count,
                    };
                }
                if per_switch_limit
                    .map(|limit| added_for_switch >= limit)
                    .unwrap_or(false)
                {
                    break 'outer;
                }
            }
        }
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

fn apply_merge(
    groups: &mut Vec<Group>,
    usage: &mut [i64],
    by_switch: &mut [Vec<usize>],
    i: usize,
    j: usize,
) -> usize {
    let fp_i = groups[i].footprint.clone();
    let fp_j = groups[j].footprint.clone();
    let new_key = merge_key(groups[i].key, groups[j].key);
    let num_switches = usage.len();
    for v in bit_iter(&fp_i).filter(|&v| v < num_switches) {
        usage[v] -= 1;
    }
    for v in bit_iter(&fp_j).filter(|&v| v < num_switches) {
        usage[v] -= 1;
    }

    let new_footprint = union_words(&fp_i, &fp_j);
    let mut members = Vec::with_capacity(groups[i].members.len() + groups[j].members.len());
    members.extend_from_slice(&groups[i].members);
    members.extend_from_slice(&groups[j].members);
    members.sort_unstable();

    groups[i].active = false;
    groups[j].active = false;

    let new_gid = groups.len();
    for v in bit_iter(&new_footprint).filter(|&v| v < num_switches) {
        usage[v] += 1;
        by_switch[v].push(new_gid);
    }
    groups.push(Group {
        size: members.len(),
        fp_count: count_bits(&new_footprint),
        footprint: new_footprint,
        members,
        key: new_key,
        active: true,
    });
    new_gid
}

fn validate_usage(groups: &[Group], num_switches: usize) -> Vec<i64> {
    let mut usage = vec![0i64; num_switches];
    for group in groups.iter().filter(|g| g.active) {
        for v in bit_iter(&group.footprint).filter(|&v| v < num_switches) {
            usage[v] += 1;
        }
    }
    usage
}

fn collect_active_by_key(
    symmetry_index: &HashMap<GroupKey, Vec<usize>>,
    groups: &[Group],
    key: GroupKey,
    limit: usize,
) -> Vec<usize> {
    let Some(list) = symmetry_index.get(&key) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(limit.min(list.len()));
    for &gid in list.iter().rev() {
        if groups[gid].active {
            out.push(gid);
            if out.len() >= limit {
                break;
            }
        }
    }
    out
}

struct SymmetryBatchStats {
    merged: usize,
    tested: usize,
}

fn apply_symmetry_batch(
    groups: &mut Vec<Group>,
    usage: &mut [i64],
    by_switch: &mut [Vec<usize>],
    symmetry_index: &mut HashMap<GroupKey, Vec<usize>>,
    caps: &[i64],
    cfg: &RunConfig,
    key_a: GroupKey,
    key_b: GroupKey,
    representative_score: ScoreKey,
) -> SymmetryBatchStats {
    if cfg.symmetry_max_batch == 0 {
        return SymmetryBatchStats {
            merged: 0,
            tested: 0,
        };
    }

    let collect_limit = cfg
        .symmetry_max_batch
        .saturating_mul(cfg.symmetry_trials_per_group.max(1))
        .saturating_mul(2)
        .max(cfg.symmetry_max_batch * 2)
        .max(16);
    let left = collect_active_by_key(symmetry_index, groups, key_a, collect_limit);
    let right = collect_active_by_key(symmetry_index, groups, key_b, collect_limit);
    if left.is_empty() || right.is_empty() {
        return SymmetryBatchStats {
            merged: 0,
            tested: 0,
        };
    }

    let mut merged = 0usize;
    let mut tested = 0usize;

    if key_a == key_b {
        let mut pos = 0usize;
        while pos + 1 < left.len() && merged < cfg.symmetry_max_batch {
            let i = left[pos];
            let j = left[pos + 1];
            pos += 2;
            if !groups[i].active || !groups[j].active || i == j {
                continue;
            }
            tested += 1;
            if let Some(score) =
                score_pair(&groups[i], &groups[j], usage, caps, cfg.group_size_limit)
            {
                if score.squared_overflow_gain * 100
                    < representative_score.squared_overflow_gain * cfg.symmetry_min_gain_pct
                    || score.shared < representative_score.shared
                {
                    continue;
                }
                let new_gid = apply_merge(groups, usage, by_switch, i, j);
                symmetry_index
                    .entry(groups[new_gid].key)
                    .or_default()
                    .push(new_gid);
                merged += 1;
            }
        }
        return SymmetryBatchStats { merged, tested };
    }

    let mut right_start = 0usize;
    for &i in &left {
        if merged >= cfg.symmetry_max_batch {
            break;
        }
        if !groups[i].active {
            continue;
        }
        let mut trials = 0usize;
        let mut rpos = right_start;
        while rpos < right.len() && trials < cfg.symmetry_trials_per_group {
            let j = right[rpos];
            rpos += 1;
            if !groups[j].active || i == j {
                continue;
            }
            trials += 1;
            tested += 1;
            if let Some(score) =
                score_pair(&groups[i], &groups[j], usage, caps, cfg.group_size_limit)
            {
                if score.squared_overflow_gain * 100
                    < representative_score.squared_overflow_gain * cfg.symmetry_min_gain_pct
                    || score.shared < representative_score.shared
                {
                    continue;
                }
                let new_gid = apply_merge(groups, usage, by_switch, i, j);
                symmetry_index
                    .entry(groups[new_gid].key)
                    .or_default()
                    .push(new_gid);
                merged += 1;
                right_start = rpos;
                break;
            }
        }
    }

    SymmetryBatchStats { merged, tested }
}

fn dragonfly_shift_pair_id(topo: &Topology, pid: usize, shift: usize) -> Option<usize> {
    let TopologyMeta::Dragonfly {
        groups,
        routers_per_group,
        ..
    } = &topo.meta
    else {
        return None;
    };
    let n = topo.num_switches;
    let (s, d) = topo.pairs[pid];
    let sg = s / routers_per_group;
    let dg = d / routers_per_group;
    let si = s % routers_per_group;
    let di = d % routers_per_group;
    let shifted_s = ((sg + shift) % groups) * routers_per_group + si;
    let shifted_d = ((dg + shift) % groups) * routers_per_group + di;
    if shifted_s == shifted_d {
        return None;
    }
    let offset = if shifted_d < shifted_s {
        shifted_d
    } else {
        shifted_d - 1
    };
    Some(shifted_s * (n - 1) + offset)
}

fn clos_shift_pair_id(topo: &Topology, pid: usize, shift: usize) -> Option<usize> {
    let TopologyMeta::Clos {
        k,
        num_tors,
        tors_per_pod,
        ..
    } = &topo.meta
    else {
        return None;
    };
    let (s, d) = topo.pairs[pid];
    let sp = s / tors_per_pod;
    let dp = d / tors_per_pod;
    let si = s % tors_per_pod;
    let di = d % tors_per_pod;
    let shifted_sp = (sp + shift) % k;
    let shifted_dp = (dp + shift) % k;
    if shifted_sp == shifted_dp {
        return None;
    }

    let shifted_s = shifted_sp * tors_per_pod + si;
    let shifted_d = shifted_dp * tors_per_pod + di;
    let per_source = num_tors - tors_per_pod;
    let skipped_start = shifted_sp * tors_per_pod;
    let dst_rank = if shifted_d < skipped_start {
        shifted_d
    } else {
        shifted_d - tors_per_pod
    };
    Some(shifted_s * per_source + dst_rank)
}

fn shift_pair_id(topo: &Topology, pid: usize, shift: usize) -> Option<usize> {
    match &topo.meta {
        TopologyMeta::Dragonfly { .. } => dragonfly_shift_pair_id(topo, pid, shift),
        TopologyMeta::Clos { .. } => clos_shift_pair_id(topo, pid, shift),
    }
}

fn shifted_members(topo: &Topology, members: &[usize], shift: usize) -> Option<Vec<usize>> {
    let mut out = Vec::with_capacity(members.len());
    for &pid in members {
        out.push(shift_pair_id(topo, pid, shift)?);
    }
    out.sort_unstable();
    Some(out)
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

struct ShiftOrbit {
    pairs: Vec<(usize, usize)>,
    tested: usize,
}

fn collect_group_shift_orbit(
    topo: &Topology,
    groups: &[Group],
    members_index: &HashMap<Vec<usize>, Vec<usize>>,
    usage: &[i64],
    caps: &[i64],
    cfg: &RunConfig,
    members_a: &[usize],
    members_b: &[usize],
    representative_score: ScoreKey,
) -> ShiftOrbit {
    let group_count = match &topo.meta {
        TopologyMeta::Dragonfly { groups, .. } => *groups,
        TopologyMeta::Clos { k, .. } => *k,
    };
    if cfg.shift_symmetry_max_batch == 0 {
        return ShiftOrbit {
            pairs: Vec::new(),
            tested: 0,
        };
    }

    let max_pairs = (cfg.shift_symmetry_max_batch + 1).min(group_count);
    let require_full_orbit = cfg.shift_symmetry_max_batch >= group_count.saturating_sub(1);
    let mut out = Vec::with_capacity(max_pairs);
    let mut seen = HashSet::<(usize, usize)>::new();
    let mut used_groups = HashSet::<usize>::new();
    let mut tested = 0usize;

    for shift in 0..group_count {
        if out.len() >= max_pairs {
            break;
        }
        let Some(shifted_a) = shifted_members(topo, members_a, shift) else {
            continue;
        };
        let Some(shifted_b) = shifted_members(topo, members_b, shift) else {
            continue;
        };
        let Some(i) = find_active_group_by_members(members_index, groups, &shifted_a) else {
            continue;
        };
        let Some(j) = find_active_group_by_members(members_index, groups, &shifted_b) else {
            continue;
        };
        if i == j {
            continue;
        }
        let key = if i < j { (i, j) } else { (j, i) };
        if !seen.insert(key) {
            continue;
        }
        tested += 1;
        let Some(score) = score_pair(&groups[i], &groups[j], usage, caps, cfg.group_size_limit)
        else {
            continue;
        };
        if score.squared_overflow_gain * 100
            < representative_score.squared_overflow_gain * cfg.shift_symmetry_min_gain_pct
        {
            continue;
        }
        if used_groups.contains(&i) || used_groups.contains(&j) {
            if require_full_orbit {
                return ShiftOrbit {
                    pairs: Vec::new(),
                    tested,
                };
            }
            continue;
        }
        used_groups.insert(i);
        used_groups.insert(j);
        out.push((i, j));
    }

    if require_full_orbit && out.len() != group_count {
        return ShiftOrbit {
            pairs: Vec::new(),
            tested,
        };
    }

    ShiftOrbit { pairs: out, tested }
}

fn apply_group_shift_orbit(
    groups: &mut Vec<Group>,
    usage: &mut [i64],
    by_switch: &mut [Vec<usize>],
    symmetry_index: &mut HashMap<GroupKey, Vec<usize>>,
    members_index: &mut HashMap<Vec<usize>, Vec<usize>>,
    orbit: ShiftOrbit,
) -> SymmetryBatchStats {
    let mut merged = 0usize;
    for (i, j) in orbit.pairs {
        if !groups[i].active || !groups[j].active || i == j {
            continue;
        }
        let new_gid = apply_merge(groups, usage, by_switch, i, j);
        symmetry_index
            .entry(groups[new_gid].key)
            .or_default()
            .push(new_gid);
        members_index
            .entry(groups[new_gid].members.clone())
            .or_default()
            .push(new_gid);
        merged += 1;
    }
    SymmetryBatchStats {
        merged,
        tested: orbit.tested,
    }
}

pub fn run_unit_merge(topo: &Topology, caps: &[i64], cfg: &RunConfig) -> RunResult {
    let start = Instant::now();
    let use_shift_symmetry = cfg.shift_symmetry_max_batch > 0
        && matches!(
            topo.meta,
            TopologyMeta::Dragonfly { .. } | TopologyMeta::Clos { .. }
        );
    let representative_switch_group = match &topo.meta {
        TopologyMeta::Dragonfly { groups, .. }
            if use_shift_symmetry && cfg.cycle_representative_search =>
        {
            Some(cfg.representative_switch_group % groups)
        }
        TopologyMeta::Clos { k, .. } if use_shift_symmetry && cfg.cycle_representative_search => {
            Some(cfg.representative_switch_group % k)
        }
        _ => None,
    };
    let switch_groups = match &topo.meta {
        TopologyMeta::Dragonfly { router_group, .. } => Some(router_group.as_slice()),
        TopologyMeta::Clos { switch_group, .. } => Some(switch_group.as_slice()),
    };
    let (mut groups, mut usage, mut by_switch, mut symmetry_index, mut members_index) =
        init_singletons(topo, use_shift_symmetry);
    let mut active_groups = groups.len();
    let mut steps = 0usize;
    let mut representative_steps = 0usize;
    let mut last_log_step = 0usize;
    let mut last_log_ms = 0u128;
    let mut last_log_overflow = total_overflow(&usage, caps);

    println!(
        "initial: groups={} overflow={} max_overflow={}",
        active_groups,
        last_log_overflow,
        max_overflow(&usage, caps)
    );

    while total_overflow(&usage, caps) > 0 {
        if cfg.max_steps.map(|max| steps >= max).unwrap_or(false) {
            break;
        }
        if cfg.rebuild_every > 0 && steps > 0 && steps % cfg.rebuild_every == 0 {
            by_switch = rebuild_index(&groups, topo.num_switches);
        }

        let candidates = best_candidate(
            &groups,
            &by_switch,
            &usage,
            caps,
            cfg.group_size_limit,
            cfg.candidate_limit,
            cfg.groups_per_switch,
            if use_shift_symmetry {
                cfg.shift_orbit_alternatives
            } else {
                0
            },
            representative_switch_group,
            switch_groups,
        );
        let Some((representative_score, i, j)) = candidates.best else {
            break;
        };

        representative_steps += 1;

        let mut selected_orbit = None;
        let mut orbit_tested = 0usize;
        if let Some(index) = members_index.as_ref() {
            let candidate_list: Vec<_> = if candidates.alternatives.is_empty() {
                vec![(representative_score, i, j)]
            } else {
                candidates.alternatives.clone()
            };
            for (score, ci, cj) in candidate_list {
                let orbit = collect_group_shift_orbit(
                    topo,
                    &groups,
                    index,
                    &usage,
                    caps,
                    cfg,
                    &groups[ci].members,
                    &groups[cj].members,
                    score,
                );
                orbit_tested += orbit.tested;
                if !orbit.pairs.is_empty() {
                    selected_orbit = Some((score, ci, cj, orbit));
                    break;
                }
            }
        }

        let mut batch = if let Some(index) = members_index.as_mut() {
            if let Some((_score, _ci, _cj, orbit)) = selected_orbit {
                let mut batch = apply_group_shift_orbit(
                    &mut groups,
                    &mut usage,
                    &mut by_switch,
                    &mut symmetry_index,
                    index,
                    orbit,
                );
                batch.tested += orbit_tested;
                batch
            } else if use_shift_symmetry && cfg.shift_symmetry_max_batch > 0 {
                break;
            } else {
                let new_gid = apply_merge(&mut groups, &mut usage, &mut by_switch, i, j);
                symmetry_index
                    .entry(groups[new_gid].key)
                    .or_default()
                    .push(new_gid);
                index
                    .entry(groups[new_gid].members.clone())
                    .or_default()
                    .push(new_gid);
                SymmetryBatchStats {
                    merged: 1,
                    tested: orbit_tested,
                }
            }
        } else {
            let new_gid = apply_merge(&mut groups, &mut usage, &mut by_switch, i, j);
            symmetry_index
                .entry(groups[new_gid].key)
                .or_default()
                .push(new_gid);
            SymmetryBatchStats {
                merged: 1,
                tested: 0,
            }
        };
        active_groups -= batch.merged;
        steps += batch.merged;

        if !use_shift_symmetry && batch.merged <= 1 {
            let key_i = groups[i].key;
            let key_j = groups[j].key;
            let extra_batch = apply_symmetry_batch(
                &mut groups,
                &mut usage,
                &mut by_switch,
                &mut symmetry_index,
                caps,
                cfg,
                key_i,
                key_j,
                representative_score,
            );
            if extra_batch.merged > 0 {
                active_groups -= extra_batch.merged;
                steps += extra_batch.merged;
                batch = SymmetryBatchStats {
                    merged: batch.merged + extra_batch.merged,
                    tested: batch.tested + extra_batch.tested,
                };
            }
        }

        if cfg.verbose_every > 0 && steps / cfg.verbose_every > last_log_step / cfg.verbose_every {
            let now_ms = start.elapsed().as_millis();
            let overflow = total_overflow(&usage, caps);
            let step_delta = steps - last_log_step;
            let ms_delta = (now_ms - last_log_ms).max(1);
            let steps_per_s = step_delta as f64 * 1000.0 / ms_delta as f64;
            let overflow_drop_per_s =
                (last_log_overflow - overflow) as f64 * 1000.0 / ms_delta as f64;
            println!(
                "step={} reps={} groups={} overflow={} max_overflow={} elapsed_s={:.1} steps/s={:.1} overflow_drop/s={:.1} overloaded={}/{} candidates={} sym_batch={} sym_tested={}",
                steps,
                representative_steps,
                active_groups,
                overflow,
                max_overflow(&usage, caps),
                now_ms as f64 / 1000.0,
                steps_per_s,
                overflow_drop_per_s,
                candidates.searched_overloaded_count,
                candidates.overloaded_count,
                candidates.considered,
                batch.merged,
                batch.tested,
            );
            last_log_step = steps;
            last_log_ms = now_ms;
            last_log_overflow = overflow;
        }
    }

    let checked = validate_usage(&groups, topo.num_switches);
    if checked != usage {
        eprintln!("warning: incremental usage differs from recomputed usage");
        usage = checked;
    }

    RunResult {
        groups,
        usage,
        steps,
        active_groups,
        elapsed_ms: start.elapsed().as_millis(),
    }
}

pub fn build_clos(k: usize) -> Topology {
    if k < 4 || k % 2 != 0 {
        panic!("--k must be an even integer >= 4");
    }

    let tors_per_pod = k / 2;
    let num_pods = k;
    let num_tors = num_pods * tors_per_pod;
    let num_aggs = num_pods * tors_per_pod;
    let num_switches = num_tors + num_aggs;
    let words = words_for_bits(num_switches);
    let tor_pod: Vec<usize> = (0..num_tors).map(|i| i / tors_per_pod).collect();
    let mut switch_group = Vec::with_capacity(num_switches);
    switch_group.extend((0..num_tors).map(|i| i / tors_per_pod));
    switch_group.extend((0..num_aggs).map(|i| i / tors_per_pod));

    let mut pairs = Vec::new();
    let mut footprints = Vec::new();
    let mut pair_keys = Vec::new();

    for s in 0..num_tors {
        let sp = tor_pod[s];
        for d in 0..num_tors {
            let dp = tor_pod[d];
            if sp == dp {
                continue;
            }
            let mut fp = vec![0u64; words];
            set_bit(&mut fp, s);
            set_bit(&mut fp, d);
            for a in 0..tors_per_pod {
                set_bit(&mut fp, num_tors + sp * tors_per_pod + a);
                set_bit(&mut fp, num_tors + dp * tors_per_pod + a);
            }
            pairs.push((s, d));
            footprints.push(fp);
            pair_keys.push(singleton_key(&[s % tors_per_pod, d % tors_per_pod]));
        }
    }

    Topology {
        name: format!("clos-k{}", k),
        num_switches,
        pairs,
        footprints,
        pair_keys,
        meta: TopologyMeta::Clos {
            k,
            num_tors,
            num_aggs,
            tors_per_pod,
            switch_group,
        },
    }
}

pub fn clos_caps(
    topo: &Topology,
    m3: usize,
    tor_cap_override: Option<i64>,
    agg_cap_override: Option<i64>,
) -> Vec<i64> {
    let TopologyMeta::Clos {
        k,
        num_tors,
        num_aggs: _,
        ..
    } = topo.meta
    else {
        panic!("not a Clos topology");
    };
    let tor_cap = tor_cap_override.unwrap_or((m3 * (k * k / 2 + k)) as i64);
    let agg_cap = agg_cap_override.unwrap_or((m3 * k * k) as i64);
    let mut caps = vec![tor_cap; num_tors];
    caps.extend(std::iter::repeat(agg_cap).take(topo.num_switches - num_tors));
    caps
}

pub fn build_dragonfly(
    groups: usize,
    routers_per_group: usize,
    global_links_per_router: usize,
    build_log_every: usize,
) -> Topology {
    if groups - 1 != routers_per_group * global_links_per_router {
        panic!("canonical dragonfly requires groups - 1 == routers_per_group * global_links_per_router");
    }
    let n = groups * routers_per_group;
    let words = words_for_bits(n);
    let router_group: Vec<usize> = (0..n).map(|i| i / routers_per_group).collect();
    let mut adj = vec![Vec::<usize>::new(); n];

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

    let build_start = Instant::now();
    let mut all_dist = Vec::with_capacity(n);
    for src in 0..n {
        all_dist.push(bfs(&adj, src));
        if build_log_every > 0 && (src + 1) % build_log_every == 0 {
            println!(
                "build: bfs {}/{} elapsed_s={:.1}",
                src + 1,
                n,
                build_start.elapsed().as_secs_f64()
            );
        }
    }
    let mut pairs = Vec::new();
    let mut footprints = Vec::new();
    let mut pair_keys = Vec::new();
    let mut path_lengths = BTreeMap::<usize, usize>::new();
    let mut footprint_sizes = BTreeMap::<usize, usize>::new();

    for s in 0..n {
        for d in 0..n {
            if s == d {
                continue;
            }
            let shortest = all_dist[s][d];
            let mut fp = vec![0u64; words];
            for v in 0..n {
                if all_dist[s][v] >= 0
                    && all_dist[d][v] >= 0
                    && all_dist[s][v] + all_dist[d][v] == shortest
                {
                    set_bit(&mut fp, v);
                }
            }
            *path_lengths.entry(shortest as usize).or_insert(0) += 1;
            *footprint_sizes.entry(count_bits(&fp)).or_insert(0) += 1;
            pairs.push((s, d));
            footprints.push(fp);
            let sg = router_group[s];
            let dg = router_group[d];
            let si = s % routers_per_group;
            let di = d % routers_per_group;
            let fields = if sg == dg {
                vec![0, si, di]
            } else {
                let delta = (dg + groups - sg) % groups;
                let reverse_delta = groups - delta;
                vec![
                    1,
                    delta,
                    si,
                    di,
                    endpoint_idx(delta),
                    endpoint_idx(reverse_delta),
                ]
            };
            pair_keys.push(singleton_key(&fields));
            if build_log_every > 0 && pairs.len() % build_log_every == 0 {
                println!(
                    "build: footprints {}/{} elapsed_s={:.1}",
                    pairs.len(),
                    n * (n - 1),
                    build_start.elapsed().as_secs_f64()
                );
            }
        }
    }

    Topology {
        name: format!(
            "dragonfly-g{}-a{}-h{}",
            groups, routers_per_group, global_links_per_router
        ),
        num_switches: n,
        pairs,
        footprints,
        pair_keys,
        meta: TopologyMeta::Dragonfly {
            groups,
            routers_per_group,
            global_links_per_router,
            path_lengths,
            footprint_sizes,
            router_group,
        },
    }
}

fn bfs(adj: &[Vec<usize>], src: usize) -> Vec<i32> {
    let mut dist = vec![-1i32; adj.len()];
    let mut q = VecDeque::new();
    dist[src] = 0;
    q.push_back(src);
    while let Some(u) = q.pop_front() {
        for &v in &adj[u] {
            if dist[v] < 0 {
                dist[v] = dist[u] + 1;
                q.push_back(v);
            }
        }
    }
    dist
}

pub fn dragonfly_natural_usage(topo: &Topology, group_size_limit: usize) -> Vec<i64> {
    let TopologyMeta::Dragonfly { router_group, .. } = &topo.meta else {
        panic!("not a Dragonfly topology");
    };
    let mut by_key = HashMap::<(usize, usize), Vec<usize>>::new();
    for (pid, &(s, d)) in topo.pairs.iter().enumerate() {
        by_key.entry((s, router_group[d])).or_default().push(pid);
    }

    let mut usage = vec![0i64; topo.num_switches];
    for pids in by_key.values() {
        let c = ceil_div(pids.len(), group_size_limit);
        let mut fp = vec![0u64; words_for_bits(topo.num_switches)];
        for &pid in pids {
            for (dst, src) in fp.iter_mut().zip(topo.footprints[pid].iter()) {
                *dst |= *src;
            }
        }
        for v in bit_iter(&fp).filter(|&v| v < topo.num_switches) {
            usage[v] += c;
        }
    }
    usage
}

pub fn print_topology_summary(topo: &Topology) {
    match &topo.meta {
        TopologyMeta::Clos {
            k,
            num_tors,
            num_aggs,
            tors_per_pod,
            ..
        } => {
            println!(
                "topology={} k={} tors_per_pod={} ToRs={} Aggs={} switches={} directed_pairs={}",
                topo.name,
                k,
                tors_per_pod,
                num_tors,
                num_aggs,
                topo.num_switches,
                topo.pairs.len()
            );
        }
        TopologyMeta::Dragonfly {
            groups,
            routers_per_group,
            global_links_per_router,
            path_lengths,
            footprint_sizes,
            ..
        } => {
            println!(
                "topology={} groups={} routers/group={} global/router={} switches={} directed_pairs={}",
                topo.name,
                groups,
                routers_per_group,
                global_links_per_router,
                topo.num_switches,
                topo.pairs.len()
            );
            println!("path_lengths={:?}", path_lengths);
            println!("footprint_sizes={:?}", footprint_sizes);
        }
    }
}

pub fn print_result(topo: &Topology, caps: &[i64], cfg: &RunConfig, result: &RunResult) {
    println!(
        "done: steps={} groups={} L={} overflow={} max_overflow={} elapsed_ms={}",
        result.steps,
        result.active_groups,
        cfg.group_size_limit,
        total_overflow(&result.usage, caps),
        max_overflow(&result.usage, caps),
        result.elapsed_ms
    );
    println!("group_sizes={:?}", size_counts(&result.groups));
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
    println!("hot={:?}", hottest(&result.usage, caps, 10));

    if let Some(path) = &cfg.output_groups {
        write_groups(path, &result.groups, topo).expect("failed to write groups");
        println!("wrote groups to {}", path);
    }
}

fn write_groups(path: &str, groups: &[Group], topo: &Topology) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);
    writeln!(w, "group_id,size,pair_ids,pairs")?;
    let mut out_gid = 0usize;
    for group in groups.iter().filter(|g| g.active) {
        let pair_ids = group
            .members
            .iter()
            .map(|pid| pid.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        let pairs = group
            .members
            .iter()
            .map(|&pid| {
                let (s, d) = topo.pairs[pid];
                format!("{}->{}", s, d)
            })
            .collect::<Vec<_>>()
            .join(" ");
        writeln!(w, "{},{},{},{}", out_gid, group.size, pair_ids, pairs)?;
        out_gid += 1;
    }
    Ok(())
}
