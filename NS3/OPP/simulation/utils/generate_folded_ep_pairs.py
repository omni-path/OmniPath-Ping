#!/usr/bin/env python3
"""Generate communication flows for folded EP training on k-ary topologies.

The script only emits directed communication pairs, not traffic sizes. Dense
ranks are described as (pp, dp, tp). MoE ranks are folded onto the same hosts as
(pp, moe_edp, moe_ep, moe_etp=0), so one dense TP group can hold different
experts instead of shards of the same expert.
"""

import argparse
import csv
import math
import pathlib
import sys
from collections import OrderedDict


DEFAULT_DOMAINS = ("TP", "PP", "DP", "EP", "EDP")
MOE_ETP_SIZE = 1


def fat_tree_counts(k):
    half = k // 2
    host_count = k * k * k // 4
    return {
        "k": k,
        "half": half,
        "host_count": host_count,
        "hosts_per_tor": half,
        "tors_per_pod": half,
        "hosts_per_pod": half * half,
        "pod_count": k,
    }


def leaf_spine_counts(k):
    half = k // 2
    host_count = k * half
    return {
        "k": k,
        "half": half,
        "host_count": host_count,
        "hosts_per_tor": half,
        "tors_per_pod": 1,
        "hosts_per_pod": half,
        "pod_count": k,
    }


def topology_counts(topology, k):
    if topology == "fat_tree":
        return fat_tree_counts(k)
    if topology == "leaf_spine":
        return leaf_spine_counts(k)
    raise ValueError("unsupported topology: %s" % topology)


def bit_reversal_order(n):
    if n <= 0:
        return []
    if n & (n - 1):
        return spread_order(n)
    width = int(math.log(n, 2))
    order = []
    for value in range(n):
        reversed_value = 0
        for bit in range(width):
            reversed_value = (reversed_value << 1) | ((value >> bit) & 1)
        order.append(reversed_value)
    return order


def spread_order(n):
    order = []

    def rec(values):
        if not values:
            return
        order.append(values[0])
        rec(values[::2][1:])
        rec(values[1::2])

    rec(list(range(n)))
    seen = set()
    result = []
    for item in order:
        if item not in seen:
            seen.add(item)
            result.append(item)
    for item in range(n):
        if item not in seen:
            result.append(item)
    return result


class Placement(object):
    def __init__(self, topology, k, pp_size, tp_size, dp_size):
        self.topology = topology
        self.counts = topology_counts(topology, k)
        self.k = k
        self.pp_size = pp_size
        self.tp_size = tp_size
        self.dp_size = dp_size

        hosts_per_tor = self.counts["hosts_per_tor"]
        tors_per_pod = self.counts["tors_per_pod"]
        if tp_size > hosts_per_tor:
            raise ValueError(
                "TP=%d cannot be packed into one ToR with only %d hosts" %
                (tp_size, hosts_per_tor)
            )
        self.tp_groups_per_tor = hosts_per_tor // tp_size
        self.tp_groups_per_pod = tors_per_pod * self.tp_groups_per_tor
        self.pods_per_pp = (dp_size + self.tp_groups_per_pod - 1) // self.tp_groups_per_pod
        if self.pods_per_pp * pp_size > k:
            raise ValueError(
                "need %d placement groups to place PP=%d, DP=%d, TP=%d with TP-local packing, "
                "but %s k=%d has only %d groups" %
                (self.pods_per_pp * pp_size, pp_size, dp_size, tp_size,
                 topology, k, self.counts["pod_count"])
            )
        if k % self.pods_per_pp != 0:
            raise ValueError("k=%d must be divisible by pods_per_pp=%d" % (k, self.pods_per_pp))

        base_pod_count = k // self.pods_per_pp
        if pp_size > base_pod_count:
            raise ValueError(
                "PP=%d exceeds available spread bases %d" % (pp_size, base_pod_count)
            )
        pp_bases = bit_reversal_order(base_pod_count)[:pp_size]
        self.pp_to_pods = OrderedDict()
        for pp in range(pp_size):
            base = pp_bases[pp]
            self.pp_to_pods[pp] = [base + lane * base_pod_count for lane in range(self.pods_per_pp)]

    def logical_to_host(self, pp, dp, tp):
        if pp < 0 or pp >= self.pp_size:
            raise ValueError("pp out of range: %d" % pp)
        if dp < 0 or dp >= self.dp_size:
            raise ValueError("dp out of range: %d" % dp)
        if tp < 0 or tp >= self.tp_size:
            raise ValueError("tp out of range: %d" % tp)

        pod_lane = dp // self.tp_groups_per_pod
        slot_in_pod = dp % self.tp_groups_per_pod
        tor_in_pod = slot_in_pod // self.tp_groups_per_tor
        tp_group_in_tor = slot_in_pod % self.tp_groups_per_tor
        host_in_tor = tp_group_in_tor * self.tp_size + tp
        pod = self.pp_to_pods[pp][pod_lane]
        return pod * self.counts["hosts_per_pod"] + tor_in_pod * self.counts["hosts_per_tor"] + host_in_tor


class FoldedLayout(object):
    def __init__(self, pp_size, dense_dp_size, dense_tp_size, moe_ep_size):
        ranks_per_pp = dense_dp_size * dense_tp_size
        if ranks_per_pp % (moe_ep_size * MOE_ETP_SIZE):
            raise ValueError(
                "ranks_per_pp=%d is not divisible by EP*ETP=%d" %
                (ranks_per_pp, moe_ep_size * MOE_ETP_SIZE)
            )
        self.pp_size = pp_size
        self.dense_dp_size = dense_dp_size
        self.dense_tp_size = dense_tp_size
        self.moe_ep_size = moe_ep_size
        self.moe_etp_size = MOE_ETP_SIZE
        self.moe_edp_size = ranks_per_pp // (moe_ep_size * MOE_ETP_SIZE)

    def dense_to_moe(self, pp, dp, tp):
        if pp < 0 or pp >= self.pp_size:
            raise ValueError("pp out of range: %d" % pp)
        if dp < 0 or dp >= self.dense_dp_size:
            raise ValueError("dp out of range: %d" % dp)
        if tp < 0 or tp >= self.dense_tp_size:
            raise ValueError("tp out of range: %d" % tp)
        flat = dp * self.dense_tp_size + tp
        ranks_per_moe_edp = self.moe_ep_size * self.moe_etp_size
        moe_edp = flat // ranks_per_moe_edp
        offset = flat % ranks_per_moe_edp
        moe_ep = offset // self.moe_etp_size
        moe_etp = offset % self.moe_etp_size
        return pp, moe_edp, moe_ep, moe_etp

    def moe_to_dense(self, pp, moe_edp, moe_ep, moe_etp=0):
        if pp < 0 or pp >= self.pp_size:
            raise ValueError("pp out of range: %d" % pp)
        if moe_edp < 0 or moe_edp >= self.moe_edp_size:
            raise ValueError("moe_edp out of range: %d" % moe_edp)
        if moe_ep < 0 or moe_ep >= self.moe_ep_size:
            raise ValueError("moe_ep out of range: %d" % moe_ep)
        if moe_etp < 0 or moe_etp >= self.moe_etp_size:
            raise ValueError("moe_etp out of range: %d" % moe_etp)
        flat = moe_edp * self.moe_ep_size * self.moe_etp_size
        flat += moe_ep * self.moe_etp_size + moe_etp
        dp = flat // self.dense_tp_size
        tp = flat % self.dense_tp_size
        return pp, dp, tp


class PairWriter(object):
    def __init__(self, output_path, placement, layout, direction, local_mask_size, write_header=True):
        self.output_path = output_path
        self.placement = placement
        self.layout = layout
        self.direction = direction
        self.local_mask_size = local_mask_size
        self.counts_by_domain = OrderedDict((domain, 0) for domain in DEFAULT_DOMAINS)
        self.masked_counts_by_domain = OrderedDict((domain, 0) for domain in DEFAULT_DOMAINS)
        self.seen = set()
        self.flow_id = 0
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.output_path.open("w", newline="")
        self.writer = csv.writer(self.handle)
        if write_header:
            self.writer.writerow([
                "flow_id",
                "domain",
                "group",
                "src",
                "dst",
                "src_pp",
                "src_dp",
                "src_tp",
                "src_moe_edp",
                "src_moe_ep",
                "src_moe_etp",
                "dst_pp",
                "dst_dp",
                "dst_tp",
                "dst_moe_edp",
                "dst_moe_ep",
                "dst_moe_etp",
            ])

    def close(self):
        self.handle.close()

    def emit(self, domain, group, a, b):
        src = self.placement.logical_to_host(*a)
        dst = self.placement.logical_to_host(*b)
        if src == dst:
            return
        if self.local_mask_size > 0 and src // self.local_mask_size == dst // self.local_mask_size:
            self.masked_counts_by_domain[domain] = self.masked_counts_by_domain.get(domain, 0) + 1
            return
        if self.direction == "directed":
            key = (domain, src, dst)
        else:
            key = (domain, min(src, dst), max(src, dst))
        if key in self.seen:
            return
        self.seen.add(key)
        self.counts_by_domain[domain] = self.counts_by_domain.get(domain, 0) + 1
        self.flow_id += 1
        src_moe = self.layout.dense_to_moe(*a)
        dst_moe = self.layout.dense_to_moe(*b)
        self.writer.writerow([
            self.flow_id,
            domain,
            group,
            src,
            dst,
            a[0],
            a[1],
            a[2],
            src_moe[1],
            src_moe[2],
            src_moe[3],
            b[0],
            b[1],
            b[2],
            dst_moe[1],
            dst_moe[2],
            dst_moe[3],
        ])


def emit_all_pairs(writer, domain, group, coords):
    if writer.direction == "directed":
        for i in range(len(coords)):
            for j in range(len(coords)):
                if i != j:
                    writer.emit(domain, group, coords[i], coords[j])
    else:
        for i in range(len(coords)):
            for j in range(i + 1, len(coords)):
                writer.emit(domain, group, coords[i], coords[j])


def emit_neighbor_pair(writer, domain, group, a, b):
    writer.emit(domain, group, a, b)
    if writer.direction == "directed":
        writer.emit(domain, group, b, a)


def halving_doubling_pairs(values):
    values = list(values)
    if len(values) <= 1:
        return
    if len(values) % 2:
        values = values[:-1]
    half = len(values) // 2
    left = values[:half]
    right = values[half:]
    for idx in range(half):
        yield left[idx], right[idx]
    for pair in halving_doubling_pairs(left):
        yield pair
    for pair in halving_doubling_pairs(right):
        yield pair


def ring_pairs(values):
    values = list(values)
    if len(values) <= 1:
        return
    for idx, value in enumerate(values):
        yield value, values[(idx + 1) % len(values)]


def generate_pairs(args):
    counts = topology_counts(args.topology, args.k)
    host_count = counts["host_count"]
    denominator = args.pp * args.tp
    if host_count % denominator:
        raise ValueError(
            "host_count=%d is not divisible by PP*TP=%d" % (host_count, denominator)
        )
    dp_size = host_count // denominator
    layout = FoldedLayout(args.pp, dp_size, args.tp, args.ep)
    edp_size = layout.moe_edp_size

    domains = tuple(domain.strip().upper() for domain in args.domains.split(",") if domain.strip())
    unknown = sorted(set(domains) - set(DEFAULT_DOMAINS))
    if unknown:
        raise ValueError("unknown domains: %s" % ",".join(unknown))

    placement = Placement(args.topology, args.k, args.pp, args.tp, dp_size)
    writer = PairWriter(
        args.output,
        placement,
        layout,
        args.direction,
        args.local_mask_size,
        write_header=not args.no_header,
    )
    try:
        if "TP" in domains:
            for pp in range(args.pp):
                for dp in range(dp_size):
                    group = "pp%d.dp%d" % (pp, dp)
                    emit_all_pairs(
                        writer,
                        "TP",
                        group,
                        [(pp, dp, tp) for tp in range(args.tp)],
                    )

        if "PP" in domains:
            for dp in range(dp_size):
                for tp in range(args.tp):
                    group = "dp%d.tp%d" % (dp, tp)
                    if args.pp_pattern == "adjacent":
                        for pp in range(args.pp - 1):
                            emit_neighbor_pair(writer, "PP", group, (pp, dp, tp), (pp + 1, dp, tp))
                    else:
                        emit_all_pairs(
                            writer,
                            "PP",
                            group,
                            [(pp, dp, tp) for pp in range(args.pp)],
                        )

        if "DP" in domains:
            for pp in range(args.pp):
                for tp in range(args.tp):
                    group = "pp%d.tp%d" % (pp, tp)
                    for dp_a, dp_b in halving_doubling_pairs(range(dp_size)):
                        emit_neighbor_pair(writer, "DP", group, (pp, dp_a, tp), (pp, dp_b, tp))

        if "EP" in domains:
            for pp in range(args.pp):
                for moe_edp in range(layout.moe_edp_size):
                    for moe_etp in range(layout.moe_etp_size):
                        group = "pp%d.moe_edp%d.moe_etp%d" % (pp, moe_edp, moe_etp)
                        coords = [
                            layout.moe_to_dense(pp, moe_edp, moe_ep, moe_etp)
                            for moe_ep in range(layout.moe_ep_size)
                        ]
                        emit_all_pairs(writer, "EP", group, coords)

        if "EDP" in domains and layout.moe_edp_size > 1:
            for pp in range(args.pp):
                for moe_ep in range(layout.moe_ep_size):
                    for moe_etp in range(layout.moe_etp_size):
                        group = "pp%d.moe_ep%d.moe_etp%d" % (pp, moe_ep, moe_etp)
                        for edp_a, edp_b in ring_pairs(range(layout.moe_edp_size)):
                            writer.emit(
                                "EDP",
                                group,
                                layout.moe_to_dense(pp, edp_a, moe_ep, moe_etp),
                                layout.moe_to_dense(pp, edp_b, moe_ep, moe_etp),
                            )
    finally:
        writer.close()

    summary = OrderedDict()
    summary["topology"] = args.topology
    summary["k"] = args.k
    summary["host_count"] = host_count
    summary["pp"] = args.pp
    summary["layout"] = "folded_ep"
    summary["tp"] = args.tp
    summary["dense_tp"] = args.tp
    summary["dp"] = dp_size
    summary["dense_dp"] = dp_size
    summary["ep"] = args.ep
    summary["moe_ep"] = args.ep
    summary["moe_etp"] = layout.moe_etp_size
    summary["edp"] = edp_size
    summary["moe_edp"] = edp_size
    summary["hosts_per_tor"] = counts["hosts_per_tor"]
    summary["tp_groups_per_tor"] = placement.tp_groups_per_tor
    summary["tp_groups_per_pod"] = placement.tp_groups_per_pod
    summary["pods_per_pp"] = placement.pods_per_pp
    summary["pp_pattern"] = args.pp_pattern
    summary["dp_collective"] = "halving_doubling"
    summary["edp_collective"] = "ring"
    summary["direction"] = args.direction
    summary["local_mask_size"] = args.local_mask_size
    summary["total_pairs"] = sum(writer.counts_by_domain.values())
    summary["total_masked_pairs"] = sum(writer.masked_counts_by_domain.values())
    for domain in DEFAULT_DOMAINS:
        summary["%s_pairs" % domain.lower()] = writer.counts_by_domain.get(domain, 0)
        summary["%s_masked_pairs" % domain.lower()] = writer.masked_counts_by_domain.get(domain, 0)
    return summary, placement


def write_summary(path, summary, placement):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["key", "value"])
        for key, value in summary.items():
            writer.writerow([key, value])
        writer.writerow(["pp_to_pods", ""])
        for pp, pods in placement.pp_to_pods.items():
            writer.writerow(["pp%d" % pp, " ".join(str(pod) for pod in pods)])


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Generate folded EP/TP/PP/DP communication flows for k-ary topologies."
    )
    parser.add_argument(
        "--topology",
        choices=("fat_tree", "leaf_spine"),
        default="fat_tree",
        help="physical topology layout used for host placement",
    )
    parser.add_argument("--k", type=int, default=32, help="fat-tree radix")
    parser.add_argument("--ep", type=int, default=64, help="expert parallel size")
    parser.add_argument("--tp", type=int, default=8, help="tensor parallel size")
    parser.add_argument("--pp", type=int, default=8, help="pipeline parallel size")
    parser.add_argument(
        "--domains",
        default=",".join(DEFAULT_DOMAINS),
        help="comma-separated domains to emit: TP,PP,DP,EP,EDP",
    )
    parser.add_argument(
        "--pp-pattern",
        choices=("adjacent", "alltoall"),
        default="adjacent",
        help="pipeline communication pattern",
    )
    parser.add_argument(
        "--direction",
        choices=("directed", "undirected"),
        default="directed",
        help="emit directed src->dst flows or undirected unique pairs",
    )
    parser.add_argument(
        "--local-mask-size",
        type=int,
        default=8,
        help="suppress flows whose host IDs fall in the same consecutive local group; use 0 to disable",
    )
    parser.add_argument("-o", "--output", required=True, type=pathlib.Path)
    parser.add_argument("--summary-output", type=pathlib.Path)
    parser.add_argument("--no-header", action="store_true")
    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    try:
        summary, placement = generate_pairs(args)
    except ValueError as error:
        print("error: %s" % error, file=sys.stderr)
        return 1
    if args.summary_output:
        write_summary(args.summary_output, summary, placement)
    for key, value in summary.items():
        print("%s: %s" % (key, value))
    if args.summary_output:
        print("summary_output: %s" % args.summary_output)
    print("output: %s" % args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
