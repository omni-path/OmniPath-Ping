# P4 Data-Plane Prototype

This directory contains the TNA P4 prototype for the OmniPath Ping (OPP)
switch-side data plane.

- `omnipath_ping.p4`: the switch pipeline program for a Tofino/TNA target.

The file implements the in-switch mechanisms described in the paper:

1. An in-network OmniPath cache built from direct-indexed registers.
2. Unicast-based simulated multicast for probe duplication.
3. Probe/ACK deduplication and result aggregation.
4. Cache-entry expiration and timeout-triggered ACK generation.

The surrounding runtime is responsible for assigning task IDs, programming
routing/bitmap tables, configuring mirror sessions, and interpreting returned
ACKs. This separation matches the paper's switch-host co-design: this directory
documents the switch pipeline, while host-side scheduling and analysis are
treated as companion runtime components.

## Target and Assumptions

The program includes `tna.p4` and uses TNA-specific metadata, registers,
register actions, mirror operations, and recirculation ports. Compile it with a
Tofino/TNA-capable P4 toolchain.

Before running the program, the control plane must configure at least:

- Mirror sessions used by the PING and PONG loopback paths.
- Recirculation/loopback port constants such as `CIRC_PORT_1`.
- Forwarding entries in `ipv4_host_t`.
- Destination-to-candidate-port bitmap entries in `ipv4_host_ping_t`.
- Port-to-bit mappings in `ingress_port_to_bit_waiting_t` and
  `ingress_port_to_bit_forward_t`.
- Bitmap-to-port mappings in `get_high_bit_port_t` and `get_high_bit_t`.
- ARP/host reachability entries in `arp_host`.
- The per-switch `SWITCH_ID` value used to encode fault/congestion links.

## Relation to the Paper

The paper presents OPP as a switch-host co-design for service tracing under
packet spraying. This P4 file implements the switch-side pieces of that design.

### Paper Section 3.2: OPP Workflow

The paper decomposes OPP into four steps:

- Step 1, host-side probe generation: the host agent generates PING probes and
  assigns task IDs/timestamps before they enter the switch pipeline.
- Step 2, in-network probe processing: implemented by the ingress parser,
  ingress control, egress control, and mirror/recirculation path.
- Step 3, in-network ACK processing: implemented by PONG parsing, OP-cache
  aggregation registers, and reverse-direction PONG duplication.
- Step 4, host-side failure localization: the P4 program returns aggregated
  fields that the host agent analyzes for congestion, loss, and loop symptoms.

In the code, PING probes are IPv4/UDP packets with `hdr.ipv4.protocol == 16`.
PONG/ACK packets use `hdr.ipv4.protocol == 100`, and timeout scanner packets
use `hdr.ipv4.protocol == 101`.

### Paper Section 4.1: OmniPath Cache

The paper describes OP cache as a direct-access stateful table indexed by a
token. In this implementation, the direct index is `task_ID`, and the cache size
is controlled by:

```p4
#define BSP_SIZE 4096
```

The `bsp_` prefix in the code denotes OP-cache/task state used by the switch
pipeline.

The main OP-cache registers are:

| Paper field | P4 register/state |
| --- | --- |
| Token_ID | `task_ID`, used as the register index |
| Token_Seq_No | `bsp_timestamp` / `task_start_tstamp` generation check |
| Flow_ID | `bsp_src_ip`, `bsp_dst_ip`, `bsp_src_dst_port` |
| Input_Ports | `bsp_waiting_ports` |
| Output_Ports | `bsp_forwarding_ports` |
| Equal_Cost_Path_Count | `bsp_path_count` |
| Maximum_QDelay | `bsp_max_delay` |
| Congested_Link | `bsp_max_delay_link` |
| Faulty_Link | `bsp_drop_link` |
| Entry_Expiration_Time / Is_Expired | `bsp_timeout_flag_time` |

The paper states that OP cache avoids hash collisions by using a direct-access
index. The same idea appears in the P4 program through direct register arrays:

```p4
Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_timestamp;
Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_waiting_ports;
Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_forwarding_ports;
Register<bit<16>, bit<16>>(BSP_SIZE, 0) bsp_path_count;
Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_max_delay;
Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_max_delay_link;
Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_drop_link;
Register<bsp_timeout_pair_h, bit<16>>(BSP_SIZE) bsp_timeout_flag_time;
```

The topology-aware scheduler described in the paper is responsible for ensuring
that different in-flight probes do not collide on the same `task_ID`.

### Paper Section 4.2: Probe and ACK Format

The P4 program uses two custom payload headers.

`ping_payload_h` carries the PING-side fields:

```p4
header ping_payload_h {
    bit<16> task_ID;
    bit<32> task_start_tstamp;
    bit<16> circ_times;
    bit<32> next_hop_bit;
    bit<8>  hop_times;
    bit<32> last_hop_delay;
    bit<32> last_hop_link;
}
```

These fields correspond to the paper's probe metadata:

- `task_ID`: direct OP-cache index, equivalent to the paper's token ID.
- `task_start_tstamp`: generation marker, playing the role of token sequence.
- `next_hop_bit`: candidate equal-cost output-port bitmap.
- `hop_times`: remaining-hop-derived timeout selector.
- `last_hop_delay`: previous-hop queueing delay.
- `last_hop_link`: previous-hop switch/port identifier.
- `circ_times`: implementation field used to distinguish original and
  recirculated copies during simulated multicast.

`pong_payload_h` carries the ACK-side fields:

```p4
header pong_payload_h {
    bit<16> task_ID;
    bit<32> task_start_tstamp;
    bit<16> path_count;
    bit<32> max_delay;
    bit<32> max_delay_link;
    bit<32> drop_link;
}
```

These fields correspond to the paper's ACK metadata:

- `path_count`: aggregated equal-cost path count.
- `max_delay`: maximum observed queueing/path delay.
- `max_delay_link`: link associated with the maximum delay.
- `drop_link`: link selected as faulty after timeout or propagated from a
  downstream ACK.

The additional Ethernet types `HBIT`, `PONG`, `FIRST_PONG`, and `CIRC` are
internal loopback/mirror wrappers used by the implementation.

### Paper Section 4.3: Switch-Side Probe Processing

For a new PING probe, the program:

1. Parses the PING payload and extracts `task_ID`, timestamp, and previous-hop
   telemetry.
2. Looks up the destination in `ipv4_host_ping_t` to obtain the equal-cost
   candidate bitmap.
3. Initializes OP-cache registers when `task_start_tstamp` is newer than the
   cached timestamp.
4. Selects one unicast egress port through `ipv4_host_t`.
5. Clears the selected port bit from `next_hop_bit`.
6. Forwards one copy to the selected egress port.
7. Mirrors a loopback copy when more candidate ports remain.

This is the code-level implementation of the paper's unicast-based simulated
multicast. The switch does not use native multicast for data-path coverage; it
iterates through normal unicast decisions while the bitmap records which ports
remain to be explored.

For duplicate PING probes that map to an existing `task_ID` and timestamp, the
program updates the ingress-port bitmap and delay-related registers, then drops
the redundant copy. This corresponds to the paper's probe deduplication step.

### Paper Section 4.4: Switch-Side ACK Processing

When a PONG/ACK returns, the program:

1. Maps the ACK ingress port into a bit using `ingress_port_to_bit_forward_t`.
2. Clears the corresponding bit in `bsp_forwarding_ports`.
3. Accumulates `path_count` into `bsp_path_count`.
4. Updates `bsp_max_delay` and `bsp_max_delay_link` when a larger delay is seen.
5. Propagates `drop_link` through `bsp_drop_link`.
6. Waits until `bsp_forwarding_ports` becomes zero, meaning all expected
   downstream ACKs have arrived.
7. Emits one aggregated PONG upstream.

The reverse-direction duplication described in the paper is implemented with
`bsp_waiting_ports`, `FIRST_PONG`, `PONG`, and the egress-side `get_high_bit_t`
table. The P4 program enumerates bits from the waiting-port bitmap and mirrors
PONG copies until all upstream ports have been covered.

### Paper Section 4.5: OP-Cache Entry Expiration

The paper uses cache-entry expiration to handle lost probes or ACKs. In the P4
program, timeout state is kept in:

```p4
Register<bsp_timeout_pair_h, bit<16>>(BSP_SIZE) bsp_timeout_flag_time;
```

The program computes an expiration time through `cal_timeout_time_t`, checks it
with `check_bsp_timeout_flag_time_t`, and uses scanner/loopback packets to walk
through task IDs. When an entry expires, `push_back_timeout_t` constructs a
PONG/ACK from cached flow information and sets `drop_link` to identify an
unreturned output link:

```p4
hdr.pong_payload.drop_link = SWITCH_ID | (bit<32>)port;
```

This is the switch-side mechanism that lets the host receive an ACK even when a
downstream probe or ACK is lost.

### Appendix E.1: Tofino/P4 Prototype

Appendix E.1 summarizes the prototype as using:

- Registers and register actions for OP cache.
- Recirculation ports and mirror features for simulated multicast.

Those two implementation choices are exactly reflected in this file:

- OP cache is implemented by `Register` arrays and `RegisterAction` blocks.
- PING loopback is driven by `circle_ping_t` and ingress deparser mirror type 1.
- Timeout/scanner loopback is driven by `mirror_circ_pkt_t` and mirror type 2.
- PONG reverse duplication is driven by egress-side mirror emission.

## Integration Scope

The P4 pipeline is one component of the complete OPP system. In a deployment,
it is paired with:

- A host probe agent that creates service-flow probes and ACKs.
- A control plane that installs route, bitmap, ARP, and mirror-session entries.
- A topology-aware scheduler that assigns task IDs/tokens according to the
  concurrency-control policy described in the paper.
- The offline grouping solver under `solver/unit-merge`, which helps derive
  scalable task/group configurations for large topologies.

Keeping these responsibilities separate makes the switch artifact easier to
inspect: the P4 program contains the data-plane state machine, while the runtime
components supply configuration and consume the aggregated ACK results.

## Build Notes

Compile this file with the Tofino SDE P4 compiler for a TNA target. The exact
command depends on the installed SDE version and local build system. A typical
workflow is:

```bash
cd <repo-root>
# Use the SDE-provided TNA P4 compiler wrapper for your environment.
<tofino-p4-compiler> p4/omnipath_ping.p4
```

Proprietary SDE files and hardware-specific deployment environments are managed
outside this portable source tree.
