# OmniPath Ping

This repository contains source artifacts for OmniPath Ping (OPP), an active
network measurement primitive for service tracing under packet spraying.

## Repository Layout

- `p4/`: TNA P4 switch data-plane prototype. It implements the in-network
  OmniPath cache, unicast-based simulated multicast, probe/ACK deduplication,
  result aggregation, and timeout-triggered ACK generation.
- `solver/unit-merge/`: Rust implementation of the unit-merge grouping solver
  used to derive scalable task/group configurations for large topologies.

## P4 Prototype

The P4 prototype targets a Tofino/TNA-style programmable switch pipeline. See
`p4/README.md` for the mapping between the P4 code and the paper design.

## Unit-Merge Solver

Build and run the reference experiments:

```bash
cd solver/unit-merge
cargo build --release
cargo run --release --bin clos_test
cargo run --release --bin dragonfly_test
```

For configurable solver commands and experiment details, see
`solver/unit-merge/README.md`.
