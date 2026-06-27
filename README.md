# OmniPath Ping

This repository contains source artifacts for OmniPath Ping (OPP), an active
network measurement primitive for service tracing under packet spraying.

## Repository Layout

- `p4/`: TNA P4 switch data-plane prototype. It implements the in-network
  OmniPath cache, unicast-based simulated multicast, probe/ACK deduplication,
  result aggregation, and timeout-triggered ACK generation.
- `solver/unit-merge/`: Rust implementation of the unit-merge grouping solver
  used to derive scalable task/group configurations for large topologies.
- `NS3/OPP/`: ns-3-based OPP simulator and experiment reproduction scripts.

## P4 Prototype

The P4 prototype targets a Tofino/TNA-style programmable switch pipeline. See
`p4/README.md` for the mapping between the P4 code and the paper design.

## Unit-Merge Solver

The unit-merge solver is under `solver/unit-merge/`. It now focuses on six
ToR/switch-scope examples:

- 32-ary Fat-tree
- Leaf-spine with 512 leaf switches and 256 spine switches
- 2D torus, 16 by 16
- 3D torus, 8 by 8 by 8
- Dragonfly with 129 groups, 16 switches/group, and 8 global links/switch
- Dense Dragonfly+ with 33 groups and 32 leaf plus 32 spine switches/group

Run all six examples:

```bash
cd solver/unit-merge
bash scripts/reproduce_six_tor_scope.sh
```

Run one example by passing its shortcut:

```bash
bash scripts/reproduce_six_tor_scope.sh fat-tree
bash scripts/reproduce_six_tor_scope.sh leaf-spine
bash scripts/reproduce_six_tor_scope.sh torus2d
bash scripts/reproduce_six_tor_scope.sh torus3d
bash scripts/reproduce_six_tor_scope.sh dragonfly
bash scripts/reproduce_six_tor_scope.sh dragonfly-plus
```

See `solver/unit-merge/README.md` for the shortcut list.

## NS-3 Simulator

The ns-3 simulator and figure reproduction scripts are under `NS3/OPP/`.
See `NS3/OPP/README.md` for build commands and experiment runner entry points.
