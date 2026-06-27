# Unit-Merge Solver

This directory contains the Rust unit-merge solver used to reproduce six
ToR/switch-scope OmniPath-Ping examples.

## Quick Start

Run all six examples sequentially. The script builds the release binary first:

```bash
bash scripts/reproduce_six_tor_scope.sh
```

Run one example:

```bash
bash scripts/reproduce_six_tor_scope.sh fat-tree
bash scripts/reproduce_six_tor_scope.sh leaf-spine
bash scripts/reproduce_six_tor_scope.sh torus2d
bash scripts/reproduce_six_tor_scope.sh torus3d
bash scripts/reproduce_six_tor_scope.sh dragonfly
bash scripts/reproduce_six_tor_scope.sh dragonfly-plus
```

Multiple examples can be listed in one command:

```bash
bash scripts/reproduce_six_tor_scope.sh fat-tree dragonfly dragonfly-plus
```

The script owns the detailed topology and solver parameters. A successful run
prints a `done:` line with `stop=NoOverflow`, `overflow=0`, and
`max_overflow=0`.

## Examples

| Shortcut | Example |
|---|---|
| `fat-tree` | 32-ary Fat-tree |
| `leaf-spine` | Leaf-spine with 512 leaf switches and 256 spine switches |
| `torus2d` | 2D torus, 16 by 16 |
| `torus3d` | 3D torus, 8 by 8 by 8 |
| `dragonfly` | Dragonfly with 129 groups, 16 switches/group, 8 global links/switch |
| `dragonfly-plus` | Dense Dragonfly+ with 33 groups, 32 leaf and 32 spine switches/group |

## Layout

- `src/solver.rs`: topology-independent unit-merge solver.
- `src/topology/`: topology builders for the six examples.
- `src/main.rs`: configurable CLI used by the reproduction script.
- `scripts/reproduce_six_tor_scope.sh`: quick launcher for the six examples.
