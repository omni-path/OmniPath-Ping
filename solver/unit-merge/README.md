# Unit-Merge Solver

This directory contains the Rust implementation of the unit-merge grouping
heuristic used by OmniPath-Ping experiments.

## Layout

- `src/solver.rs`: topology-independent footprint solver.
- `src/topology/clos.rs`: Clos demand, footprint, capacity, source-group, and symmetry builder.
- `src/topology/dragonfly.rs`: Dragonfly builder with all-shortest-path footprints.
- `src/topology/dragonfly_plus.rs`: Dragonfly+ leaf/spine builder, including dense-global mode.
- `src/topology/leaf_spine.rs`: leaf-spine builder.
- `src/topology/torus2d.rs`: 2D torus builder.
- `src/topology/torus3d.rs`: 3D torus builder.
- `src/lib.rs`: public module exports.
- `src/main.rs`: command-line entry point with configurable experiment options.
- `scripts/reproduce_six_tor_scope.sh`: one-command reproduction script for
  the six ToR/switch-scope runs.

## Build

```bash
cargo build --release
```

## Run the configurable CLI

From this directory:

```bash
cargo run --release -- clos --k 32 --m3 1 --group-size-limit 16
cargo run --release -- dragonfly --groups 129 --routers-per-group 16 --global-links-per-router 8 --cap 5212 --group-size-limit 8
```

Common solver controls include `--candidate-limit`, `--groups-per-node`,
`--orbit-alternative-limit`, `--no-representative-search`, `--max-steps`, and
`--progress-every`. The solver uses strict complete symmetry orbits when a
topology provides cyclic symmetry. Topology builders also provide source merge
groups; the solver only merges communication pairs whose `src` nodes are in the
same source group.

Use `--help` to see all options:

```bash
cargo run --release -- --help
```

## Reproduce the six ToR/switch-scope runs

The following commands reproduce the six representative runs with a common
search configuration. The control domain is the source ToR/switch wherever that
choice is meaningful:

- Clos: `--source-group-scope tor`
- Leaf-spine: source leaf switch, fixed by the topology builder
- 2D/3D torus: `--source-group-scope host`
- Dragonfly: `--source-group-scope router`
- Dragonfly+: `--source-group-scope tor`

Use the release binary from this directory:

```bash
cargo build --release
```

To run all six cases sequentially:

```bash
bash scripts/reproduce_six_tor_scope.sh
```

The script prints the solver output for each case. Wrap it with `time` or run
the commands below individually if wall-clock timing is needed.

Common search flags:

```text
--candidate-limit 500
--groups-per-node 256
--orbit-alternative-limit 1000
--max-steps 4500000
--progress-every 0
--rebuild-every 1000
--skip-symmetry-validation
```

`--skip-symmetry-validation` only skips the expensive one-time closure check;
the solver still uses strict complete symmetry orbits because symmetry remains
enabled. Remove this flag when validating a newly added topology.

### 32-ary Fat-tree

This run includes all ToR-to-ToR demands, including same-pod pairs.

```bash
./target/release/rust-unit-merge clos \
  --k 32 \
  --m3 1 \
  --include-intra-pod \
  --tor-cap 1024 \
  --agg-cap 1024 \
  --source-group-scope tor \
  --group-size-limit 16 \
  --candidate-limit 500 \
  --groups-per-node 256 \
  --orbit-alternative-limit 1000 \
  --max-steps 4500000 \
  --progress-every 0 \
  --rebuild-every 1000 \
  --skip-symmetry-validation
```

### Leaf-spine, 512 leaf and 256 spine

```bash
./target/release/rust-unit-merge leaf-spine \
  --leaves 512 \
  --spines 256 \
  --leaf-cap 1024 \
  --group-size-limit 1 \
  --candidate-limit 500 \
  --groups-per-node 256 \
  --orbit-alternative-limit 1000 \
  --max-steps 4500000 \
  --progress-every 0 \
  --rebuild-every 1000 \
  --skip-symmetry-validation
```

### 2D torus, 16 by 16

```bash
./target/release/rust-unit-merge torus2d \
  --n 16 \
  --cap 2048 \
  --source-group-scope host \
  --group-size-limit 6 \
  --candidate-limit 500 \
  --groups-per-node 256 \
  --orbit-alternative-limit 1000 \
  --max-steps 4500000 \
  --progress-every 0 \
  --rebuild-every 1000 \
  --skip-symmetry-validation
```

### 3D torus, 8 by 8 by 8

```bash
./target/release/rust-unit-merge torus3d \
  --n 8 \
  --cap 4096 \
  --source-group-scope host \
  --group-size-limit 17 \
  --candidate-limit 500 \
  --groups-per-node 256 \
  --orbit-alternative-limit 1000 \
  --max-steps 4500000 \
  --progress-every 0 \
  --rebuild-every 1000 \
  --skip-symmetry-validation
```

### Dragonfly, 129 groups, 16 switches/group, 8 global links/switch

```bash
./target/release/rust-unit-merge dragonfly \
  --groups 129 \
  --routers-per-group 16 \
  --global-links-per-router 8 \
  --cap 4096 \
  --source-group-scope router \
  --group-size-limit 20 \
  --candidate-limit 500 \
  --groups-per-node 256 \
  --orbit-alternative-limit 1000 \
  --max-steps 4500000 \
  --progress-every 0 \
  --rebuild-every 1000 \
  --skip-symmetry-validation
```

### Dragonfly+, 33 groups, 32 leaf and 32 spine/group

```bash
./target/release/rust-unit-merge dragonfly-plus \
  --groups 33 \
  --leaves-per-group 32 \
  --spines-per-group 32 \
  --dense-global \
  --cap 4096 \
  --source-group-scope tor \
  --group-size-limit 20 \
  --candidate-limit 500 \
  --groups-per-node 256 \
  --orbit-alternative-limit 1000 \
  --max-steps 4500000 \
  --progress-every 0 \
  --rebuild-every 1000 \
  --skip-symmetry-validation
```

Each command prints a `done:` line. A successful reproduction has
`stop=NoOverflow`, `overflow=0`, and `max_overflow=0`.
