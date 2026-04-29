# Unit-Merge Solver

This directory contains the Rust implementation of the unit-merge grouping
heuristic used by OmniPath-Ping experiments.

## Layout

- `src/lib.rs`: topology builders and the core unit-merge solver.
- `src/main.rs`: command-line entry point with configurable experiment options.
- `src/bin/clos_test.rs`: reference Clos experiment.
- `src/bin/dragonfly_test.rs`: reference Dragonfly experiment.

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

Use `--help` to see all options:

```bash
cargo run --release -- --help
```

## Run the reference experiments

```bash
cargo run --release --bin clos_test
cargo run --release --bin dragonfly_test
```

The reference binaries print topology statistics, final group sizes, switch
usage counts, and build/solve/total timing.
