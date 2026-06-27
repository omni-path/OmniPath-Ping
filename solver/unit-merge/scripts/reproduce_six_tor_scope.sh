#!/usr/bin/env bash
set -euo pipefail

cargo build --release

BIN="./target/release/rust-unit-merge"
COMMON=(
  --candidate-limit 500
  --groups-per-node 256
  --orbit-alternative-limit 1000
  --max-steps 4500000
  --progress-every 0
  --rebuild-every 1000
  --skip-symmetry-validation
)

run_case() {
  local name="$1"
  shift
  printf '\n===== %s =====\n' "$name"
  "$BIN" "$@" "${COMMON[@]}"
}

run_case "32-ary Fat-tree" \
  clos \
  --k 32 \
  --m3 1 \
  --include-intra-pod \
  --tor-cap 1024 \
  --agg-cap 1024 \
  --source-group-scope tor \
  --group-size-limit 16

run_case "Leaf-spine, 512 leaf and 256 spine" \
  leaf-spine \
  --leaves 512 \
  --spines 256 \
  --leaf-cap 1024 \
  --group-size-limit 1

run_case "2D torus, 16 by 16" \
  torus2d \
  --n 16 \
  --cap 2048 \
  --source-group-scope host \
  --group-size-limit 6

run_case "3D torus, 8 by 8 by 8" \
  torus3d \
  --n 8 \
  --cap 4096 \
  --source-group-scope host \
  --group-size-limit 17

run_case "Dragonfly, 129 groups, 16 switches/group, 8 global links/switch" \
  dragonfly \
  --groups 129 \
  --routers-per-group 16 \
  --global-links-per-router 8 \
  --cap 4096 \
  --source-group-scope router \
  --group-size-limit 20

run_case "Dragonfly+, 33 groups, 32 leaf and 32 spine/group" \
  dragonfly-plus \
  --groups 33 \
  --leaves-per-group 32 \
  --spines-per-group 32 \
  --dense-global \
  --cap 4096 \
  --source-group-scope tor \
  --group-size-limit 20
