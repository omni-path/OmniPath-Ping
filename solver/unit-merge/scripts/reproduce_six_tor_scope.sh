#!/usr/bin/env bash
set -euo pipefail

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

run_fat_tree() {
  run_case "32-ary Fat-tree" \
    clos \
    --k 32 \
    --m3 1 \
    --include-intra-pod \
    --tor-cap 1024 \
    --agg-cap 1024 \
    --source-group-scope tor \
    --group-size-limit 16
}

run_leaf_spine() {
  run_case "Leaf-spine, 512 leaf and 256 spine" \
    leaf-spine \
    --leaves 512 \
    --spines 256 \
    --leaf-cap 1024 \
    --group-size-limit 1
}

run_torus2d() {
  run_case "2D torus, 16 by 16" \
    torus2d \
    --n 16 \
    --cap 2048 \
    --source-group-scope host \
    --group-size-limit 6
}

run_torus3d() {
  run_case "3D torus, 8 by 8 by 8" \
    torus3d \
    --n 8 \
    --cap 4096 \
    --source-group-scope host \
    --group-size-limit 17
}

run_dragonfly() {
  run_case "Dragonfly, 129 groups, 16 switches/group, 8 global links/switch" \
    dragonfly \
    --groups 129 \
    --routers-per-group 16 \
    --global-links-per-router 8 \
    --cap 4096 \
    --source-group-scope router \
    --group-size-limit 20
}

run_dragonfly_plus() {
  run_case "Dragonfly+, 33 groups, 32 leaf and 32 spine/group" \
    dragonfly-plus \
    --groups 33 \
    --leaves-per-group 32 \
    --spines-per-group 32 \
    --dense-global \
    --cap 4096 \
    --source-group-scope tor \
    --group-size-limit 20
}

run_all() {
  run_fat_tree
  run_leaf_spine
  run_torus2d
  run_torus3d
  run_dragonfly
  run_dragonfly_plus
}

usage() {
  printf '%s\n' \
    "Usage: bash scripts/reproduce_six_tor_scope.sh [all|fat-tree|leaf-spine|torus2d|torus3d|dragonfly|dragonfly-plus]..." \
    "" \
    "With no arguments, all six examples are run."
}

if [[ "$#" -eq 0 ]]; then
  set -- all
fi

for example in "$@"; do
  case "$example" in
    all | fat-tree | leaf-spine | torus2d | torus3d | dragonfly | dragonfly-plus)
      ;;
    -h | --help | help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

cargo build --release

for example in "$@"; do
  case "$example" in
    all)
      run_all
      ;;
    fat-tree)
      run_fat_tree
      ;;
    leaf-spine)
      run_leaf_spine
      ;;
    torus2d)
      run_torus2d
      ;;
    torus3d)
      run_torus3d
      ;;
    dragonfly)
      run_dragonfly
      ;;
    dragonfly-plus)
      run_dragonfly_plus
      ;;
  esac
done
