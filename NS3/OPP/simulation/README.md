# OPP Simulator

This simulator inherits from the HPCC ns-3 simulator.

## Build

Run from this directory:

```bash
./waf configure
./waf build
```

If the default compiler is too new for ns-3.17, use gcc/g++ 5:

```bash
CC=gcc-5 CXX=g++-5 ./waf configure
./waf build
```

## Experiments

The experiment runners are outside this directory:

```text
../experiment_reproduction/
```

## OPP Core Code

The main OPP implementation is in:

```text
scratch/third.cc
src/point-to-point/model/rdma-hw.cc
src/point-to-point/model/rdma-hw.h
src/point-to-point/model/opp-token-manager.cc
src/point-to-point/model/opp-token-manager.h
src/point-to-point/model/switch-node.cc
src/point-to-point/model/switch-node.h
src/point-to-point/model/qbb-header.cc
src/point-to-point/model/qbb-header.h
```
