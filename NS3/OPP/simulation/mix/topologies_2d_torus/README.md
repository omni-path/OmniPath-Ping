# 2D Torus Topologies

Generated topology files for square 2D torus networks.

Format follows `simulation/scratch/third.cc`:

- First line: `node_num switch_num link_num`
- Second line: all switch node IDs
- Remaining lines: `src dst data_rate link_delay error_rate`

ID layout for an `N x N` torus:

- Server IDs: `0 .. N*N-1`
- ToR switch IDs: `N*N .. 2*N*N-1`
- Server `i` connects to ToR `N*N + i`
- ToRs are connected as a bidirectional 2D torus by one horizontal ring link and one vertical ring link per ToR

All links use `100Gbps`, `1000ns` delay, and `0.000000` error rate.
