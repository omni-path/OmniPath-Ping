# 3D Torus Topologies

Generated topology files for cubic 3D torus networks.

Format follows `simulation/scratch/third.cc`:

- First line: `node_num switch_num link_num`
- Second line: all switch node IDs
- Remaining lines: `src dst data_rate link_delay error_rate`

ID layout for an `N x N x N` torus:

- Server IDs: `0 .. N*N*N-1`
- ToR switch IDs: `N*N*N .. 2*N*N*N-1`
- Server `i` connects to ToR `N*N*N + i`
- ToRs are connected as a bidirectional 3D torus by one x-ring link, one y-ring link, and one z-ring link per ToR

All links use `100Gbps`, `1000ns` delay, and `0.000000` error rate.
