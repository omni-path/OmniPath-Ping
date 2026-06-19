# Leaf-spine Topologies

Generated k-radix leaf-spine topology files for `simulation/scratch/third.cc`.

Format:

- First line: `node_num switch_num link_num`
- Second line: all switch node IDs
- Remaining lines: `src dst data_rate link_delay error_rate`

ID layout for k-radix leaf-spine:

- Host IDs: `0 .. k*k/2-1`
- Leaf switch IDs: immediately after hosts, `k` switches
- Spine switch IDs: immediately after leaf switches, `k/2` switches
- Each leaf switch has `k/2` directly attached hosts
- Each leaf switch connects to every spine switch

All links use `100Gbps`, `1000ns` delay, and `0.000000` error rate.
