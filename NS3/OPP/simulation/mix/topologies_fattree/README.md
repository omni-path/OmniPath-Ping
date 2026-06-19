# Fat-tree Topologies

Generated k-ary fat-tree topology files for `simulation/scratch/third.cc`.

Format:

- First line: `node_num switch_num link_num`
- Second line: all switch node IDs
- Remaining lines: `src dst data_rate link_delay error_rate`

ID layout for k-ary fat-tree matches the compressed `FAT_TREE` mode in `third.cc`:

- Host IDs: `0 .. k^3/4-1`
- Edge switch IDs: immediately after hosts, `k^2/2` switches
- Aggregation switch IDs: immediately after edge switches, `k^2/2` switches
- Core switch IDs: immediately after aggregation switches, `k^2/4` switches

Links:

- Each host connects to its canonical edge switch
- Each edge switch connects to all aggregation switches in the same pod
- Each aggregation switch connects to the matching core group

All links use `100Gbps`, `1000ns` delay, and `0.000000` error rate.
