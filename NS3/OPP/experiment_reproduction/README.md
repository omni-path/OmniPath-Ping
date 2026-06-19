# Experiment Reproduction

Each figure directory contains its own README with the command to run that
figure.

Generated results are written under each figure directory's `results/` folder.

Figure directories:

```text
experiment_reproduction/figure6/
experiment_reproduction/figure7/
experiment_reproduction/figure8-9/
experiment_reproduction/figure10-11/
experiment_reproduction/figure13/
experiment_reproduction/figure14-table7/
experiment_reproduction/figure15/
```

Shared files:

```text
experiment_reproduction/topologies/
experiment_reproduction/runner_path_utils.py
```

To make reproduction practical, some workloads may be trimmed to traffic from a
single ToR or a single PoD, so each experiment can finish within a few hours;
this does not affect the main conclusions.
