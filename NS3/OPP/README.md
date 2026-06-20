# OPP

This repository contains the OPP simulator and experiment reproduction scripts.
The simulator is built on the HPCC ns-3 simulator, which itself is based on
ns-3.17.

The two main directories are kept side by side:

```text
simulation/
experiment_reproduction/
```

`simulation/` contains the ns-3 simulator implementation. Build the simulator
there before running experiments.

`experiment_reproduction/` contains the figure/table runners and shared
experiment inputs. Each figure directory contains its own README with the
command to run that figure.

## Workflow

First build the simulator:

```bash
cd simulation
./waf configure
./waf build
```

Then run the experiment scripts from `experiment_reproduction/`, for example:

```bash
python3 experiment_reproduction/figure14/run_figure14.py
```

Generated results are written under each figure directory's `results/` folder.

## Acknowledgements

This codebase inherits from the HPCC simulator and ns-3. We retain the upstream
license and author information in `simulation/LICENSE` and `simulation/AUTHORS`.
