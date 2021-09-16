#!/bin/bash --login
# The --login ensures the bash configuration is loaded,
# enabling Conda.
# set -eu pipefail
# conda activate pangolin
pangolin -v
# exec python run.py