#!/usr/bin/env bash
set -e

source /opt/cycle/jetpack/system/bootstrap/gridenginevenv/bin/activate

jobs_and_nodes=$(azge jobs_and_nodes)
# here we just invoke the cli to get the jobs and nodes and spit them
# feel free to parse this json and make relevant modifications
# before printing to stdout
printf ${jobs_and_nodes}

