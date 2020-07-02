#!/usr/bin/env bash
set -e

cc_home=${CYCLECLOUD_HOME:-/opt/cycle/jetpack}

activation=${cc_home}/system/bootstrap/gridenginevenv/bin/activate

if [ -e $activation ]; then
    source $activation
fi

read jobs
# you can parse ${jobs} as json and make any modifications before printing
# to stdout.
# e.g.
# 123> printf ${jobs} | python preprocess_jobs.py
printf "${jobs}"