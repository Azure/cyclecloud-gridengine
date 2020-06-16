#!/usr/bin/env bash

# $@ are the hostnames

for hostname in $@; do
    qmod -d '*@'${hostname}
    if [ $? == 0 ]; then
        echo ${hostname}
    fi
done
