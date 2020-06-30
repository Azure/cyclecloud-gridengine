#!/bin/bash

source /opt/cycle/jetpack/system/bin/cyclecloud-env.sh

NODE=$(jetpack config cyclecloud.instance.id)
jetpack log "$NODE received button/power.* event, autostopping..."

# TODO this does not exist
/opt/cycle/jetpack/system/bootstrap/autostopcallback.sh