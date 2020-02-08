#!/bin/bash

source /opt/cycle/jetpack/system/bin/cyclecloud-env.sh

NODE=$(jetpack config cyclecloud.instance.id)
jetpack log "$NODE received button/power.* event, autostopping..."

/opt/cycle/jetpack/system/bootstrap/autostopcallback.sh