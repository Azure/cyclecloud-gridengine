#!/usr/bin/env bash

CLUSTER_NAME=
USERNAME=
PASSWORD=
CC_URL=
INSTALLDIR=/opt/cycle/gridengine
RELEVANT_COMPLEXES=slots,slot_type,nodearray,m_mem_free,exclusive
IDLE_TIMEOUT=300
OUTPUT_PATH=

function helpmsg(){
    echo ./generate_autoscale_json.sh --cluster-name CLUSTER --username USER --password PASS --url CYCLECLOUD_URL [--install-dir /opt/cycle/gridengine] [--idle-timoeut $IDLE_TIMEOUT]
    exit 1
}

while (( "$#" )); do
    case "$1" in
        --cluster-name)
            CLUSTER_NAME=$2
            shift 2
            ;;
        --username)
            USERNAME=$2
            shift 2
            ;;
        --password)
            PASSWORD=$2
            shift 2
            ;;
        --url)
            CC_URL=$2
            shift 2
            ;;
        --install-dir)
            INSTALLDIR=$2
            shift 2
            ;;
        --relevant-complexes)
            RELEVANT_COMPLEXES=$2
            shift 2
            ;;
        --idle-timeout)
            IDLE_TIMEOUT=$2
            shift 2
            ;;
        --output)
            OUTPUT_PATH=$2
            shift 2
            ;;
        --help)
            helpmsg
            ;;

        -*|--*=)
            echo "Unknown option $1" >&2
            exit 1
            ;;
        *)
            echo "Unknown option  $1" >&2
            exit 1
            ;;
    esac
done

if [ -z "$CLUSTER_NAME" ]; then helpmsg; fi
if [ -z "$USERNAME" ]; then helpmsg; fi
if [ -z "$PASSWORD" ]; then helpmsg; fi
if [ -z "$CC_URL" ]; then helpmsg; fi

if [ ! -e $INSTALLDIR ]; then
    echo $INSTALLDIR does not exist. >&2
    exit 1
fi

default_resources=

qconf -sc | grep -E '^slot_type ' > /dev/null 2>&1
if [ $? == 0 ]; then
    default_resources="$default_resources --default-resource {\"select\":{},\"name\":\"slot_type\",\"value\":\"node.nodearray\"}"
else
    echo Complex slot_type does not exist. Skipping default_resource definition. >&2
fi

qconf -sc | grep -E '^nodearray ' > /dev/null 2>&1
if [ $? == 0 ]; then
    default_resources="$default_resources --default-resource {\"select\":{},\"name\":\"nodearray\",\"value\":\"node.nodearray\"}"
else
    echo Complex nodearray does not exist. Skipping default_resource definition. >&2
fi

default_hostgroups=
hostgroup_constraints=
qconf -shgrp @cyclempi > /dev/null 2>&1
if [ $? == 0 ]; then
    default_hostgroups="$default_hostgroups --default-hostgroups {\"select\":{\"node.colocated\":true},\"hostgroups\":[\"@cyclempi\"]}"
    hostgroup_constraints="$hostgroup_constraints --hostgroup-constraint @cyclempi={\"node.colocated\":true}"
else
    echo Hostgroup @cyclempi does not exist. Creating a default_hostgroup that you can edit manually. See @enter_default_mpi_hostgroup >&2
    default_hostgroups="$default_hostgroups --default-hostgroups {\"select\":{\"node.colocated\":false},\"hostgroups\":[\"@enter_default_mpi_hostgroup\"]}"
fi

qconf -shgrp @cyclehtc > /dev/null 2>&1
if [ $? == 0 ]; then
    default_hostgroups="$default_hostgroups --default-hostgroups {\"select\":{\"node.colocated\":true},\"hostgroups\":[\"@cyclehtc\"]}"
    hostgroup_constraints="$hostgroup_constraints --hostgroup-constraint @cyclehtc={\"node.colocated\":false}"
else
    echo Hostgroup @cyclehtc does not exist. Creating a default_hostgroup that you can edit manually. Defaulting to @allhosts >&2
    default_hostgroups="$default_hostgroups --default-hostgroups {\"select\":{\"node.colocated\":false},\"hostgroups\":[\"@allhosts\"]}"
fi

disable_pgs_for_pe=
# LEGACY: we supply a 'make' pe that requires this setting to ensure it does not createe
# nodes in a placement group
qconf -sp make > /dev/null 2>&1
if [ $? == 0 ]; then
    disable_pgs_for_pe="--disable-pgs-for-pe make"
fi

relevant_complexes_validated=
for c in $( echo $RELEVANT_COMPLEXES | tr , " " ); do

    qconf -sc | grep -E "^$c " > /dev/null 2>&1
    if [ $? == 0 ]; then
        prefix=","
        if [ "$relevant_complexes_validated" == "" ]; then
            prefix=""
        fi
        relevant_complexes_validated="$relevant_complexes_validated$prefix$c"
    else
        echo Complex $c does not exist. Excluding from relevant_complexes. >&2
    fi
done


azge initconfig --cluster-name $CLUSTER_NAME \
                --username     $USERNAME \
                --password     $PASSWORD \
                --url          $CC_URL \
                --lock-file    $INSTALLDIR/scalelib.lock \
                --log-config   $INSTALLDIR/logging.conf \
                --default-resource '{"select": {}, "name": "slots", "value": "node.vcpu_count"}' \
                $default_resources \
                --default-resource '{"select": {}, "name": "m_mem_free", "value": "node.resources.memgb", "subtract": "1g"}' \
                --default-resource '{"select": {}, "name": "mfree", "value": "node.resources.m_mem_free"}' \
                --default-resource '{"select": {}, "name": "exclusive", "value": "true"}' \
                $default_hostgroups \
                $hostgroup_constraints \
                $disable_pgs_for_pe \
                --idle-timeout $IDLE_TIMEOUT \
                --relevant-complexes $relevant_complexes_validated > ${OUTPUT_PATH:-$INSTALLDIR/autoscale.json}
