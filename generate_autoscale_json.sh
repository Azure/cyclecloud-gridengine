#!/usr/bin/env bash
CLUSTER_NAME=
USERNAME=
PASSWORD=
CC_URL=
INSTALLDIR=/opt/cycle/gridengine
RELEVANT_COMPLEXES=slots,slot_type,nodearray,m_mem_free,exclusive

function helpmsg(){
    echo ./generate_autoscale_json.sh --cluster-name CLUSTER --username USER --password PASS --url CYCLECLOUD_URL [--install-dir /opt/cycle/gridengine]
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

azge initconfig --cluster-name $CLUSTER_NAME \
                --username     $USERNAME \
                --password     $PASSWORD \
                --url          $CC_URL \
                --lock-file    $INSTALLDIR/scalelib.lock \
                --log-config   $INSTALLDIR/logging.conf \
                --default-resource '{"select": {}, "name": "slots", "value": "node.vcpu_count"}' \
                --default-resource '{"select": {}, "name": "slot_type", "value": "node.nodearray"}' \
                --default-resource '{"select": {}, "name": "nodearray", "value": "node.nodearray"}' \
                --default-resource '{"select": {}, "name": "m_mem_free", "value": "node.resources.memgb", "subtract": "1g"}' \
                --default-resource '{"select": {}, "name": "mfree", "value": "node.resources.m_mem_free"}' \
                --default-resource '{"select": {}, "name": "exclusive", "value": "true"}' \
                --default-hostgroups '{"select": {"node.colocated": true}, "hostgroups": ["@cyclempi"]}' \
                --default-hostgroups '{"select": {"node.colocated": false}, "hostgroups": ["@cyclehtc"]}' \
                --disable-pgs-for-pe make \
                --hostgroup-constraint @cyclempi='{"node.colocated": true}' \
                --hostgroup-constraint @cyclehtc='{"node.colocated": false}' \
                --idle-timeout 1800 \
                --relevant-complexes $RELEVANT_COMPLEXES > $INSTALLDIR/autoscale.json