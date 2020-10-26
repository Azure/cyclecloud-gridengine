#!/bin/bash

function qconf {
    echo $@
}

pe_file=$1
queue_file=$1.q
pe=$2
queue=$2.q
host_list=$2.h

cat > ${pe_file} <<EOF
pe_name            ${pe}
slots              99999
user_lists         NONE
xuser_lists        NONE
start_proc_args    /bin/true
stop_proc_args     /bin/true
allocation_rule    4
control_slaves     TRUE
job_is_first_task  FALSE
urgency_slots      min
accounting_summary FALSE
EOF

echo
echo "BEGIN pe_file ${pe_file}"
cat ${pe_file}
echo "END pe_file ${pe_file}"
echo

add_or_modify=$( (qconf -spl | egrep -q "^${pe}\$" && echo M) || echo A)
qconf -${add_or_modify}p $1 || exit 1

cat >${queue_file} <<EOF
suspend_method        ${SGE_ROOT}/SGESuspend.sh \$job_pid \$job_id \$job_owner
terminate_method      ${SGE_ROOT}/SGETerm.sh \$job_pid \$job_id \$job_owner 90
shell                 /bin/sh
shell_start_mode      unix_behavior
pe_list               ${pe}
host_list             @${host_list}
rerun TRUE
EOF

echo
echo "BEGIN queue_file ${queue_file}"
cat ${queue_file}
echo "END queue_file ${queue_file}"
echo

add_or_modify=$( (qconf -sql | egrep -q "^${queue}\$" && echo M) || echo A)
qconf -${add_or_modify}q ${queue_file}

