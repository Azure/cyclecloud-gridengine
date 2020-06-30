#!/bin/bash
# This script should be added as the SUSPEND_METHOD in the
# queue definition with a $job_pid, $job_id, and $job_owner arguments.
# e.g. script.sh $job_pid $job_id $job_owner

if [ -z "$3" ]
then
    echo "Usage: $0 \$job_pid \$job_id \$job_owner"
    exit 1
fi

stat=`pgrep -g $1`
if [ ! -z "$stat" ]
then
    #echo "Sending $sig to $1" >> ~$3/qdel_log.log
    /bin/kill -s SIGTSTP -$1
else
    echo "Process $1 not found for job $2" >> ~$3/qdel_log.log
    echo "Unable to suspend." >> ~$3/qdel_log.log
    exit 1
fi

#uncomment the following for debugging
#echo "Suspending Job $2 " >> ~$3/qdel_log.log
