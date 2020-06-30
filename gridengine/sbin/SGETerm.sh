#!/bin/bash
# This script should be added as the TERMINATE_METHOD in the
# queue definition with $job_pid, $job_id, $job_owner, and interval arguments.
# e.g.  script.sh $job_pid $job_id $job_owner 90

if [ -z "$4" ]
then
    echo "Usage: $0 \$job_pid \$job_id \$job_owner interval" 
    exit 1
fi

#echo "Term script Running on: $USER $1 $2 $3 $4" >> ~$3/qdel_log.log
#echo `pgrep -g $1` >> ~$3/qdel_log.log

stat=`pgrep -g $1 -u $3`
if [ ! -z "$stat" ]
then
  if [ -f ~$3/killsig ]
  then
    read sig < ~$3/killsig
    # echo "Sending sig $sig to $1" >> ~$3/qdel_log.log
    /bin/kill -s $sig -$1
    rm -f ~$3/killsig
  else
    # echo "Sending sig 2 to $1" >> ~$3/qdel_log.log
    /bin/kill -s 2 -$1
  fi
else
    break
fi

#uncomment the following for debugging
#echo "Job $2 killed." >> ~$3/qdel_log.log
