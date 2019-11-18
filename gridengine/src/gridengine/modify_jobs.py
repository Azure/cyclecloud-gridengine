#!/usr/bin/env python
import datetime
import logging
import os
import subprocess
import sys
import tempfile

import sge
from gridengine import parallel_environments
from placement_groups import PlacementGroupSet

try:
    import jetpack.config as jetpack_config
except ImportError:
    jetpack_config = {}


logging.basicConfig(stream=sys.stderr,
                    format="%(asctime)s %(levelname)s: %(message)s",
                    level=logging.DEBUG if os.getenv('MODIFY_JOBS_DEBUG', 0) else logging.INFO)


def _build_placement_groups(hosts_to_reqs):
    pg_set = PlacementGroupSet()
    for host, reqs_dict in hosts_to_reqs.items():
        if reqs_dict.get("state") == "adp":
            logging.debug("Skipping host %s because its state is 'adp'" % host)
            continue
        if reqs_dict.get("placement_group", "default") == "default":
            continue
        pg = pg_set.create_placement_group(reqs_dict["slot_type"], reqs_dict["placement_group"])
        pg.add_host(host)
        pg.add_slots(reqs_dict["num_proc"])
    
    for pg in pg_set.query_placement_groups():
        logging.info(repr(pg))
    
    return pg_set


def _job_get(job, name):
    """ Does the name exist in hard or soft resources """
    if 'hard_resources' in job and name in job['hard_resources']:
        return job['hard_resources'][name]
    elif 'soft_resources' in job and name in job['soft_resources']:
        return job['soft_resources'][name]
    else:
        return None


def assign_job_to_pg(job, pg_set, groups_enabled, max_group_backlog=1):
    logging.debug("Processing job - job_number=%s state=%s", job["job_number"], job["state"])

    updated_job_attrs = {}

    # backwards compatability
    if "affinity_group" in job['hard_resources']:
        affinity_group = job['hard_resources'].pop("affinity_group")
        if "placement_group" not in job["hard_resources"]:
            job['hard_resources']["placement_group"] = affinity_group
        updated_job_attrs["affinity_group"] = None

    slot_type = _job_get(job, 'slot_type')
    if slot_type is None:
        slot_type = 'execute'
        logging.debug("Assigning default slot_type (htc) to job_number %s", job["job_number"])
        updated_job_attrs['slot_type'] = slot_type

    if not parallel_environments.is_job_candidate_for_placement(job, jetpack_config):
        logging.debug("Job %s doesn't need to be grouped.", job["job_number"])
        if "placement_group" not in job:
            updated_job_attrs["placement_group"] = "default"
        
        if not _job_get(job, 'slot_type'):
            logging.debug("Assigning default slot_type (htc) to job_number %s", job["job_number"])
            updated_job_attrs["slot_type"] = "htc"
        return updated_job_attrs
    else:
        if not _job_get(job, 'slot_type'):
            logging.debug("Assigning default slot_type (htc) to job_number %s", job["job_number"])
            updated_job_attrs["slot_type"] = "execute"

    if not groups_enabled:
        logging.debug("Job %s could be grouped but grouping is disabled.", job["job_number"])
        return updated_job_attrs

    current_pg_id = _job_get(job, 'placement_group')
    slots_requested = int(float(job['pe_range']['max']))

    # this job was already assigned, update the in memory PlacementGroup object and return.
    if current_pg_id:
        logging.debug("Job %s already has been assigned to %s", job["job_number"], current_pg_id)
        current_pg = pg_set.get_placement_group(current_pg_id)
        if current_pg:
            current_pg.request_slots(slots_requested)
            return updated_job_attrs
        else:
            if job["state"] == "pending":
                logging.warn("No hosts were found for placement group %s - unassigning job %s so it can be rescheduled.",
                        current_pg_id, job["job_number"])
                current_pg_id = None
            else:
                logging.warn("No hosts were found for placement group %s - the job %s is not in a pending state (%s) so ignoring for now.",
                        current_pg_id, job["job_number"], job["state"])
                return updated_job_attrs

    matched_pgs = pg_set.query_placement_groups(slot_type, slots_requested)
    if matched_pgs:
        ordered_by_queue_depth = sorted(matched_pgs, lambda x, y: x.queue_depth() - y.queue_depth())
        pg = ordered_by_queue_depth[0]
        if pg.queue_depth() >= max_group_backlog:
            logging.debug("All placement groups for job %s have more than %d jobs queued. Skipping.",
                            job["job_number"], max_group_backlog)
            return updated_job_attrs
        logging.debug("Assigning job_number %s (%d slots requested) to placement group %s because queue_depth %d is < max group backlog (%d)",
                            job["job_number"], slots_requested, pg.group_id, pg.queue_depth(), max_group_backlog)
        pg.request_slots(slots_requested)
        updated_job_attrs["placement_group"] = pg.group_id
    else:
        logging.debug("No placement group matched job %s for slot_type %s and slot count %d",
                        job["job_number"], slot_type, slots_requested)

    return updated_job_attrs


def _to_job_state_map(sge_jobs):
    ret = {}
    for job in sge_jobs:
        ret[job["job_number"]] = job["state"]
    return ret


def main():
    # keep the previous behavior
    print("%s" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        sys.path.append("/opt/cycle/jetpack/config")
        from autoscale import sge_job_handler  # This should always fail - "blah" should be a module name that we define for all autoscaling stuff
    except ImportError:
        sge_job_handler = assign_job_to_pg
        pass  # The default function above will be used instead

    host_complexes = sge.get_host_complexes(['slot_type', 'placement_group', 'num_proc'])
    pg_set = _build_placement_groups(host_complexes)
    
    # note - there is a race condition here if we reorder these two calls.
    # _details doesn't include state, whereas get_sge_jobs does but is missing resources.
    # Since we are merging in states into job_details and not vice versa, call get_sge_jobs first. -rdh
    jobs = sge.get_sge_jobs(parse=False)
    all_jobs = sge.get_sge_job_details(parse=False)

    job_states = _to_job_state_map(jobs.parse())
    all_jobs = all_jobs.parse()
    
    for job in all_jobs.values():
        job["state"] = job_states.get(job["job_number"], "unknown")
    
    # Check for updates to the job
    resources = {}
    tempfile.mktemp(".sh")
    groups_enabled = jetpack_config.get('cyclecloud.cluster.autoscale.use_node_groups')
    max_group_backlog = jetpack_config.get('gridengine.max_group_backlog', 1)

    # we need to process already assigned jobs before unassigned. We'll treat jobs assigned to nonexistent 
    # placement groups as unassigned for now.
    already_assigned_jobs = []
    unassigned_jobs = []
    for job in all_jobs.values():
        pg_id = _job_get(job, "placement_group")
        if pg_id and pg_set.get_placement_group(pg_id):
            already_assigned_jobs.append(job)
        else:
            unassigned_jobs.append(job)
    
    # sort by job_number
    already_assigned_jobs = sorted(already_assigned_jobs, lambda x, y: x["job_number"] - y["job_number"])
    unassigned_jobs = sorted(unassigned_jobs, lambda x, y: x["job_number"] - y["job_number"])
    
    for job in (already_assigned_jobs + unassigned_jobs):
        # where sge_job_handler == assign_job_to_pg unless overridden.
        updates = sge_job_handler(job, pg_set, groups_enabled=groups_enabled, max_group_backlog=max_group_backlog)
        updates_to_apply = {}
        # Check the updates that were sent back to make sure they aren't already set, no need to reset them
        for k, v in updates.items():
            if k in job['hard_resources'] and job['hard_resources'][k] == v:
                pass
            else:
                updates_to_apply[k] = v

        if updates_to_apply:

            # Update the job details
            job['hard_resources'].update(updates_to_apply)

            # Build up the command line and use that as a key to group up jobs with the same command
            command = ""
            for k, v in job['hard_resources'].items():
                # if it is None, we are removing it.
                if v is not None:
                    assert v is not None and v != "None"
                    command += "%s=%s," % (k, v)

            if command not in resources:
                resources[command] = []
            resources[command].append(job['job_number'])

    # For each group of updates, apply them to all jobs in bulk for speed
    for command, jobs in resources.items():
        command = ". /etc/cluster-setup.sh && qalter %s -l %s" % (",".join([str(j) for j in jobs]), command)
        logging.debug(command)
        subprocess.check_call(command, shell=True)
    
    logging.debug("Final state of placement groups after this round:")
    for pg in pg_set.query_placement_groups():
        logging.debug(str(pg))


if __name__ == "__main__":
    import jetpack.util
    pidfile = "/var/run/modify_jobs.pid"
    if jetpack.util.already_running(pidfile):
        sys.stderr.write("Modify jobs already running!\n")
        sys.exit(1)

    try:
        main()
    finally:
        # Clean up our pidfile so another instance can start
        if os.path.isfile(pidfile):
            os.unlink(pidfile)
