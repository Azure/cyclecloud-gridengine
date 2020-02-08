#!/usr/bin/env python

from sge import get_sge_jobs, get_sge_job_details
import jetpack.config
import parallel_environments
import sys
import os

def _get_jobs():
    parallel_environments._build_parellel_envs()

    # Get the results of the two queries before spending time parsing them to limit any race conditions with jobs coming and going
    jobs = get_sge_jobs(parse=False)
    job_details = get_sge_job_details(parse=False)

    # Parse the results, depending on the number of jobs this could take a while    
    jobs = jobs.parse()
    job_details = job_details.parse()

    # Process all the jobs
    autoscale_jobs = []
    
    groups_enabled = jetpack.config.get('cyclecloud.cluster.autoscale.use_node_groups') is True
    
    warned_about_allocation_rule = False

    pe_counts = {}

    for job in jobs:

        # Ignore jobs in "held" or "error" states
        if "h" in job["job_state"] or "e" in job["job_state"]:
            continue
        
        # If we don't have any job details we can't really process this job
        if job["job_number"] not in job_details:
            continue

        detail = job_details[job["job_number"]]
        
        pe_name = detail.get("pe")
        
        if pe_name and pe_name not in pe_counts:
            pe_counts[pe_name] = 0
        
        pe = parallel_environments.get_pe(pe_name) or {}
        try:
            if pe.get("allocation_rule").isdigit():
                if not warned_about_allocation_rule:
                    print ("autostart ignores parallel environments with integer allocation_rules -" +
                                    ": pe=%s allocation_rule=%s") % (detail.get("pe"), pe.get("allocation_rule"))
                    warned_about_allocation_rule = True
                continue
        except:
            pass

        slot_type = None
        if 'hard_resources' in detail:
            slot_type = detail["hard_resources"].get("slot_type", None)

        slots_per_job = 1
        if 'pe_range' in detail and 'max' in detail['pe_range']:
            slots_per_job = int(detail['pe_range']['max'])

        average_runtime = None
        if 'context' in detail and 'average_runtime' in detail['context']:
            average_runtime = int(detail['context']['average_runtime'])
        
        if pe_name:
            pe_max_slots = int(pe.get("slots", 0))
            
            if slots_per_job > pe_max_slots:
                print "Ignoring job %s - requesting more slots than pe %s allows" % (job["job_number"], pe_name)
                continue
            
            if pe_max_slots > 0:
                new_count = min(pe_max_slots, pe_counts[pe_name] + slots_per_job)
                slots_per_job = new_count - pe_counts[pe_name]

        autoscale_job = {
            'name': job['job_number'],
            'nodearray': slot_type,
            'request_cpus': slots_per_job,
            'average_runtime': average_runtime
        }

        # If it's an MPI job and grouping is enabled
        # we want to use a grouped request to get tightly coupled nodes
        if slots_per_job > 1 and groups_enabled:
            # sorts out whether this is actually an MPI like job or SMP like job.
            autoscale_job['grouped'] = parallel_environments.is_job_candidate_for_placement(detail, jetpack.config)

        autoscale_jobs.append(autoscale_job)

    return autoscale_jobs


def autoscale():
    from jetpack import autoscale
    jobs = _get_jobs()
    
    array_definitions = {}
    for job in jobs:
        nodearray = job.get("nodearray") or "execute"
        if job["nodearray"] not in array_definitions:
            array_definitions[nodearray + ":grouped"] = {"Name": nodearray + ":grouped",
                                                                "Configuration": {
                                                                    "gridengine": {
                                                                        "is_grouped": True}}}
            
    autoscale.scale_by_jobs(jobs, array_definitions.values())


if __name__ == "__main__":
    import jetpack.util
    
    pidfile = "/var/run/autostart.pid"
    if jetpack.util.already_running(pidfile):
        sys.stderr.write("Autostart already running!\n")
        sys.exit(1)
    
    try:
        autoscale()
    finally:
        # Clean up our pidfile so another instance can start
        if os.path.isfile(pidfile):
            os.unlink(pidfile)
