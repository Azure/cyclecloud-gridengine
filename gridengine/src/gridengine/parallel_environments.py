# for backwards compatability, this is a global var instead of an argument
# to sge_job_handler.
import logging
import os
import re
import subprocess

QCONF_PATH = os.path.expanduser("~/bin/qconf")

_PE_CACHE = {'make': {'pe_name': 'make', 'slots': '999', 'user_lists': 'NONE', 'xuser_lists': 'NONE', 'start_proc_args': 'NONE', 'stop_proc_args': 'NONE', 'allocation_rule': '$round_robin', 'control_slaves': 'TRUE', 'job_is_first_task': 'FALSE', 'urgency_slots': 'min', 'accounting_summary': 'TRUE'}, 'mpi': {'pe_name': 'mpi', 'slots': '999', 'user_lists': 'NONE', 'xuser_lists': 'NONE', 'start_proc_args': '/sched/sge/sge-2011.11/mpi/startmpi.sh -unique $pe_hostfile', 'stop_proc_args': '/sched/sge/sge-2011.11/mpi/stopmpi.sh', 'allocation_rule': '$fill_up', 'control_slaves': 'TRUE', 'job_is_first_task': 'FALSE', 'urgency_slots': 'min', 'accounting_summary': 'FALSE'}, 'mpislots': {'pe_name': 'mpislots', 'slots': '999', 'user_lists': 'NONE', 'xuser_lists': 'NONE', 'start_proc_args': '/bin/true', 'stop_proc_args': '/bin/true', 'allocation_rule': '$round_robin', 'control_slaves': 'TRUE', 'job_is_first_task': 'FALSE', 'urgency_slots': 'min', 'accounting_summary': 'FALSE'}, 'smpslots': {'pe_name': 'smpslots', 'slots': '999', 'user_lists': 'NONE', 'xuser_lists': 'NONE', 'start_proc_args': '/bin/true', 'stop_proc_args': '/bin/true', 'allocation_rule': '$pe_slots', 'control_slaves': 'FALSE', 'job_is_first_task': 'TRUE', 'urgency_slots': 'min', 'accounting_summary': 'FALSE'}}


def set_pe(pe_name, pe):
    global _PE_CACHE
    if _PE_CACHE is None:
        _PE_CACHE = {}
    _PE_CACHE[pe_name] = pe


def build_parellel_envs():
    global _PE_CACHE
    if _PE_CACHE:
        return _PE_CACHE
    
    parallel_envs = {}
    
    pe_names = subprocess.check_output([QCONF_PATH, "-spl"]).decode().splitlines()
    for pe_name in pe_names:
        pe_name = pe_name.strip()
        pe = {}

        lines = subprocess.check_output([QCONF_PATH, "-sp", pe_name]).decode().splitlines(False)
        for line in lines:
            toks = re.split(r'[ \t]+', line, 1)
            if len(toks) != 2:
                continue
            pe[toks[0].strip()] = toks[1].strip()
        parallel_envs[pe_name] = pe
    
    _PE_CACHE = parallel_envs
    print(parallel_envs)
    return parallel_envs


def _use_placement_group(jetpack_config, pe_name):
    try:
        key = "gridengine.parallel_environment.%s.use_placement_groups" % pe_name
        value = jetpack_config.get(key)
        if value in [True, False]:
            return value
        if value is not None:
            logging.warn("Expected a boolean for %s" % key)
        return None
    except:
        return None


def is_job_candidate_for_placement(job_detail, jetpack_config):
    # not a parallel job
    if "pe_range" not in job_detail or "max" not in job_detail["pe_range"]:
        return False

    pe_name = job_detail.get("pe")
    # likely impossible this is ever undefined if pe_range is defined.
    if not pe_name:
        return False
    
    pe = build_parellel_envs().get(pe_name)
    if not pe:
        logging.error("Unknown parallel environment %s" % pe_name)
        # TODO should raise an exception that says skip this job
        return False
    
    allocation_rule = pe.get("allocation_rule")

    if allocation_rule == "$pe_slots":
        # I don't care what the user said, this job will only run on one machine
        return False
    
    user_defined_default = _use_placement_group(jetpack_config, "default")
    user_defined_pe = _use_placement_group(jetpack_config, pe_name)
    
    if user_defined_pe is not None:
        return user_defined_pe
    
    if user_defined_default is not None:
        return user_defined_default
    
    return True


def derive_default(pe, parallel_environments):
    '''
    We aren't actually calling this function, but the logic is reasonable for 
    figuring out whether or not these jobs require placement groups so I
    am leaving it here in case we change our mind.
    '''
    allocation_rule = parallel_environments[pe].get("allocation_rule")
    
    # not sure this can ever be undefined.
    if not allocation_rule:
        return False
    
    # $pe_slots means the entire process has to run on one machine
    # $fill_up means what it sounds like, fill up a host and
    # keep spilling over to the next host until it is satisfied.
    if allocation_rule in ["$fill_up", "$pe_slots"]:
        return False
       
    # all that is left is $round_robin or an integer of 1-pe_slots
    if allocation_rule == "$round_robin":
        return True
    try:
        return int(allocation_rule) > 0
    except ValueError:
        logging.warn("Don't know how to handle allocation_rule %s. Assuming this is not an MPI like job." % allocation_rule)
        return False
