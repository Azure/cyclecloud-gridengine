# for backwards compatability, this is a global var instead of an argument
# to sge_job_handler.
import logging
import re
import subprocess


def get_pe(pe_name):
    _build_parellel_envs()
    return _PARALLEL_ENVS.get(pe_name, {})


def set_pe(pe_name, pe):
    _PARALLEL_ENVS[pe_name] = pe


_PARALLEL_ENVS = {}


def _build_parellel_envs():
    if _PARALLEL_ENVS:
        return _PARALLEL_ENVS

    pe_names = subprocess.check_output(["qconf", "-spl"]).splitlines()
    for pe_name in pe_names:
        pe_name = pe_name.strip()
        pe = {}

        lines = subprocess.check_output(["qconf", "-sp", pe_name]).splitlines(False)
        for line in lines:
            toks = re.split(r'[ \t]+', line, 1)
            if len(toks) != 2:
                continue
            pe[toks[0].strip()] = toks[1].strip()
        set_pe(pe_name, pe)


def _use_placement_group(jetpack_config, pe):
    try:
        key = "gridengine.parallel_environment.%s.use_placement_groups" % pe
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

    pe = job_detail.get("pe")
    # likely impossible this is ever undefined if pe_range is defined.
    if not pe:
        raise ValueError("pe is undefined for job %s" % job_detail)

    allocation_rule = get_pe(pe).get("allocation_rule")

    if allocation_rule == "$pe_slots":
        # I don't care what the user said, this job will only run on one machine
        return False

    if allocation_rule.isdigit():
        # allocation_rule of 1 means each host will only get 1 processor.
        # so we have to allocate one VM per task... HOWEVER, we can't 
        # autoscale correctly, so the only way to use this correctly
        # is to manually add  nodes
        return False
    
    user_defined_default = _use_placement_group(jetpack_config, "default")
    user_defined_pe = _use_placement_group(jetpack_config, pe)
    
    if user_defined_pe is not None:
        return user_defined_pe
    
    if user_defined_default is not None:
        return user_defined_default
    
    return True


def derive_default(pe):
    '''
    We aren't actually calling this function, but the logic is reasonable for 
    figuring out whether or not these jobs require placement groups so I
    am leaving it here in case we change our mind.
    '''
    allocation_rule = get_pe(pe).get("allocation_rule")
    
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
