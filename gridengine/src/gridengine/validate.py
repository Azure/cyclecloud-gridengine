from typing import Callable, Dict, List, Set

from hpc.autoscale.job.demandcalculator import DemandCalculator
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodemanager import NodeManager
from hpc.autoscale.util import partition_single

from gridengine.environment import GridEngineEnvironment
from gridengine.qbin import QBin
from gridengine.queue import GridEngineQueue
from gridengine.util import get_node_hostgroups

UNBOLD = "\033[0m"
BOLD = "\033[1m"


def bold(s: str) -> str:
    return BOLD + s + UNBOLD


WarnFunction = Callable[..., None]


def validate_queue_has_hosts(
    queue: GridEngineQueue, qbin: QBin, warn_function: WarnFunction
) -> bool:
    # ensure that there is at least one host for each queue.
    empty_hostgroups = []
    for hostgroup in queue.hostlist:
        if hostgroup == "NONE":
            continue
        if not hostgroup.startswith("@"):
            # explicit hostnames are obviously
            return True

        if qbin.qconf(["-shgrp_resolved", hostgroup]).strip():
            return True
        empty_hostgroups.append(hostgroup)

    warn_function(
        "Queue %s has no hosts assigned to any of its hostgroups and you will not be able to submit to it.",
        queue.qname,
    )
    warn_function("    Empty hostgroups = %s", " ".join(empty_hostgroups))
    return False


def validate_hg_intersections(
    ge_env: GridEngineEnvironment, node_mgr: NodeManager, warn_function: WarnFunction
) -> bool:
    bucket_to_hgs: Dict[str, Set[str]] = {}
    for bucket in node_mgr.get_buckets():
        if bucket.bucket_id not in bucket_to_hgs:
            bucket_to_hgs[str(bucket)] = set()

    by_str = partition_single(node_mgr.get_buckets(), str)

    for queue in ge_env.queues.values():
        if not queue.autoscale_enabled:
            continue

        for hostgroup in queue.bound_hostgroups.values():
            for bucket in node_mgr.get_buckets():
                is_satisfied = True
                for constraint in hostgroup.constraints:
                    result = constraint.satisfied_by_bucket(bucket)
                    if not result:
                        is_satisfied = False
                        break
                if is_satisfied:
                    bucket_to_hgs[str(bucket)].add(hostgroup.name)

    failure = False
    for bkey, matches in bucket_to_hgs.items():
        bucket = by_str[bkey]
        if not matches:
            warn_function(
                "%s is not matched by any hostgroup. This is not an error.", bucket,
            )
        elif len(matches) > 1:
            # seq_no will be used to determine ties
            if not ge_env.scheduler.sort_by_seqno:
                warn_function(
                    "%s is matched by more than one hostgroup %s. This is not an error.",
                    bucket,
                    ",".join(matches),
                )
    return failure


def validate_ht_hostgroup(
    queue: GridEngineQueue, ge_env: GridEngineEnvironment, warn_function: WarnFunction
) -> bool:
    warn = warn_function

    ht_groups = queue.get_hostgroups_for_ht()
    if len(ht_groups) == 1:
        return True

    if len(ht_groups) > 1:
        if ge_env.scheduler.sort_by_seqno:
            return True
        warn(
            "Queue %s has multiple hostgroups that will map to non-pe jobs.",
            queue.qname,
        )
        if ge_env.is_uge:
            warn(
                "  Set weight_queue_host_sort < weight_queue_seqno in qconf -msconf to enable"
            )
        else:
            warn("  Set queue_sort_method to seqno in qconf -msconf to enable")
        warn("  sorting by seq_no to deal with this ambiguity.")
        return False

    warn(
        "Queue %s has no hostgroup in its hostlist that is not associated "
        + "with a multi-node parallel environment (excluding SMP/$pe_slots)",
        queue.qname,
    )

    for pe in queue.get_pes("*"):
        if pe.requires_placement_groups:
            pe_hostgroups = queue.get_hostgroups_for_pe(pe.name)
            pe_hostgroups = sorted(pe_hostgroups)
            pe_hg_expr = " ".join(pe_hostgroups)
            warn("           pe=%-15s hostlist=%s", pe.name, pe_hg_expr)
            if "@allhosts" in pe_hostgroups:
                warn(
                    "           ^ This pe uses @allhosts and has an allocation_rule of %s.",
                    pe.allocation_rule.name,
                )
                warn(
                    "             If you need to disable placement groups for this pe, add the following to your autoscale.json"
                )
                warn(
                    '             {"gridengine": {"pes": {"%s": {"requires_placement_groups": false}}}}',
                    pe.name,
                )
    return False


def validate_pe_hostgroups(queue: GridEngineQueue, warn_function: WarnFunction) -> bool:
    # TODO
    return True


def validate_nodes(config: Dict, dcalc: DemandCalculator, warn: WarnFunction) -> bool:
    failure = False

    pg_to_hg: Dict[str, Dict[str, List[Node]]] = {}

    for node in dcalc.node_mgr.get_nodes():
        hostgroups = get_node_hostgroups(node)
        if not hostgroups:
            failure = True
            warn(
                "%s does not define gridengine_hostgroups and will not be able to join the cluster.",
                node,
            )

        if not node.placement_group:
            continue

        if node.placement_group not in pg_to_hg:
            pg_to_hg[node.placement_group] = {}

        for hg in hostgroups:

            if hg not in pg_to_hg[node.placement_group]:
                pg_to_hg[node.placement_group][hg] = []

            pg_to_hg[node.placement_group][hg].append(node)

    duplicates: Dict[str, List[str]] = {}
    processed: Set[str] = set()
    for pg_name, hg_dict in pg_to_hg.items():
        for hg, node_list in hg_dict.items():
            if hg in processed:
                failure = True
                duplicates[hg].append(pg_name)
        processed.add(hg)
        if hg not in duplicates:
            # ideal case is they are all size 1 lists
            duplicates[hg] = [pg_name]

    warned_longform = False
    for pg_name, multiple_hgs in duplicates.items():
        if len(duplicates[pg_name]) <= 1:
            continue
        warn(
            "Found nodes from multiple placement_groups that will join the same hostgroup: "
            + bold("%s=%s"),
            pg_name,
            ",".join(multiple_hgs),
        )
        if not warned_longform:
            warn(
                "    Any job that relies on colocation must either specify "
                + bold("-l placement_group-[one of the above]")
            )
            warn(
                "    or update their queue config ("
                + bold("qconf -mq")
                + ") to point the relevant parallel environments"
            )
            warn("    to a different hostgroup.")

            warned_longform = True

        warn("    Affected nodes:")
        for bad_hg in multiple_hgs:
            for bad_pg in pg_to_hg[bad_hg]:
                names = ",".join(
                    ["%s(%s)" % (x.name, x.hostname) for x in pg_to_hg[bad_hg][bad_pg]]
                )
                warn("        %s = %s", bad_pg, names)

    return not failure
