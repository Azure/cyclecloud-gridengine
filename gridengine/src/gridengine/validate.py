import re
from typing import Callable, Dict, List, Set

from hpc.autoscale.node.node import Node

from gridengine import autoscaler
from gridengine.parallel_environments import GridEngineQueue

UNBOLD = "\033[0m"
BOLD = "\033[1m"


def bold(s: str) -> str:
    return BOLD + s + UNBOLD


WarnFunction = Callable[..., None]


def validate_ht_hostgroup(queue: GridEngineQueue, warn_function: WarnFunction) -> bool:
    if queue.get_hostgroups_for_ht():
        return True
    warn = warn_function
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


def validate_nodes(config: Dict, warn: WarnFunction) -> bool:
    failure = False

    dcalc = autoscaler.new_demand_calculator(config)
    pg_to_hg: Dict[str, Dict[str, List[Node]]] = {}

    for node in dcalc.node_mgr.get_nodes():
        qname_expr = node.software_configuration.get("gridengine_hostgroups", "")
        if not qname_expr:
            failure = True
            warn(
                "%s does not define gridengine_qname defined and will not be able to join the cluster.",
                node,
            )

        hg_expr = node.software_configuration.get("gridengine_hostgroups", "")
        if not hg_expr:
            failure = True
            warn(
                "%s does not define gridengine_hostgroups and will not be able to join the cluster.",
                node,
            )

        if not node.placement_group:
            continue

        if node.placement_group not in pg_to_hg:
            pg_to_hg[node.placement_group] = {}

        hostgroups = re.split(",|[ \t]+", hg_expr)
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
