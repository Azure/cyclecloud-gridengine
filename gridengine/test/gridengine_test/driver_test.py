from typing import Any, Dict, Optional

import pytest
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node import constraints
from hpc.autoscale.node.constraints import XOr

from gridengine import driver, util
from gridengine.hostgroup import BoundHostgroup, Hostgroup
from gridengine_test.autoscaler_test import common_ge_env


def setup_module() -> None:
    SchedulerNode.ignore_hostnames = True


@pytest.mark.skip
def test_custom_parser() -> None:
    qc = driver.HostgroupConstraint
    hg = Hostgroup("@htc_q_mpipg0", {"node.nodearray": "htc"})
    q = qc(hg, hg.name.replace("@", "pg0"))
    expected_dict: Dict[str, Optional[Any]] = {
        "hostgroups-and-pg": {
            "hostgroups": ["@htc_q_mpipg0"],
            "placement-group": "pg0",
        }
    }
    assert q.to_dict() == expected_dict
    parsed = constraints.get_constraint(q.to_dict())
    assert parsed.hostgroups_set == q.hostgroups_set
    assert parsed.hostgroups_sorted == q.hostgroups_sorted
    assert parsed.placement_group == q.placement_group

    q = qc("htc.q", ["@htc.q", "@another"], None)
    expected_dict = {
        "hostgroups-and-pg": {
            "hostgroups": ["@another", "@htc.q"],  # sort the hostgroups for consistency
            "placement-group": None,
        }
    }
    assert q.to_dict() == expected_dict
    parsed = constraints.get_constraint(q.to_dict())
    assert parsed.hostgroups_set == q.hostgroups_set
    assert parsed.hostgroups_sorted == q.hostgroups_sorted
    assert parsed.placement_group == q.placement_group

    node = SchedulerNode("tux", {"_gridengine_hostgroups": q.hostgroups_sorted},)
    assert q.satisfied_by_node(node)
    assert q.do_decrement(node)

    node = SchedulerNode("tux", {"_gridengine_hostgroups": q.hostgroups_sorted},)
    node.placement_group = "pg0"
    assert not q.satisfied_by_node(node)

    node = SchedulerNode("tux", {})
    node.exists = True
    assert not q.satisfied_by_node(node)

    node.exists = False
    assert q.satisfied_by_node(node)
    assert q.do_decrement(node)
    assert node.available["_gridengine_hostgroups"] == q.hostgroups_sorted
    assert node.software_configuration["gridengine_hostgroups"] == " ".join(
        q.hostgroups_sorted
    )


def test_preprocess_configs() -> None:
    ge_env = common_ge_env()

    d = driver.new_driver({}, ge_env)

    # If the user did not define any placement groups, define the defaults.
    pgs = [
        b.name.replace(".", "_").replace("@", "")
        for b in ge_env.queues["hpc.q"].bound_hostgroups.values()
    ]
    assert {
        "nodearrays": {"default": {"placement_groups": pgs}}
    } == d.preprocess_config({})

    # if they did define defaults, make no changes
    custom_config = {
        "nodearrays": {"default": {"placement_groups": ["hpc_q_mpi_CUSTOM"]}}
    }
    assert custom_config == d.preprocess_config(custom_config)


def test_hostgroup_constraint() -> None:
    ge_env = common_ge_env()
    hostgroup = Hostgroup("@htc.q", {}, members=["tux42"])
    bound = BoundHostgroup(ge_env.queues["htc.q"], hostgroup)
    cons = driver.HostgroupConstraint(bound, "pg1")

    def new_node(
        pg: Optional[str] = None,
        hostname: str = "tux1",
        hostgroup: Optional[Hostgroup] = None,
    ) -> SchedulerNode:
        node = SchedulerNode(hostname, {"slot_type": "highmem"})

        if pg:
            node.placement_group = pg

        if hostgroup:
            util.add_node_to_hostgroup(node, hostgroup)

        return node

    # node is not in a pg
    result = cons.satisfied_by_node(new_node(None, "tux42", hostgroup))
    assert not result
    assert result.status == "WrongPlacementGroup"
    # wrong pg
    result = cons.satisfied_by_node(new_node("pg2", "tux42", hostgroup))
    assert not result
    assert result.status == "WrongPlacementGroup"
    # not in the hostgroup
    result = cons.satisfied_by_node(new_node("pg1", "tux42"))
    assert not result
    assert result.status == "WrongHostgroup"
    # happy path
    assert cons.satisfied_by_node(new_node("pg1", "tux42", hostgroup))

    # reject this because node.nodearray != lowmem
    hostgroup = Hostgroup("@hg1", {"slot_type": "lowmem"}, members=["tux42"])
    bound = BoundHostgroup(ge_env.queues["htc.q"], hostgroup)
    cons = driver.HostgroupConstraint(bound, "pg1")
    result = cons.satisfied_by_node(new_node("pg1", "tux42", hostgroup))
    assert not result
    assert result.status == "InvalidOption"

    hostgroup = Hostgroup("@hg1", {"slot_type": "highmem"}, members=["tux42"])
    bound = BoundHostgroup(ge_env.queues["htc.q"], hostgroup)
    cons = driver.HostgroupConstraint(bound, "pg1")
    assert cons.satisfied_by_node(new_node("pg1", "tux42", hostgroup))

    cons_list = []
    for pg in ["pg1", "pg2", "pg3"]:
        hostgroup = Hostgroup("@" + pg, {"slot_type": "highmem"}, members=["tux42"])
        bound = BoundHostgroup(ge_env.queues["htc.q"], hostgroup)
        cons = driver.HostgroupConstraint(bound, pg)
        cons_list.append(cons)

    pg1_hostgroup = Hostgroup("@pg1", {"slot_type": "highmem"}, members=["tux42"])
    assert XOr(*cons_list).satisfied_by_node(new_node("pg1", "tux42", pg1_hostgroup))
    pg2_hostgroup = Hostgroup("@pg2", {"slot_type": "highmem"}, members=["tux42"])
    assert XOr(*cons_list).satisfied_by_node(new_node("pg2", "tux42", pg2_hostgroup))
    pg3_hostgroup = Hostgroup("@pg3", {"slot_type": "highmem"}, members=["tux42"])
    assert XOr(*cons_list).satisfied_by_node(new_node("pg3", "tux42", pg3_hostgroup))
    pg4_hostgroup = Hostgroup("@pg4", {"slot_type": "highmem"}, members=["tux42"])
    assert not XOr(*cons_list).satisfied_by_node(
        new_node("pg4", "tux42", pg4_hostgroup)
    )
