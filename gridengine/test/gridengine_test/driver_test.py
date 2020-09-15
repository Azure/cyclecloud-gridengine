from typing import Any, Dict, Optional

from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node import constraints

from gridengine import driver
from gridengine.allocation_rules import AllocationRule
from gridengine.complex import Complex
from gridengine.parallel_environments import new_parallel_environment
from gridengine.queue import new_gequeue


def test_custom_parser() -> None:
    qc = driver.QueueAndHostgroupConstraint
    q = qc("htc.q", ["@htc_q_mpipg0"], "htc_q_mpipg0")
    expected_dict: Dict[str, Optional[Any]] = {
        "queue-and-hostgroups": {
            "qname": "htc.q",
            "hostgroups": ["@htc_q_mpipg0"],
            "placement-group": "htc_q_mpipg0",
        }
    }
    assert q.to_dict() == expected_dict
    parsed = constraints.get_constraint(q.to_dict())
    assert parsed.qname == q.qname
    assert parsed.hostgroups_set == q.hostgroups_set
    assert parsed.hostgroups_sorted == q.hostgroups_sorted
    assert parsed.placement_group == q.placement_group

    q = qc("htc.q", ["@htc.q", "@another"], None)
    expected_dict = {
        "queue-and-hostgroups": {
            "qname": "htc.q",
            "hostgroups": ["@another", "@htc.q"],  # sort the hostgroups for consistency
            "placement-group": None,
        }
    }
    assert q.to_dict() == expected_dict
    parsed = constraints.get_constraint(q.to_dict())
    assert parsed.qname == q.qname
    assert parsed.hostgroups_set == q.hostgroups_set
    assert parsed.hostgroups_sorted == q.hostgroups_sorted
    assert parsed.placement_group == q.placement_group

    node = SchedulerNode(
        "tux",
        {"_gridengine_qname": q.qname, "_gridengine_hostgroups": q.hostgroups_sorted},
    )
    assert q.satisfied_by_node(node)
    assert q.do_decrement(node)

    node = SchedulerNode(
        "tux",
        {"_gridengine_qname": q.qname, "_gridengine_hostgroups": q.hostgroups_sorted},
    )
    node.placement_group = "pg0"
    assert not q.satisfied_by_node(node)

    node = SchedulerNode("tux", {})
    node.exists = True
    assert not q.satisfied_by_node(node)

    node.exists = False
    assert q.satisfied_by_node(node)
    assert q.do_decrement(node)
    assert node.available["_gridengine_qname"] == q.qname
    assert node.available["_gridengine_hostgroups"] == q.hostgroups_sorted
    assert node.software_configuration["gridengine_qname"] == q.qname
    assert node.software_configuration["gridengine_hostgroups"] == " ".join(
        q.hostgroups_sorted
    )


def test_preprocess_configs() -> None:
    pes = {
        "mpi": new_parallel_environment(
            "mpi", 999, AllocationRule.value_of("$round_robin")
        ),
        "mpislots": new_parallel_environment(
            "mpislots", 999, AllocationRule.value_of("$fill_up")
        ),
        "smp": new_parallel_environment(
            "smp", 999, AllocationRule.value_of("$pe_slots")
        ),
        "single": new_parallel_environment("single", 999, AllocationRule.value_of("1")),
    }
    complexes = {"slots": Complex("slots", "s", "int", "<=", True, True, "", 0)}
    gequeues = {
        "hpc.q": new_gequeue(
            "hpc.q",
            "@hpc",
            ["mpi", "smp", "single"],
            [],
            complexes=complexes,
            parallel_envs=pes,
        )
    }
    d = driver.new_driver({}, [], [], pes, gequeues)

    # If the user did not define any placement groups, define the defaults.
    assert {
        "nodearrays": {"default": {"placement_groups": ["hpc_q_mpi", "hpc_q_single"]}}
    } == d.preprocess_config({})

    # if they did define defaults, make no changes
    custom_config = {
        "nodearrays": {"default": {"placement_groups": ["hpc_q_mpi_CUSTOM"]}}
    }
    assert custom_config == d.preprocess_config(custom_config)
