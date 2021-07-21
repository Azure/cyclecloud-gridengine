from subprocess import CalledProcessError
from typing import Any, Dict, Optional
from unittest import mock

import pytest
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node import constraints
from hpc.autoscale.node.constraints import XOr

from gridengine import driver, util
from gridengine.driver import GridEngineDriver
from gridengine.hostgroup import BoundHostgroup, Hostgroup
from gridengine.util import json_dump
from gridengine_test.autoscaler_test import common_ge_env


def setup_module() -> None:
    SchedulerNode.ignore_hostnames = True


@pytest.mark.skip
def test_custom_parser() -> None:
    ge_env = common_ge_env()
    qc = driver.HostgroupConstraint
    hg = Hostgroup("@htc_q_mpipg0", {"node.nodearray": "htc"})
    bhg = BoundHostgroup(ge_env.queues["htc.q"], hg, 0)
    q = qc(bhg, bhg.name.replace("@", "pg0"))
    json_dump(q.to_dict())
    expected_dict: Dict[str, Optional[Any]] = {
        "hostgroup-and-pg": {
            "hostgroup": "@htc_q_mpipg0",
            "user": None,
            "project": None,
            "placement-group": "pg0htc_q_mpipg0",
            "seq-no": 0,
            "constraints": [{"nodearray": ["htc"]}],
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
        "nodearrays": {"default": {"placement_groups": pgs}},
        "default_resources": [],
    } == d.preprocess_config({})

    # if they did define defaults, make no changes
    custom_config = {
        "nodearrays": {"default": {"placement_groups": ["hpc_q_mpi_CUSTOM"]}},
        "default_resources": [],
    }
    assert custom_config == d.preprocess_config(custom_config)

    # ensure we handle duplicating shortcut or long form for the user.
    assert {
        "nodearrays": {"default": {"placement_groups": pgs}},
        "default_resources": [
            {"name": "s", "select": {}, "value": 1},
            {"name": "slots", "select": {}, "value": 1},
        ],
    } == d.preprocess_config(
        {"default_resources": [{"name": "s", "select": {}, "value": 1}]}
    )

    assert {
        "nodearrays": {"default": {"placement_groups": pgs}},
        "default_resources": [
            {"name": "slots", "select": {}, "value": 1},
            {"name": "s", "select": {}, "value": 1},
        ],
    } == d.preprocess_config(
        {"default_resources": [{"name": "slots", "select": {}, "value": 1}]}
    )

    # ensure that ccnodeid is appended to relevant_complexes by default
    # note: if relevant_complexes is not defined, then every complex is 'relevant'
    # so no need to add it (and it would in fact break things)
    assert {
        "nodearrays": {"default": {"placement_groups": pgs}},
        "default_resources": [
            {"name": "slots", "select": {}, "value": 1},
            {"name": "s", "select": {}, "value": 1},
        ],
        "gridengine": {"relevant_complexes": ["slots", "ccnodeid"]},
    } == d.preprocess_config(
        {
            "default_resources": [{"name": "slots", "select": {}, "value": 1}],
            "gridengine": {"relevant_complexes": ["slots"]},
        }
    )


def test_hostgroup_constraint() -> None:
    ge_env = common_ge_env()
    hostgroup = Hostgroup("@htc.q", {}, members=["tux42"])
    bound = BoundHostgroup(ge_env.queues["htc.q"], hostgroup, 0)
    cons = driver.HostgroupConstraint({}, bound, "pg1")

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
    bound = BoundHostgroup(ge_env.queues["htc.q"], hostgroup, 0)
    cons = driver.HostgroupConstraint({}, bound, "pg1")
    result = cons.satisfied_by_node(new_node("pg1", "tux42", hostgroup))
    assert not result
    assert result.status == "InvalidOption"

    hostgroup = Hostgroup("@hg1", {"slot_type": "highmem"}, members=["tux42"])
    bound = BoundHostgroup(ge_env.queues["htc.q"], hostgroup, 0)
    cons = driver.HostgroupConstraint({}, bound, "pg1")
    assert cons.satisfied_by_node(new_node("pg1", "tux42", hostgroup))

    cons_list = []
    for pg in ["pg1", "pg2", "pg3"]:
        hostgroup = Hostgroup("@" + pg, {"slot_type": "highmem"}, members=["tux42"])
        bound = BoundHostgroup(ge_env.queues["htc.q"], hostgroup, 0)
        cons = driver.HostgroupConstraint({}, bound, pg)
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


def test_initialize() -> None:
    ge_env = common_ge_env()

    # ok - make sure we propagate an unknown error
    ge_env.qbin.qconf = mock.MagicMock(
        ["-sce", "ccnodeid"],
        side_effect=CalledProcessError(
            1, cmd=["-sce", "ccnodeid"], output="Unknown error".encode()
        ),
    )
    ge_driver = GridEngineDriver({}, ge_env)
    try:
        ge_driver.initialize_environment()
    except CalledProcessError as e:
        assert e.stdout.decode() == "Unknown error"
    ge_env.qbin.qconf.assert_has_calls([mock.call(["-sss"]), mock.call(["-sc"])])

    # ok - make sure we propagate an unknown error
    ge_env.qbin.qconf = mock.MagicMock(["-sc"], return_value="",)
    ge_driver = GridEngineDriver({"read_only": True}, ge_env)
    ge_driver.initialize_environment()

    # now it does exist
    ge_env.qbin.qconf = mock.MagicMock(return_value="ccnodeid ccnodeid ...")
    ge_driver = GridEngineDriver({}, ge_env)
    ge_driver.initialize_environment()
    # ge_env.qbin.qconf.assert_called_once()

    # TODO I can't figure out how to make this throw
    # an exception the first call but not the next
    # now it does not exist, so we will be created

    class FakeQConf:
        def __init__(self) -> None:
            self.call_count = 0

        def __call__(self, args):  # type: ignore
            self.call_count += 1
            if args == ["-sss"]:
                assert self.call_count == 1
                return ""
            if args == ["-sc"]:
                assert self.call_count == 2
                return ""
            elif args[0] == "-Ace":
                assert self.call_count == 3
                return ""
            else:
                raise AssertionError("Unexpected call {}".format(args))

    ge_env.qbin.qconf = FakeQConf()
    ge_driver = GridEngineDriver({}, ge_env)
    ge_driver.initialize_environment()
