from hpc.autoscale.job.schedulernode import SchedulerNode

from gridengine.environment import GridEngineEnvironment
from gridengine.qbin import QBinImpl
from gridengine.scheduler import GridEngineScheduler


def setup_module() -> None:
    SchedulerNode.ignore_hostnames = True


def test_add_remove_nodes() -> None:
    scheduler = GridEngineScheduler({}, "localhost")
    qbin = QBinImpl(is_uge=True)
    ge_env = GridEngineEnvironment(scheduler, qbin=qbin)

    # should have set like semantics for adding/removing
    ge_env.add_node(SchedulerNode("tux"))
    assert len(ge_env.nodes) == 1
    assert ge_env.current_hostnames == ["tux"]

    ge_env.add_node(SchedulerNode("tux"))
    assert len(ge_env.nodes) == 1
    assert ge_env.current_hostnames == ["tux"]

    ge_env.delete_node(SchedulerNode("tux"))
    assert len(ge_env.nodes) == 0
    assert ge_env.current_hostnames == []

    # add remove two nodes
    ge_env.add_node(SchedulerNode("tux1"))
    ge_env.add_node(SchedulerNode("tux2"))
    assert len(ge_env.nodes) == 2
    assert sorted(ge_env.current_hostnames) == sorted(["tux1", "tux2"])

    ge_env.delete_node(SchedulerNode("tux1"))
    ge_env.delete_node(SchedulerNode("tux2"))
    assert len(ge_env.nodes) == 0
    assert sorted(ge_env.current_hostnames) == sorted([])
