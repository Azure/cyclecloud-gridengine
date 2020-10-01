from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.constraints import And, Never

from gridengine.hostgroup import BoundHostgroup
from gridengine_test.autoscaler_test import common_ge_env


def setup_module() -> None:
    SchedulerNode.ignore_hostnames = True


def test_projects() -> None:
    ge_env = common_ge_env()
    hpc_q = ge_env.queues["hpc.q"]
    # make sure common_ge_env didn't add projects
    assert not hpc_q.projects
    assert not hpc_q.xprojects

    # this may seem odd, but this is how these become expressed
    # i.e. this hostgroup can run prj1 and prj2, but not prj2
    hpc_q.projects["@hpc.q_rr0"] = ["prj1", "prj2"]
    hpc_q.xprojects["@hpc.q_rr0"] = ["prj2"]
    hg = ge_env.hostgroups["@hpc.q_rr0"]
    bh = BoundHostgroup(hpc_q, hg, 0)

    # no project, ben and random should never succeed
    # yes - GridEngine will NOT schedule a job if a project is not defined
    assert isinstance(bh.make_constraint(project=None), Never)
    assert isinstance(bh.make_constraint(project="prj2"), Never)
    assert isinstance(bh.make_constraint(project="random"), Never)

    # ok, the real constraint - project==prj1
    prj1_cons = bh.make_constraint(project="prj1")
    assert isinstance(prj1_cons, And)

    node = SchedulerNode("tux")
    node._Node__nodearray = "hpc"
    assert prj1_cons.satisfied_by_node(node)


def test_users() -> None:
    ge_env = common_ge_env()
    hpc_q = ge_env.queues["hpc.q"]
    # make sure common_ge_env didn't add users
    assert not hpc_q.user_lists
    assert not hpc_q.xuser_lists

    # this may seem odd, but this is how these become expressed
    # i.e. this hostgroup can run users ryan and ben, but not ben
    hpc_q.user_lists["@hpc.q_rr0"] = ["ryan", "ben"]
    hpc_q.xuser_lists["@hpc.q_rr0"] = ["ben"]
    hg = ge_env.hostgroups["@hpc.q_rr0"]
    bh = BoundHostgroup(hpc_q, hg, 0)

    # no user, ben and random should never succeed
    assert isinstance(bh.make_constraint(user=None), Never)
    assert isinstance(bh.make_constraint(user="ben"), Never)
    assert isinstance(bh.make_constraint(user="random"), Never)

    # ok, the real constraint - user==ryan
    user_cons = bh.make_constraint(user="ryan")
    assert isinstance(user_cons, And)

    node = SchedulerNode("tux")
    node._Node__nodearray = "hpc"
    assert user_cons.satisfied_by_node(node)
