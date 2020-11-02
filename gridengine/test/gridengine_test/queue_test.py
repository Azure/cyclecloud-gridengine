from gridengine.complex import Complex
from gridengine.hostgroup import Hostgroup
from gridengine.parallel_environments import ParallelEnvironment
from gridengine.queue import GridEngineQueue, parse_slots


def test_parse_slots() -> None:
    slots_with_0 = parse_slots("0, [@allhosts=0], [tux=4], [@onprem=16]")
    slots_without_0 = parse_slots("[@allhosts=0], [tux=4], [@onprem=16]")
    assert slots_with_0 == {None: 0, "@allhosts": 0, "tux": 4, "@onprem": 16}
    assert slots_without_0 == {"@allhosts": 0, "tux": 4, "@onprem": 16}


def test_hostlist() -> None:
    "seqno, complex_values, slots, users..."
    pes = {
        "make": ParallelEnvironment({"pe_name": "make", "allocation_rule": "1"}),
        "mpi": ParallelEnvironment(
            {"pe_name": "mpi", "allocation_rule": "$round_robin"}
        ),
    }

    queue_config = {
        "qname": "testq",
        "hostlist": "@hostlisthg",
        "user_lists": "[@userlisthg=user]",
        "xuser_lists": "[@xuserlisthg=user]",
        "projects": "[@projecthg=prj]",
        "xprojects": "[@xprojecthg=prj]",
        "seq_no": "0,[@seqnohg=100]",
        "complex_values": "pcpu=2,[@complexvalueshg=pcpu=1]",
        "pe_list": "make,[@pelisthg=mpi]",
    }

    unbound_hostgroups = {
        "@hostlisthg": Hostgroup("@hostlisthg"),
        "@userlisthg": Hostgroup("@userlisthg"),
        "@xuserlisthg": Hostgroup("@xuserlisthg"),
        "@projecthg": Hostgroup("@projecthg"),
        "@xprojecthg": Hostgroup("@xprojecthg"),
        "@seqnohg": Hostgroup("@seqnohg"),
        "@complexvalueshg": Hostgroup("@complexvalueshg"),
        "@pelisthg": Hostgroup("@pelisthg"),
        "@notreferenced": Hostgroup("@notreferenced"),
    }

    complex_values = {None: {"pcpu": 2}, "@complexvalueshg": {"pcpu": 1}}

    queue = GridEngineQueue(queue_config, pes, unbound_hostgroups, complex_values)
    assert "@complexvalueshg" in queue.complex_values
    unreferenced = set(unbound_hostgroups.keys()) - set(queue.hostlist)
    assert unreferenced == set(["@notreferenced"])

    pcpu_c = Complex("pcpu", "p", "INT", "<=", True, True, "123", 100)
    assert 1 == queue.get_quota(pcpu_c, "@complexvalueshg")
    assert 2 == queue.get_quota(pcpu_c, "@asdf")
