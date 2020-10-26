from gridengine.util import _tokenize_ge_list, parse_hostgroup_mapping


def test_parse_hostgroup_mapping() -> None:
    assert {None: ["make"]} == parse_hostgroup_mapping("make")
    assert {None: ["make", "smpslots"]} == parse_hostgroup_mapping("make smpslots")
    assert {None: ["make", "smpslots"]} == parse_hostgroup_mapping("make,smpslots")
    assert {None: ["make", "smpslots"]} == parse_hostgroup_mapping(
        "make,  \t  smpslots"
    )

    assert {} == parse_hostgroup_mapping("make", [])
    assert {} == parse_hostgroup_mapping("make, smpslots", [])

    assert {"@allhosts": ["make"]} == parse_hostgroup_mapping("make", ["@allhosts"])
    assert {"@allhosts": ["make", "smpslots"]} == parse_hostgroup_mapping(
        "make, smpslots", ["@allhosts"]
    )
    assert {"@allhosts": ["make"], "@cloudhosts": ["make"]} == parse_hostgroup_mapping(
        "make", ["@allhosts", "@cloudhosts"]
    )

    assert {"@allhosts": ["make"], "@mpi": ["mpi"]} == parse_hostgroup_mapping(
        "make, [@mpi=mpi]", ["@allhosts"]
    )

    assert {"@allhosts": ["make"], "@mpi.a": ["mpi"]} == parse_hostgroup_mapping(
        "make, [@mpi.a=mpi]", ["@allhosts"]
    )

    assert {
        "@buffergrp1": ["PRJ1"],
        "@buffergrp2": ["PRJ1", "PRJ2"],
        "@buffergrp3": ["PRJ1", "PRJ2", "PRJ3"],
    } == parse_hostgroup_mapping(
        "NONE,[@buffergrp1=PRJ1],[@buffergrp2=PRJ1,PRJ2],  [@buffergrp3=PRJ1,PRJ2,PRJ3]",
        [],
    )


def test_ge_list_tokenizing() -> None:
    assert ["NONE"] == _tokenize_ge_list("NONE")

    assert ["mpi", "mpislots",] == _tokenize_ge_list("mpi mpislots")

    assert ["[@mpihosts=mpi]",] == _tokenize_ge_list("[@mpihosts=mpi]")

    assert ["NONE", "[@mpihosts=mpi]",] == _tokenize_ge_list("NONE,[@mpihosts=mpi]")

    assert [
        "NONE",
        "[@mpihosts=mpi mpislots]",
        "[@smphosts=smp1,smp2]",
    ] == _tokenize_ge_list("NONE,[@mpihosts=mpi mpislots],[@smphosts=smp1,smp2]")
