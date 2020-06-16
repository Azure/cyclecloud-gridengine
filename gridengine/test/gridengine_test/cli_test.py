from io import StringIO
from typing import Dict, List

from gridengine import cli


def test_hg_to_pes() -> None:
    def _config(**queues: Dict) -> Dict[str, Dict[str, List[str]]]:
        return cli._hostgroup_to_pes({"gridengine": {"queues": queues}}, [],)

    assert _config(
        hpcq={
            "pes": {
                "mpi": {"hostgroups": ["@hpcmpipg0"]},
                "mpislots": {"hostgroups": ["@hpcmpipg0"]},
            },
        }
    )["hpcq"] == {"@hpcmpipg0": ["mpi", "mpislots"]}

    assert _config(
        hpcq={
            "pes": {
                "mpi": {"hostgroups": ["@hpcmpipg0"]},
                "mpislots": {"hostgroups": ["@hpcmpipg0"]},
            },
        }
    )["hpcq"] == {"@hpcmpipg0": ["mpi", "mpislots"]}

    assert _config(
        hpcq={
            "pes": {
                "mpi": {"hostgroups": ["@hpcmpipg0"]},
                "mpislots": {"hostgroups": ["@hpcmpislotspg0"]},
            },
        },
        hpcotherq={
            "pes": {
                "mpi": {"hostgroups": ["@hpcotherqpg0"]},
                "mpislots": {"hostgroups": ["@hpcotherqpg0"]},
            },
        },
    ) == {
        "hpcq": {
            "@hpcmpipg0": ["mpi"],
            "@hpcmpislotspg0": ["mpislots"],
        },  # noqa: ignore=E231
        "hpcotherq": {"@hpcotherqpg0": ["mpi", "mpislots"],},  # noqa: ignore=E231
    }


def test_queue_file() -> None:
    fw = StringIO()
    h2p_by_q = cli._hostgroup_to_pes(
        {
            "gridengine": {
                "queues": {
                    "hpc.q": {
                        "pes": {
                            "mpi": {"hostgroups": ["@hpcmpipg0"], "slots": 100},
                            "mpislots": {"hostgroups": ["@hpcmpipg0"], "slots": 100},
                            "smpslots": {"hostgroups": ["@hpc.q"], "slots": 500},
                        },
                    },
                }
            }
        },
        [],
    )

    h2p = h2p_by_q["hpc.q"]

    cli._create_queue_file(
        fw, ["qname", "hostlist", "pe_list", "slots"], "hpc.q", ["@hpc.q"], h2p
    )
    qname, hostlist, pe_list, slots = fw.getvalue().strip().splitlines()
    assert qname.split() == ["qname", "hpc.q"]
    assert hostlist.split() == ["hostlist", "@hpc.q", "@hpcmpipg0"]
    assert pe_list.split(None, 1) == [
        "pe_list",
        "NONE,[@hpcmpipg0=mpi mpislots],[@hpc.q=smpslots]",
    ]
    assert slots.split(None, 1) == [
        "slots",
        "0",
    ]

    fw = StringIO()
    cli._create_queue_file(fw, ["qname", "hostlist", "pe_list"], "hpc.q", ["NONE"], h2p)
    qname, hostlist, pe_list = fw.getvalue().strip().splitlines()
    assert qname.split() == ["qname", "hpc.q"]
    assert hostlist.split() == ["hostlist", "@hpcmpipg0", "@hpc.q"]
    assert pe_list.split(None, 1) == [
        "pe_list",
        "NONE,[@hpcmpipg0=mpi mpislots],[@hpc.q=smpslots]",
    ]

    fw = StringIO()
    cli._create_queue_file(
        fw, ["qname", "hostlist", "pe_list"], "hpc.q", ["@hpc.q", "@other.q"], h2p
    )
    qname, hostlist, pe_list = fw.getvalue().strip().splitlines()
    assert qname.split() == ["qname", "hpc.q"]
    assert hostlist.split() == ["hostlist", "@hpc.q", "@other.q", "@hpcmpipg0"]
    assert pe_list.split(None, 1) == [
        "pe_list",
        "NONE,[@hpcmpipg0=mpi mpislots],[@hpc.q=smpslots]",
    ]

    # make sure we handle multi lines
    fw = StringIO()
    cli._create_queue_file(
        fw,
        ["qname", "hostlist multi\\", " line   \\", "test", "pe_list"],
        "hpc.q",
        ["@hpc.q", "@other.q"],
        h2p,
    )
    assert 3 == len(fw.getvalue().strip().splitlines())
