import os
from typing import Dict, List, Optional

from hpc.autoscale import hpclogging
from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.hpctypes import Memory
from hpc.autoscale.job.demandcalculator import DemandCalculator
from hpc.autoscale.job.job import Job
from hpc.autoscale.results import DefaultContextHandler, register_result_handler
from hpc.autoscale.util import partition, partition_single

from gridengine import autoscaler
from gridengine.allocation_rules import FillUp, FixedProcesses, RoundRobin
from gridengine.complex import Complex
from gridengine.environment import GridEngineEnvironment
from gridengine.parallel_environments import new_parallel_environment as new_pe
from gridengine.queue import new_gequeue
from gridengine_test import mock_driver

SLOTS_COMPLEX = Complex("slots", "s", "INT", "<=", True, True, "1", 1000)
MFREE_COMPLEX = Complex("m_mem_free", "mfree", "MEMORY", "<=", True, True, "0", 0)
EXCL_COMPLEX = Complex("exclusive", "excl", "BOOL", "EXCL", True, True, "0", 1000)
CONTEXT = DefaultContextHandler("[default]")


def setup_module() -> None:
    hpclogging.initialize_logging(mock_config(None))
    register_result_handler(CONTEXT)


def test_non_exclusive_htc_arrays() -> None:
    # ask for exactly the available count 10
    common_cluster_test(["-l nodearray=htc -t 1-40  -q htc.q sleep.sh"], htc=10)

    # ask for more than 10, hit limit
    common_cluster_test(["-t 1-44  -q htc.q sleep.sh"], htc=10)

    # ask for over the limit across two jobs
    common_cluster_test(
        [
            "-l nodearray=htc -t 1-40  -q htc.q sleep.sh",
            "-l nodearray=htc -t 1-40  -q htc.q sleep.sh",
        ],
        htc=10,
    )
    common_cluster_test(
        [
            "-l nodearray=htc -t 1-30  -q htc.q sleep.sh",
            "-l nodearray=htc -t 1-30  -q htc.q sleep.sh",
        ],
        htc=10,
    )
    common_cluster_test(
        [
            "-l nodearray=htc -t 1-30  -q htc.q sleep.sh",
            "-l nodearray=htc -t 1-30  -q htc.q sleep.sh",
            "-l nodearray=htc -q htc.q sleep.sh",
        ],
        htc=10,
    )

    # # ask for exactly the num slots
    common_cluster_test(["-t 1-4  -q htc.q sleep.sh"], htc=1)
    # same, except split across two jobs
    common_cluster_test(2 * ["-t 1-2  -q htc.q sleep.sh"], htc=1)


def test_non_exclusive_htc_jobs() -> None:
    # ask for exactly the available count 10
    # 4 2gb slots with an 8gb vm_size
    common_cluster_test(1 * ["-l mfree=2g -q htc.q sleep.sh"], htc=1)
    common_cluster_test(4 * ["-l mfree=2g -q htc.q sleep.sh"], htc=1)
    common_cluster_test(5 * ["-l mfree=2g -q htc.q sleep.sh"], htc=2)
    common_cluster_test(8 * ["-l mfree=2g -q htc.q sleep.sh"], htc=2)
    common_cluster_test(1 * ["-l slots=2 -q htc.q sleep.sh"], htc=1)
    common_cluster_test(2 * ["-l slots=2 -q htc.q sleep.sh"], htc=1)
    common_cluster_test(3 * ["-l slots=2 -q htc.q sleep.sh"], htc=2)
    common_cluster_test(4 * ["-l slots=2 -q htc.q sleep.sh"], htc=2)
    # ask for 100 jobs. We only have 10 4 slot nodes though.
    common_cluster_test(100 * ["-l slots=2 -q htc.q sleep.sh"], htc=10)


def test_complex_shortcut_parsing() -> None:
    ge_env = common_ge_env()
    qsub1 = mock_driver.MockQsub(ge_env)
    qsub1.qsub("-l m_mem_free=2g -l exclusive=true -q htc.q sleep.sh")
    qsub2 = mock_driver.MockQsub(ge_env)
    qsub2.qsub("-l mfree=2g -l excl=true -q  htc.q sleep.sh")
    longform = qsub1.parse_jobs()
    shortform = qsub2.parse_jobs()
    assert longform
    assert "mfree" not in str(longform)
    assert longform[0].to_dict() == shortform[0].to_dict()


def test_complex_shortcut() -> None:
    # make sure that if a user mixes the shortcut and long form
    # we still handle that.
    dcalc = common_cluster_test(
        [
            "-l m_mem_free=2g -q htc.q sleep.sh",
            "-l m_mem_free=2g -q htc.q sleep.sh",
            "-l m_mem_free=2g -q htc.q sleep.sh",
            "-l mfree=2g      -q htc.q sleep.sh",
            "-l mfree=2g      -q htc.q sleep.sh",
            "-l mfree=2g      -q htc.q sleep.sh",
            # "-l m_mem_free=2g -q htc.q sleep.sh",
            # "-l m_mem_free=2g -q htc.q sleep.sh",
            # "-l m_mem_free=2g -q htc.q sleep.sh",
        ],
        htc=2,
    )
    eg = dcalc.node_mgr.example_node("westus", "Standard_F4")

    new_nodes = dcalc.get_demand().new_nodes
    by_name = partition_single(new_nodes, lambda n: n.name)

    def m(expr: str) -> Memory:
        return Memory.value_of(expr)

    assert eg.memory == m("8g")

    assert by_name["htc-1"].memory == m("8g")
    assert by_name["htc-1"].resources["m_mem_free"] == m("8g")
    assert by_name["htc-1"].resources["mfree"] == m("8g")
    assert by_name["htc-1"].available["m_mem_free"] == m("0g")
    assert by_name["htc-1"].available["mfree"] == m("0g")

    assert by_name["htc-2"].resources["m_mem_free"] == m("8g")
    assert by_name["htc-2"].resources["mfree"] == m("8g")
    assert by_name["htc-2"].available["m_mem_free"] == m("4g")
    assert by_name["htc-2"].available["mfree"] == m("4g")


def test_fixed() -> None:
    # ask for more than the available count 100
    common_cluster_test(["-pe fp* 101  -q hpc.q sleep.sh"])

    # ask for more than the max vmss size
    common_cluster_test(["-pe fp* 14  -q hpc.q sleep.sh"])

    # ask for exactly the max vmss size
    common_cluster_test(["-pe fp* 10  -q hpc.q sleep.sh"], {"hpc_q_fp0": 5}, hpc=5)

    # same, except split across two jobs
    common_cluster_test(
        [
            "-l exclusive=true -pe fp* 4  -q hpc.q sleep.sh",
            "-l exclusive=true -pe fp* 6  -q hpc.q sleep.sh",
        ],
        {"hpc_q_fp0": 5},
        hpc=5,
    )

    # let's allocate in two different pgs
    common_cluster_test(
        [
            "-l exclusive=true -pe fp* 6  -q hpc.q sleep.sh",
            "-l exclusive=true -pe fp* 6  -q hpc.q sleep.sh",
        ],
        {"hpc_q_fp0": 3, "hpc_q_fp1": 3},
        hpc=6,
    )

    # let's allocate in all three pgs plus reach capacity.
    common_cluster_test(
        ["-l exclusive=true -pe fp* 3 -q hpc.q sleep.sh"] * 4,
        pg_counts={"hpc_q_fp2": 3, "hpc_q_fp1": 3, "hpc_q_fp0": 3},
        hpc=9,
    )

    # same as above, but let's use an array
    common_cluster_test(
        ["-l exclusive=true -pe fp* 3 -t 1-4 -q hpc.q sleep.sh"] * 4,
        pg_counts={"hpc_q_fp2": 3, "hpc_q_fp1": 3, "hpc_q_fp0": 3},
        hpc=9,
    )


def test_fill_up_and_round_robin() -> None:
    # With FillUp, GE will spread the processes across the machines as tightly as possible.
    # We are using an F4 here, so slots=4
    #
    common_cluster_test(["-pe fu* 40  -q hpc.q sleep.sh"], pg_counts={})
    common_cluster_test(["-l -pe rr* 40  -q hpc.q sleep.sh"], pg_counts={})
    # ask for more than the max vmss size
    common_cluster_test(["-pe fu* 28  -q hpc.q sleep.sh"], pg_counts={})
    common_cluster_test(["-pe rr* 28  -q hpc.q sleep.sh"], pg_counts={})

    # ask for exactly the max vmss size
    common_cluster_test(
        ["-pe fu* 20  -q hpc.q sleep.sh"], pg_counts={"hpc_q_fu0": 5}, hpc=5,
    )
    common_cluster_test(
        ["-pe rr* 20  -q hpc.q sleep.sh"], pg_counts={"hpc_q_rr0": 5}, hpc=5,
    )

    # same, except split across two jobs
    common_cluster_test(
        [
            "-l exclusive=true -pe fu* 8  -q hpc.q sleep.sh",
            "-l exclusive=true -pe fu* 12  -q hpc.q sleep.sh",
        ],
        pg_counts={"hpc_q_fu0": 5},
        hpc=5,
    )
    common_cluster_test(
        [
            "-l exclusive=true -pe rr* 8  -q hpc.q sleep.sh",
            "-l exclusive=true -pe rr* 12  -q hpc.q sleep.sh",
        ],
        pg_counts={"hpc_q_rr0": 5},
        hpc=5,
    )

    # let's allocate in two different pgs
    common_cluster_test(
        [
            "-l exclusive=true -pe fu* 12  -q hpc.q sleep.sh",
            "-l exclusive=true -pe fu* 12  -q hpc.q sleep.sh",
        ],
        pg_counts={"hpc_q_fu0": 3, "hpc_q_fu1": 3},
        hpc=6,
    )
    common_cluster_test(
        [
            "-l exclusive=true -pe rr* 12  -q hpc.q sleep.sh",
            "-l exclusive=true -pe rr* 12  -q hpc.q sleep.sh",
        ],
        pg_counts={"hpc_q_rr0": 3, "hpc_q_rr1": 3},
        hpc=6,
    )

    # let's allocate in all three pgs plus reach capacity.
    common_cluster_test(
        ["-l exclusive=true -pe fu* 12 -q hpc.q sleep.sh"] * 4,
        pg_counts={"hpc_q_fu2": 3, "hpc_q_fu1": 3, "hpc_q_fu0": 3},
        hpc=9,
    )
    common_cluster_test(
        ["-l exclusive=true -pe rr* 12 -q hpc.q sleep.sh"] * 4,
        pg_counts={"hpc_q_rr2": 3, "hpc_q_rr1": 3, "hpc_q_rr0": 3},
        hpc=9,
    )

    # same as above, but let's use an array
    common_cluster_test(
        ["-l exclusive=true -pe fu* 12 -t 1-4 -q hpc.q sleep.sh"],
        pg_counts={"hpc_q_fu2": 3, "hpc_q_fu1": 3, "hpc_q_fu0": 3},
        hpc=9,
    )
    common_cluster_test(
        ["-l exclusive=true -pe rr* 12 -t 1-4 -q hpc.q sleep.sh"],
        pg_counts={"hpc_q_rr2": 3, "hpc_q_rr1": 3, "hpc_q_rr0": 3},
        hpc=9,
    )


def _job(qsub_cmd: str, job_id: int) -> Job:
    ge_env = common_ge_env()
    qsub = mock_driver.MockQsub(ge_env)
    qsub.current_job_number = job_id
    qsub.qsub(qsub_cmd)
    qsub.qstat()
    return qsub.parse_jobs()[0]


def test_overalocation_bug() -> None:
    # qsub -pe 'pe*' 6 -b y -q hpc.q sleep 100
    # qsub -pe 'pe*' 4 -b y -q hpc.q sleep 100
    # with 3 $round_robin pes

    qsub_cmds = [
        "-l exclusive=1 -pe rr* 6 -q hpc.q sleep 100",
        "-l exclusive=1 -pe rr* 4 -q hpc.q sleep 100",
    ]
    dcalc = common_cluster(qsub_cmds)
    assert len(dcalc.get_demand().new_nodes) == 4


def mock_config(bindings: MockClusterBinding) -> Dict:
    logging_config = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "conf", "logging.conf")
    )
    assert os.path.exists(logging_config), logging_config

    pgs = []
    for prefix in ["rr", "fp", "fu"]:
        for i in range(3):
            pgs.append("hpc_q_{}{}".format(prefix, i))

    return {
        "_mock_bindings": bindings,
        "lock_file": None,
        "logging": {"config_file": logging_config},
        "default_resources": [
            {"name": "slots", "select": {}, "value": "node.vcpu_count"},
            {"name": "m_mem_free", "select": {}, "value": "node.resources.memgb"},
            {"name": "mfree", "select": {}, "value": "node.resources.m_mem_free"},
        ],
        "nodearrays": {"hpc": {"placement_groups": pgs}},
    }


def common_cluster_test(
    qsub_commands: List[str],
    pg_counts: Optional[Dict[str, int]] = None,
    **array_counts: int
) -> DemandCalculator:
    pg_counts = pg_counts or {}
    dcalc = common_cluster(qsub_commands)
    demand = dcalc.get_demand()

    # sanity check that we don't recreate the same node
    partition_single(demand.new_nodes, lambda n: n.name)
    by_array = partition(demand.new_nodes, lambda n: n.nodearray)
    by_pg = partition(demand.new_nodes, lambda n: n.placement_group)
    if set(by_pg.keys()) != set([None]):
        if set(by_pg.keys()) != set(pg_counts.keys()):
            assert False, "\n%s\n%s" % (
                [(x, len(y)) for x, y in by_pg.items()],
                pg_counts,
            )
        assert set(by_pg.keys()) == set(pg_counts.keys())
        assert not (bool(by_pg) ^ bool(pg_counts))

    if pg_counts:
        for pg_name, count in pg_counts.items():
            assert pg_name in by_pg
            assert (
                len(by_pg[pg_name]) == count
            ), "Expected pg {} to have {} nodes. Found {}. Full {}".format(
                pg_name,
                count,
                len(by_pg[pg_name]),
                [(x, len(y)) for x, y in by_pg.items()],
            )

        for pg_name in by_pg:
            assert pg_name in pg_counts

    for nodearray_name, count in array_counts.items():
        assert nodearray_name in by_array
        assert len(by_array[nodearray_name]) == count, [
            n.name for n in by_array[nodearray_name]
        ]

    for nodearray_name, node_list in by_array.items():
        assert nodearray_name in array_counts

    return dcalc


def common_cluster(qsub_commands: List[str]) -> DemandCalculator:
    ge_env = common_ge_env()
    # allq = new_gequeue("all.q", "@allhosts", ["make"], [], complexes=complexes, parallel_envs=pes)

    qsub = mock_driver.MockQsub(ge_env)
    for qsub_cmd in qsub_commands:
        qsub.qsub(qsub_cmd)

    jobs = qsub.parse_jobs()

    def _bindings() -> MockClusterBinding:
        mock_bindings = MockClusterBinding()
        mock_bindings.add_nodearray("hpc", {}, max_placement_group_size=5)
        mock_bindings.add_bucket("hpc", "Standard_F4", 100, 100)

        mock_bindings.add_nodearray("htc", {}, max_count=10)
        mock_bindings.add_bucket("htc", "Standard_F4", 10, 10)
        return mock_bindings

    mdriver = mock_driver.MockGridEngineDriver(ge_env)

    for job in jobs:
        ge_env.add_job(job)

    return autoscaler.calculate_demand(
        mock_config(_bindings()), ge_env, mdriver, CONTEXT
    )


def common_ge_env() -> GridEngineEnvironment:
    ge_env = GridEngineEnvironment()
    pe_list = ["NONE"]
    for pe_name in ["rr0", "rr1", "rr2", "fp0", "fp1", "fp2", "fu0", "fu1", "fu2"]:
        if pe_name.startswith("rr"):
            ge_env.add_pe(new_pe(pe_name, 0, RoundRobin()))
        elif pe_name.startswith("fu"):
            ge_env.add_pe(new_pe(pe_name, 0, FillUp()))
        elif pe_name.startswith("fp"):
            ge_env.add_pe(new_pe(pe_name, 0, FixedProcesses("2")))
        # ge pe list syntax -> [@hostgroup=pe_name]
        # we create one hostgroup per queue/pe_name combo
        pe_list.append("[@hpc.q_{}={}]".format(pe_name, pe_name))
    ge_env.add_complex(SLOTS_COMPLEX)
    ge_env.add_complex(MFREE_COMPLEX)
    ge_env.add_complex(EXCL_COMPLEX)

    ge_env.add_queue(
        new_gequeue(
            qname="hpc.q",
            hostlist=["@hpc.q"],
            pe_list=pe_list,
            constraints=[{"node.nodearray": "hpc"}],
            slots_expr="",
            ge_env=ge_env,
        )
    )

    ge_env.add_queue(
        new_gequeue(
            qname="htc.q",
            hostlist=["@htc.q"],
            pe_list=[],
            constraints=[{"node.nodearray": "htc"}],
            slots_expr="",
            ge_env=ge_env,
        )
    )

    return ge_env
