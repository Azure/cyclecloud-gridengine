from typing import Optional, Union
from unittest import mock

from hpc.autoscale.hpctypes import Memory
from hpc.autoscale.job.schedulernode import SchedulerNode

from gridengine.complex import Complex, parse_queue_complex_values, read_complexes
from gridengine.hostgroup import make_quota_bound_consumable_constraint, process_quotas
from gridengine.qbin import QBinImpl
from gridengine.util import json_dump
from gridengine_test.autoscaler_test import common_ge_env


def test_int_parsing() -> None:
    for itype in ["INT", "RSMAP"]:
        c = Complex("slots", "s", itype, "<=", True, True, "1", 1000)
        assert c.parse("100") == 100
        assert c.default == 1
        assert c.name == "slots"
        assert c.shortcut == "s"

        for expr in ["100.1", "a", None, ""]:
            c.parse(expr) == expr


def test_memory_parsing() -> None:
    c = Complex("m_mem_free", "mfree", "MEMORY", "<=", True, True, "0", 0)
    assert c.parse("100") == Memory.value_of("100b")
    assert c.parse("100g") == Memory.value_of("100g")
    assert c.parse("100G") == Memory.value_of("100G")
    assert c.parse("100g") != Memory.value_of("100G")
    assert c.parse("42.123t") == Memory.value_of("42.123t")
    assert c.name == "m_mem_free"
    assert c.shortcut == "mfree"

    for expr in ["blah.", "10gigs", None, ""]:
        assert c.parse(expr) == expr


def test_excl_parsing() -> None:
    c = Complex("exclusive", "excl", "BOOL", "EXCL", True, True, "0", 1000)
    assert c.default == Memory.value_of("0b")
    assert c.parse("TRuE")
    assert c.parse("true")
    assert c.parse("1")
    assert not c.parse("FALsE")
    assert not c.parse("false")
    assert not c.parse("0")

    for expr in ["yes", "y", "SDF", None, ""]:
        assert c.parse(expr) == expr


def test_double_parsing() -> None:
    c = Complex("disk", "d", "DOUBLE", "<=", True, True, "123.123", 0)
    assert c.name == "disk"
    assert c.shortcut == "d"
    assert c.default == 123.123
    assert c.parse("234.234") == 234.234
    assert c.parse("infinity") == float("inf")

    for expr in ["SDF", None, ""]:
        assert c.parse(expr) == expr


def test_cstring() -> None:
    c = Complex("dept", "d", "CSTRING", "<=", True, False, "ABC", 0)
    assert c.name == "dept"
    assert c.shortcut == "d"
    assert c.default == "abc"
    assert c.parse("MixedCase") == "mixedcase"
    assert c.parse(None) is None


def test_other_strings() -> None:
    for stype in ["RESTRING", "TIME", "STRING", "HOST"]:
        c = Complex("compy", "c", stype, "<=", True, False, "ABC", 0)
        assert c.name == "compy"
        assert c.shortcut == "c"
        assert c.default == "ABC"
        assert c.parse("MixedCase") == "MixedCase"
        assert c.parse(None) is None


def test_read_complexes() -> None:
    raw_contents = """#name               shortcut     type        relop   requestable consumable default  urgency
#--------------------------------------------------------------------------------------------
affinity_group      affinity_group        RESTRING    ==      YES      NO         NONE     0
affinity_group_cores affinity_group_cores  INT         ==      YES         NO         0        0
arch                a            RESTRING    ==      YES         NO         NONE     0
average_runtime     avg          INT         <=      YES         NO         0        0
calendar            c            RESTRING    ==      YES         NO         NONE     0
cpu                 cpu          DOUBLE      >=      YES         NO         0        0
d_rt                d_rt         TIME        <=      YES         NO         0:0:0    0
display_win_gui     dwg          BOOL        ==      YES         NO         0        0
exclusive           exclusive    BOOL        EXCL    YES         YES        0        1000
h_core              h_core       MEMORY      <=      YES         NO         0        0
h_cpu               h_cpu        TIME        <=      YES         NO         0:0:0    0
h_data              h_data       MEMORY      <=      YES         NO         0        0
h_fsize             h_fsize      MEMORY      <=      YES         NO         0        0
h_rss               h_rss        MEMORY      <=      YES         NO         0        0
h_rt                h_rt         TIME        <=      YES         NO         0:0:0    0
h_stack             h_stack      MEMORY      <=      YES         NO         0        0
h_vmem              h_vmem       MEMORY      <=      YES         NO         0        0
hostname            h            HOST        ==      YES         NO         NONE     0
instance_id         instance_id  RESTRING    ==      YES         NO         NONE     0
load_avg            la           DOUBLE      >=      NO          NO         0        0
load_long           ll           DOUBLE      >=      NO          NO         0        0
load_medium         lm           DOUBLE      >=      NO          NO         0        0
load_short          ls           DOUBLE      >=      NO          NO         0        0
m_cache_l1          mcache1      MEMORY      <=      YES         NO         0        0
m_cache_l2          mcache2      MEMORY      <=      YES         NO         0        0
m_cache_l3          mcache3      MEMORY      <=      YES         NO         0        0
m_core              core         INT         <=      YES         NO         0        0
m_mem_free          mfree        MEMORY      <=      YES         YES        0        0
m_mem_free_n0       mfree0       MEMORY      <=      YES         YES        0        0
m_mem_free_n1       mfree1       MEMORY      <=      YES         YES        0        0
m_mem_free_n2       mfree2       MEMORY      <=      YES         YES        0        0
m_mem_free_n3       mfree3       MEMORY      <=      YES         YES        0        0
m_mem_total         mtotal       MEMORY      <=      YES         YES        0        0
m_mem_total_n0      mmem0        MEMORY      <=      YES         YES        0        0
m_mem_total_n1      mmem1        MEMORY      <=      YES         YES        0        0
m_mem_total_n2      mmem2        MEMORY      <=      YES         YES        0        0
m_mem_total_n3      mmem3        MEMORY      <=      YES         YES        0        0
m_mem_used          mused        MEMORY      >=      YES         NO         0        0
m_mem_used_n0       mused0       MEMORY      >=      YES         NO         0        0
m_mem_used_n1       mused1       MEMORY      >=      YES         NO         0        0
m_mem_used_n2       mused2       MEMORY      >=      YES         NO         0        0
m_mem_used_n3       mused3       MEMORY      >=      YES         NO         0        0
m_numa_nodes        nodes        INT         <=      YES         NO         0        0
m_socket            socket       INT         <=      YES         NO         0        0
m_thread            thread       INT         <=      YES         NO         0        0
m_topology          topo         RESTRING    ==      YES         NO         NONE     0
m_topology_inuse    utopo        RESTRING    ==      YES         NO         NONE     0
m_topology_numa     unuma        RESTRING    ==      YES         NO         NONE     0
machinetype         machinetype  RESTRING    ==      YES         NO         NONE     0
mem_free            mf           MEMORY      <=      YES         NO         0        0
mem_total           mt           MEMORY      <=      YES         NO         0        0
mem_used            mu           MEMORY      >=      YES         NO         0        0
min_cpu_interval    mci          TIME        <=      NO          NO         0:0:0    0
nodearray           nodearray    RESTRING    ==      YES         NO         NONE     0
np_load_avg         nla          DOUBLE      >=      NO          NO         0        0
np_load_long        nll          DOUBLE      >=      NO          NO         0        0
np_load_medium      nlm          DOUBLE      >=      NO          NO         0        0
np_load_short       nls          DOUBLE      >=      NO          NO         0        0
num_proc            p            INT         ==      YES         NO         0        0
onsched             os           BOOL        ==      YES         NO         0        0
placement_group     group        RESTRING    ==      YES         NO         NONE     0
placement_group_cores group_size INT         ==      YES         NO         0        0
qname               q            RESTRING    ==      YES         NO         NONE     0
rerun               re           BOOL        ==      NO          NO         0        0
s_core              s_core       MEMORY      <=      YES         NO         0        0
s_cpu               s_cpu        TIME        <=      YES         NO         0:0:0    0
s_data              s_data       MEMORY      <=      YES         NO         0        0
s_fsize             s_fsize      MEMORY      <=      YES         NO         0        0
s_rss               s_rss        MEMORY      <=      YES         NO         0        0
s_rt                s_rt         TIME        <=      YES         NO         0:0:0    0
s_stack             s_stack      MEMORY      <=      YES         NO         0        0
s_vmem              s_vmem       MEMORY      <=      YES         NO         0        0
seq_no              seq          INT         ==      NO          NO         0        0
slot_type           slot_type    RESTRING    ==      YES         NO         NONE     0
slots               s            INT         <=      YES         YES        1        1000
swap_free           sf           MEMORY      <=      YES         NO         0        0
swap_rate           sr           MEMORY      >=      YES         NO         0        0
swap_rsvd           srsv         MEMORY      >=      YES         NO         0        0
swap_total          st           MEMORY      <=      YES         NO         0        0
swap_used           su           MEMORY      >=      YES         NO         0        0
tmpdir              tmp          RESTRING    ==      NO          NO         NONE     0
virtual_free        vf           MEMORY      <=      YES         YES        0        0
virtual_total       vt           MEMORY      <=      YES         NO         0        0
virtual_used        vu           MEMORY      >=      YES         NO         0        0
# >#< starts a comment but comments are not saved across edits --------
"""
    qbin = QBinImpl(is_uge=True)
    qbin.qconf = mock.MagicMock(return_value=raw_contents)
    complexes = read_complexes({}, qbin)
    assert len(complexes) == 143  # hand calculated

    complexes = read_complexes({"gridengine": {"relevant_complexes": []}}, qbin)
    complexes = read_complexes({}, qbin)
    assert len(complexes) == 143  # hand calculated

    complexes = read_complexes({"gridengine": {"relevant_complexes": None}}, qbin)
    complexes = read_complexes({}, qbin)
    assert len(complexes) == 143  # hand calculated

    complexes = read_complexes({"gridengine": {"relevant_complexes": ["slots"]}}, qbin)
    assert len(complexes) == 2
    assert set(complexes.keys()) == set(["slots", "s"])

    complexes = read_complexes(
        {"gridengine": {"relevant_complexes": ["h", "mfree"]}}, qbin
    )
    assert set(complexes.keys()) == set(["h", "hostname", "mfree", "m_mem_free"])


def test_complex_parsing_of_queue() -> None:
    pcpu = Complex("pcpu", "pcpu", "INT", "<=", True, True, "1", 0)
    pmem = Complex("pmem", "pmem", "INT", "<=", True, True, "1", 0)
    ldf = Complex("ldf", "ldf", "BOOL", "<=", True, True, "1", 0)
    c = {"pcpu": pcpu, "pmem": pmem, "ldf": ldf}
    f = parse_queue_complex_values
    assert {
        None: {},
        "@ldek5": {"pcpu": 24, "pmem": 376, "ldf": True},
        "@lhaa5": {"pcpu": 4, "pmem": 32},
    } == f("None,[@ldek5=pcpu=24,pmem=376,ldf=1],[@lhaa5=pcpu=4,pmem=32]", c, "q1")

    assert {
        None: {"pcpu": 2, "pmem": 4},
        "@ldek5": {"pcpu": 24, "pmem": 376, "ldf": True},
        "@lhaa5": {"pcpu": 4, "pmem": 32},
    } == f(
        "pcpu=2,pmem=4,[@ldek5=pcpu=24,pmem=376,ldf=1],[@lhaa5=pcpu=4,pmem=32]", c, "q1"
    )


def test_process_numeric_quotas() -> None:
    # test a node that is part of multiple hostgroups
    # what about default values in complexes file?
    N = Union[int, float]

    def run_test(
        ctype: str,
        node_pcpu: Optional[N],
        hg_pcpu: N,
        q_default_pcpu: N,
        complex_default: Optional[N],
    ) -> SchedulerNode:
        cast = float if ctype == "DOUBLE" else int
        node_res = {}
        if node_pcpu is not None:
            node_res["pcpu"] = cast(node_pcpu)
            node_res["p"] = cast(node_pcpu)

        node = SchedulerNode("tux", node_res)
        ge_env = common_ge_env()

        q = ge_env.queues["hpc.q"]
        complex_default_str = (
            "NONE" if complex_default is None else str(complex_default)
        )
        ge_env.complexes["pcpu"] = Complex(
            "pcpu", "p", ctype, "<=", True, True, complex_default_str, 0
        )

        q.complex_values[None] = {"pcpu": cast(q_default_pcpu)}
        q.complex_values["@hpc.q"] = {"pcpu": cast(hg_pcpu)}

        assert node.available.get("pcpu") == node_pcpu
        process_quotas(node, ge_env.complexes, ["@hpc.q"], [q])
        return node

    for ctype in ["INT", "RSMAP", "DOUBLE", "MEMORY"]:
        node = run_test(ctype, 12, 8, 10, 0)
        assert node.available["pcpu"] == 12
        assert node.available["p"] == 12
        json_dump(node.available)
        assert node.available["hpc.q@pcpu"] == 8
        assert node.available["hpc.q@p"] == 8
        assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["pcpu"] == 8
        assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["p"] == 8

        node = run_test(ctype, 12, 10, 8, 0)
        # assert node.available["pcpu"] == 12
        assert node.available["hpc.q@pcpu"] == 10
        assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["pcpu"] == 10

        node = run_test(ctype, 6, 8, 10, 0)
        # assert node.available["pcpu"] == 6
        assert node.available["hpc.q@pcpu"] == 6
        assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["pcpu"] == 6

        node = run_test(ctype, None, 8, 10, 0)
        # assert node.available["pcpu"] == 8
        assert node.available["hpc.q@pcpu"] == 8
        assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["pcpu"] == 8


# fix asap
def test_process_string_quotas() -> None:
    def run_test(
        ctype: str,
        node_lic: Optional[str],
        hg_lic: str,
        q_default_lic: str,
        complex_default: Optional[str],
    ) -> SchedulerNode:
        def cast(x: Optional[str]) -> Optional[str]:
            if x is None:
                return None
            if ctype == "CSTRING":
                return x.lower()
            return x

        node_res = {}
        if node_lic is not None:
            node_res["lic"] = cast(node_lic)
            node_res["l"] = cast(node_lic)

        node = SchedulerNode("tux", node_res)
        ge_env = common_ge_env()

        q = ge_env.queues["hpc.q"]
        complex_default_str = (
            "NONE" if complex_default is None else str(complex_default)
        )
        ge_env.complexes["lic"] = Complex(
            "lic", "l", ctype, "<=", True, True, complex_default_str, 0
        )

        q.complex_values[None] = {"lic": cast(q_default_lic)}
        q.complex_values["@hpc.q"] = {"lic": cast(hg_lic)}

        assert node.available.get("lic") == node_lic
        process_quotas(node, ge_env.complexes, ["@hpc.q"], [q])
        return node

    for ctype in ["STRING", "RESTRING", "HOST", "CSTRING"]:
        node = run_test(ctype, "abc", "bcd", "cde", None)
        # assert node.available["lic"] == "abc"
        assert node.available["hpc.q@lic"] == "abc"
        assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["lic"] == "abc"

        node = run_test(ctype, None, "bcd", "cde", None)
        # assert node.available["lic"] == "bcd"
        assert node.available["hpc.q@lic"] == "bcd"
        assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["lic"] == "bcd"

        node = run_test(ctype, "xyz", None, None, "xyz")  # type: ignore
        # assert node.available["lic"] == "xyz"
        assert node.available["hpc.q@lic"] == "xyz"
        assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["lic"] == "xyz"


def test_quota_bool_resource() -> None:
    def run_test(
        ctype: str,
        node_lic: Optional[bool],
        hg_lic: bool,
        q_default_lic: bool,
        complex_default: Optional[bool],
    ) -> SchedulerNode:
        node_res = {}
        if node_lic is not None:
            node_res["lic"] = node_lic
            node_res["l"] = node_lic

        node = SchedulerNode("tux", node_res)
        ge_env = common_ge_env()

        q = ge_env.queues["hpc.q"]
        complex_default_str = (
            "NONE" if complex_default is None else str(complex_default)
        )
        ge_env.complexes["lic"] = Complex(
            "lic", "l", ctype, "<=", True, True, complex_default_str, 0
        )

        q.complex_values[None] = {"lic": q_default_lic}
        q.complex_values["@hpc.q"] = {"lic": hg_lic}

        assert node.available.get("lic") == node_lic
        process_quotas(node, ge_env.complexes, ["@hpc.q"], [q])
        return node

    node = run_test("BOOL", True, False, False, None)
    # assert node.available["lic"] is True
    assert node.available["hpc.q@lic"] is True
    assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["lic"] is True

    node = run_test("BOOL", None, False, False, None)
    # assert node.available["lic"] is False
    assert node.available["hpc.q@lic"] is False
    assert node.metadata["quotas"]["hpc.q"]["@hpc.q"]["lic"] is False


def test_quota_bound_resource_number() -> None:
    ge_env = common_ge_env()
    hpcq = ge_env.queues["hpc.q"]
    htcq = ge_env.queues["htc.q"]
    hpcq.complex_values[None] = {"pcpu": 6}
    htcq.complex_values[None] = {"pcpu": 4}

    node = SchedulerNode("tux", resources={"pcpu": 8})

    node.available["hpc.q@pcpu"] = 6
    node.available["htc.q@pcpu"] = 4

    c1 = make_quota_bound_consumable_constraint("pcpu", 1, hpcq, ge_env, ["@hpc.q"])
    c2 = make_quota_bound_consumable_constraint("pcpu", 2, htcq, ge_env, ["@htc.q"])
    # imagine the node has 8 pcpus, but hpc.q limits it to 6, and htc.q to 4
    assert node.available["pcpu"] == 8

    assert node.available["hpc.q@pcpu"] == 6
    assert node.available["htc.q@pcpu"] == 4

    # the total amount and hpc.q are decremented, htc.q untouched
    assert c1.satisfied_by_node(node)
    assert c1.do_decrement(node)
    assert node.available["pcpu"] == 7
    assert node.available["hpc.q@pcpu"] == 5
    assert node.available["htc.q@pcpu"] == 4

    # the total amount and htc.q are decremented, hpc.q untouched
    assert c2.satisfied_by_node(node)
    assert c2.do_decrement(node)
    assert node.available["pcpu"] == 5
    assert node.available["hpc.q@pcpu"] == 5
    assert node.available["htc.q@pcpu"] == 2

    # the total amount and htc.q are decremented, hpc.q is floored
    # to the total amount
    assert c2.satisfied_by_node(node)
    assert c2.do_decrement(node)
    assert node.available["pcpu"] == 3
    assert node.available["hpc.q@pcpu"] == 3
    assert node.available["htc.q@pcpu"] == 0

    # take out the remaining amount
    assert not c2.satisfied_by_node(node)
    for _ in range(3):
        assert c1.satisfied_by_node(node)
        assert c1.do_decrement(node)

    assert not c1.satisfied_by_node(node)
    assert node.available["pcpu"] == 0
    assert node.available["hpc.q@pcpu"] == 0
    assert node.available["htc.q@pcpu"] == 0
