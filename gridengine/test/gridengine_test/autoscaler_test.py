from copy import deepcopy
from typing import Any, List, Optional

from gridengine import autoscaler, parallel_environments
from gridengine.driver import QueueAndHostgroupConstraint
from gridengine_test import mock_driver
from hpc.autoscale import hpclogging
from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.job import demandprinter
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.constraints import Or
from hpc.autoscale.node.nodehistory import NullNodeHistory
from hpc.autoscale.results import DefaultContextHandler, register_result_handler


def test_basic() -> None:

    with open("conf/complexes") as fr:
        parallel_environments.set_complexes({}, fr.readlines())

    def _bindings() -> MockClusterBinding:
        mock_bindings = MockClusterBinding()
        mock_bindings.add_nodearray("htc", {"customer_htc_flag": True})
        mock_bindings.add_bucket("htc", "Standard_F2", 10, 8)
        mock_bindings.add_bucket("htc", "Standard_F4", 5, 4)
        return mock_bindings

    class SubTest:
        def __init__(  # type: ignore
            self,
            scheduler_nodes=None,
            jobs=None,
            mock_bindings=None,
            matched=None,
            unmatched=None,
            new_nodes=None,
        ) -> None:
            self.scheduler_nodes = scheduler_nodes or []
            self.jobs = jobs or []
            self.mock_bindings = mock_bindings or _bindings()
            self.expected_matched = matched or 0
            self.expected_unmatched = unmatched or 0
            self.expected_new_nodes = new_nodes or 0

        def run_test(self) -> DemandResult:
            assert len(set([job.name for job in self.jobs])) == len(
                self.jobs
            ), "duplicate job id"
            ge_driver = mock_driver.MockGridEngineDriver(
                self.scheduler_nodes, self.jobs
            )

            config = {"_mock_bindings": self.mock_bindings, "lock_file": None}

            demand_result = autoscaler.autoscale_grid_engine(
                config, ge_driver, node_history=NullNodeHistory()
            )

            assert self.expected_unmatched == len(demand_result.unmatched_nodes)
            assert self.expected_matched == len(demand_result.matched_nodes)
            assert self.expected_new_nodes == len(demand_result.new_nodes)

            return demand_result

    def run_test(
        scheduler_nodes: List[SchedulerNode],
        jobs: List[Job],
        unmatched: int,
        matched: int,
        new: int,
        mock_bindings: Optional[MockClusterBinding] = None,
    ) -> None:
        SubTest(
            scheduler_nodes, jobs, mock_bindings, matched, unmatched, new
        ).run_test()

    def snodes() -> List[SchedulerNode]:
        return [SchedulerNode("ip-010A0005", {"slots": 4})]

    def _xjob(jobid: str, constraints: Optional[Any] = None) -> Job:
        constraints = constraints or []
        if not isinstance(constraints, list):
            constraints = [constraints]
        constraints += [{"exclusive": True}]
        return Job(jobid, constraints=constraints)

    # fmt:off
    run_test(snodes(), [], unmatched=1, matched=0, new=0)
    run_test(snodes(), [_xjob("1")],                                unmatched=0, matched=1, new=0)  # noqa
    run_test(snodes(), [_xjob("1"), _xjob("2")],                    unmatched=0, matched=2, new=1)  # noqa

    run_test(snodes(), [_xjob("1", {"customer_htc_flag": False}),
                        _xjob("2", {"customer_htc_flag": False})],  unmatched=1, matched=0, new=0)  # noqa

    run_test(snodes(), [_xjob("1", {"customer_htc_flag": True}),
                        _xjob("2", {"customer_htc_flag": True})],   unmatched=1, matched=2, new=2)  # noqa

    # ok, now let's make that scheduler node something CC is managing
    mock_bindings = _bindings()
    mock_bindings.add_node("htc-5", "htc", "Standard_F2", hostname="ip-010A0005")
    run_test(snodes(), [],                       unmatched=1, matched=0, new=0, mock_bindings=mock_bindings)  # noqa
    run_test(snodes(), [_xjob("1")],             unmatched=0, matched=1, new=0, mock_bindings=mock_bindings)  # noqa
    run_test(snodes(), [_xjob("1"), _xjob("2")], unmatched=0, matched=2, new=1, mock_bindings=mock_bindings)
    # fmt:on


def test_hpc() -> None:
    register_result_handler(DefaultContextHandler("[default]"))

    # with open("conf/complexes") as fr:
    #     parallel_environments.set_complexes({}, fr.readlines())

    def _bindings() -> MockClusterBinding:
        mock_bindings = MockClusterBinding()
        mock_bindings.add_nodearray("hpc1", {}, max_placement_group_size=11)
        mock_bindings.add_bucket(
            "hpc1",
            "Standard_F4",
            100,
            100,
            placement_groups=["hpc_q_mpislots1", "hpc_q_mpislots2"],
        )
        mock_bindings.add_nodearray("hpc2", {}, max_placement_group_size=13)
        mock_bindings.add_bucket(
            "hpc2", "Standard_F2", 100, 100, placement_groups=["hpc2_q_pg0"]
        )
        return mock_bindings

    def run_test(
        expected: int, snodes: List[SchedulerNode], *qsubs: str
    ) -> DemandResult:
        config = {
            "_mock_bindings": _bindings(),
            "lock_file": None,
            "logging": {"config_file": "conf/logging.conf"},
            "default_resources": [
                {"select": {}, "name": "slots", "value": "node.vcpu_count"}
            ],
            "gridengine": {"relevant_complexes": ["slots", "exclusive"]},
        }
        hpclogging.initialize_logging(config)

        parallel_environments.initialize(
            config,
            "conf/complexes",
            {
                "mpislots1": "conf/mpislots",
                "mpislots2": "conf/mpislots",
                "smpslots": "conf/smpslots",
            },
            {
                "hpc.q": {
                    "qname": "hpc.q",
                    "hostlist": "@hpc.q",
                    "pe_list": "mpislots1 mpislots2 smpslots",
                }
            },
        )

        qsub = mock_driver.MockQsub()
        for qsub_cmd in qsubs:
            qsub.qsub(qsub_cmd)
        jobs = qsub.parse_jobs()
        assert len(jobs) == len(qsubs)
        import sys, json

        json.dump(jobs, sys.stdout, indent=2, default=lambda x: x.to_dict())
        ge_driver = mock_driver.MockGridEngineDriver(snodes, jobs)
        demand_result = autoscaler.autoscale_grid_engine(
            config, ge_driver, node_history=NullNodeHistory()
        )

        for node in demand_result.new_nodes:
            print("HDR", node, node.placement_group, node.available["slots"])

        assert expected == len(demand_result.new_nodes), "%s != %s" % (
            expected,
            len(demand_result.new_nodes),
        )

        return demand_result

    # using F4s, so 4 cores
    run_test(0, [], "-l exclusive -pe mpi* 14  -q hpc.q sleep.sh")
    # run_test(0, [], "-l exclusive -pe mpi* 16  -q hpc.q sleep.sh")
    # run_test(8, [], "-l exclusive -pe mpi* 8  -q hpc.q sleep.sh")


test_hpc()
