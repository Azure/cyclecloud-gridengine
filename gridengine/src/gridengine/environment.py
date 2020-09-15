from typing import Dict, List, Optional

from hpc.autoscale.job.job import Job
from hpc.autoscale.node.node import Node

from gridengine.complex import Complex, read_complexes
from gridengine.parallel_environments import (
    ParallelEnvironment,
    read_parallel_environments,
)
from gridengine.qbin import QBin, QBinImpl
from gridengine.queue import GridEngineQueue, read_queue_configs


class GridEngineEnvironment:
    def __init__(
        self,
        jobs: Optional[List[Job]] = None,
        nodes: Optional[List[Node]] = None,
        queues: Optional[Dict[str, GridEngineQueue]] = None,
        pes: Optional[Dict[str, ParallelEnvironment]] = None,
        complexes: Optional[Dict[str, Complex]] = None,
        qbin: Optional[QBin] = None,
    ) -> None:
        self.__jobs: List[Job] = jobs or []
        self.__nodes: List[Node] = nodes or []
        self.__queues: Dict[str, GridEngineQueue] = queues or {}
        self.__pes: Dict[str, ParallelEnvironment] = pes or {}
        self.__complexes: Dict[str, Complex] = complexes or {}
        self.__qbin = qbin or QBinImpl()

    @property
    def jobs(self) -> List[Job]:
        return self.__jobs

    def add_job(self, job: Job) -> None:
        self.__jobs.append(job)

    @property
    def nodes(self) -> List[Node]:
        return self.__nodes

    def add_node(self, node: Node) -> None:
        self.__nodes.append(node)

    def delete_node(self, node: Node) -> None:
        self.__nodes = [n for n in self.nodes if n.hostname != node.hostname]

    @property
    def current_hostnames(self) -> List[str]:
        return [n.hostname for n in self.nodes]

    @property
    def queues(self) -> Dict[str, GridEngineQueue]:
        return self.__queues

    @property
    def qbin(self) -> QBin:
        return self.__qbin

    def add_queue(self, gequeue: GridEngineQueue) -> None:
        assert gequeue
        assert gequeue.qname
        assert gequeue.qname not in self.__queues
        self.__queues[gequeue.qname] = gequeue

    @property
    def pes(self) -> Dict[str, ParallelEnvironment]:
        return self.__pes

    def add_pe(self, pe: ParallelEnvironment) -> None:
        assert pe
        assert pe.name
        assert pe.name not in self.__pes
        self.__pes[pe.name] = pe

    @property
    def complexes(self) -> Dict[str, Complex]:
        return self.__complexes

    def add_complex(self, complex: Complex) -> None:
        assert complex
        assert complex.name
        assert complex.shortcut not in self.__complexes
        assert complex.name not in self.__complexes
        self.__complexes[complex.name] = complex
        self.__complexes[complex.shortcut] = complex


def from_qconf(
    autoscale_config: Dict, qbin: Optional[QBin] = None
) -> GridEngineEnvironment:
    from gridengine import driver

    qbin = qbin or QBinImpl()

    pes = read_parallel_environments(autoscale_config, qbin)
    complexes = read_complexes(autoscale_config, qbin)
    ge_env = GridEngineEnvironment([], [], {}, pes, complexes)

    read_queue_configs(autoscale_config, ge_env)
    jobs, nodes = driver._get_jobs_and_nodes(autoscale_config, qbin, ge_env.queues)
    ge_env.jobs.extend(jobs)
    ge_env.nodes.extend(nodes)
    return ge_env
