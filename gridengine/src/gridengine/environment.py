from copy import deepcopy
from typing import Dict, List, Optional

from hpc.autoscale.job.job import Job
from hpc.autoscale.node.node import Node
from hpc.autoscale.util import partition_single

from gridengine import hostgroup as hglib
from gridengine.complex import Complex, read_complexes
from gridengine.parallel_environments import (
    ParallelEnvironment,
    read_parallel_environments,
)
from gridengine.qbin import QBin, QBinImpl
from gridengine.queue import GridEngineQueue, read_queues
from gridengine.scheduler import GridEngineScheduler, read_scheduler


class GridEngineEnvironment:
    def __init__(
        self,
        scheduler: GridEngineScheduler,
        jobs: Optional[List[Job]] = None,
        nodes: Optional[List[Node]] = None,
        queues: Optional[Dict[str, GridEngineQueue]] = None,
        hostgroups: Optional[List[hglib.Hostgroup]] = None,
        pes: Optional[Dict[str, ParallelEnvironment]] = None,
        complexes: Optional[Dict[str, Complex]] = None,
        unfiltered_complexes: Optional[Dict[str, Complex]] = None,
        qbin: Optional[QBin] = None,
    ) -> None:
        self.__scheduler = scheduler
        self.__jobs: List[Job] = jobs or []
        self.__nodes: Dict[str, Node] = partition_single(
            nodes or [], lambda n: n.hostname_or_uuid.lower()
        )
        self.__queues: Dict[str, GridEngineQueue] = queues or {}
        self.__pes: Dict[str, ParallelEnvironment] = pes or {}
        self.__complexes: Dict[str, Complex] = complexes or {}

        if unfiltered_complexes:
            self.__unfiltered_complexes = unfiltered_complexes
        else:
            self.__unfiltered_complexes = deepcopy(self.__complexes)

        self.__qbin = qbin or QBinImpl()

        self.__hostgroups = partition_single(hostgroups or [], lambda h: h.name)
        self.__host_memberships: Dict[str, List[str]] = {}

        for hostgroup in self.__hostgroups.values():
            for host in hostgroup.members:
                if host not in self.__host_memberships:
                    self.__host_memberships[host] = []
                self.__host_memberships[host].append(hostgroup.name)

    @property
    def scheduler(self) -> GridEngineScheduler:
        return self.__scheduler

    @property
    def jobs(self) -> List[Job]:
        return self.__jobs

    @property
    def nodes(self) -> List[Node]:
        return list(self.__nodes.values())

    def add_node(self, node: Node) -> None:
        self.__nodes[node.hostname_or_uuid.lower()] = node

    def delete_node(self, node: Node) -> None:
        self.__nodes.pop(node.hostname_or_uuid.lower(), 1)

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

    @property
    def unfiltered_complexes(self) -> Dict[str, Complex]:
        return self.__unfiltered_complexes

    @property
    def hostgroups(self) -> Dict[str, hglib.Hostgroup]:
        return self.__hostgroups

    @property
    def host_memberships(self) -> Dict[str, List[str]]:
        return self.__host_memberships

    @property
    def is_uge(self) -> bool:
        return self.qbin.is_uge


def from_qconf(
    autoscale_config: Dict, qbin: Optional[QBin] = None
) -> GridEngineEnvironment:
    from gridengine import driver

    qbin = qbin or QBinImpl()

    scheduler = read_scheduler(qbin)
    hostgroups = hglib.read_hostgroups(autoscale_config, qbin)
    pes = read_parallel_environments(autoscale_config, qbin)
    complexes = read_complexes(autoscale_config, qbin)

    unfiltered_complexes = read_complexes({}, qbin)

    queues = read_queues(autoscale_config, scheduler, pes, hostgroups, complexes, qbin)

    ge_env = GridEngineEnvironment(
        scheduler,
        hostgroups=hostgroups,
        pes=pes,
        complexes=complexes,
        queues=queues,
        unfiltered_complexes=unfiltered_complexes,
    )

    jobs, nodes = driver._get_jobs_and_nodes(autoscale_config, ge_env)

    ge_env.jobs.extend(jobs)

    for node in nodes:
        ge_env.add_node(node)

    return ge_env
