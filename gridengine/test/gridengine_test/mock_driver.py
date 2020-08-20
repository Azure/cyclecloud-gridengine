import logging
import sys
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, TreeBuilder

from gridengine import parallel_environments
from gridengine.driver import GENodeQueue, _parse_jobs
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.nodequeue import NodeQueue
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.node import Node
from hpc.autoscale.results import EarlyBailoutResult
from hpc.autoscale.util import partition_single


def _elem(parent: Element, tag: str, data: Optional[str] = None) -> Element:
    b = TreeBuilder()
    b.start(tag)  # types: ignore
    if data:
        b.data(data)
    ret = b.end(tag)
    parent.append(ret)
    return ret


class MockQsub:
    def __init__(self) -> None:
        self.doc = Element("gridengine")
        self.job_info = _elem(self.doc, "job_info")
        self.current_job_number = 1

    def qsub(self, expr: str) -> None:
        job_list = _elem(self.job_info, "job_list")
        _elem(job_list, "JB_job_number", str(self.current_job_number))
        self.current_job_number += 1

        n = 0
        toks = expr.split()
        qname = None
        pe_name = None
        pe_size = None
        resources = {}
        slots = "1"

        while n < len(toks):
            c = toks[n]
            print("parse", c)
            if c == "-l":
                n = n + 1
                for stok in toks[n].split(":"):
                    if "=" in stok:
                        key, value = stok.split("=")
                    else:
                        key = stok
                        value = "1"
                    resources[key] = value
            elif c == "-q":
                n = n + 1
                qname = toks[n]
            elif c == "-pe":
                pe_name = toks[n + 1]
                pe_size = toks[n + 2]
                slots = str(pe_size)
                n = n + 2
            elif c == "-t":
                n = n + 1
                resources["tasks"] = toks[n]
            else:
                print("ignore", c)

            n = n + 1

        _elem(job_list, "slots", slots)

        for key, value in resources.items():
            _elem(job_list, "hard_resource", value).attrib["name"] = key

        if qname:
            _elem(job_list, "hard_req_queue", qname)

        if pe_name:
            pe_elem = _elem(job_list, "requested_pe", pe_size)
            pe_elem.attrib["name"] = pe_name

    def qstat(self, writer: Any = sys.stdout) -> None:
        from xml.dom import minidom

        writer.write(ElementTree.tostring(self.doc).decode())
        xmlstr = minidom.parseString(
            ElementTree.tostring(self.doc).decode()
        ).toprettyxml(indent="   ")
        writer.write(xmlstr)

    def parse_jobs(self) -> List[Job]:
        ge_queues = partition_single(
            parallel_environments.read_queue_configs({}), lambda g: g.qname
        )
        print(ge_queues)
        return _parse_jobs(self.doc, ge_queues)


class MockGridEngineDriver:
    def __init__(self, scheduler_nodes: List[SchedulerNode], jobs: List[Job]):
        self.scheduler_nodes = scheduler_nodes
        self.jobs = jobs
        self.drained: Dict[str, Node] = {}

    def node_prioritizer(self, node: Node) -> int:
        return 0

    def early_bailout(self, node: Node) -> EarlyBailoutResult:
        return EarlyBailoutResult("success")

    @property
    def current_hostnames(self) -> List[str]:
        return [n.hostname for n in self.scheduler_nodes]

    def preprocess_config(self, config: Dict) -> Dict:
        return config

    def handle_failed_nodes(self, nodes: List[Node]) -> List[Node]:
        return []

    def handle_post_join_cluster(self, nodes: List[Node]) -> List[Node]:
        return nodes

    def handle_draining(self, unmatched_nodes: List[Node]) -> None:
        for node in unmatched_nodes:
            if node.hostname not in self.drained:
                if node.hostname in self.current_hostnames:
                    self.drained[node.hostname] = node
            else:
                assert self.drained[node.hostname] == node

    def handle_join_cluster(self, matched_nodes: List[Node]) -> List[Node]:
        ret = []
        for node in matched_nodes:
            if node.hostname and node.hostname not in self.current_hostnames:
                self.scheduler_nodes.append(
                    SchedulerNode(node.hostname, node.resources)
                )
                ret.append(node)
        return ret

    def handle_post_delete(self, nodes_to_delete: List[Node]) -> None:
        if not nodes_to_delete:
            logging.warn("Empty or None passed into handle_post_delete")
            return

        for node in nodes_to_delete:
            if node.hostname in self.current_hostnames:
                self.scheduler_nodes = [
                    t for t in self.scheduler_nodes if t.hostname != node.hostname
                ]

    def handle_undraining(self, matched_nodes: List[Node]) -> None:
        for node in matched_nodes:
            self.drained.pop(node.hostname, node)

    def clean_hosts(self, invalid_nodes: Optional[List[Node]]) -> None:
        pass

    def new_node_queue(self) -> NodeQueue:
        return GENodeQueue()
