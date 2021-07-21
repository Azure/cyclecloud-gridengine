import logging
import sys
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, TreeBuilder

from hpc.autoscale.job.job import Job
from hpc.autoscale.job.nodequeue import NodeQueue
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.node import Node
from hpc.autoscale.results import EarlyBailoutResult

from gridengine.driver import GENodeQueue, GridEngineDriver, _parse_jobs
from gridengine.environment import GridEngineEnvironment


def _elem(parent: Element, tag: str, data: Optional[str] = None) -> Element:
    b = TreeBuilder()
    b.start(tag, {})  # types: ignore
    if data:
        b.data(data)
    ret = b.end(tag)
    parent.append(ret)
    return ret


class MockQsub:
    def __init__(self, ge_env: GridEngineEnvironment, config: Dict = {}) -> None:
        self.doc = Element("gridengine")
        self.job_info = _elem(self.doc, "job_info")
        self.current_job_number = 1
        self.ge_env = ge_env
        self.config = config

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
        # optional - only used for array jobs, i.e. qsub -t 100
        tasks = None

        while n < len(toks):
            c = toks[n]

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
                tasks = toks[n]
                if "-" in tasks and ":" not in tasks:
                    tasks = "{}:1".format(tasks)

            n = n + 1

        _elem(job_list, "slots", slots)

        for key, value in resources.items():
            _elem(job_list, "hard_request", value).attrib["name"] = key

        if qname:
            _elem(job_list, "hard_req_queue", qname)

        if tasks:
            _elem(job_list, "tasks", tasks)

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
        ret = _parse_jobs(self.doc, self.ge_env, self.config)

        from gridengine import util

        util.json_dump(ret)
        return ret


class MockGridEngineDriver:
    def __init__(self, ge_env: GridEngineEnvironment) -> None:
        self.ge_env = ge_env
        self.drained: Dict[str, Node] = {}

    def node_prioritizer(self, node: Node) -> int:
        return 0

    def early_bailout(self, node: Node) -> EarlyBailoutResult:
        return EarlyBailoutResult("success")

    def preprocess_config(self, config: Dict) -> Dict:
        conf = GridEngineDriver(config, self.ge_env).preprocess_config(config)
        return conf

    def handle_failed_nodes(self, nodes: List[Node]) -> List[Node]:
        return []

    def handle_post_join_cluster(self, nodes: List[Node]) -> List[Node]:
        return nodes

    def handle_draining(self, unmatched_nodes: List[Node]) -> None:
        for node in unmatched_nodes:
            if node.hostname not in self.drained:
                if node.hostname in self.ge_env.current_hostnames:
                    self.drained[node.hostname] = node
            else:
                assert self.drained[node.hostname] == node

    def handle_join_cluster(self, matched_nodes: List[Node]) -> List[Node]:
        ret = []
        for node in matched_nodes:
            if node.hostname and node.hostname not in self.ge_env.current_hostnames:
                self.ge_env.add_node(SchedulerNode(node.hostname, node.resources))
                ret.append(node)
        return ret

    def handle_post_delete(self, nodes_to_delete: List[Node]) -> None:
        if not nodes_to_delete:
            logging.warn("Empty or None passed into handle_post_delete")
            return

        for node in nodes_to_delete:
            self.ge_env.delete_node(node)

    def handle_undraining(self, matched_nodes: List[Node]) -> None:
        for node in matched_nodes:
            self.drained.pop(node.hostname, node)

    def clean_hosts(self, invalid_nodes: Optional[List[Node]]) -> None:
        pass

    def new_node_queue(self) -> NodeQueue:
        return GENodeQueue()
