import json
import os
from typing import Dict, List, Optional, Tuple

from hpc.autoscale import hpclogging as logging
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.node import Node
from hpc.autoscale.util import partition_single

from gridengine.driver import GridEngineDriver
from gridengine.util import check_output


class DeferredDriver(GridEngineDriver):
    def __init__(self, autoscale_config: Dict) -> None:

        cyclecloud_home = os.getenv("CYCLECLOUD_HOME", "/opt/cycle/jetpack")
        default_dir = os.path.join(
            cyclecloud_home, "system", "bootstrap", "gridengine", "driver_scripts"
        )
        self.scripts_dir = os.path.abspath(
            autoscale_config.get("gridengine", {}).get(
                "driver_scripts_dir", default_dir
            )
        )

        if not os.path.exists(self.scripts_dir):
            logging.warning(
                "Driver scripts directory does not exist: %s", self.scripts_dir
            )

        logging.warning("Overriding driver with scripts is an experimental feature.")

        super().__init__(autoscale_config)

    def get_jobs_and_nodes(self) -> Tuple[List[Job], List[SchedulerNode]]:
        script = os.path.join(self.scripts_dir, "get_jobs_and_nodes.sh")

        if os.path.exists(script):
            response_str = check_output([script]).decode()
            response = json.loads(response_str)

            jobs = [Job.from_dict(d) for d in response["jobs"]]
            nodes = [SchedulerNode.from_dict(d) for d in response["nodes"]]
            return (jobs, nodes)

        logging.debug("%s does not exist, using default implementation.", script)
        return super().get_jobs_and_nodes()

    def handle_draining(self, unmatched_nodes: List[Node]) -> List[Node]:
        return self.defer("handle_draining", unmatched_nodes)

    def handle_join_cluster(self, matched_nodes: List[Node]) -> List[Node]:
        return self.defer("handle_join_cluster", matched_nodes)

    def handle_post_join_cluster(self, nodes: List[Node]) -> List[Node]:
        return self.defer("handle_post_join_cluster", nodes)

    def handle_post_delete(self, nodes_to_delete: List[Node]) -> None:
        self.defer("handle_post_delete", nodes_to_delete)

    def add_nodes_to_cluster(self, nodes: List[Node]) -> List[Node]:
        return self.defer("add_nodes_to_cluster", nodes)

    def handle_failed_nodes(self, nodes: List[Node]) -> List[Node]:
        return self.defer("handle_failed_nodes", nodes)

    def handle_boot_timeout(self, nodes: List[Node]) -> List[Node]:
        return self.defer("handle_boot_timeout", nodes)

    def clean_hosts(self, invalid_nodes: Optional[List[Node]]) -> None:
        if invalid_nodes is None:
            invalid_nodes = [
                n for n in self.scheduler_nodes if n.metadata["state"] == "au"
            ]
        self.defer("clean_hosts", invalid_nodes)

    def defer(self, func_name: str, nodes: List[Node]) -> List[Node]:
        script = os.path.join(self.scripts_dir, "{}.sh".format(func_name))
        if os.path.exists(script):
            hostnames = [x.hostname for x in nodes if x.hostname]
            if not hostnames:
                return []

            specified_hostnames = (
                check_output([script] + hostnames).decode().lower().split()
            )
            by_hostname = partition_single(
                nodes, lambda node: node.hostname_or_uuid.lower()
            )
            ret = []
            for hostname in specified_hostnames:
                node = by_hostname.get(hostname)
                if node:
                    ret.append(node)
            return ret
        else:
            logging.debug("%s does not exist, using default implementation.", script)
            method = getattr(GridEngineDriver, func_name)
            return method(self, nodes)

    def __str__(self) -> str:
        return "Deferred({}, script_dir={})".format(super().__str__(), self.scripts_dir)
