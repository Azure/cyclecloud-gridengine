import math
import socket
from subprocess import CalledProcessError, STDOUT
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from xml.etree import ElementTree
from xml.etree.ElementTree import Element

import six
from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.job.demandcalculator import DemandCalculator
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.nodequeue import NodeQueue
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.constraints import (
    BaseNodeConstraint,
    NodeConstraint,
    ReadOnlyAlias,
    XOr,
    register_parser,
)
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodemanager import NodeManager
from hpc.autoscale.results import EarlyBailoutResult, SatisfiedResult
from hpc.autoscale.util import partition

from gridengine import environment as envlib
from gridengine.parallel_environments import (
    FixedProcesses,
    GridEngineQueue,
    ParallelEnvironment,
)
from gridengine.util import (
    QCONF_PATH,
    QMOD_PATH,
    QSTAT_PATH,
    call,
    check_call,
    check_output,
)


def new_driver(
    autoscale_config: Dict, ge_env: envlib.GridEngineEnvironment,
) -> "GridEngineDriver":
    return GridEngineDriver(autoscale_config, ge_env)


class GridEngineDriver:
    def __init__(
        self, autoscale_config: Dict, ge_env: envlib.GridEngineEnvironment
    ) -> None:
        self.autoscale_config = autoscale_config
        self.ge_env = ge_env
        self.read_only = autoscale_config.get("read_only", False)
        if self.read_only is None:
            self.read_only = False

    def handle_draining(
        self, unmatched_nodes: List[SchedulerNode]
    ) -> List[SchedulerNode]:
        if self.read_only:
            return []

        to_shutdown: List[SchedulerNode] = []
        for node in unmatched_nodes:
            if node.hostname:
                wc_queue_list_expr = "*@{}".format(node.hostname)
                try:
                    check_output([QMOD_PATH, "-d", wc_queue_list_expr], stderr=STDOUT)
                    to_shutdown.append(node)
                except CalledProcessError as e:
                    msg = 'invalid queue'

                    if e.stdout and msg in e.stdout.decode():
                        # the node isn't even part of any queue anyways.
                        logging.info("Ignoring failed qmod -d, as the hostname is no longer associated with a queue")
                        to_shutdown.append(node)
                    else:
                        logging.error(
                            "Could not drain %s: %s. Will not shutdown node. ",
                            node,
                            e.stdout.decode() if e.stdout else str(e),
                        )
        return to_shutdown

    def get_jobs_and_nodes(self) -> Tuple[List[Job], List[SchedulerNode]]:
        return _get_jobs_and_nodes(self.autoscale_config, self.ge_env.queues)

    def handle_post_delete(self, nodes_to_delete: List[Node]) -> None:
        if self.read_only:
            return

        logging.getLogger("gridengine.driver").info("handle_post_delete")

        fqdns = check_output([QCONF_PATH, "-sh"]).decode().lower().split()
        admin_hosts = [n.split(".")[0] for n in fqdns]

        fqdns = check_output([QCONF_PATH, "-ss"]).decode().lower().split()
        submit_hosts = [n.split(".")[0] for n in fqdns]

        fqdns = check_output([QCONF_PATH, "-sel"]).decode().lower().split()
        exec_hosts = [n.split(".")[0] for n in fqdns]

        hostlists = check_output([QCONF_PATH, "-shgrpl"]).decode().lower().split()

        by_hostlist: Dict[str, List[str]] = {}
        for hostlist in hostlists:
            fqdns = (
                check_output([QCONF_PATH, "-shgrp_resolved", hostlist])
                .decode()
                .lower()
                .split()
            )
            by_hostlist[hostlist] = [n.split(".")[0] for n in fqdns]

        by_queue = partition(
            nodes_to_delete,
            lambda n: (n.resources.get("slot_type") or n.nodearray) + ".q",
        )

        hostnames_to_delete: Set[str] = set()

        for queue_name, nodes in by_queue.items():

            for node in nodes:
                if not node.hostname:
                    continue

                hostname = node.hostname.lower()

                try:
                    logging.info(
                        "Removing host %s via qconf -dh, -de and -ds", hostname
                    )
                    # we need to remove these from the hostgroups first, otherwise
                    # we can't remove the node
                    for hostlist_name, hosts in by_hostlist.items():
                        if hostname in hosts:
                            call(
                                [
                                    QCONF_PATH,
                                    "-dattr",
                                    "hostgroup",
                                    "hostlist",
                                    node.hostname,
                                    hostlist_name,
                                ]
                            )
                    if queue_name not in self.ge_env.queues:
                        if queue_name != "unknown.q":
                            logging.error(
                                "Queue %s does not exist? Ignoring node %s",
                                queue_name,
                                node,
                            )
                        logging.info("Queue is unknown.q for %s.", node.hostname)
                    else:                        
                        queue_config = self.ge_env.queues[queue_name]

                        if hostname in queue_config.slots:
                            queue_host = "{}@{}".format(queue_name, hostname)
                            slots = queue_config.slots[hostname]

                            call(
                                [
                                    QCONF_PATH,
                                    "-dattr",
                                    "queue",
                                    "slots",
                                    str(slots),
                                    queue_host,
                                ]
                            )

                    hostnames_to_delete.add(hostname)

                except CalledProcessError as e:
                    logging.warning(str(e))

        # now that we have removed all slots entries from all queues, we can
        # delete the hosts. If you don't do this, it will complain that the hosts
        # are still referenced.
        try:
            for hostname in hostnames_to_delete:
                if hostname in admin_hosts:
                    call([QCONF_PATH, "-dh", node.hostname])

                if hostname in submit_hosts:
                    call([QCONF_PATH, "-ds", node.hostname])

                if hostname in exec_hosts:
                    call([QCONF_PATH, "-de", node.hostname])
                else:
                    logging.warning("%s not in %s", hostname, exec_hosts)
        except CalledProcessError as e:
            logging.warning(str(e))

    def handle_undraining(self, matched_nodes: List[Node]) -> List[Node]:
        if self.read_only:
            return []

        # TODO get list of hosts in @disabled
        logging.getLogger("gridengine.driver").info("handle_undraining")

        undrained: List[SchedulerNode] = []
        for node in matched_nodes:
            if node.hostname:
                wc_queue_list_expr = "*@{}".format(node.hostname)
                try:
                    check_call([QMOD_PATH, "-e", wc_queue_list_expr])
                    undrained.append(node)
                except CalledProcessError as e:
                    logging.error(
                        "Could not undrain %s: %s.", node, str(e),
                    )
        return undrained

    def handle_join_cluster(self, matched_nodes: List[Node]) -> List[Node]:
        """
        """
        if self.read_only:
            return []

        logging.getLogger("gridengine.driver").info("handle_join_cluster")

        # TODO rethink this RDH
        # self.handle_undraining(matched_nodes)

        _hostgroup_cache: Dict[str, List[str]] = {}

        def _get_hostlist(hostgroup: str) -> List[str]:
            if hostgroup not in _hostgroup_cache:
                fqdns = (
                    check_output([QCONF_PATH, "-shgrp_resolved", hostgroup])
                    .decode()
                    .lower()
                    .split()
                )
                _hostgroup_cache[hostgroup] = [fqdn.split(".")[0] for fqdn in fqdns]
            return _hostgroup_cache[hostgroup]

        joined_nodes = self.add_nodes_to_cluster(matched_nodes)

        completed_nodes = []

        for node in joined_nodes:
            hostgroups_expr = node.software_configuration.get("gridengine_hostgroups")
            if not hostgroups_expr:
                logging.warning(
                    "No hostgroups found for node %s - %s",
                    node,
                    node.software_configuration,
                )
                continue

            # TODO assert these are @hostgroups
            hostgroups = hostgroups_expr.split(" ")

            for hostgroup in hostgroups:
                if not node.hostname:
                    continue

                if not hostgroup.startswith("@"):
                    # hostgroups have to start with @
                    continue

                hostlist_for_hg = _get_hostlist(hostgroup)
                if node.hostname.lower() in hostlist_for_hg:
                    continue

                logging.info(
                    "Adding hostname %s to hostgroup %s", node.hostname, hostgroup
                )

                check_call(
                    [
                        QCONF_PATH,
                        "-aattr",
                        "hostgroup",
                        "hostlist",
                        node.hostname,
                        hostgroup,
                    ]
                )

                for res_name, res_value in node.resources.items():
                    if res_name not in self.ge_env.complexes:
                        logging.fine("Ignoring unknown complex %s", res_name)
                        continue

                    check_call(
                        [
                            QCONF_PATH,
                            "-mattr",
                            "exechost",
                            "complex_values",
                            "{}={}".format(res_name, res_value),
                            node.hostname
                        ]
                    )

            completed_nodes.append(node)

        return completed_nodes

    def handle_post_join_cluster(self, nodes: List[Node]) -> List[Node]:
        """
            feel free to set complexes / resources on the node etc.
        """
        if self.read_only:
            return []

        return nodes

    def handle_failed_nodes(self, nodes: List[Node]) -> List[Node]:
        if self.read_only:
            return []

        if not nodes:
            return nodes
        logging.error("The following nodes are in a failed state: %s", nodes)
        return nodes

    def handle_boot_timeout(self, nodes: List[Node]) -> List[Node]:
        if self.read_only:
            return []

        if not nodes:
            return nodes
        logging.error("The following nodes have not booted in time: %s", nodes)
        return nodes

    def add_nodes_to_cluster(self, nodes: List[Node]) -> List[Node]:
        if self.read_only:
            return []

        logging.getLogger("gridengine.driver").info("add_nodes_to_cluster")

        if not nodes:
            return []

        ret: List[Node] = []
        fqdns = check_output([QCONF_PATH, "-sh"]).decode().splitlines()
        admin_hostnames = [fqdn.split(".")[0] for fqdn in fqdns]

        fqdns = check_output([QCONF_PATH, "-ss"]).decode().splitlines()
        submit_hostnames = [fqdn.split(".")[0] for fqdn in fqdns]

        for node in nodes:
            if self._add_node_to_cluster(node, admin_hostnames, submit_hostnames):
                ret.append(node)
        return ret

    def _add_node_to_cluster(
        self, node: Node, admin_hostnames: List[str], submit_hostnames: List[str]
    ) -> bool:
        if node.state == "Failed":
            logging.warning("Ignoring failed node %s", node)
            return False

        if not node.exists:
            logging.trace("%s does not exist yet, can not add to cluster.", node)
            return False

        if not node.hostname:
            logging.trace(
                "%s does not have a hostname yet, can not add to cluster.", node
            )
            return False

        # let's make sure the hostname is valid and reverse
        # dns compatible before adding to GE
        try:
            addr_info = socket.gethostbyaddr(node.private_ip)
        except Exception as e:
            logging.error(
                "Could not convert private_ip(%s) to hostname using gethostbyaddr() for %s: %s",
                node.private_ip,
                node,
                str(e),
            )
            return False

        addr_info_ips = addr_info[-1]
        if isinstance(addr_info_ips, str):
            addr_info_ips = [addr_info_ips]

        if node.private_ip not in addr_info_ips:
            logging.warning(
                "Node %s has a hostname that does not match the"
                + " private_ip (%s) reported by cyclecloud (%s)! Skipping",
                node,
                addr_info_ips,
                node.private_ip,
            )
            return False

        addr_info_hostname = addr_info[0].split(".")[0]
        if addr_info_hostname.lower() != node.hostname.lower():
            logging.warning(
                "Node %s has a hostname that can not be queried via reverse"
                + " dns (private_ip=%s cyclecloud hostname=%s reverse dns hostname=%s)."
                + " This is common and usually repairs itself. Skipping",
                node,
                node.private_ip,
                node.hostname,
                addr_info_hostname,
            )
            return False

        try:
            if node.hostname not in admin_hostnames:
                logging.debug("Adding %s as administrative host", node)
                check_call([QCONF_PATH, "-ah", node.hostname])

            if node.hostname not in submit_hostnames:
                logging.debug("Adding %s as submit host", node)

                ge_qname = node.software_configuration.get("gridengine_qname", "")
                if not ge_qname:
                    logging.warning(
                        "gridengine_qname is not set on %s. Ignoring.", node
                    )
                    return False

                for qname in ["all.q", ge_qname]:
                    check_call(
                        [
                            QCONF_PATH,
                            "-mattr",
                            "queue",
                            "slots",
                            str(node.resources["slots"]),
                            "{}@{}".format(qname, node.hostname),
                        ]
                    )

                check_call([QCONF_PATH, "-as", node.hostname])

        except CalledProcessError as e:
            logging.warn("Could not add %s to cluster: %s", node, str(e))
            return False

        return True

    def clean_hosts(self, invalid_nodes: List[SchedulerNode]) -> None:
        logging.getLogger("gridengine.driver").info("clean_hosts")

        if not invalid_nodes:
            return

        logging.warning(
            "Cleaning out the following hosts in state=au: %s", invalid_nodes
        )
        self.handle_post_delete(invalid_nodes)
        
        to_clean = list([n for n in self.ge_env.nodes if n not in invalid_nodes])

        for node in to_clean:
            self.ge_env.nodes.remove(node)

    def preprocess_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
            # set site specific defaults. In this example, slots are defined
            # as the number of gigs per node.
            from hpc.autoscale import hpctypes as ht

            config["gridengine"] = ge_config = config.get("gridengine", {})
            if not ge_config.get("default_resources"):
                one_gig = ht.Memory.value_of("1g")
                ge_config["default_resources"] = [
                    {
                        "select": {},
                        "name": "slots",
                        "value": lambda node: node.memory // one_gig,
                    }
                ]

            return config
        """

        config["nodearrays"] = nodearrays = config.get("nodearrays", {})
        nodearrays["default"] = default = nodearrays.get("default", {})
        default["placement_groups"] = default_pgs = default.get("placement_groups", [])

        if default_pgs:
            return config

        for gqueue in self.ge_env.queues.values():
            for pe in gqueue.get_pes():
                if pe:
                    pg_name = pe.get_placement_group(gqueue.qname)
                    if pg_name:
                        default_pgs.append(pg_name)
        return config

    def preprocess_buckets(self, node_mgr: NodeManager) -> NodeManager:
        """
            # A toy example: disable all but certain regions given
            # time of day
            import time
            now = time.localtime()
            if now.tm_hour < 9:
                limit_regions = ["westus", "westus2"]
            else:
                limit_regions = ["eastus"]

            for bucket in node_mgr.get_buckets():

                if bucket.location not in limit_regions:
                    bucket.enabled = False
        """
        # by_pg = partition(node_mgr.get_buckets(), lambda b: b.placement_group)
        # for queue_config in parallel_environments.read_queue_configs():
        #     for pe in queue_config.get_pes():
        #         if pe.requires_placement_groups:
        #             placement_group = ht.PlacementGroup("{}_{}".format(ge_queue.qname, pe.name))
        #             placement_group = re.sub("[^a-zA-z0-9-_]", "_", placement_group)
        #             """
        #         Just pre-declare the pgs in the autoscale config!
        #         """
        #             if placement_group not in by_pg:
        #                 node_mgr._add_bucket()

        return node_mgr

    def preprocess_node_mgr(self, node_mgr: NodeManager) -> NodeManager:
        """
            # A toy example: disable all but certain regions given
            # time of day
            import time
            now = time.localtime()
            if now.tm_hour < 9:
                limit_regions = ["westus", "westus2"]
            else:
                limit_regions = ["eastus"]

            for bucket in node_mgr.get_buckets():

                if bucket.location not in limit_regions:
                    bucket.enabled = False
        """

        return node_mgr

    def preprocess_demand_calculator(self, dcalc: DemandCalculator) -> DemandCalculator:
        """
            dcalc.allocate({})
        """

        return dcalc

    def postprocess_demand_calculator(
        self, dcalc: DemandCalculator
    ) -> DemandCalculator:
        """
            # simple example to ensure there are at least 100 nodes
            minimum = 100
            to_allocate = minimum - len(dcalc.get_nodes())
            if to_allocate > 0:
                result = dcalc.allocate(
                    {"node.nodearray": "htc", "exclusive": 1}, node_count=to_allocate
                )
                if not result:
                    logging.warning("Failed to allocate minimum %s", result)
                else:
                    logging.info(
                        "Allocated %d more nodes to reach minimum %d", to_allocate, minimum
                    )
        """
        return dcalc

    def new_node_queue(self) -> NodeQueue:
        return GENodeQueue()

    def __str__(self) -> str:
        # TODO RDH
        return "GEDriver(jobs={}, scheduler_nodes={})".format(
            self.ge_env.jobs[:100], self.ge_env.nodes[:100]
        )

    def __repr__(self) -> str:
        return str(self)


def _get(e: Any, attr_name: str) -> Optional[str]:
    child = e.find(attr_name)
    if child is None:
        return None
    return child.text


def _getr(e: Any, attr_name: str) -> str:
    ret = _get(e, attr_name)
    assert ret, "{} was not defined for element {}".format(attr_name, e)
    assert ret is not None
    return ret


def _get_jobs_and_nodes(
    autoscale_config: Dict, ge_queues: Dict[str, GridEngineQueue],
) -> Tuple[List[Job], List[SchedulerNode]]:
    # some magic here for the args
    # -g d -- show all the tasks, do not group
    # -u * -- all users
    # -s  pr -- show only pending or running
    # -f -- full output. Ambiguous what this means, but in this case it includes host information so that
    # we can get one consistent view (i.e. not split between two calls, introducing a race condition)
    cmd = [QSTAT_PATH, "-xml", "-s", "pr", "-r", "-f", "-F"]
    relevant_complexes = (
        autoscale_config.get("gridengine", {}).get("relevant_complexes") or []
    )
    if relevant_complexes:
        cmd.append(" ".join(relevant_complexes))
    logging.debug("Query jobs and nodes with cmd '%s'", " ".join(cmd))
    raw_xml = check_output(cmd).decode()
    raw_xml_file = six.StringIO(raw_xml)
    doc = ElementTree.parse(raw_xml_file)
    root = doc.getroot()

    nodes, running_jobs = _parse_scheduler_nodes(root, ge_queues)
    pending_jobs = _parse_jobs(root, ge_queues)
    return running_jobs + pending_jobs, nodes


def _parse_scheduler_nodes(
    root: Element, ge_queues: Dict[str, GridEngineQueue]
) -> Tuple[List[SchedulerNode], List[Job]]:
    running_jobs: Dict[str, Job] = {}
    schedulers = check_output([QCONF_PATH, "-sss"]).decode().splitlines()

    compute_nodes: Dict[str, SchedulerNode] = {}

    elems = list(root.findall("queue_info/Queue-List"))
    log_warnings = set()

    def keyfunc(e: Element) -> str:
        try:
            name_elem = e.find("name")
            if name_elem and name_elem.text:
                return name_elem.text.split("@")[0]
        except Exception:
            pass
        return str(e)

    for qiqle in sorted(elems, key=keyfunc):
        # TODO need a better way to hide the master
        name = _getr(qiqle, "name")

        queue_name, fqdn = name.split("@", 1)

        ge_queue = ge_queues.get(queue_name)
        if not ge_queue:
            logging.error("Unknown queue %s.", queue_name)
            continue

        hostname = fqdn.split(".", 1)[0]
        if hostname in schedulers:
            continue

        if hostname in compute_nodes:
            compute_node = compute_nodes[hostname]
            _assign_jobs(compute_node, qiqle, running_jobs, ge_queues)
            continue

        slots_total = int(_getr(qiqle, "slots_total"))
        resources: dict = {"slots": slots_total}
        if "slots" in ge_queue.complexes:
            resources[ge_queue.complexes["slots"].shortcut] = slots_total

        for name, default_complex in ge_queue.complexes.items():

            if name == "slots":
                continue

            if default_complex.default is None:
                continue

            if not default_complex.requestable:
                continue

            resources[name] = default_complex.default

        for res in qiqle.iter("resource"):
            resource_name = res.attrib["name"]
            complex = ge_queue.complexes.get(resource_name)
            text = res.text or "NONE"

            if complex is None:
                resources[resource_name] = text
                if resource_name not in log_warnings:
                    logging.warning(
                        "Unknown resource %s. This will be treated as a string internally and "
                        + "may cause issues with grid engine's ability to schedule this job.",
                        resource_name,
                    )
                    log_warnings.add(resource_name)
            else:
                resources[resource_name] = complex.parse(text)
                resources[complex.shortcut] = resources[resource_name]

        compute_node = SchedulerNode(hostname, resources)

        compute_node.metadata["state"] = _get(qiqle, "state") or ""

        # decrement using compute_node.available. Just use slots here
        compute_node.available["slots"] = (
            slots_total
            - int(_getr(qiqle, "slots_used"))
            + int(_getr(qiqle, "slots_resv"))
        )  # TODO resv?

        if "slots" in ge_queue.complexes:
            compute_node.available[
                ge_queue.complexes["slots"].shortcut
            ] = compute_node.available["slots"]
        _assign_jobs(compute_node, qiqle, running_jobs, ge_queues)

        compute_nodes[compute_node.hostname] = compute_node

    return list(compute_nodes.values()), list(running_jobs.values())


def _assign_jobs(
    compute_node: SchedulerNode,
    e: Element,
    running_jobs: Dict[str, Job],
    ge_queues: Dict[str, GridEngineQueue],
) -> None:
    # use assign_job so we just accept that this job is running on this node.
    for jle in e.findall("job_list"):
        parsed_running_jobs = _parse_job(jle, ge_queues) or []
        for running_job in parsed_running_jobs:
            if not running_jobs.get(running_job.name):
                running_jobs[running_job.name] = running_job
            running_jobs[running_job.name].executing_hostnames.append(
                compute_node.hostname
            )
        compute_node.assign(_getr(jle, "JB_job_number"))


def _parse_jobs(root: Element, ge_queues: Dict[str, GridEngineQueue]) -> List[Job]:
    autoscale_jobs: List[Job] = []

    for jijle in root.findall("job_info/job_list"):
        jobs = _parse_job(jijle, ge_queues)
        if jobs:
            autoscale_jobs.extend(jobs)

    return autoscale_jobs


def _parse_job(jijle: Element, ge_queues: Dict[str, GridEngineQueue]) -> Optional[Job]:
    job_state = _get(jijle, "state")

    requested_queues = [str(x.text) for x in jijle.findall("hard_req_queue")]

    if not requested_queues:
        requested_queues = ["all.q"]

    log_warnings = set()

    job_id = _getr(jijle, "JB_job_number")
    slots = int(_getr(jijle, "slots"))

    if len(requested_queues) != 1 or ("*" in requested_queues[0]):
        logging.error(
            "We support submitting to at least one and only one queue."
            + " Wildcards are also not supported."
            + " Ignoring job %s submitted to %s.",
            job_id,
            requested_queues,
        )
        return None

    qname = requested_queues[0]
    if qname not in ge_queues:
        logging.error("Unknown queue %s for job %s", qname, job_id)
        return None

    ge_queue: GridEngineQueue = ge_queues[qname]

    num_tasks = 1
    tasks_expr = _get(jijle, "tasks")

    if tasks_expr:
        num_tasks = _parse_tasks(tasks_expr)

    constraints: List[Dict] = [{"slots": slots}] + ge_queue.constraints
    alias_constraints: List[Dict] = []

    job_resources: Dict[str, str] = {}

    for req in jijle.iter("hard_request"):
        if req.text is None:
            logging.warning(
                "Found null hard_request (%s) for job %s, skipping", req, job_id
            )
            continue

        resource_name = req.attrib["name"]
        complex = ge_queue.complexes.get(resource_name)
        if not complex:
            if resource_name not in log_warnings:
                logging.warning(
                    "Unknown resource %s. This will be treated as a string internally and "
                    + "may cause issues with grid engine's ability to schedule this job.",
                    resource_name,
                )
                log_warnings.add(resource_name)
            req_value: Any = req.text
        else:
            req_value = complex.parse(req.text)

        if complex and complex.shortcut != complex.name and complex.relop != "EXCL":
            # create an alias for the shortcut
            if resource_name == complex.shortcut:
                resource_name = complex.shortcut
            alias_constraints.append(ReadOnlyAlias(complex.shortcut, complex.name))
            # else:
            #     alias_constraints.append(ReadOnlyAlias(complex.name, complex.shortcut))

        if complex and complex.name == "slots":
            # ensure we don't double count slots if someone specifies it
            constraints[0]["slots"] = req_value
        else:
            constraints.append({resource_name: req_value})
        job_resources[resource_name] = req_value

    jobs: List[Job]

    constraints.extend(alias_constraints)

    pe_elem = jijle.find("requested_pe")
    if pe_elem is not None:
        # dealing with parallel jobs (qsub -pe ) is much more complicated
        pe_jobs = _pe_job(ge_queue, pe_elem, job_id, constraints, slots, num_tasks)
        if pe_jobs is None:
            return None
        else:
            jobs = pe_jobs
    else:
        constraints.append(
            QueueAndHostgroupConstraint(
                ge_queue.qname, list(ge_queue.hostlist_groups), None
            )
        )
        jobs = [Job(job_id, constraints, iterations=num_tasks)]

    for job in jobs:
        job.metadata["gridengine"] = {
            "resources": job_resources,
        }

        job.metadata["job_state"] = job_state

    return jobs


def _pe_job(
    ge_queue: GridEngineQueue,
    pe_elem: Element,
    job_id: str,
    constraints: List[Dict],
    slots: int,
    num_tasks: int,
) -> Optional[List[Job]]:
    pe_name_expr = pe_elem.attrib["name"]
    array_size = num_tasks

    assert pe_name_expr, "{} has no attribute 'name'".format(pe_elem)
    assert pe_elem.text, "{} has no body".format(pe_elem)

    if not ge_queue.has_pe(pe_name_expr):
        logging.error(
            "Queue %s does not support pe %s. Ignoring job %s",
            ge_queue.qname,
            pe_name_expr,
            job_id,
        )
        return None

    if not ge_queue.has_pe(pe_name_expr):
        logging.error(
            "Queue %s does not support pe %s. Ignoring job %s",
            ge_queue.qname,
            pe_name_expr,
            job_id,
        )
        return None

    pes: List[ParallelEnvironment] = ge_queue.get_pes(pe_name_expr)
    queue_and_hostgroup_constraints = []
    for pe in pes:
        hostgroups = ge_queue.get_hostgroups_for_pe(pe.name)
        pe_count = int(pe_elem.text)

        # optional - can  be None if this is an htc style bucket.
        placement_group = pe.get_placement_group(ge_queue.qname)

        queue_and_hostgroup_constraints.append(
            QueueAndHostgroupConstraint(ge_queue.qname, hostgroups, placement_group)
        )

    if len(queue_and_hostgroup_constraints) > 1:
        constraints.append(XOr(*queue_and_hostgroup_constraints))
    else:
        constraints.append(queue_and_hostgroup_constraints[0])

    job_constructor: Callable[[str], Job]

    if pe.is_fixed:
        assert isinstance(pe.allocation_rule, FixedProcesses)
        alloc_rule: FixedProcesses = pe.allocation_rule  # type: ignore
        constraints[0]["slots"] = alloc_rule.fixed_processes
        num_nodes = int(math.ceil(slots / float(alloc_rule.fixed_processes)))

        def job_constructor(job_id: str) -> Job:
            return Job(job_id, constraints, node_count=num_nodes, colocated=True)

    elif pe.allocation_rule.name == "$pe_slots":
        constraints[0]["slots"] = pe_count
        # this is not colocated, so we can skip the redirection
        return [Job(job_id, constraints, iterations=num_tasks)]

    elif pe.allocation_rule.name == "$round_robin":
        constraints[0]["slots"] = 1

        def job_constructor(job_id: str) -> Job:
            return Job(
                job_id,
                constraints,
                node_count=slots,
                colocated=True,
                packing_strategy="scatter",
            )

    elif pe.allocation_rule.name == "$fill_up":
        constraints[0]["slots"] = 1

        def job_constructor(job_id: str) -> Job:
            return Job(
                job_id,
                constraints,
                iterations=pe_count,
                colocated=True,
                packing_strategy="pack",
            )

    else:
        # this should never happen
        logging.error(
            "Unsupported allocation_rule %s for job %s. Ignoring",
            pe.allocation_rule.name,
            job_id,
        )
        return None

    if array_size == 1:
        # not an array, do the simple thing
        return [job_constructor(job_id)]

    ret = []
    for t in range(array_size):
        ret.append(job_constructor("{}.{}".format(job_id, t)))
    return ret


class QueueAndHostgroupConstraint(BaseNodeConstraint):
    def __init__(
        self,
        qname: str,
        hostgroups: List[str],
        placement_group: Optional[ht.PlacementGroup],
    ) -> None:
        self.qname = qname
        assert self.qname
        self.hostgroups_set = set(hostgroups)
        self.hostgroups_sorted = sorted(hostgroups)
        assert hostgroups, "Must specify at least one hostgroup"
        self.placement_group = placement_group

    def satisfied_by_node(self, node: Node) -> SatisfiedResult:

        if self.placement_group:
            if node.placement_group != self.placement_group:

                return SatisfiedResult(
                    "WrongPlacementGroup",
                    self,
                    node,
                    reasons=[
                        "Node {} is in a different pg: {} != {}".format(
                            node, node.placement_group, self.placement_group
                        )
                    ],
                )
        elif node.placement_group:
            return SatisfiedResult(
                "NodeInAPlacementGroup",
                self,
                node,
                reasons=[
                    "Node {} is in a pg but our job is not colocated: {}".format(
                        node, node.placement_group
                    )
                ],
            )

        node_qname = node.available.get("_gridengine_qname")

        # nodes that don't exist are not in a queue, by definition
        if not node_qname and not node.exists:
            node_qname = self.qname

        if node_qname != self.qname:
            return SatisfiedResult(
                "WrongQueue",
                self,
                node,
                reasons=[
                    "Node {} is in a different queue: {} != {}".format(
                        node, node_qname, self.qname
                    )
                ],
            )

        node_hostgroups: Optional[Set[str]] = node.available.get(
            "_gridengine_hostgroups"
        )
        if not node_hostgroups:
            node_hostgroups = set() if node.exists else self.hostgroups_set
        else:
            node_hostgroups = set(node_hostgroups)

        if not node_hostgroups.intersection(self.hostgroups_sorted):
            return SatisfiedResult(
                "WrongHostgroup",
                self,
                node,
                reasons=[
                    "Node {} is not in any of the target hostgroups: {} != {}".format(
                        node, node_hostgroups, self.hostgroups_sorted
                    )
                ],
            )

        return SatisfiedResult("success", self, node)

    def do_decrement(self, node: Node) -> bool:
        node_qname = node.available.get("_gridengine_qname") or self.qname
        assert node_qname == self.qname
        node.available["_gridengine_qname"] = self.qname

        node_hostgroups = set(
            node.available.get("_gridengine_hostgroups") or self.hostgroups_set
        )
        assert node_hostgroups == self.hostgroups_set

        node.available["_gridengine_hostgroups"] = sorted(list(node_hostgroups))
        assert node.placement_group in [
            None,
            self.placement_group,
        ], "placement group %s != %s" % (node.placement_group, self.placement_group)

        node.placement_group = self.placement_group

        if not node.exists:
            # for new nodes, set this attribute.
            node.node_attribute_overrides["Configuration"] = {
                "gridengine_qname": self.qname,
                "gridengine_hostgroups": " ".join(self.hostgroups_sorted),
            }
        return True

    def __str__(self) -> str:
        return "QueueAndHostgroups(qname={}, hostgroups={}, placement_group={})".format(
            self.qname, ",".join(self.hostgroups_sorted), self.placement_group
        )

    def to_dict(self) -> Dict:
        return {
            "queue-and-hostgroups": {
                "qname": self.qname,
                "hostgroups": self.hostgroups_sorted,
                "placement-group": self.placement_group,
            }
        }

    @staticmethod
    def from_dict(d: Dict) -> NodeConstraint:
        if set(d.keys()) != set(["queue-and-hostgroups"]):
            raise RuntimeError(
                "Unexpected dictionary for QueueAndHostgroups: {}".format(d)
            )

        c = d["queue-and-hostgroups"]
        # TODO validation
        specified = set(c.keys())
        valid = set(["qname", "hostgroups", "placement-group"])
        unexpected = specified - valid
        assert not unexpected, "Unexpected attribute - {}".format(unexpected)
        return QueueAndHostgroupConstraint(
            c["qname"], c["hostgroups"], c.get("placement-group")
        )


class GENodeQueue(NodeQueue):
    """
    Custom NodeQueue that prioritizes nodes by slots and bails out
    when there are no slots.
    """

    def node_priority(self, node: Node) -> int:
        return node.available.get("slots", node.available.get("ncpus", 0))

    def early_bailout(self, node: Node) -> EarlyBailoutResult:
        prio = self.node_priority(node)
        if prio > 0:
            return EarlyBailoutResult("success")
        return EarlyBailoutResult("NoSlots", node, ["No more slots/ncpus remaining"])


register_parser("queue-and-hostgroups", QueueAndHostgroupConstraint.from_dict)


def _parse_tasks(expr: str) -> int:

    try:
        num_tasks = 0
        for sub_expr in expr.split(","):
            if "-" in sub_expr:
                start, rest = sub_expr.split("-")
                stop, step = rest.split(":")
                num_tasks += len(range(int(start), int(stop) + 1, int(step)))
            else:
                num_tasks += 1
        return num_tasks
    except Exception as e:
        logging.error("Could not parse expr %s: %s", expr, e)
        raise
