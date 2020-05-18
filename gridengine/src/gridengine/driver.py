import math
import os
import re
import socket
from shutil import which
from subprocess import CalledProcessError
from subprocess import check_call as _check_call
from subprocess import check_output as _check_output
from typing import Any, Dict, List, Optional, Set, Tuple
from xml.etree import ElementTree
from xml.etree.ElementTree import Element

import six
from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.job.job import Job
from hpc.autoscale.node.constraints import (
    BaseNodeConstraint,
    NodeConstraint,
    register_parser,
)
from hpc.autoscale.node.node import Node
from hpc.autoscale.results import SatisfiedResult
from hpc.autoscale.util import partition, partition_single

from gridengine import parallel_environments
from gridengine.parallel_environments import (
    FixedProcesses,
    GridEngineQueue,
    ParallelEnvironment,
    read_parallel_environments,
)

QSTAT_PATH = which("qstat") or ""
QCONF_PATH = which("qconf") or ""
QMOD_PATH = which("qmod") or ""
if not QSTAT_PATH:
    logging.error("Could not find qstat in PATH: {}".format(os.environ))
if not QCONF_PATH:
    logging.error("Could not find qstat in PATH: {}".format(os.environ))


DISABLED_RESOURCE_GROUP = "limitcycleclouddisabled"
DISABLED_HOST_GROUP = "@cycleclouddisabled"


def check_call(cmd: List[str], *args: Any, **kwargs: Any) -> None:
    logging.trace("Running '%s'", " ".join(cmd))
    try:
        _check_call(cmd, *args, **kwargs)
    except Exception:
        logging.error("'%s'", " ".join(cmd))
        raise


def call(cmd: List[str], *args: Any, **kwargs: Any) -> None:
    try:
        check_call(cmd, *args, **kwargs)
    except CalledProcessError as e:
        logging.warning("Previous command failed: %s", str(e))


def check_output(cmd: List[str], *args: Any, **kwargs: Any) -> Any:
    logging.trace("Running '%s'", " ".join(cmd))
    try:
        return _check_output(cmd, *args, **kwargs)
    except Exception:
        logging.error("'%s'", " ".join(cmd))
        raise


class GridEngineDriver:
    def __init__(self, autoscale_config: Dict) -> None:
        jobs, scheduler_nodes = _get_jobs_and_nodes(autoscale_config)
        self.jobs = jobs
        self.scheduler_nodes = scheduler_nodes
        self.parallel_envs = read_parallel_environments()

    def handle_draining(
        self, unmatched_nodes: List[SchedulerNode]
    ) -> List[SchedulerNode]:
        to_shutdown: List[SchedulerNode] = []
        for node in unmatched_nodes:
            if node.hostname:
                wc_queue_list_expr = "*@{}".format(node.hostname)
                try:
                    check_call([QMOD_PATH, "-d", wc_queue_list_expr])
                    to_shutdown.append(node)
                except CalledProcessError as e:
                    msg = 'invalid queue "{}"'.format(wc_queue_list_expr)
                    if msg in str(e):
                        # the node isn't even part of any queue anyways.
                        to_shutdown.append(node)
                    else:
                        logging.error(
                            "Could not drain %s: %s. Will not shutdown node.",
                            node,
                            str(e),
                        )
        return to_shutdown

    def handle_post_delete(self, nodes_to_delete: List[Node]) -> None:
        fqdns = check_output([QCONF_PATH, "-sh"]).decode().lower().split()
        admin_hosts = [n.split(".")[0] for n in fqdns]

        fqdns = check_output([QCONF_PATH, "-sss"]).decode().lower().split()
        submit_hosts = [n.split(".")[0] for n in fqdns]

        fqdns = check_output([QCONF_PATH, "-sel"]).decode().lower().split()
        exec_hosts = [n.split(".")[0] for n in fqdns]

        queues = check_output([QCONF_PATH, "-sql"]).decode().lower().split()
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
            lambda n: n.resources.get("slot_type") or n.nodearray + ".q",
        )

        for queue_name, nodes in by_queue.items():
            if queue_name in queues:
                check_call([QMOD_PATH, "-d", queue_name])
                check_call([QMOD_PATH, "-rq", queue_name])

            for node in nodes:
                if not node.hostname:
                    continue

                try:
                    logging.info(
                        "Removing host %s via qconf -dh and -ds", node.hostname
                    )
                    # we need to remove these from the hostgroups first, otherwise
                    # we can't remove the node
                    for hostlist_name, hosts in by_hostlist.items():
                        if node.hostname.lower() in hosts:
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

                    if node.hostname.lower() in admin_hosts:
                        call([QCONF_PATH, "-dh", node.hostname])

                    if node.hostname.lower() in submit_hosts:
                        call([QCONF_PATH, "-ds", node.hostname])

                    if node.hostname.lower() in exec_hosts:
                        call([QCONF_PATH, "-de", node.hostname])

                except CalledProcessError as e:
                    logging.warning(str(e))

    def handle_undraining(self, matched_nodes: List[Node]) -> List[Node]:
        # TODO get list of hosts in @disabled

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

    def handle_join_cluster(self, matched_nodes: List[Node]) -> None:
        """
        1) remove the host from @disabled
        2) create compnode.conf
        3) qconf -ah #{fname} && qconf -as #{fname}
        """

        # TODO rethink this RDH
        # self.handle_undraining(matched_nodes)

        _hostlist_cache: Dict[str, List[str]] = {}

        def _get_hostlist(hostlist: str) -> List[str]:
            if hostlist not in _hostlist_cache:
                fqdns = (
                    check_output([QCONF_PATH, "-shgrp_resolved", hostlist])
                    .decode()
                    .split()
                )
                _hostlist_cache[hostlist] = [fqdn.split(".")[0] for fqdn in fqdns]
            return _hostlist_cache[hostlist]

        for node in matched_nodes:
            if not self.add_node_to_cluster(node):
                continue

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
                if not hostgroup.startswith("@"):
                    # hostgroups have to start with @
                    continue

                if node.hostname in _get_hostlist(hostgroup):
                    continue

                hostlist_hostnames = _get_hostlist(hostgroup)
                if node.hostname not in hostlist_hostnames:
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

    def add_node_to_cluster(self, node: Node) -> bool:
        if not node.exists:
            logging.trace("%s does not exist yet, can not add to cluster.", node)
            return False

        if not node.hostname:
            logging.trace(
                "%s does not have a hostname yet, can not add to cluster.", node
            )
            return False

        fqdns = check_output([QCONF_PATH, "-sh"]).decode().splitlines()
        admin_hostnames = [fqdn.split(".")[0] for fqdn in fqdns]

        fqdns = check_output([QCONF_PATH, "-ss"]).decode().splitlines()
        submit_hostnames = [fqdn.split(".")[0] for fqdn in fqdns]

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
                + "Often this repairs itself. Skipping",
                node,
                node.private_ip,
                node.hostname,
                addr_info_hostname,
            )
            return False

        logging.info("Adding hostname %s", node.hostname)
        try:
            if node.hostname not in admin_hostnames:
                logging.debug("Adding %s as administrative host", node)
                check_call([QCONF_PATH, "-ah", node.hostname])

            for qname in ["all.q", node.software_configuration["gridengine_qname"]]:
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

            if node.hostname not in submit_hostnames:
                logging.debug("Adding %s as submit host", node)
                check_call([QCONF_PATH, "-as", node.hostname])

        except CalledProcessError as e:
            logging.warn("Could not add %s to cluster: %s", node, str(e))
            return False

        return True

    def clean_hosts(self, invalid_nodes: Optional[List[SchedulerNode]]) -> None:
        if invalid_nodes is None:
            invalid_nodes = [
                n for n in self.scheduler_nodes if n.metadata["state"] == "au"
            ]

        if not invalid_nodes:
            return

        logging.warning(
            "Cleaning out the following hosts in state=au: %s", invalid_nodes
        )
        self.handle_post_delete(invalid_nodes)
        self.scheduler_nodes = [
            n for n in self.scheduler_nodes if n not in invalid_nodes
        ]

    def __str__(self) -> str:
        return "GEDriver(jobs={}, scheduler_nodes={})".format(
            self.jobs[:100], self.scheduler_nodes[:100]
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
    autoscale_config: Dict,
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

    ge_queues = partition_single(
        parallel_environments.read_queue_configs(autoscale_config), lambda q: q.qname
    )
    nodes = _parse_scheduler_nodes(root, ge_queues)
    jobs = _parse_jobs(root, ge_queues)
    return jobs, nodes


def _parse_scheduler_nodes(
    root: Element, ge_queues: Dict[str, GridEngineQueue]
) -> List[SchedulerNode]:
    schedulers = check_output([QCONF_PATH, "-sss"]).decode().splitlines()

    compute_nodes: Dict[str, SchedulerNode] = {}

    for qiqle in root.findall("queue_info/Queue-List"):
        # TODO need a better way to hide the master
        name = _getr(qiqle, "name")

        queue_name, fqdn = name.split("@", 1)
        if queue_name == "all.q":
            continue

        ge_queue = ge_queues.get(queue_name)
        if not ge_queue:
            logging.error("Unknown queue %s.", queue_name)
            continue

        hostname = fqdn.split(".", 1)[0]
        if hostname in schedulers:
            continue

        if hostname in compute_nodes:
            logging.warning(
                "We do not support hosts that exist in more than one queue! %s",
                hostname,
            )
            continue

        slots_total = int(_getr(qiqle, "slots_total"))
        resources: dict = {"slots": slots_total}

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

        # use assign_job so we just accept that this job is running on this node.
        for jle in qiqle.findall("job_list"):
            compute_node.assign(_getr(jle, "JB_job_number"))

        compute_nodes[compute_node.hostname] = compute_node

    return list(compute_nodes.values())


def _parse_jobs(root: Element, ge_queues: Dict[str, GridEngineQueue]) -> List[Job]:
    autoscale_jobs: List[Job] = []

    for jijle in root.findall("job_info/job_list"):
        if _get(jijle, "state") == "running":
            # running jobs are already accounted for above
            continue

        requested_queues = [str(x.text) for x in jijle.findall("hard_req_queue")]

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
            continue

        qname = requested_queues[0]
        if qname not in ge_queues:
            logging.error("Unknown queue %s for job %s", qname, job_id)
            continue

        ge_queue: GridEngineQueue = ge_queues[qname]

        num_tasks = 1
        tasks_expr = _get(jijle, "tasks")

        if tasks_expr:
            num_tasks = _parse_tasks(tasks_expr)

        constraints: List[Dict] = [{"slots": slots}] + ge_queue.constraints

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
                req_value: Any = req.text
            else:
                req_value = complex.parse(req.text)

            constraints.append({resource_name: req_value})
            job_resources[resource_name] = req_value

            if complex and complex.shortcut != complex.name:
                constraints.append({complex.shortcut: req_value})
                job_resources[complex.shortcut] = req_value

        job: Job

        pe_elem = jijle.find("requested_pe")
        if pe_elem is not None:
            # dealing with parallel jobs (qsub -pe ) is much more complicated
            job = _pe_job(ge_queue, pe_elem, job_id, constraints, slots, num_tasks)
            if job is None:
                continue
        else:
            constraints.append(
                QueueAndHostgroupConstraint(
                    ge_queue.qname, list(ge_queue.hostlist_groups), None
                )
            )
            job = Job(job_id, constraints, iterations=num_tasks)

        job.metadata["gridengine"] = {
            "resources": job_resources,
        }

        autoscale_jobs.append(job)

    return autoscale_jobs


def _pe_job(
    ge_queue: GridEngineQueue,
    pe_elem: Element,
    job_id: str,
    constraints: List[Dict],
    slots: int,
    num_tasks: int,
) -> Optional[Job]:
    pe_name = pe_elem.attrib["name"]
    assert pe_name, "{} has no attribute 'name'".format(pe_elem)
    assert pe_elem.text, "{} has no body".format(pe_elem)
    if not ge_queue.has_pe(pe_name):
        logging.error(
            "Queue %s does not support pe %s. Ignoring job %s",
            ge_queue.qname,
            pe_name,
            job_id,
        )
        return None

    if not ge_queue.has_pe(pe_name):
        logging.error(
            "Queue %s does not support pe %s. Ignoring job %s",
            ge_queue.qname,
            pe_name,
            job_id,
        )
        return None

    pe: ParallelEnvironment = ge_queue.get_pe(pe_name)
    hostgroups = ge_queue.get_hostgroups_for_pe(pe_name)
    pe_count = int(pe_elem.text)

    if pe.requires_placement_groups:
        placement_group = ht.PlacementGroup("{}_{}".format(ge_queue.qname, pe_name))
        placement_group = re.sub("[^a-zA-z0-9-_]", "_", placement_group)
    else:
        placement_group = None

    constraints.append(
        QueueAndHostgroupConstraint(ge_queue.qname, hostgroups, placement_group)
    )

    if pe.is_fixed:
        assert isinstance(pe.allocation_rule, FixedProcesses)
        alloc_rule: FixedProcesses = pe.allocation_rule  # type: ignore
        constraints[0]["slots"] = alloc_rule.fixed_processes
        num_tasks = int(math.ceil(slots / float(alloc_rule.fixed_processes)))
        return Job(job_id, constraints, iterations=num_tasks)

    elif pe.allocation_rule.name == "$pe_slots":
        constraints[0]["slots"] = pe_count
        return Job(job_id, constraints, iterations=num_tasks)

    elif pe.allocation_rule.name == "$round_robin":
        constraints[0]["slots"] = 1
        constraints.append({"exclusive": True})
        num_tasks = slots
        return Job(job_id, constraints, iterations=num_tasks)

    elif pe.allocation_rule.name == "$fill_up":
        constraints[0]["slots"] = 1
        num_tasks = pe_count * num_tasks
        return Job(job_id, constraints, iterations=num_tasks)
    else:
        # this should never happen
        logging.error(
            "Unsupported allocation_rule %s for job %s. Ignoring",
            pe.allocation_rule.name,
            job_id,
        )
        return None


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
            if node.placement_group and node.placement_group != self.placement_group:
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
        assert (
            node.placement_group is None or node.placement_group == self.placement_group
        )
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
