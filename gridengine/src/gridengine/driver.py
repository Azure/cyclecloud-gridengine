import json
import math
import os
import socket
import tempfile
from copy import deepcopy
from subprocess import CalledProcessError
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
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
    Constraint,
    NodeConstraint,
    Or,
    XOr,
    register_parser,
)
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodemanager import NodeManager
from hpc.autoscale.results import EarlyBailoutResult, SatisfiedResult

from gridengine import environment as envlib
from gridengine import validate as validatelib
from gridengine.allocation_rules import FixedProcesses
from gridengine.complex import Complex
from gridengine.environment import GridEngineEnvironment
from gridengine.hostgroup import BoundHostgroup, QuotaConstraint
from gridengine.parallel_environments import ParallelEnvironment
from gridengine.qbin import check_output
from gridengine.queue import GridEngineQueue
from gridengine.util import add_node_to_hostgroup, get_node_hostgroups


def new_driver(
    autoscale_config: Dict, ge_env: envlib.GridEngineEnvironment,
) -> "GridEngineDriver":
    return GridEngineDriver(autoscale_config, ge_env)


# TODO RDH logging


class GridEngineDriver:
    def __init__(
        self, autoscale_config: Dict, ge_env: envlib.GridEngineEnvironment
    ) -> None:
        self.autoscale_config = autoscale_config
        self.ge_env = ge_env
        self.read_only = autoscale_config.get("read_only", False)
        if self.read_only is None:
            self.read_only = False
        self._hostgroup_cache: Dict[str, List[str]] = {}

    def initialize_environment(self) -> None:
        if self.read_only:
            return

        expected = 'Complex attribute "ccnodeid" does not exist'
        try:
            if "ccnodeid " in self.ge_env.qbin.qconf(["-sc"]):
                return
        except CalledProcessError as e:
            if e.stdout and e.stdout.decode().strip() != expected:
                raise
        ccnodeid_path = None
        try:
            if self.ge_env.is_uge:
                contents = """name        ccnodeid
shortcut    ccnodeid
type        RESTRING
relop       ==
requestable YES
consumable  NO
default     NONE
urgency     0
aapre       NO
affinity    0.000000"""
                fd, ccnodeid_path = tempfile.mkstemp()

                logging.getLogger("gridengine.driver").info(
                    "ccnodeid complex contents written to %s", ccnodeid_path
                )
                logging.getLogger("gridengine.driver").info(contents)
                with open(fd, "w") as fw:
                    fw.write(contents)
                self.ge_env.qbin.qconf(["-Ace", ccnodeid_path])
            else:
                current_sc = self.ge_env.qbin.qconf(["-sc"])
                current_sc = "\n".join(current_sc.strip().splitlines()[:-1])
                # name shortcut type relop requestable consumable default urgency
                contents = (
                    current_sc.strip()
                    + "\nccnodeid ccnodeid RESTRING == YES NO NONE 0\n"
                )
                fd, ccnodeid_path = tempfile.mkstemp()

                logging.getLogger("gridengine.driver").info(
                    "ccnodeid appended to complex: contents written to %s",
                    ccnodeid_path,
                )
                logging.getLogger("gridengine.driver").info(contents)
                with open(fd, "w") as fw:
                    fw.write(contents)
                self.ge_env.qbin.qconf(["-Mc", ccnodeid_path])
        finally:
            if ccnodeid_path and os.path.exists(ccnodeid_path):
                try:
                    os.remove(ccnodeid_path)
                except Exception:
                    logging.exception(
                        "Failed to remove temporary file %s.", ccnodeid_path
                    )

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
                    self.ge_env.qbin.qmod(["-d", wc_queue_list_expr])
                    to_shutdown.append(node)
                except CalledProcessError as e:
                    msg = "invalid queue"

                    if e.stdout and msg in e.stdout.decode():
                        # the node isn't even part of any queue anyways.
                        logging.info(
                            "Ignoring failed qmod -d, as the hostname is no longer associated with a queue"
                        )
                        to_shutdown.append(node)
                    else:
                        logging.error(
                            "Could not drain %s: %s. Will not shutdown node. ",
                            node,
                            e.stdout.decode() if e.stdout else str(e),
                        )
        return to_shutdown

    def get_jobs_and_nodes(self) -> Tuple[List[Job], List[SchedulerNode]]:
        return _get_jobs_and_nodes(self.autoscale_config, self.ge_env)

    def handle_post_delete(self, nodes_to_delete: List[Node]) -> None:
        if self.read_only:
            return

        logging.getLogger("gridengine.driver").info("handle_post_delete")

        fqdns = self.ge_env.qbin.qconf(["-sh"]).lower().split()
        admin_hosts = [n.split(".")[0] for n in fqdns]

        fqdns = self.ge_env.qbin.qconf(["-ss"]).lower().split()
        submit_hosts = [n.split(".")[0] for n in fqdns]

        fqdns = self.ge_env.qbin.qconf(["-sel"]).lower().split()
        exec_hosts = [n.split(".")[0] for n in fqdns]

        hostlists = self.ge_env.qbin.qconf(["-shgrpl"]).lower().split()

        by_hostlist: Dict[str, List[str]] = {}
        for hostlist in hostlists:
            fqdns = (
                self.ge_env.qbin.qconf(["-shgrp_resolved", hostlist]).lower().split()
            )
            by_hostlist[hostlist] = [n.split(".")[0] for n in fqdns]

        hostnames_to_delete: Set[str] = set()

        for node in nodes_to_delete:
            if not node.hostname:
                continue

            hostname = node.hostname.lower()

            try:
                logging.info("Removing host %s via qconf -dh, -de and -ds", hostname)
                # we need to remove these from the hostgroups first, otherwise
                # we can't remove the node
                for hostlist_name, hosts in by_hostlist.items():
                    if hostname in hosts:
                        self.ge_env.qbin.qconf(
                            [
                                "-dattr",
                                "hostgroup",
                                "hostlist",
                                node.hostname,
                                hostlist_name,
                            ],
                            check=False,
                        )

                for qname in self.ge_env.queues:
                    self._delete_host_from_slots(qname, hostname)

                hostnames_to_delete.add(hostname)

            except CalledProcessError as e:
                logging.warning(str(e))

        # now that we have removed all slots entries from all queues, we can
        # delete the hosts. If you don't do this, it will complain that the hosts
        # are still referenced.
        try:
            for host_to_delete in hostnames_to_delete:
                if host_to_delete in admin_hosts:
                    self.ge_env.qbin.qconf(["-dh", host_to_delete], check=False)

                if host_to_delete in submit_hosts:
                    self.ge_env.qbin.qconf(["-ds", host_to_delete], check=False)

                if host_to_delete in exec_hosts:
                    self.ge_env.qbin.qconf(["-de", host_to_delete], check=False)

        except CalledProcessError as e:
            logging.warning(str(e))

    def _delete_host_from_slots(self, qname: str, hostname: str) -> None:
        queue_host = "{}@{}".format(qname, hostname)

        hostname = hostname.lower()

        queue = self.ge_env.queues.get(qname)
        if not queue:
            logging.warning(
                "Queue %s does not exist. Attempted to remove %s from the slots declaration",
                qname,
                hostname,
            )
            return

        if hostname not in queue.slots:
            # already removed
            return

        logging.info(
            "Purging slots definition for host=%s queue=%s", hostname, queue.qname
        )

        self.ge_env.qbin.qconf(
            ["-purge", "queue", "slots", queue_host,]
        )

        queue.slots.pop(hostname)

    def _get_hostlist(self, hostgroup: str) -> List[str]:
        if hostgroup not in self._hostgroup_cache:
            fqdns = (
                self.ge_env.qbin.qconf(["-shgrp_resolved", hostgroup]).lower().split()
            )
            self._hostgroup_cache[hostgroup] = [fqdn.split(".")[0] for fqdn in fqdns]
        return self._hostgroup_cache[hostgroup]

    def handle_join_cluster(self, matched_nodes: List[Node]) -> List[Node]:
        """
        """
        self._hostgroup_cache = {}

        if self.read_only:
            return []

        logging.getLogger("gridengine.driver").info("handle_join_cluster")

        managed_matched = [n for n in matched_nodes if n.managed]

        return self.add_nodes_to_cluster(managed_matched)

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
        fqdns = self.ge_env.qbin.qconf(["-sh"]).splitlines()
        admin_hostnames = [fqdn.split(".")[0] for fqdn in fqdns]

        fqdns = self.ge_env.qbin.qconf(["-ss"]).splitlines()
        submit_hostnames = [fqdn.split(".")[0] for fqdn in fqdns]

        filtered = [
            n for n in nodes if n.exists and n.hostname and n.resources.get("ccnodeid")
        ]

        for node in filtered:
            if self._add_node_to_cluster(node, admin_hostnames, submit_hostnames):
                ret.append(node)
        return ret

    def _add_node_to_cluster(
        self, node: Node, admin_hostnames: List[str], submit_hostnames: List[str]
    ) -> bool:
        # this is the very last thing we do, so this is basically 'committing'
        # the node.

        if not node.resources.get("ccnodeid"):
            return False

        if not self._validate_add_node_to_cluster(node, submit_hostnames):
            logging.fine(
                "Validation of %s did not succeed. Not adding to cluster.", node
            )
            return False

        hostgroups = get_node_hostgroups(node)
        if not hostgroups:
            logging.fine(
                "No hostgroups were found for %s. Can not add to cluster.", node
            )
            return False

        if not self._validate_reverse_dns(node):
            logging.fine(
                "%s still has a hostname that can not be looked via reverse dns", node
            )
            return False

        try:

            self.add_exec_host(node)
            if not self._add_slots(node):
                logging.warning("Adding slots to queues failed for %s", node)
                return False
            self._add_to_hostgroups(node, hostgroups)
            # finally enable it
            self.ge_env.qbin.qmod(["-e", "*@" + node.hostname])

        except CalledProcessError as e:
            logging.warn("Could not add %s to cluster: %s", node, str(e))
            try:
                self.ge_env.qbin.qmod(["-d", "*@" + node.hostname])
            except CalledProcessError as e2:
                logging.exception(e2.stdout)
            return False

        return True

    def _validate_add_node_to_cluster(
        self, node: Node, submit_hostnames: List[str]
    ) -> bool:
        if node.hostname in submit_hostnames:
            if "d" not in node.metadata.get("state", ""):
                return False

        if node.state == "Failed":
            logging.warning("Ignoring failed %s", node)
            return False

        if not node.exists:
            logging.trace("%s does not exist yet, can not add to cluster.", node)
            return False

        if not node.hostname:
            logging.trace(
                "%s does not have a hostname yet, can not add to cluster.", node
            )
            return False
        return True

    def _validate_reverse_dns(self, node: Node) -> bool:
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
                "%s has a hostname that does not match the"
                + " private_ip (%s) reported by cyclecloud (%s)! Skipping",
                node,
                addr_info_ips,
                node.private_ip,
            )
            return False

        addr_info_hostname = addr_info[0].split(".")[0]
        if addr_info_hostname.lower() != node.hostname.lower():
            logging.warning(
                "%s has a hostname that can not be queried via reverse"
                + " dns (private_ip=%s cyclecloud hostname=%s reverse dns hostname=%s)."
                + " This is common and usually repairs itself. Skipping",
                node,
                node.private_ip,
                node.hostname,
                addr_info_hostname,
            )
            return False
        return True

    def get_host_template(self, node: Node) -> str:
        ge_config = self.autoscale_config.get("gridengine", {})
        script_path = ge_config.get("get_host_template")
        if not script_path:
            return self._default_get_host_template(node)

        if not os.path.exists(script_path):
            logging.warning("%s does not exist! Using default host template behavior")
            return self._default_get_host_template(node)

        return check_output([script_path], input=json.dumps(node.to_dict()))

    def _default_get_host_template(self, node: Node) -> str:
        complex_values: List[str] = []

        for res_name, res_value in node.resources.items():
            if isinstance(res_value, bool):
                res_value = str(res_value).lower()

            if isinstance(res_value, str) and res_value.lower() in ["true", "false"]:
                if res_value.lower() == "true":
                    res_value = 1
                else:
                    res_value = 0

            if res_name not in self.ge_env.unfiltered_complexes:
                logging.fine("Ignoring unknown complex %s", res_name)
                continue

            if not res_value:
                logging.fine("Ignoring blank complex %s for %s", res_name, node)
                continue

            complex = self.ge_env.unfiltered_complexes[res_name]

            if complex.complex_type in ["TIME"]:
                logging.fine("Ignoring TIME complex %s for %s", res_name, node)
                continue

            if complex.name != complex.shortcut and res_name == complex.shortcut:
                # this is just a duplicate of the long form
                continue

            complex_values.append("{}={}".format(res_name, res_value))

        complex_values_csv = ",".join(complex_values)
        base_template = """hostname              {hostname}
load_scaling          NONE
complex_values        {complex_values_csv}
user_lists            NONE
xuser_lists           NONE
projects              NONE
xprojects             NONE
usage_scaling         NONE
report_variables      NONE""".format(
            hostname=node.hostname, complex_values_csv=complex_values_csv
        )
        if self.ge_env.is_uge:
            uge_license_template = """
license_constraints   NONE
license_oversubscription NONE"""
            return base_template + uge_license_template
        return base_template

    def _add_slots(self, node: Node) -> bool:
        queues = []

        for hostgroup in get_node_hostgroups(node):
            for queue in self.ge_env.queues.values():
                if hostgroup in queue.hostlist:
                    queues.append(queue.qname)

        for qname in queues:
            logging.debug("Adding slots for %s to queue %s", node.hostname, qname)
            self.ge_env.qbin.qconf(
                [
                    "-mattr",
                    "queue",
                    "slots",
                    str(node.resources["slots"]),
                    "{}@{}".format(qname, node.hostname),
                ]
            )
        return True

    def add_exec_host(self, node: Node) -> None:
        if not node.resources.get("ccnodeid"):
            return

        tempp = tempfile.mktemp()
        try:
            host_template = self.get_host_template(node)
            logging.getLogger("gridengine.driver").info(
                "host template contents written to %s", tempp
            )
            logging.getLogger("gridengine.driver").info(host_template)

            with open(tempp, "w") as tempf:
                tempf.write(host_template)
            try:
                self.ge_env.qbin.qconf(["-Ae", tempp])
            except CalledProcessError as e:
                if "already exists" in e.stdout.decode():
                    self.ge_env.qbin.qconf(["-Me", tempp])
                else:
                    raise
        except Exception:
            if os.path.exists(tempp):
                try:
                    os.remove(tempp)
                except Exception:
                    pass
            raise

        self.ge_env.qbin.qconf(["-as", node.hostname])
        self.ge_env.qbin.qconf(["-ah", node.hostname])

    def _add_to_hostgroups(self, node: Node, hostgroups: List[str]) -> None:
        if not node.resources.get("ccnodeid"):
            return

        if not node.hostname:
            return

        for hostgroup in hostgroups:
            if not hostgroup.startswith("@"):
                # hostgroups have to start with @
                continue

            hostlist_for_hg = self._get_hostlist(hostgroup)
            if node.hostname.lower() in hostlist_for_hg:
                continue

            logging.info("Adding hostname %s to hostgroup %s", node.hostname, hostgroup)

            self.ge_env.qbin.qconf(
                ["-aattr", "hostgroup", "hostlist", node.hostname, hostgroup,]
            )

    def clean_hosts(self, invalid_nodes: List[SchedulerNode]) -> List[Node]:
        logging.getLogger("gridengine.driver").info("clean_hosts")

        filtered = []

        for node in invalid_nodes:
            if node.keep_alive:
                logging.fine("Not removing %s because keep_alive=true", node)
                continue
            if not node.resources.get("ccnodeid"):
                logging.fine(
                    "Not removing %s because it does not define ccnodeid", node
                )
                continue
            filtered.append(node)

        if not filtered:
            return []

        logging.warning(
            "Cleaning out the following hosts in state=au: %s",
            [str(x) for x in filtered],
        )
        self.handle_post_delete(filtered)

        return filtered

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

        if not default_pgs:
            for gqueue in self.ge_env.queues.values():
                for pe in gqueue.get_pes():
                    if pe:
                        pe_pg_name = gqueue.get_placement_group(pe.name)
                        if pe_pg_name:
                            default_pgs.append(pe_pg_name)

            if "default_resources" not in config:
                config["default_resources"] = []

        # make sure that user doess not need to specify both shortcut and full name
        new_default_resources = []
        for dr in config["default_resources"]:
            c = self.ge_env.unfiltered_complexes.get(dr.get("name"))
            new_default_resources.append(dr)
            assert c, "%s %s" % (dr, self.ge_env.complexes)
            if not c:
                continue
            if c.name == c.shortcut:
                continue

            other_dr = deepcopy(dr)
            if dr["name"] == c.name:
                other_dr["name"] = c.shortcut
            else:
                other_dr["name"] = c.name
            new_default_resources.append(other_dr)

        config["default_resources"] = new_default_resources

        if not config.get("gridengine", {}).get("relevant_complexes"):
            return config

        if "ccnodeid" not in config["gridengine"]["relevant_complexes"]:
            config["gridengine"]["relevant_complexes"].append("ccnodeid")

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


_LOGGED_WARNINGS: Set[str] = set()


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
    autoscale_config: Dict, ge_env: envlib.GridEngineEnvironment,
) -> Tuple[List[Job], List[SchedulerNode]]:
    # some magic here for the args
    # -g d -- show all the tasks, do not group
    # -u * -- all users
    # -s  pr -- show only pending or running
    # -f -- full output. Ambiguous what this means, but in this case it includes host information so that
    # we can get one consistent view (i.e. not split between two calls, introducing a race condition)
    # qstat -xml -s prs -r -f -ext -explain c -F
    cmd = ["-xml", "-s", "prs", "-r", "-f", "-ext", "-explain", "c", "-F"]
    relevant_complexes = (
        autoscale_config.get("gridengine", {}).get("relevant_complexes") or []
    )

    if relevant_complexes:
        if "ccnodeid" not in relevant_complexes:
            relevant_complexes.append("ccnodeid")
        cmd.append(" ".join(relevant_complexes))

    logging.debug("Query jobs and nodes with cmd '%s'", " ".join(cmd))
    raw_xml = ge_env.qbin.qstat(cmd)
    raw_xml_file = six.StringIO(raw_xml)
    doc = ElementTree.parse(raw_xml_file)
    root = doc.getroot()

    nodes, running_jobs = _parse_scheduler_nodes(root, ge_env)
    pending_jobs = _parse_jobs(root, ge_env)
    return pending_jobs, nodes


def _parse_scheduler_nodes(
    root: Element, ge_env: envlib.GridEngineEnvironment,
) -> Tuple[List[SchedulerNode], List[Job]]:
    qbin = ge_env.qbin
    ge_queues = ge_env.queues

    running_jobs: Dict[str, Job] = {}
    schedulers = qbin.qconf(["-sss"]).splitlines()

    compute_nodes: Dict[str, SchedulerNode] = {}

    elems = list(root.findall("queue_info/Queue-List"))

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

        slots_total = int(_getr(qiqle, "slots_total"))

        # if slots_total == 0:
        #     logging.error("Ignoring nodes with 0 slots: %s", name)
        #     continue

        resources: dict = {"slots": slots_total}
        if "slots" in ge_env.complexes:
            resources[ge_env.complexes["slots"].shortcut] = slots_total

        for name, default_complex in ge_env.complexes.items():

            if name == "slots":
                continue

            if default_complex.default is None:
                continue

            if not default_complex.requestable:
                continue

            resources[name] = default_complex.default

        for res in qiqle.iter("resource"):
            resource_name = res.attrib["name"]

            # qc == quota complex, hc == host complex
            resource_type = res.attrib.get("type", "hc")
            is_quota_complex = resource_type.startswith("q")

            complex = ge_env.complexes.get(resource_name)
            text = res.text or "NONE"

            # we currently won't do anything with any TIME complex type
            def filter_if_time(c: Complex) -> bool:

                if c.complex_type in ["TIME"]:
                    if resource_name not in _LOGGED_WARNINGS:
                        logging.warning(
                            "Ignoring TIME resource %s on job %s", resource_name
                        )
                    _LOGGED_WARNINGS.add(resource_name)
                    return True
                return False

            if complex is None:
                unfiltered_complex = ge_env.unfiltered_complexes.get(resource_name)
                if unfiltered_complex and filter_if_time(unfiltered_complex):
                    continue
                resources[resource_name] = text
                if resource_name not in _LOGGED_WARNINGS:
                    logging.warning(
                        "Unknown resource %s. This will be treated as a string internally and "
                        + "may cause issues with grid engine's ability to schedule this job.",
                        resource_name,
                    )
                    _LOGGED_WARNINGS.add(resource_name)
            else:

                if filter_if_time(complex):
                    continue

                # actual host complex, not just a quota constraint
                # host complexes can go to .available/resources but
                # quota constraints are namespaced.
                if complex.is_excl:
                    resources["exclusive"] = complex.parse(text)
                    continue

                name_key = "{}@{}".format(queue_name, complex.name)
                shortcut_key = "{}@{}".format(queue_name, complex.shortcut)
                resources[name_key] = complex.parse(text)
                resources[shortcut_key] = complex.parse(text)
                if not is_quota_complex:
                    resources[complex.name] = complex.parse(text)
                    resources[complex.shortcut] = resources[complex.name]

        if hostname in compute_nodes:
            compute_node = compute_nodes[hostname]
        else:
            compute_node = SchedulerNode(
                hostname, {"ccnodeid": resources.get("ccnodeid")}
            )

        compute_node.available.update(resources)
        compute_node._resources.update(resources)

        compute_node.metadata["state"] = _get(qiqle, "state") or ""

        # decrement using compute_node.available. Just use slots here
        slots_key = "{}@slots".format(queue_name)
        compute_node.available[slots_key] = (
            slots_total
            - int(_getr(qiqle, "slots_used"))
            + int(_getr(qiqle, "slots_resv"))
        )  # TODO resv?

        if "slots" in ge_env.complexes:
            s = ge_env.complexes["slots"].shortcut
            compute_node.available[s] = compute_node.available[slots_key]

        _assign_jobs(compute_node, qiqle, running_jobs, ge_env)

        # TODO RDH
        compute_node.metadata["gridengine_hostgroups"] = ",".join(
            ge_env.host_memberships.get(
                compute_node.hostname.lower(), str(ge_env.host_memberships)
            )
        )
        compute_nodes[compute_node.hostname] = compute_node

    # if every queue is using a slots quota, you won't get the accurate number for slots
    # on the global level. Might as well just set it to the max quota seen here.
    for compute_node in compute_nodes.values():
        for name, complex in ge_env.complexes.items():

            if name in compute_node.resources:
                continue

            # TODO RDH how to propagate non-numerics?
            if not complex.is_numeric:
                continue

            max_value = 0
            suffix = "@{}".format(name)
            for k, v in compute_node.available:
                if k.endswith(suffix):
                    max_value = max(max_value, v)

            compute_node._resources[name] = max_value
            compute_node.available[name] = max_value

    return list(compute_nodes.values()), list(running_jobs.values())


def _assign_jobs(
    compute_node: SchedulerNode,
    e: Element,
    running_jobs: Dict[str, Job],
    ge_env: GridEngineEnvironment,
) -> None:
    # use assign_job so we just accept that this job is running on this node.
    for jle in e.findall("job_list"):
        parsed_running_jobs = _parse_job(jle, ge_env) or []
        for running_job in parsed_running_jobs:
            if not running_jobs.get(running_job.name):
                running_jobs[running_job.name] = running_job
            running_jobs[running_job.name].executing_hostnames.append(
                compute_node.hostname
            )
        compute_node.assign(_getr(jle, "JB_job_number"))


def _parse_jobs(root: Element, ge_env: GridEngineEnvironment) -> List[Job]:
    assert isinstance(ge_env, GridEngineEnvironment)
    autoscale_jobs: List[Job] = []

    for jijle in root.findall("job_info/job_list"):
        jobs = _parse_job(jijle, ge_env)
        if jobs:
            autoscale_jobs.extend(jobs)

    return autoscale_jobs


IGNORE_STATES = set("hEe")


def _parse_job(jijle: Element, ge_env: GridEngineEnvironment) -> Optional[Job]:
    assert isinstance(ge_env, GridEngineEnvironment)

    ge_queues: Dict[str, GridEngineQueue] = ge_env.queues

    job_state = _get(jijle, "state")

    # linters yell at this (which is good!) but set("abc") == set(["a", "b", "c"])
    if job_state and IGNORE_STATES.intersection(set(job_state)):  # type: ignore
        return None

    requested_queues = [str(x.text) for x in jijle.findall("hard_req_queue")]

    if not requested_queues:
        requested_queues = ["all.q"]

    requested_queues = [
        x for x in requested_queues if ge_env.queues[x].autoscale_enabled
    ]

    job_id = _getr(jijle, "JB_job_number")
    slots = int(_getr(jijle, "slots"))

    if len(requested_queues) != 1 or ("*" in requested_queues[0]):
        logging.error(
            "We support submitting to at least one and only one queue."
            + " Wildcards are also not supported. If you meant to disable all but one of these queues"
            + ' for autoscale, change {"gridengine": {"queues": {"%s": {"autoscale_enabled": false}}}}'
            + " Ignoring job %s submitted to %s.",
            "|".join(requested_queues),
            job_id,
            requested_queues,
        )
        return None

    qname = requested_queues[0]
    if qname not in ge_queues:
        logging.error("Unknown queue %s for job %s", qname, job_id)
        return None

    ge_queue: GridEngineQueue = ge_queues[qname]

    if not ge_queue.autoscale_enabled:
        return None

    num_tasks = 1
    tasks_expr = _get(jijle, "tasks")

    if tasks_expr:
        num_tasks = _parse_tasks(tasks_expr)

    constraints: List[Union[QuotaConstraint, Dict]] = [{"slots": slots}]

    job_resources: Dict[str, Any] = {}
    project = _get(jijle, "JB_project")
    owner = _get(jijle, "JB_owner")

    jobs: List[Job]

    pe_elem = jijle.find("requested_pe")
    if pe_elem is not None:
        # dealing with parallel jobs (qsub -pe ) is much more complicated
        pe_jobs = _pe_job(
            ge_env,
            ge_queue,
            jijle,
            pe_elem,
            job_id,
            constraints,
            slots,
            num_tasks,
            owner,
            project,
        )
        if pe_jobs is None:
            return None
        else:
            jobs = pe_jobs
    else:
        hostgroup_names = ge_queue.get_hostgroups_for_ht()

        if not hostgroup_names:
            validatelib.validate_ht_hostgroup(ge_queue, ge_env, logging.warning)
            logging.error("No hostgroup for job %s. See warnings above.", job_id)
            return None

        queue_and_hostgroup_constraints = []
        _apply_constraints(
            jijle=jijle,
            job_id=job_id,
            ge_queue=ge_queue,
            ge_env=ge_env,
            job_resources=job_resources,
            constraints_out=constraints,
        )
        for hg_name in hostgroup_names:
            hostgroup = ge_queue.bound_hostgroups.get(hg_name)

            if not hostgroup:
                raise AssertionError(
                    "{} not defined in {}".format(
                        hg_name, list(ge_env.hostgroups.keys())
                    )
                )

            child_constraint = hostgroup.make_constraint(ge_env, owner, project, constraints[0])  # type: ignore
            queue_and_hostgroup_constraints.append(
                HostgroupConstraint(hostgroup, None, child_constraint)
            )

        if len(queue_and_hostgroup_constraints) > 1:
            # if there is a weighting already in the scheduler config,
            # we can resolve conflicts using this. Ties are undefined.
            if ge_env.scheduler.sort_by_seqno:

                queue_and_hostgroup_constraints = sorted(
                    queue_and_hostgroup_constraints,
                    key=lambda c: (c.hostgroup.seq_no, c.hostgroup.name),
                )
                constraints.append(Or(*queue_and_hostgroup_constraints))
            else:
                constraints.append(XOr(*queue_and_hostgroup_constraints))
        else:
            constraints.append(queue_and_hostgroup_constraints[0])

        jobs = [Job(job_id, constraints[1:], iterations=num_tasks)]

    for job in jobs:
        job.metadata["gridengine"] = {
            "resources": job_resources,
        }

        job.metadata["job_state"] = job_state

    return jobs


def _apply_constraints(
    jijle: Element,
    job_id: str,
    ge_queue: GridEngineQueue,
    ge_env: GridEngineEnvironment,
    job_resources: Dict,
    constraints_out: List[Constraint],
) -> None:
    """

    """
    for req in jijle.iter("hard_request"):
        if req.text is None:
            logging.warning(
                "Found null hard_request (%s) for job %s, skipping", req, job_id
            )
            continue
        resource_name = req.attrib["name"]

        # we currently won't do anything with any TIME complex type
        def filter_if_time(c: Complex) -> bool:
            if c.complex_type in ["TIME"]:
                if resource_name not in _LOGGED_WARNINGS:
                    logging.warning("Ignoring TIME resource %s", resource_name)
                _LOGGED_WARNINGS.add(resource_name)
                return True
            return False

        complex = ge_env.complexes.get(resource_name)

        if not complex:
            unfiltered_complex = ge_env.unfiltered_complexes.get("resource_name")
            if unfiltered_complex and filter_if_time(unfiltered_complex):
                continue

            if resource_name not in _LOGGED_WARNINGS:
                logging.warning(
                    "Unknown resource %s. This will be ignored for autoscaling purposes and "
                    + "may cause issues with grid engine's ability to schedule this job.",
                    resource_name,
                )
                _LOGGED_WARNINGS.add(resource_name)
            continue
        else:
            req_value = complex.parse(req.text)

        if complex:
            if filter_if_time(complex):
                continue

            if complex.is_excl:
                constraints_out.append({complex.name: req_value})
                continue

            constraints_out[0][complex.name] = req_value

            if complex.shortcut != complex.name and not complex.is_excl:
                # add in the shortcut as well
                constraints_out[0][complex.shortcut] = req_value
            job_resources[complex.name] = req_value
        else:
            constraints_out[0][resource_name] = req_value
            job_resources[resource_name] = req_value


def _pe_job(
    ge_env: GridEngineEnvironment,
    ge_queue: GridEngineQueue,
    jijle: Element,
    pe_elem: Element,
    job_id: str,
    constraints: List[Union[Constraint, Dict]],
    slots: int,
    num_tasks: int,
    owner: Optional[str],
    project: Optional[str],
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

    if not pes:
        logging.error(
            "Could not find a matching pe for '%s' and queue %s",
            pe_name_expr,
            ge_queue.qname,
        )
        return None

    pe = pes[0]
    pe_count = int(pe_elem.text)

    def do_apply_constraints() -> None:
        _apply_constraints(
            jijle=jijle,
            job_id=job_id,
            ge_queue=ge_queue,
            ge_env=ge_env,
            job_resources={},
            constraints_out=constraints,
        )
        queue_and_hostgroup_constraints = []
        for pe in pes:
            hostgroups = ge_queue.get_hostgroups_for_pe(pe.name)

            # optional - can  be None if this is an htc style bucket.
            placement_group = ge_queue.get_placement_group(pe.name)

            for hg_name in hostgroups:
                hostgroup = ge_queue.bound_hostgroups[hg_name]
                child_constraint = hostgroup.make_constraint(ge_env, owner, project, constraints[0])  # type: ignore
                queue_and_hostgroup_constraints.append(
                    HostgroupConstraint(hostgroup, placement_group, child_constraint)
                )

        if len(queue_and_hostgroup_constraints) > 1:
            constraints.append(XOr(*queue_and_hostgroup_constraints))
        else:
            constraints.append(queue_and_hostgroup_constraints[0])

    job_constructor: Callable[[str], Job]

    if pe.is_fixed:
        assert isinstance(pe.allocation_rule, FixedProcesses)
        alloc_rule: FixedProcesses = pe.allocation_rule  # type: ignore
        alloc_rule.fixed_processes
        # constraints[0] = make_quota_bound_consumable_constraint(
        #     "slots", alloc_rule.fixed_processes, ge_queue, ge_env, list(all_hostgroups),
        # )
        constraints[0]["slots"] = alloc_rule.fixed_processes
        num_nodes = int(math.ceil(slots / float(alloc_rule.fixed_processes)))
        constraints.append({"exclusive_task": True})
        do_apply_constraints()

        def job_constructor(job_id: str) -> Job:
            return Job(
                job_id,
                constraints[1:],
                node_count=num_nodes,
                colocated=True,
                packing_strategy="scatter",
            )

    elif pe.allocation_rule.name == "$pe_slots":
        # constraints[0] = make_quota_bound_consumable_constraint(
        #     "slots", pe_count, ge_queue, ge_env, list(all_hostgroups)
        # )
        constraints[0]["slots"] = pe_count
        # this is not colocated, so we can skip the redirection
        do_apply_constraints()
        return [Job(job_id, constraints[1:], iterations=num_tasks)]

    elif pe.allocation_rule.name in ["$round_robin", "$fill_up"]:
        # constraints[0] = make_quota_bound_consumable_constraint(
        #     "slots", 1, ge_queue, ge_env, list(all_hostgroups)
        # )
        constraints[0]["slots"] = 1
        do_apply_constraints()

        def job_constructor(job_id: str) -> Job:
            return Job(
                job_id,
                constraints[1:],
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

    ret: List[Job] = []
    for t in range(array_size):
        task_id = job_id if array_size == 1 else "{}.{}".format(job_id, t)
        # not an array, do the simple thing
        job = job_constructor(task_id)

        ret.append(job)
    return ret


# hostlist              @short.q @fillup0 @fillup1 @fillup2 @roundrobin0 \
#                       @roundrobin1 @roundrobin2 @fixed0 @fixed1 @fixed2


class HostgroupConstraint(BaseNodeConstraint):
    def __init__(
        self,
        hostgroup: BoundHostgroup,
        placement_group: Optional[ht.PlacementGroup],
        child_constraint: Optional[NodeConstraint] = None,
    ) -> None:
        self.hostgroup = hostgroup
        self.placement_group = placement_group
        self.child_constraint = child_constraint

    def satisfied_by_node(self, node: Node) -> SatisfiedResult:
        if self.placement_group:
            if node.placement_group != self.placement_group:

                return SatisfiedResult(
                    "WrongPlacementGroup",
                    self,
                    node,
                    reasons=[
                        "{} is in a different pg: {} != {}".format(
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
                    "{} is in a pg but our job is not colocated: {}".format(
                        node, node.placement_group
                    )
                ],
            )

        node_hostgroups: List[str] = get_node_hostgroups(node)

        if not node_hostgroups:
            node_hostgroups = [] if node.exists else [self.hostgroup.name]

        if self.hostgroup.name not in node_hostgroups:
            msg = "{} is not in the target hostgroup {}: {}".format(
                node, self.hostgroup.name, node_hostgroups
            )
            logging.trace("WrongHostgroup %s", msg)
            return SatisfiedResult("WrongHostgroup", self, node, reasons=[msg],)

        # Checking child constraints after our own because otherwise
        # there will be pointless results logged that will confuse users.
        for child_constraint in self.get_children():
            result = child_constraint.satisfied_by_node(node)
            if not result:
                return result

        return SatisfiedResult("success", self, node)

    def do_decrement(self, node: Node) -> bool:
        current_hostgroups = get_node_hostgroups(node)
        node_hostgroups = set(current_hostgroups or [self.hostgroup.name])

        hg_intersect = self.hostgroup.name in node_hostgroups
        if not hg_intersect:
            raise AssertionError(
                "{} does not intersect {}".format(node_hostgroups, self.hostgroup.name)
            )

        assert node.placement_group in [
            None,
            self.placement_group,
        ], "placement group %s != %s" % (node.placement_group, self.placement_group)

        if node.exists:
            assert (
                node.placement_group == self.placement_group
            ), "placement group %s != %s" % (node.placement_group, self.placement_group)

        else:

            assert node.placement_group in [None, self.placement_group]

            node.placement_group = self.placement_group

        if self.hostgroup.name not in current_hostgroups:
            add_node_to_hostgroup(node, self.hostgroup)

        # hostgroup.constraints should not actually decrement anything
        # they are minimums
        # for child in self.hostgroup.constraints:
        #     assert child.do_decrement(node)

        if self.child_constraint:
            return self.child_constraint.do_decrement(node)
        return True

    def get_children(self) -> List[NodeConstraint]:
        my_cons = [self.child_constraint] if self.child_constraint else []
        return self.hostgroup.constraints + my_cons

    def minimum_space(self, node: Node) -> int:
        m = -1
        for child in self.get_children():
            child_min = child.minimum_space(node)
            # doesn't fit
            if child_min == 0:
                return child_min

            # no opinion (non-consumable constraints)
            if child_min < 0:
                continue

            if m < 0:
                m = child_min
            m = min(m, child_min)

        return m

    def __str__(self) -> str:
        return "HostgroupConstraint(hostgroup={}, placement_group={}, constraints={})".format(
            self.hostgroup.name, self.placement_group, self.child_constraint
        )

    def to_dict(self) -> Dict:
        return {
            "hostgroup-and-pg": {
                "hostgroup": self.hostgroup.to_dict(),
                "placement-group": self.placement_group,
                "seq-no": self.hostgroup.seq_no,
                "constraints": [x.to_dict() for x in self.get_children()],
            }
        }

    @staticmethod
    def from_dict(d: Dict) -> NodeConstraint:
        raise RuntimeError("Not implemented yet: hostgroup-and-pg")
        # if set(d.keys()) != set(["hostgroup-and-pg"]):
        #     raise RuntimeError(
        #         "Unexpected dictionary for QueueAndHostgroups: {}".format(d)
        #     )

        # c = d["hostgroup-and-pg"]
        # # TODO validation
        # specified = set(c.keys())
        # valid = set(
        #     [
        #         "hostgroup",
        #         "user",
        #         "project",
        #         "placement-group",
        #         "seq-no",
        #         "placement-group",
        #         "constraints",
        #     ]
        # )
        # unexpected = specified - valid
        # assert not unexpected, "Unexpected attribute - {}".format(unexpected)

        # return HostgroupConstraint(
        #     BoundHostgroup.from_dict(c["hostgroup"]),
        #     c.get("placement-group"),
        #     c.get("user"),
        #     c.get("project"),
        # )


class GENodeQueue(NodeQueue):
    """
    Custom NodeQueue that prioritizes nodes by slots and bails out
    when there are no slots.
    """

    def node_priority(self, node: Node) -> int:
        return node.available.get("slots", node.available.get("ncpus", 0))

    def early_bailout(self, node: Node) -> EarlyBailoutResult:
        return EarlyBailoutResult("success")
        # prio = self.node_priority(node)
        # if prio > 0:
        #     return EarlyBailoutResult("success")
        # return EarlyBailoutResult("NoSlots", node, ["No more slots/ncpus remaining"])


register_parser("hostgroup-and-pg", HostgroupConstraint.from_dict)


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
