import logging
import math
import os
import socket
import subprocess
import tempfile
import typing
from shutil import which
from subprocess import CalledProcessError, check_call, check_output
from typing import List
from xml.etree import ElementTree

import six
from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.node.node import Node
from hpc.autoscale.job.job import Job


from gridengine import parallel_environments

if not which("qstat"):
    QSTAT_PATH = os.path.expanduser("~/bin/qstat")
    QCONF_PATH = os.path.expanduser("~/bin/qconf")
else:
    QSTAT_PATH = which("qstat") or ""
    QCONF_PATH = which("qconf") or ""
    if not QSTAT_PATH:
        raise RuntimeError("Could not find qstat in PATH: {}".format(os.environ))
    if not QCONF_PATH:
        raise RuntimeError("Could not find qstat in PATH: {}".format(os.environ))


DISABLED_RESOURCE_GROUP = "limitcycleclouddisabled"
DISABLED_HOST_GROUP = "@cycleclouddisabled"


class GridEngineDriver:
    def __init__(self) -> None:
        self.init_draining()
        jobs, scheduler_nodes = _get_jobs_and_nodes()
        self.jobs = jobs
        self.scheduler_nodes = scheduler_nodes

    def init_draining(self) -> None:

        host_groups = check_output([QCONF_PATH, "-shgrpl"]).decode().split()
        logging.debug("Found host groups %s", host_groups)
        if DISABLED_HOST_GROUP not in host_groups:
            fd, create_hgrp_path = tempfile.mkstemp(
                prefix="cycleclouddisabled", suffix=".hgrp"
            )

            with os.fdopen(fd, "w") as fw:
                fw.write("group_name {}\n".format(DISABLED_HOST_GROUP))
                fw.write("hostlist NONE")
            new_env = dict(os.environ)
            new_env["DEBUG_FILE"] = create_hgrp_path
            check_call([QCONF_PATH, "-Ahgrp", create_hgrp_path], env=new_env)

        try:
            resource_groups = (
                check_output([QCONF_PATH, "-srqsl"], stderr=subprocess.PIPE)
                .decode()
                .split()
            )
        except CalledProcessError as e:
            if e.stderr and "no resource quota set list defined" in e.stderr.decode():
                logging.info("'No resource quota set lists defined' error ignored.")
                resource_groups = []
            else:
                # TODO RDH
                raise

        if DISABLED_RESOURCE_GROUP not in resource_groups:
            fd, create_limit_path = tempfile.mkstemp(
                prefix="cycleclouddisabled", suffix=".limit"
            )
            with os.fdopen(fd, "w") as fw:
                fw.write("{\n")
                fw.write("   name         {}\n".format(DISABLED_RESOURCE_GROUP))
                fw.write("   description  NONE\n")
                fw.write("   enabled      FALSE\n")
                fw.write("   limit        to slots=0\n")
                fw.write("}")
            new_env = dict(os.environ)
            new_env["DEBUG_FILE"] = create_limit_path
            check_call([QCONF_PATH, "-Arqs", create_limit_path], env=new_env)

    def handle_draining(
        self, unmatched_nodes: List[SchedulerNode]
    ) -> List[SchedulerNode]:
        # TODO get list of hosts in @disabled
        self.init_draining()
        fqdns = (
            check_output([QCONF_PATH, "-shgrp_resolved", "@cycleclouddisabled"])
            .decode()
            .split()
        )
        disabled_hosts = [x.split(".")[0] for x in fqdns]
        to_shutdown = []

        for node in unmatched_nodes:
            if not node.hostname:
                continue

            if node.hostname not in disabled_hosts:
                try:
                    check_call(
                        [
                            QCONF_PATH,
                            "-aattr",
                            "hostgroup",
                            "hostlist",
                            node.hostname,
                            "@cycleclouddisabled",
                        ]
                    )
                    to_shutdown.append(node)
                except CalledProcessError as e:
                    logging.warning(str(e))

        # check if these hosts are busy...
        return to_shutdown

    def handle_post_delete(self, nodes_to_delete: List[Node]) -> None:

        for node in nodes_to_delete:
            if not node.hostname:
                continue

            try:
                logging.info("Removing host %s via qconf -dh and -ds", node.hostname)
                check_call([QCONF_PATH, "-dh", node.hostname])
                check_call([QCONF_PATH, "-ds", node.hostname])
            except CalledProcessError as e:
                logging.warning(str(e))

    def handle_undraining(self, matched_nodes: List[Node]) -> List[Node]:
        # TODO get list of hosts in @disabled
        self.init_draining()
        resource_groups_stdout = check_output(
            [QCONF_PATH, "-shgrp_resolved", "@cycleclouddisabled"]
        ).decode()
        disabled_hosts = [x.split(".")[0] for x in resource_groups_stdout.split()]
        undrained = []
        for node in matched_nodes:
            if not node.hostname:
                continue

            if node.hostname in disabled_hosts:
                try:
                    check_call(
                        [
                            QCONF_PATH,
                            "-dattr",
                            "hostgroup",
                            "hostlist",
                            node.hostname,
                            "@cycleclouddisabled",
                        ]
                    )
                    undrained.append(node.hostname)
                except CalledProcessError as e:
                    logging.warning(str(e))

        # check if these hosts are busy...
        if undrained:
            logging.info(
                "Removed hostnames %s from @cycleclouddisabled", ",".join(undrained)
            )
        else:
            logging.debug("No hosts were removed from @cycleclouddisabled")

        return undrained

    def handle_join_cluster(self, matched_nodes: List[Node]) -> None:
        """
        1) remove the host from @disabled
        2) create compnode.conf
        3) qconf -ah #{fname} && qconf -as #{fname}
        """
        self.handle_undraining(matched_nodes)

        fqdns = check_output([QCONF_PATH, "-sh"]).decode().splitlines()
        hostnames = [fqdn.split(".")[0] for fqdn in fqdns]

        for compute_node in matched_nodes:
            if not compute_node.exists:
                continue

            if not compute_node.hostname:
                continue

            if compute_node.hostname in hostnames:
                continue
            
            # let's make sure the hostname is valid and reverse
            # dns compatible before adding to GE
            socket.gethostbyname(compute_node.hostname)
            addr_info = socket.gethostbyaddr(compute_node.hostname)
            if addr_info[-1] != compute_node.private_ip:
                logging.warning(
                    "Node %s has a hostname that does not match the"
                    + " private_ip (%s) reported by cyclecloud! Skipping",
                    compute_node,
                    compute_node.private_ip,
                )
                continue

            addr_info_hostname = addr_info[0].split(".")[0]
            if addr_info_hostname.lower() != compute_node.hostname.lower():
                logging.warning(
                    "Node %s has a hostname that can not be queried via reverse"
                    + " dns (private_ip=%s cyclecloud hostname=%s reverse dns hostname=%s)."
                    + "Often this repairs itself. Skipping",
                    compute_node,
                    compute_node.private_ip,
                    compute_node.hostname,
                    addr_info_hostname
                )
                continue

            logging.info("Adding hostname %s", compute_node.hostname)
            try:
                check_call([QCONF_PATH, "-ah", compute_node.hostname])
                check_call([QCONF_PATH, "-as", compute_node.hostname])
            except CalledProcessError as e:
                logging.warn(
                    "Could not add %s to cluster: %s", compute_node.hostname, str(e)
                )

    def __str__(self) -> str:
        return "GEDriver(jobs={}, scheduler_nodes={})".format(
            self.jobs[:100], self.scheduler_nodes[:100]
        )

    def __repr__(self) -> str:
        return str(self)


def _get_jobs_and_nodes() -> typing.Tuple[List[Job], List[SchedulerNode]]:
    # some magic here for the args
    # -g d -- show all the tasks, do not group
    # -u * -- all users
    # -s  pr -- show only pending or running
    # -f -- full output. Ambiguous what this means, but in this case it includes host information so that
    # we can get one consistent view (i.e. not split between two calls, introducing a race condition)
    schedulers = check_output([QCONF_PATH, "-sss"]).decode().splitlines()
    raw_xml = check_output(
        [QSTAT_PATH, "-xml", "-s", "pr", "-r", "-f", "-F", "placement_group"]
    ).decode()
    raw_xml_file = six.StringIO(raw_xml)
    raw_jobs = ElementTree.parse(raw_xml_file)
    root = raw_jobs.getroot()

    pes = parallel_environments.build_parellel_envs()

    def _get(e: typing.Any, attr_name: str) -> typing.Optional[str]:
        child = e.find(attr_name)
        if child is None:
            return None
        return child.text

    def _getr(e: typing.Any, attr_name: str) -> str:
        ret = _get(e, attr_name)
        assert ret, "{} was not defined for element {}".format(attr_name, e)
        assert ret is not None
        return ret

    compute_nodes = []
    for qiqle in root.findall("queue_info/Queue-List"):
        # TODO need a better way to hide the master
        name = _getr(qiqle, "name")

        queue_name, fqdn = name.split("@", 1)
        hostname = fqdn.split(".", 1)[0]
        if hostname in schedulers:
            continue

        slots_total = int(_getr(qiqle, "slots_total"))
        resources: dict = {"slots": slots_total}
        for re in qiqle.iter("resource"):
            resources[re.attrib["name"]] = re.text

        compute_node = SchedulerNode(hostname, resources)

        # decrement using compute_node.available. Just use slots here
        compute_node.available["slots"] = (
            slots_total
            - int(_getr(qiqle, "slots_used"))
            + int(_getr(qiqle, "slots_resv"))
        )  # TODO resv?

        # use assign_job so we just accept that this job is running on this node.
        for jle in qiqle.findall("job_list"):
            compute_node.assign(_getr(jle, "JB_job_number"))

        compute_nodes.append(compute_node)

    autoscale_jobs = []

    for jijle in raw_jobs.findall("job_info/job_list"):
        if _get(jijle, "state") == "running":
            # running jobs are already accounted for above
            continue

        job_id = _getr(jijle, "JB_job_number")
        slots = int(_getr(jijle, "slots"))

        num_tasks = 1
        tasks_expr = _get(jijle, "tasks")
        if tasks_expr:
            num_tasks = _parse_tasks(tasks_expr)
        constraints: typing.List[typing.Dict] = [{"slots": slots}]
        for re in jijle.iter("hard_resource"):
            constraints.append({re.attrib["name"]: re.text})

        pe_elem = jijle.find("requested_pe")
        if pe_elem is not None:
            pe_name = pe_elem.attrib["name"]
            assert pe_name, "{} has no attribute 'name'".format(pe_elem)
            assert pe_elem.text, "{} has no body".format(pe_elem)
            pe_count = int(pe_elem.text)
            if pe_name not in pes:
                # TODO error
                logging.warning("Unknown parellel environment {}!".format(pe_name))
                continue
            pe = pes[pe_name]
            rule = pe["allocation_rule"]
            if rule.isdigit():
                constraints[0]["slots"] = int(rule)
                num_tasks = int(math.ceil(slots / float(rule)))
                autoscale_jobs.append(Job(job_id, constraints, iterations=num_tasks))
            elif rule == "$pe_slots":
                constraints[0]["slots"] = pe_count
                autoscale_jobs.append(Job(job_id, constraints, iterations=num_tasks))
            elif rule == "$round_robin":
                constraints[0]["slots"] = 1
                constraints.append({"exclusive": True})
                num_tasks = slots
                autoscale_jobs.append(Job(job_id, constraints, iterations=num_tasks))
            elif rule == "$fill_up":
                constraints[0]["slots"] = 1
                num_tasks = pe_count * num_tasks
                autoscale_jobs.append(Job(job_id, constraints, iterations=num_tasks))
        else:
            autoscale_jobs.append(Job(job_id, constraints, iterations=num_tasks))

    return autoscale_jobs, compute_nodes


def _parse_tasks(expr: str) -> int:
    try:
        return int(expr)
    except ValueError:
        pass
    start, rest = expr.split("-")
    stop, step = rest.split(":")
    return len(range(int(start), int(stop) + 1, int(step)))
