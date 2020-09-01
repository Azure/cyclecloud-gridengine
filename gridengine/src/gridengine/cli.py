import code
import io
import json
import os
import socket
import sys
import tempfile
import typing
from argparse import ArgumentParser
from copy import deepcopy
from io import TextIOWrapper
from typing import Any, Callable, Dict, Iterable, List, Optional, TextIO, Tuple

from hpc.autoscale import hpclogging as logging
from hpc.autoscale.job import demandprinter
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.demandcalculator import DemandCalculator
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.bucket import NodeBucket
from hpc.autoscale.node.constraints import NodeConstraint, get_constraints
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodemanager import new_node_manager
from hpc.autoscale.results import DefaultContextHandler, register_result_handler
from hpc.autoscale.util import partition, partition_single

from gridengine import autoscaler, environment, parallel_environments
from gridengine.driver import QCONF_PATH, GridEngineDriver, check_call, check_output


def error(msg: Any, *args: Any) -> None:
    print(str(msg) % args, file=sys.stderr)
    sys.exit(1)


def autoscale(
    config: Dict,
    output_columns: Optional[List[str]] = None,
    output_format: Optional[str] = None,
) -> None:
    """Runs actual autoscale process"""
    ctx_handler = register_result_handler(DefaultContextHandler("[initialization]"))
    if output_columns:
        config["output_columns"] = output_columns

    if output_format:
        config["output_format"] = output_format

    autoscaler.autoscale_grid_engine(config, ctx_handler=ctx_handler)


def join_cluster(config: Dict, hostnames: List[str], node_names: List[str]) -> None:
    """Allow nodes to join the cluster"""
    ge_driver, demand_calc, nodes = _find_nodes(config, hostnames, node_names)
    ge_driver.add_nodes_to_cluster(nodes)


def drain_node(
    config: Dict, hostnames: List[str], node_names: List[str], force: bool = False
) -> None:
    """
        Prevent new jobs from running on the node so that it can be safely taken
        offline
    """
    ge_driver, demand_calc, nodes = _find_nodes(config, hostnames, node_names)
    ge_driver.handle_draining(nodes)


def delete_nodes(
    config: Dict, hostnames: List[str], node_names: List[str], force: bool = False
) -> None:
    """Deletes node, including draining post delete handling"""
    ge_driver, demand_calc, nodes = _find_nodes(config, hostnames, node_names)

    if not force:
        for node in nodes:
            if node.assignments:
                error(
                    "Node %s is currently matched to one or more jobs (%s)."
                    + " Please specify --force to continue.",
                    node,
                    node.assignments,
                )

            if node.required:
                error(
                    "Node %s is unmatched but is flagged as required."
                    + " Please specify --force to continue.",
                    node,
                )

    ge_driver.handle_draining(nodes)
    print("Drained {}".format(nodes))

    demand_calc.delete(nodes)
    print("Deleting {}".format(nodes))

    ge_driver.handle_post_delete(nodes)
    print("Deleted {}".format(nodes))


def _find_nodes(
    config: Dict, hostnames: List[str], node_names: List[str]
) -> Tuple[GridEngineDriver, DemandCalculator, List[Node]]:
    hostnames = hostnames or []
    node_names = node_names or []
    ge_env = environment.from_qconf(config)
    ge_driver = autoscaler.new_driver(config, ge_env)

    demand_calc = autoscaler.calculate_demand(config, ge_env, ge_driver)
    demand_result = demand_calc.finish()
    by_hostname = partition_single(
        demand_result.compute_nodes, lambda n: n.hostname_or_uuid.lower()
    )
    by_node_name = partition_single(
        demand_result.compute_nodes, lambda n: n.name.lower()
    )
    found_nodes = []
    for hostname in hostnames:
        if not hostname:
            error("Please specify a hostname")

        if hostname.lower() not in by_hostname:
            # it doesn't exist in CC, but we still want to delete it
            # from the cluster
            by_hostname[hostname.lower()] = SchedulerNode(hostname, {})

        found_nodes.append(by_hostname[hostname.lower()])

    for node_name in node_names:
        if not node_name:
            error("Please specify a node_name")

        if node_name.lower() not in by_node_name:
            error(
                "Could not find a CycleCloud node that has node_name %s."
                + " Run 'nodes' to see available nodes.",
                node_name,
            )
        found_nodes.append(by_node_name[node_name.lower()])

    return ge_driver, demand_calc, found_nodes


def queues(config: Dict) -> None:
    ge_env = environment.from_qconf(config)
    schedulers = check_output([QCONF_PATH, "-sss"]).decode()
    rows: List[List[str]] = []

    for qname, ge_queue in ge_env.queues.items():

        for hgrp in ge_queue.hostlist_groups:
            fqdns = check_output([QCONF_PATH, "-shgrp", hgrp]).decode().splitlines()
            for line in fqdns:
                line = line.strip()
                if not line:
                    continue

                if line.startswith("group_name"):
                    continue

                # trim this out
                if line.startswith("hostlist "):
                    line = line[len("hostlist ") :]  # noqa: E203

                for fqdn_expr in line.split():
                    fqdn_expr = fqdn_expr.strip()
                    if not fqdn_expr or fqdn_expr == "\\":
                        continue
                    host = fqdn_expr.split(".")[0]

                    if host in schedulers:
                        continue

                    rows.append([qname, hgrp, host])

    demandprinter.print_rows(
        columns=["QNAME", "HOSTGROUP", "HOSTNAME"],
        rows=rows,
        stream=sys.stdout,
        output_format="table",
    )


def _master_hostname(config: Dict) -> str:
    master_hostname = config.get("gridengine", {}).get("master")
    if not master_hostname:
        master_hostname = socket.gethostname().split(".")[0]
    return master_hostname


def create_queues(config: Dict) -> None:
    """Creates GE queues based on Configuration.gridengine.queues"""
    check_call(
        [
            QCONF_PATH,
            "-mattr",
            "queue",
            "slots",
            "0",
            "{}@{}".format("all.q", _master_hostname(config)),
        ]
    )

    template = check_output([QCONF_PATH, "-sq", "all.q"]).decode().splitlines()
    template = parallel_environments._flatten_lines(template)

    existing_queues = check_output([QCONF_PATH, "-sql"]).decode().split()
    existing_hostgroups = check_output([QCONF_PATH, "-shgrpl"]).decode().split()
    existing_pes = check_output([QCONF_PATH, "-spl"]).decode().split()

    hostgroup_to_pes_by_q = _hostgroup_to_pes(config, existing_pes)
    queues = config.get("gridengine", {}).get("queues", {})

    for queue_name, queue_config in queues.items():
        hostlist = queues.get("hostlist", ["@" + queue_name])

        if hostlist is None or isinstance(hostlist, str):
            hostlist = [hostlist]

        if not hostlist or hostlist == [None]:
            hostlist = ["NONE"]

        create_queue(
            config,
            template,
            queue_name,
            hostlist,
            hostgroup_to_pes_by_q[queue_name],
            existing_queues,
            existing_hostgroups,
        )


def _hostgroup_to_pes(
    config: Dict, existing_pes: List[str]
) -> Dict[str, Dict[str, List[str]]]:
    default_pes = {}
    for pe_name in existing_pes:
        default_pes[pe_name] = {"hostgroups": [None]}

    hostgroup_to_pes: Dict[str, Dict[str, List[str]]] = {}

    queues = config.get("gridengine", {}).get("queues", {})

    for queue_name, queue_config in queues.items():
        hostgroup_to_pes[queue_name] = {}
        pes: Dict[str, Dict] = queue_config.get("pes", default_pes)

        for pe_name, pe in pes.items():
            hostgroups = pe.get("hostgroups", [])
            if isinstance(hostgroups, str):
                hostgroups = [hostgroups]

            if not hostgroups:
                hostgroups = [None]

            for hg in hostgroups:
                if hg not in hostgroup_to_pes[queue_name]:
                    hostgroup_to_pes[queue_name][hg] = []
                hostgroup_to_pes[queue_name][hg].append(pe_name)

    return hostgroup_to_pes


def create_queue(
    config: Dict,
    template: List[str],
    queue_name: str,
    hostlist: List[str],
    hostgroup_to_pes: Dict[str, List[str]],
    existing_queues: List[str],
    existing_hostgroups: List[str],
) -> None:
    """
    $ qconf -sq parallel
    ...
    seq_no    2,[@quadcore=3],[@hexcore-eth=4],...
    ...
    pe_list   NONE,[@quadcore=make mpi-8 smp],[@hexcore=make mpi-12 smp],...
    ...
    slots     0,[@quadcore=8],[@hexcore=12],...
    ...
    """
    all_hostgroups = []
    for expr in hostlist:
        if expr and expr.startswith("@"):
            all_hostgroups.append(expr)
    all_hostgroups.extend(hostgroup_to_pes.keys())

    master_hostname = _master_hostname(config)

    for hostgroup in all_hostgroups:
        if hostgroup is None:
            continue

        if hostgroup in existing_hostgroups:
            print("Hostgroup {} already exists".format(hostgroup))
        else:
            _create_hostgroup(master_hostname, queue_name, hostgroup)
            existing_hostgroups.append(hostgroup)

    exists = queue_name in existing_queues
    if exists:
        print("Queue {} already exists".format(queue_name))

    _create_queue(
        template, master_hostname, queue_name, hostlist, hostgroup_to_pes, exists
    )


def _create_queue(
    template: List[str],
    master_hostname: str,
    queue_name: str,
    hostlist: List[str],
    hostgroup_to_pes: Dict[str, List[str]],
    exists: bool,
) -> None:
    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "w") as fw:
            _create_queue_file(fw, template, queue_name, hostlist, hostgroup_to_pes)
    finally:
        pass
        # try:
        #     os.remove(path)
        # except Exception:
        #     pass
    check_call([QCONF_PATH, "-Mq" if exists else "-Aq", path])

    check_call(
        [
            QCONF_PATH,
            "-mattr",
            "queue",
            "slots",
            "0",
            "{}@{}".format(queue_name, master_hostname),
        ]
    )


def _create_queue_file(
    fw: TextIOWrapper,
    template: List[str],
    queue_name: str,
    hostlist: List[str],
    hostgroup_to_pes: Dict[str, List[str]],
) -> None:

    template = parallel_environments._flatten_lines(template)

    print("Creating queue {}".format(queue_name))
    full_hostlist = hostlist + []
    for hostgroup in hostgroup_to_pes:
        # make sure that we also include the relevant pg hostgroups
        if hostgroup not in full_hostlist:
            full_hostlist.append(hostgroup)

    if len(full_hostlist) > 1:
        # NONE doesn't make sense if there actually is a hostgroup.
        full_hostlist = [x for x in full_hostlist if x not in [None, "NONE"]]
    else:
        full_hostlist = ["NONE" for x in full_hostlist if x is None]

    for line in template:
        if line.startswith("qname"):
            fw.write("qname                 ")
            fw.write(queue_name)

        elif line.startswith("hostlist"):
            fw.write("hostlist              ")
            fw.write(" ".join(full_hostlist))

        elif line.startswith("pe_list"):
            fw.write("pe_list               NONE")
            for hostgroup, pes in hostgroup_to_pes.items():
                pe_expr = " ".join(pes)
                if hostgroup is None:
                    for hostslist_item in hostlist:
                        fw.write(",[{}={}]".format(hostslist_item, pe_expr))
                else:
                    fw.write(",[{}={}]".format(hostgroup, pe_expr))
        elif line.startswith("slots"):
            fw.write("slots               0")
        else:
            fw.write(line)

        fw.write("\n")


def _create_hostgroup(master_hostname: str, queue_name: str, hostgroup: str) -> None:
    print("Creating hostgroup {}".format(hostgroup))

    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "w") as fw:
            fw.write("group_name {}\n".format(hostgroup))
            fw.write("hostlist NONE")

        check_call([QCONF_PATH, "-Ahgrp", path])
        hostname = socket.gethostname().split(".")[0]

        check_call([QCONF_PATH, "-aattr", "hostgroup", "hostlist", hostname, hostgroup])

    finally:
        try:
            os.remove(path)
        except Exception:
            pass


def amend_queue_config(config: Dict, writer: TextIO = sys.stdout) -> None:
    """
    Used during initialization to create the default autoscale queue config.
    """

    # """
    # ...
    # "gridengine": {
    # "queues": {
    #   "hpc.q": {
    #     "constraints": [{"node.nodearray": "hpc"}],
    #     "hostlist": "NONE",
    #     "pes": {
    #       "make": {"hostgroups": ["@hpc.q_make"]},
    #       "mpi": {"hostgroups": ["@hpc.q_mpi"]},
    #       "mpislots": {"hostgroups": ["@hpc.q_mpislots"]},
    #       "smpslots": {"hostgroups": []}}},
    #   "htc.q": {
    #     "constraints": [{"node.nodearray": "htc"}],
    #     "hostlist": "@htc.q",
    #     "pes": {
    #       "smpslots": { "hostgroups": [] },
    #       "make": { "hostgroups": [] }
    #     }}}}}
    # """

    node_mgr = new_node_manager(config)
    new_config = deepcopy(config)
    new_config["gridengine"] = ge_config = new_config.get("gridengine", {})
    ge_config["queues"] = queues = ge_config.get("queues", {})
    assert isinstance(
        queues, dict
    ), "Invalid config. gridengine.queues must be a dictionary."

    by_nodearray = partition(node_mgr.get_buckets(), lambda b: b.nodearray)
    ge_env = environment.from_qconf(new_config)

    for nodearray, buckets in by_nodearray.items():
        bucket = buckets[0]
        nodearray_ge_config = bucket.software_configuration.get("gridengine", {})
        qname = nodearray_ge_config.get("qname", "{}.q".format(nodearray))
        queues[qname] = queue = queues.get(nodearray, {})
        colocated = str(nodearray_ge_config.get("colocated", False)).lower() == "true"
        pes = nodearray_ge_config.get("pes", " ".join(ge_env.pes.keys())).split()
        queue["constraints"] = [{"node.nodearray": nodearray}]
        if colocated:
            queue["hostlist"] = "NONE"
        else:
            queue["hostlist"] = "@{}".format(qname)

        for pe_name in pes:
            if pe_name not in ge_env.pes:
                raise RuntimeError(
                    "Unknown parallel_environment {}: Expected one of {}".format(
                        pe_name, " ".join(ge_env.pes.keys())
                    )
                )

            if "pes" not in queue:
                queue["pes"] = {}

            queue["pes"][pe_name] = {"hostgroups": []}

            if not colocated:
                queue["pes"][pe_name]["hostgroups"].append(None)
                continue

            pe = ge_env.pes[pe_name]
            if pe.requires_placement_groups:
                queue["pes"][pe_name]["hostgroups"].append(
                    "@{}_{}".format(qname, pe_name)
                )

    json.dump(new_config, writer, indent=2)


def demand(
    config: Dict,
    jobs: Optional[str] = None,
    scheduler_nodes: Optional[str] = None,
    output_columns: Optional[List[str]] = None,
    output_format: Optional[str] = None,
) -> None:
    """Runs autoscale in dry run mode to see the demand for new nodes"""
    ctx = DefaultContextHandler("[demand-cli]")
    register_result_handler(ctx)
    ge_env = environment.from_qconf(config)
    ge_driver = autoscaler.new_driver(config, ge_env)
    config = ge_driver.preprocess_config(config)
    demand_calc = autoscaler.calculate_demand(config, ge_env, ge_driver, ctx)
    demand_result = demand_calc.finish()

    autoscaler.print_demand(config, demand_result, output_columns, output_format)


def nodes(
    config: Dict,
    constraint_expr: str,
    output_columns: Optional[List[str]] = None,
    output_format: Optional[str] = None,
) -> None:
    """Query nodes"""
    ge_env = environment.from_qconf(config)
    ge_driver = autoscaler.new_driver(config, ge_env)
    dcalc = autoscaler.new_demand_calculator(config, ge_env, ge_driver)
    filtered = _query_with_constraints(
        config, constraint_expr, dcalc.node_mgr.get_nodes()
    )

    demand_result = DemandResult([], filtered, [], [])
    autoscaler.print_demand(config, demand_result, output_columns)


def jobs(config: Dict) -> None:
    """Writes out Job objects as json"""
    ge_env = environment.from_qconf(config)
    json.dump(ge_env.jobs, sys.stdout, indent=2, default=lambda x: x.to_dict())


def scheduler_nodes(config: Dict) -> None:
    """Writes out SchedulerNode objects as json"""
    ge_env = environment.from_qconf(config)
    json.dump(ge_env.nodes, sys.stdout, indent=2, default=lambda x: x.to_dict())


def jobs_and_nodes(config: Dict) -> None:
    """Writes out SchedulerNode and Job objects as json - simultaneously to avoid race"""
    ge_env = environment.from_qconf(config)
    to_dump = {"jobs": ge_env.jobs, "nodes": ge_env.nodes}
    json.dump(to_dump, sys.stdout, indent=2, default=lambda x: x.to_dict())


def complexes(config: Dict, include_irrelevant: bool = False) -> None:
    """Prints out, by default, only relevant complexes"""
    relevant: typing.Optional[typing.Set[str]]
    if include_irrelevant:
        ge_config = config.get("gridengine", {})
        if "relevant_complexes" in ge_config:
            ge_config.pop("relevant_complexes")

    relevant = set(config.get("gridengine", {}).get("relevant_complexes", []))

    already_printed: typing.Set[str] = set()
    for complex in parallel_environments.read_complexes(config).values():
        if (
            include_irrelevant
            or complex.name in relevant
            and complex.name not in already_printed
        ):
            print(repr(complex))
            already_printed.add(complex.name)


def buckets(
    config: Dict,
    constraint_expr: str,
    output_columns: Optional[List[str]] = None,
    output_format: Optional[str] = None,
) -> None:
    """Prints out autoscale bucket information, like limits etc"""
    ge_env = environment.from_qconf(config)
    ge_driver = autoscaler.new_driver(config, ge_env)
    config = ge_driver.preprocess_config(config)
    node_mgr = new_node_manager(config)
    specified_output_columns = output_columns
    output_columns = output_columns or [
        "nodearray",
        "placement_group",
        "vm_size",
        "vcpu_count",
        "pcpu_count",
        "memory",
        "available_count",
    ]

    if specified_output_columns is None:
        for bucket in node_mgr.get_buckets():
            for resource_name in bucket.resources:
                if resource_name not in output_columns:
                    output_columns.append(resource_name)

        for attr in dir(bucket.limits):
            if attr[0].isalpha() and "count" in attr:
                value = getattr(bucket.limits, attr)
                if isinstance(value, int):
                    bucket.resources[attr] = value
                    bucket.example_node._resources[attr] = value

    filtered = _query_with_constraints(config, constraint_expr, node_mgr.get_buckets())

    demand_result = DemandResult([], [f.example_node for f in filtered], [], [])

    if "all" in output_columns:
        output_columns = ["all"]
    config["output_columns"] = output_columns

    autoscaler.print_demand(config, demand_result, output_columns, output_format)


def resources(config: Dict, constraint_expr: str) -> None:
    ge_env = environment.from_qconf(config)
    ge_driver = autoscaler.new_driver(config, ge_env)
    node_mgr = new_node_manager(config, existing_nodes=ge_driver)

    filtered = _query_with_constraints(config, constraint_expr, node_mgr.get_buckets())

    columns = set()
    for node in filtered:
        columns.update(set(node.resources.keys()))
        columns.update(set(node.resources.keys()))
    config["output_columns"]


def initconfig(**config: Dict) -> None:
    #     autoscale_config = {
    #   :cluster_name => node[:cyclecloud][:cluster][:name],
    #   :username => node[:cyclecloud][:config][:username],
    #   :password => node[:cyclecloud][:config][:password],
    #   :url => node[:cyclecloud][:config][:web_server],
    #   :lock_file => "#{node[:cyclecloud][:bootstrap]}/scalelib.lock",
    #   :logging => {:config_file => "#{node[:cyclecloud][:bootstrap]}/gridengine/logging.conf"},
    #   :default_resources => [ { :select => {}, :name => "slots", :value => "node.vcpu_count"} ],
    #   :gridengine => {:queues => { },
    #                   :relevant_complexes =>relevant_complexes,
    #                   :idle_timeout => node[:gridengine][:idle_timeout]}
    # }
    if "gridengine" not in config:
        config["gridengine"] = {}

    for key in list(config.keys()):

        if "__" in key:
            parent, child = key.split("__")
            if parent not in config:
                config[parent] = {}
            config[parent][child] = config.pop(key)

    amend_queue_config(config, sys.stdout)


def _parse_contraint(constraint_expr: str) -> List[NodeConstraint]:
    try:
        constraint_parsed = json.loads(constraint_expr)
    except Exception as e:
        print(
            "Could not parse constraint as json '{}' - {}".format(constraint_expr, e),
            file=sys.stderr,
        )
        sys.exit(1)

    if not isinstance(constraint_parsed, list):
        constraint_parsed = [constraint_parsed]

    return get_constraints(constraint_parsed)


T = typing.TypeVar("T", Node, NodeBucket)


def _query_with_constraints(
    config: Dict, constraint_expr: str, targets: List[T]
) -> List[T]:
    constraints = _parse_contraint(constraint_expr)

    filtered: List[T] = []
    for t in targets:
        satisfied = True
        for c in constraints:
            if isinstance(t, Node):
                result = c.satisfied_by_node(t)
            else:
                result = c.satisfied_by_bucket(t)
            if not result:
                satisfied = False
                print(result)
                break

        if satisfied:
            filtered.append(t)
    return filtered


class ReraiseAssertionInterpreter(code.InteractiveConsole):
    def __init__(
        self,
        locals: Optional[Dict] = None,
        filename: str = "<console>",
        reraise: bool = True,
    ) -> None:
        code.InteractiveConsole.__init__(self, locals=locals, filename=filename)
        self.reraise = reraise
        hist_file = os.path.expanduser("~/.cyclegehistory")

        if os.path.exists(hist_file):
            with open(hist_file) as fr:
                self.history_lines = fr.readlines()
        else:
            self.history_lines = []
        self.history_fw = open(hist_file, "a")

    def raw_input(self, prompt: str = "") -> str:
        line = super().raw_input(prompt)
        if line.strip():
            self.history_fw.write(line)
            self.history_fw.write("\n")
            self.history_fw.flush()
        return line

    def showtraceback(self) -> None:
        if self.reraise:
            _, value, _ = sys.exc_info()
            if isinstance(value, AssertionError) or isinstance(value, SyntaxError):
                raise value

        return code.InteractiveConsole.showtraceback(self)


def shell(config: Dict) -> None:
    """
        Provides read only interactive shell. type gehelp()
        in the shell for more information
    """
    ctx = DefaultContextHandler("[interactive-readonly]")

    ge_env = environment.from_qconf(config)
    ge_driver = autoscaler.new_driver(config, ge_env)
    config = ge_driver.preprocess_config(config)
    ge_env = environment.from_qconf(config)
    demand_calc = autoscaler.new_demand_calculator(config, ge_env, ge_driver, ctx)

    queues = ge_env.queues

    def gehelp() -> None:
        print("config       - dict representing autoscale configuration.")
        print("dbconn       - Read-only SQLite conn to node history")
        print("demand_calc  - DemandCalculator")
        print("ge_driver    - GEDriver object.")
        print("jobs         - List[Job] from ge_driver")
        print("node_mgr     - NodeManager")
        print("logging      - HPCLogging module")
        print("queues      - GridEngineQueue objects")

    shell_locals = {
        "config": config,
        "ctx": ctx,
        "ge_driver": ge_driver,
        "demand_calc": demand_calc,
        "node_mgr": demand_calc.node_mgr,
        "jobs": ge_env.jobs,
        "dbconn": demand_calc.node_history.conn,
        "gehelp": gehelp,
        "queues": queues,
        "ge_env": ge_env,
    }
    banner = "\nCycleCloud GE Autoscale Shell"
    interpreter = ReraiseAssertionInterpreter(locals=shell_locals)
    try:
        __import__("readline")
        # some magic - create a completer that is bound to the locals in this interpreter and not
        # the __main__ interpreter.
        interpreter.push("import readline, rlcompleter")
        interpreter.push('readline.parse_and_bind("tab: complete")')
        interpreter.push("_completer = rlcompleter.Completer(locals())")
        interpreter.push("def _complete_helper(text, state):")
        interpreter.push("    ret = _completer.complete(text, state)")
        interpreter.push('    ret = ret + ")" if ret[-1] == "(" else ret')
        interpreter.push("    return ret")
        interpreter.push("")
        interpreter.push("readline.set_completer(_complete_helper)")
        for item in interpreter.history_lines:
            if '"""' in item:
                interpreter.push("readline.add_history('''%s''')" % item.rstrip("\n"))
            else:
                interpreter.push('readline.add_history("""%s""")' % item.rstrip("\n"))
        interpreter.push("from hpc.autoscale.job.job import Job\n")
        interpreter.push("from hpc.autoscale import hpclogging as logging\n")

    except ImportError:
        banner += (
            "\nWARNING: `readline` is not installed, so autocomplete will not work."
        )

    interpreter.interact(banner=banner)


def main(argv: Iterable[str] = None) -> None:
    default_install_dir = os.path.join("/", "opt", "cycle", "gridengine")

    parser = ArgumentParser()
    sub_parsers = parser.add_subparsers()

    def csv_list(x: str) -> List[str]:
        return [x.strip() for x in x.split(",")]

    help_msg = io.StringIO()

    def add_parser(
        name: str, func: Callable, read_only: bool = True, skip_config: bool = False
    ) -> ArgumentParser:
        doc_str = (func.__doc__ or "").strip()
        doc_str = " ".join([x.strip() for x in doc_str.splitlines()])
        help_msg.write("\n    {:20} - {}".format(name, doc_str))

        default_config: Optional[str]
        default_config = os.path.join(default_install_dir, "autoscale.json")
        if not os.path.exists(default_config):
            default_config = None

        new_parser = sub_parsers.add_parser(name)
        new_parser.set_defaults(func=func, read_only=read_only)

        if skip_config:
            return new_parser

        def parse_config(c: str) -> Dict:
            try:
                if not c:
                    raise RuntimeError("Did not specify config -c/--config")
                with open(c) as fr:
                    config = json.load(fr)
                # make sure we initialize logging ASAP
                logging.initialize_logging(config)
                return config
            except Exception as e:
                print(str(e), file=sys.stderr)
                sys.exit(1)

        new_parser.add_argument(
            "--config",
            "-c",
            default=default_config,
            required=not bool(default_config),
            type=parse_config,  # type: ignore
        )
        return new_parser

    def str_list(c: str) -> List[str]:
        return c.split(",")

    def add_parser_with_columns(
        name: str, func: Callable, read_only: bool = True
    ) -> ArgumentParser:
        parser = add_parser(name, func, read_only)

        def parse_format(c: str) -> str:
            c = c.lower()
            if c in ["json", "table", "table_headerless"]:
                return c
            print("Expected json, table or table_headerless - got", c, file=sys.stderr)
            sys.exit(1)

        parser.add_argument("--output-columns", "-o", type=str_list)
        parser.add_argument("--output-format", "-F", type=parse_format)
        return parser

    add_parser_with_columns("autoscale", autoscale, read_only=False)

    add_parser_with_columns("buckets", buckets).add_argument(
        "--constraint-expr", "-C", default="[]"
    )

    add_parser("complexes", complexes).add_argument(
        "-a", "--include-irrelevant", action="store_true", default=False
    )

    delete_parser = add_parser("delete_nodes", delete_nodes, read_only=False)
    delete_parser.add_argument("-H", "--hostnames", type=str_list, default=[])
    delete_parser.add_argument("-N", "--node-names", type=str_list, default=[])
    delete_parser.add_argument("--force", action="store_true", default=False)

    add_parser_with_columns("demand", demand).add_argument(
        "--jobs", "-j", default=None, required=False
    )

    add_parser("drain_node", drain_node, read_only=False).add_argument(
        "-H", "--hostname", required=True
    )

    initconfig_parser = add_parser(
        "initconfig", initconfig, read_only=False, skip_config=True
    )

    initconfig_parser.add_argument("--cluster-name", required=True)
    initconfig_parser.add_argument("--username", required=True)
    initconfig_parser.add_argument("--password")
    initconfig_parser.add_argument("--url", required=True)
    initconfig_parser.add_argument(
        "--log-config",
        default=os.path.join(default_install_dir, "logging.conf"),
        dest="logging__config_file",
    )
    initconfig_parser.add_argument(
        "--lock-file", default=os.path.join(default_install_dir, "scalelib.lock")
    )
    initconfig_parser.add_argument(
        "--default-resource",
        type=json.loads,
        action="append",
        default=[],
        dest="default_resources",
    )
    initconfig_parser.add_argument(
        "--relevant-complexes",
        default=["slots", "slot_type", "exclusive"],
        type=csv_list,
        dest="gridengine__relevant_complexes",
    )

    initconfig_parser.add_argument(
        "--idle-timeout", default=300, type=int, dest="gridengine__idle_timeout"
    )

    add_parser("jobs", jobs)
    add_parser("jobs_and_nodes", jobs_and_nodes)

    add_parser("join_cluster", join_cluster).add_argument(
        "-H", "--hostname", type=str_list, required=True
    )

    add_parser_with_columns("nodes", nodes).add_argument(
        "--constraint-expr", "-C", default="[]"
    )

    add_parser("scheduler_nodes", scheduler_nodes)

    help_msg.write("\nadvanced usage:")
    add_parser("amend_queue_config", amend_queue_config, read_only=False)
    add_parser("create_queues", create_queues, read_only=False)
    add_parser("queues", queues, read_only=True)
    add_parser("shell", shell)

    parser.usage = help_msg.getvalue()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    if args.read_only:
        args.config["read_only"] = True
        args.config["lock_file"] = None

    kwargs = {}
    for k in dir(args):
        if k[0].islower() and k not in ["read_only", "func"]:
            kwargs[k] = getattr(args, k)

    try:
        args.func(**kwargs)
    except Exception as e:
        logging.exception(str(e))
        print(str(e))
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
