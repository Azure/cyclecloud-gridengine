import code
import io
import json
import os
import re
import sys
import typing
from argparse import ArgumentParser
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, TextIO, Tuple

from hpc.autoscale import hpclogging as logging
from hpc.autoscale.job import demandprinter
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.demandcalculator import DemandCalculator
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.bucket import NodeBucket
from hpc.autoscale.node.constraints import NodeConstraint, get_constraints
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodemanager import new_node_manager
from hpc.autoscale.results import (
    DefaultContextHandler,
    MatchResult,
    register_result_handler,
)
from hpc.autoscale.util import partition_single

from gridengine import autoscaler, environment, parallel_environments, validate
from gridengine.driver import (
    QCONF_PATH,
    GridEngineDriver,
    QueueAndHostgroupConstraint,
    check_output,
)


def error(msg: Any, *args: Any) -> None:
    print(str(msg) % args, file=sys.stderr)
    sys.exit(1)


def warn(msg: Any, *args: Any) -> None:
    print(str(msg) % args, file=sys.stderr)


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
    config: Dict,
    hostnames: List[str],
    node_names: List[str],
    force: bool = False,
    do_delete: bool = True,
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

            if node.keep_alive:
                error(
                    "Node %s is marked as KeepAlive=true. Please exclude this.", node,
                )

            if node.required:
                error(
                    "Node %s is unmatched but is flagged as required."
                    + " Please specify --force to continue.",
                    node,
                )

    ge_driver.handle_draining(nodes)
    print("Drained {}".format([str(n) for n in nodes]))

    if do_delete:
        demand_calc.delete(nodes)
        print("Deleting {}".format(nodes))

    ge_driver.handle_post_delete(nodes)
    print("Removed from cluster {}".format([str(n) for n in nodes]))


def remove_nodes(
    config: Dict, hostnames: List[str], node_names: List[str], force: bool = False
) -> None:
    delete_nodes(config, hostnames, node_names, force, do_delete=False)


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


def validate_func(config: Dict) -> None:
    ge_env = environment.from_qconf(config)
    queue: parallel_environments.GridEngineQueue
    failure = False
    failure = validate.validate_nodes(config, warn) or failure
    for qname, queue in ge_env.queues.items():
        failure = validate.validate_ht_hostgroup(queue, warn) or failure
        failure = validate.validate_pe_hostgroups(queue, warn) or failure

    if failure:
        sys.exit(1)


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


def initconfig(writer: TextIO = sys.stdout, **config: Dict) -> None:
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
    json.dump(config, writer, indent=2)


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


def analyze(config: Dict, job_id: str) -> None:

    ctx = DefaultContextHandler("[demand-cli]")

    register_result_handler(ctx)
    ge_env = environment.from_qconf(config)
    ge_driver = autoscaler.new_driver(config, ge_env)
    config = ge_driver.preprocess_config(config)
    autoscaler.calculate_demand(config, ge_env, ge_driver, ctx)

    key = "[job {}]".format(job_id)
    results = ctx.by_context[key]
    for result in results:
        if isinstance(result, MatchResult) and result:
            continue
        QueueAndHostgroupConstraint
        if isinstance(result, QueueAndHostgroupConstraint) and not result:
            continue
        print(result.message)
    # autoscaler.print_demand(config, demand_result, output_columns, output_format)


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
            try:
                if '"""' in item:
                    interpreter.push(
                        "readline.add_history('''%s''')" % item.rstrip("\n")
                    )
                else:
                    interpreter.push(
                        'readline.add_history("""%s""")' % item.rstrip("\n")
                    )
            except Exception:
                pass
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

    remove_parser = add_parser("remove_nodes", remove_nodes, read_only=False)
    remove_parser.add_argument("-H", "--hostnames", type=str_list, default=[])
    remove_parser.add_argument("-N", "--node-names", type=str_list, default=[])
    remove_parser.add_argument("--force", action="store_true", default=False)

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
    add_parser("validate", validate_func, read_only=True)
    add_parser("queues", queues, read_only=True)
    add_parser("shell", shell)
    analyze_parser = add_parser("analyze", analyze)
    analyze_parser.add_argument("--job-id", "-j")

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
        print(str(e), file=sys.stderr)
        if hasattr(e, "message"):
            print(getattr(e, "message"), file=sys.stderr)
        logging.debug("Full stacktrace", exc_info=sys.exc_info())
        sys.exit(1)


if __name__ == "__main__":
    main()
