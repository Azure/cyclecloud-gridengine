import json
import os
import sys
import typing
from argparse import ArgumentParser
from typing import Any, Dict, List, Optional

from hpc.autoscale import hpclogging as logging
from hpc.autoscale.job import demandcalculator as dcalclib
from hpc.autoscale.job import demandprinter
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.demandcalculator import DemandCalculator
from hpc.autoscale.node.nodehistory import NodeHistory, SQLiteNodeHistory
from hpc.autoscale.results import DefaultContextHandler, register_result_handler
from hpc.autoscale.util import SingletonLock

from gridengine import environment as envlib
from gridengine.environment import GridEngineEnvironment

if typing.TYPE_CHECKING:
    from gridengine.driver import GridEngineDriver

_exit_code = 0


def autoscale_grid_engine(
    config: Dict[str, Any],
    ge_env: Optional[GridEngineEnvironment] = None,
    ge_driver: Optional["GridEngineDriver"] = None,
    ctx_handler: Optional[DefaultContextHandler] = None,
    node_history: Optional[NodeHistory] = None,
    dry_run: bool = False,
) -> DemandResult:
    global _exit_code

    assert not config.get("read_only", False)
    if dry_run:
        logging.warning("Running gridengine autoscaler in dry run mode")
        # allow multiple instances
        config["lock_file"] = None
        # put in read only mode
        config["read_only"] = True

    if ge_env is None:
        ge_env = envlib.from_qconf(config)

    # interface to GE, generally by cli
    if ge_driver is None:
        # allow tests to pass in a mock
        ge_driver = new_driver(config, ge_env)

    config = ge_driver.preprocess_config(config)

    logging.debug("Driver = %s", ge_driver)

    invalid_nodes = []

    for node in ge_env.nodes:
        # many combinations of a u and other states. However,
        # as long as a and u are in there it is down
        state = node.metadata.get("state", "")
        if "a" in state and "u" in state:
            invalid_nodes.append(node)

    ge_driver.clean_hosts(invalid_nodes)

    demand_calculator = calculate_demand(
        config, ge_env, ge_driver, ctx_handler, node_history
    )

    ge_driver.handle_failed_nodes(demand_calculator.node_mgr.get_failed_nodes())

    demand_result = demand_calculator.finish()

    if ctx_handler:
        ctx_handler.set_context("[joining]")

    # details here are that we pass in nodes that matter (matched) and the driver figures out
    # which ones are new and need to be added via qconf
    joined = ge_driver.handle_join_cluster(
        [x for x in demand_result.compute_nodes if x.exists]
    )

    ge_driver.handle_post_join_cluster(joined)

    if ctx_handler:
        ctx_handler.set_context("[scaling]")

    # bootup all nodes. Optionally pass in a filtered list
    if demand_result.new_nodes:
        if not dry_run:
            demand_calculator.bootup()

    if not dry_run:
        demand_calculator.update_history()

    # we also tell the driver about nodes that are unmatched. It filters them out
    # and returns a list of ones we can delete.
    idle_timeout = int(config.get("idle_timeout", 300))
    boot_timeout = int(config.get("boot_timeout", 3600))
    logging.fine("Idle timeout is %s", idle_timeout)

    unmatched_for_5_mins = demand_calculator.find_unmatched_for(at_least=idle_timeout)
    timed_out_booting = demand_calculator.find_booting(at_least=boot_timeout)

    timed_out_to_deleted = []
    unmatched_nodes_to_delete = []

    if timed_out_booting:
        logging.info(
            "The following nodes have timed out while booting: %s", timed_out_booting
        )
        timed_out_to_deleted = ge_driver.handle_boot_timeout(timed_out_booting) or []

    if unmatched_for_5_mins:
        logging.info("unmatched_for_5_mins %s", unmatched_for_5_mins)
        unmatched_nodes_to_delete = (
            ge_driver.handle_draining(unmatched_for_5_mins) or []
        )

    nodes_to_delete = []
    for node in timed_out_to_deleted + unmatched_nodes_to_delete:
        if node.assignments:
            logging.warning(
                "%s has jobs assigned to it so we will take no action.", node
            )
            continue
        nodes_to_delete.append(node)

    if nodes_to_delete:
        try:
            logging.info("Deleting %s", [str(n) for n in nodes_to_delete])
            delete_result = demand_calculator.delete(nodes_to_delete)

            if delete_result:
                # in case it has anything to do after a node is deleted (usually just remove it from the cluster)
                ge_driver.handle_post_delete(delete_result.nodes)
        except Exception as e:
            _exit_code = 1
            logging.warning("Deletion failed, will retry on next iteration: %s", e)
            logging.exception(str(e))

    print_demand(config, demand_result, log=not dry_run)

    return demand_result


def new_demand_calculator(
    config: Dict,
    ge_env: Optional[GridEngineEnvironment] = None,
    ge_driver: Optional["GridEngineDriver"] = None,
    ctx_handler: Optional[DefaultContextHandler] = None,
    node_history: Optional[NodeHistory] = None,
    singleton_lock: Optional[SingletonLock] = None,
) -> DemandCalculator:
    if ge_env is None:
        ge_env = envlib.from_qconf(config)

    if ge_driver is None:
        ge_driver = new_driver(config, ge_env)

    if node_history is None:
        db_path = config.get("nodehistorydb")
        if not db_path:
            db_dir = "/opt/cycle/jetpack/system/bootstrap/gridengine"
            if not os.path.exists(db_dir):
                db_dir = os.getcwd()
            db_path = os.path.join(db_dir, "nodehistory.db")

        read_only = config.get("read_only", False)
        node_history = SQLiteNodeHistory(db_path, read_only)

        node_history.create_timeout = config.get("boot_timeout", 3600)
        node_history.last_match_timeout = config.get("idle_timeout", 300)

    return dcalclib.new_demand_calculator(
        config,
        existing_nodes=ge_env.nodes,
        node_history=node_history,
        node_queue=ge_driver.new_node_queue(),
        singleton_lock=singleton_lock,  # it will handle the none case
    )


def calculate_demand(
    config: Dict,
    ge_env: GridEngineEnvironment,
    ge_driver: "GridEngineDriver",
    ctx_handler: Optional[DefaultContextHandler] = None,
    node_history: Optional[NodeHistory] = None,
) -> DemandCalculator:

    demand_calculator = new_demand_calculator(
        config, ge_env, ge_driver, ctx_handler, node_history
    )

    for name, default_complex in ge_env.complexes.items():
        if name == "slots":
            continue

        if default_complex.default is None:
            continue

        if not default_complex.requestable:
            continue

        logging.trace("Adding default resource %s=%s", name, default_complex.default)
        demand_calculator.node_mgr.add_default_resource(
            {}, name, default_complex.default
        )

    for bucket in demand_calculator.node_mgr.get_buckets():
        if "slots" not in bucket.resources:
            default = (
                '"default_resources": [{"select": {"node.nodearray": "%s"}, "name": "slots", "value": "node.vcpu_count"}]'
                % (bucket.nodearray)
            )
            demand_calculator.node_mgr.add_default_resource(
                selection={"node.nodearray": bucket.nodearray},
                resource_name="slots",
                default_value="node.vcpu_count",
            )

            logging.warning(
                """slots is not defined for bucket {}. Try adding {} to your config.""".format(
                    bucket, default
                )
            )

    for job in ge_env.jobs:
        if job.metadata.get("job_state") == "running":
            continue

        if ctx_handler:
            ctx_handler.set_context("[job {}]".format(job.name))
        demand_calculator.add_job(job)

    return demand_calculator


def print_demand(
    config: Dict,
    demand_result: DemandResult,
    output_columns: Optional[List[str]] = None,
    output_format: Optional[str] = None,
    log: bool = False,
) -> None:
    # and let's use the demand printer to print the demand_result.
    if not output_columns:
        output_columns = config.get(
            "output_columns",
            [
                "name",
                "hostname",
                "job_ids",
                "exists",
                "required",
                "managed",
                "slots",
                "*slots",
                "vm_size",
                "memory",
                "vcpu_count",
                "state",
                "placement_group",
                "create_time_remaining",
                "idle_time_remaining",
            ],
        )

    if "all" in output_columns:  # type: ignore
        output_columns = []

    output_format = output_format or "table"

    demandprinter.print_demand(
        output_columns, demand_result, output_format=output_format, log=log,
    )
    return demand_result


def new_driver(config: Dict, ge_env: GridEngineEnvironment) -> "GridEngineDriver":
    import importlib

    ge_config = config.get("gridengine", {})

    # # just shorthand for gridengine.deferdriver.DeferredDriver
    # if ge_config.get("driver_scripts_dir"):
    #     deferred_qname = "gridengine.deferdriver.DeferredDriver"
    #     if ge_config.get("driver", deferred_qname) == deferred_qname:
    #         ge_config["driver"] = deferred_qname

    driver_expr = ge_config.get("driver", "gridengine.driver.new_driver")

    if "." not in driver_expr:
        raise BadDriverError(driver_expr)

    module_expr, func_or_class_name = driver_expr.rsplit(".", 1)

    try:
        module = importlib.import_module(module_expr)
    except Exception as e:
        logging.exception(
            "Could not load module %s. Is it in the"
            + " PYTHONPATH environment variable? %s",
            str(e),
            sys.path,
        )
        raise

    func_or_class = getattr(module, func_or_class_name)
    return func_or_class(config, ge_env)


class BadDriverError(RuntimeError):
    def __init__(self, bad_expr: str) -> None:
        super().__init__()
        self.bad_expr = bad_expr
        self.message = str(self)

    def __str__(self) -> str:
        return (
            "Expected gridengine.driver=module.func_name"
            + " or gridengine.driver=module.class_name. Got {}".format(self.bad_expr)
        )

    def __repr__(self) -> str:
        return str(self)


def main() -> int:
    ctx_handler = register_result_handler(DefaultContextHandler("[initialization]"))

    parser = ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="Path to autoscale config.", required=True
    )
    args = parser.parse_args()
    config_path = os.path.expanduser(args.config)

    if not os.path.exists(config_path):
        print("{} does not exist.".format(config_path), file=sys.stderr)
        return 1

    with open(config_path) as fr:
        config = json.load(fr)

    autoscale_grid_engine(config, ctx_handler=ctx_handler)

    return _exit_code


if __name__ == "__main__":
    sys.exit(main())
