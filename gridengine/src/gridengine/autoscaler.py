import json
import os
import sys
from argparse import ArgumentParser
from typing import Any, Dict, List, Optional

from hpc.autoscale import hpclogging as logging
from hpc.autoscale.job import demandprinter
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.demandcalculator import DemandCalculator, new_demand_calculator
from hpc.autoscale.node.node import Node
from hpc.autoscale.results import DefaultContextHandler, register_result_handler

from gridengine import parallel_environments
from gridengine.driver import GridEngineDriver

_exit_code = 0


def autoscale_grid_engine(
    config: Dict[str, Any],
    ge_driver: Optional[GridEngineDriver] = None,
    ctx_handler: DefaultContextHandler = None,
    dry_run: bool = False,
) -> DemandResult:
    global _exit_code

    # interface to GE, generally by cli
    if ge_driver is None:
        # allow tests to pass in a mock
        ge_driver = GridEngineDriver(config)

    logging.debug("Driver = %s", ge_driver)

    invalid_nodes = []

    for node in ge_driver.scheduler_nodes:
        # many combinations of a u and other states. However,
        # as long as a and u are in there it is down
        state = node.metadata.get("state", "")
        if "a" in state and "u" in state:
            invalid_nodes.append(node)

    ge_driver.clean_hosts(invalid_nodes)

    demand_calculator = calculate_demand(config, ge_driver, ctx_handler)

    demand_result = demand_calculator.finish()

    if ctx_handler:
        ctx_handler.set_context("[joining]")

    # details here are that we pass in nodes that matter (matched) and the driver figures out
    # which ones are new and need to be added via qconf
    ge_driver.handle_join_cluster([x for x in demand_result.compute_nodes if x.exists])

    if ctx_handler:
        ctx_handler.set_context("[scaling]")

    # bootup all nodes. Optionally pass in a filtered list
    if demand_result.new_nodes:
        if not dry_run:
            demand_calculator.bootup()

    # we also tell the driver about nodes that are unmatched. It filters them out
    # and returns a list of ones we can delete.
    idle_timeout = int(config.get("gridengine", {}).get("idle_timeout", 300))
    logging.fine("Idle timeout is %s", idle_timeout)
    unmatched_for_5_mins = demand_calculator.find_unmatched_for(at_least=idle_timeout)

    nodes_to_delete = ge_driver.handle_draining(unmatched_for_5_mins)

    if nodes_to_delete:
        try:
            delete_result = demand_calculator.delete(nodes_to_delete)

            if delete_result:
                # in case it has anything to do after a node is deleted (usually just remove it from the cluster)
                ge_driver.handle_post_delete(delete_result.nodes)
        except Exception as e:
            _exit_code = 1
            logging.warning("Deletion failed, will retry on next iteration: %s", e)
            logging.exception(str(e))

    print_demand(config, demand_result)

    return demand_result


def calculate_demand(
    config: Dict,
    ge_driver: GridEngineDriver,
    ctx_handler: Optional[DefaultContextHandler] = None,
) -> DemandCalculator:
    # it has two member variables - jobs
    # ge_driver.jobs - autoscale Jobs
    # ge_driver.compute_nodes - autoscale ComputeNodes

    demand_calculator = new_demand_calculator(
        config, existing_nodes=ge_driver.scheduler_nodes
    )

    demand_calculator.node_mgr.add_default_resource(
        {},
        "_gridengine_qname",
        lambda node: node.software_configuration.get("gridengine_qname") or "",
    )

    def parse_gridengine_hostgroups(node: Node) -> List[str]:
        hostgroups_expr = node.software_configuration.get("gridengine_hostgroups")
        if hostgroups_expr:
            return hostgroups_expr.split(",")
        return []

    demand_calculator.node_mgr.add_default_resource(
        {}, "_gridengine_hostgroups", parse_gridengine_hostgroups
    )

    for name, default_complex in parallel_environments.read_complexes(config).items():
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
                '"default_resources": [{"node.nodearray": %s, "slots", "node.vcpu_count"]'
                % (bucket.nodearray)
            )
            logging.warning(
                """slots is not defined for bucket {}. Try adding {} to your config.""".format(
                    bucket, default
                )
            )

    for job in ge_driver.jobs:
        if ctx_handler:
            ctx_handler.set_context("[job {}]".format(job.name))
        demand_calculator.add_job(job)

    return demand_calculator


def print_demand(
    config: Dict,
    demand_result: DemandResult,
    output_columns: Optional[List[str]] = None,
) -> None:
    # and let's use the demand printer to print the demand_result.
    output_columns = output_columns or config.get(
        "output_columns",
        [
            "name",
            "hostname",
            "job_ids",
            "exists",
            "required",
            "slots",
            "*slots",
            "vm_size",
            "memory",
            "vcpu_count",
            "state",
            "placement_group",
        ],
    )
    demandprinter.print_demand(output_columns, demand_result)
    return demand_result


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
