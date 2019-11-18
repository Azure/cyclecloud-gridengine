from hpc.autoscale import hpclogging as logging
import os
import sys

from typing import Dict, Any, Optional

from hpc.autoscale.job import demandprinter
from hpc.autoscale.job.demandcalculator import new_demand_calculator
import hpc.autoscale.util

from gridengine.driver import GridEngineDriver
from argparse import ArgumentParser
import json
from hpc.autoscale.results import DefaultContextHandler, register_result_handler


_exit_code = 0


def autoscale_grid_engine(
    config: Dict[str, Any],
    ge_driver: Optional[GridEngineDriver] = None,
    ctx_handler: DefaultContextHandler = None,
) -> None:
    global _exit_code

    # interface to GE, generally by cli
    if ge_driver is None:
        # allow tests to pass in a mock
        ge_driver = GridEngineDriver()

    logging.info("Driver = %s", ge_driver)

    # it has two member variables - jobs
    # ge_driver.jobs - autoscale Jobs
    # ge_driver.compute_nodes - autoscale ComputeNodes

    demand_calculator = new_demand_calculator(
        config, existing_nodes=ge_driver.scheduler_nodes
    )

#    assert demand_calculator.node_mgr.cluster_consumed_core_count > 0

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

    if ctx_handler:
        ctx_handler.set_context("[scaling]")

    demand_result = demand_calculator.finish()

    # bootup all nodes. Optionally pass in a filtered list
    if demand_result.new_nodes:
        demand_calculator.bootup()

    # details here are that we pass in nodes that matter (matched) and the driver figures out
    # which ones are new and need to be added via qconf
    ge_driver.handle_join_cluster(demand_result.matched_nodes)

    # we also tell the driver about nodes that are unmatched. It filters them out
    # and returns a list of ones we can delete.

    unmatched_for_5_mins = demand_calculator.find_unmatched_for(at_least=300)

    nodes_to_delete = ge_driver.handle_draining(unmatched_for_5_mins)

    if nodes_to_delete:
        try:
            successfully_deleted = demand_calculator.delete(nodes_to_delete)

            # in case it has anything to do after a node is deleted (usually just remove it from the cluster)
            ge_driver.handle_post_delete(successfully_deleted)
        except Exception as e:
            _exit_code = 1
            logging.warning("Deletion failed, will retry on next iteration: %s", e)
            logging.exception(str(e))

    # and let's use the demand printer to print the demand_result.
    output_columns = config.get(
        "output_columns",
        [
            "name",
            "hostname",
            "job_ids",
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
        print("{} does not exist.", file=sys.stderr)
        return 1

    with open(config_path) as fr:
        config = json.load(fr)

    autoscale_grid_engine(config, ctx_handler=ctx_handler)

    return _exit_code


if __name__ == "__main__":
    sys.exit(main())
