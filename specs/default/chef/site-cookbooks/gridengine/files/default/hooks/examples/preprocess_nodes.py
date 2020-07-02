import json
import sys
from typing import List

from hpc.autoscale.job.schedulernode import SchedulerNode


def preprocess_nodes(nodes: List[SchedulerNode]) -> List[SchedulerNode]:
    modified_nodes = []

    for node in nodes:

        # you can modify available resources or add metadata
        node.available["slots"] -= 1
        node.metadata["tracking_id"] = "tracking-123"

        modified_nodes.append(node)

    return modified_nodes


def preprocess_nodes_stdin() -> None:
    # load the json from stdin
    node_dicts = json.load(sys.stdin)

    # parse the job dictionaries into hpc Job objects
    nodes = [SchedulerNode.from_dict(n) for n in node_dicts]

    # run our preprocessing
    modified_nodes = preprocess_nodes(nodes)

    # finally dump the modified jobs out to stdout
    json.dump(modified_nodes, sys.stdout, default=lambda x: x.to_dict())


if __name__ == "__main__":
    preprocess_nodes_stdin()
