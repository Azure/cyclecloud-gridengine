import re
from typing import Dict, List

from hpc.autoscale.node.node import Node


def flatten_lines(lines: List[str]) -> List[str]:
    ret = []
    n = 0
    while n < len(lines):
        line = lines[n]
        while line.endswith("\\") and n < len(lines) - 1:
            n += 1
            next_line = lines[n]
            line = line[:-1] + " " + next_line.strip()
        ret.append(line)
        n += 1
    return ret


def parse_ge_config(lines: List[str]) -> Dict[str, str]:
    config = {}

    for line in flatten_lines(lines):
        toks = re.split(r"[ \t]+", line, 1)
        if len(toks) != 2:
            continue
        config[toks[0].strip()] = toks[1].strip()

    return config


def get_node_qname(node: Node) -> str:
    qname = node.metadata.get("gridengine_qname")

    if not qname:
        qname = node.software_configuration.get("gridengine_qname")

    return qname or ""


def get_node_hostgroups(node: Node) -> List[str]:
    hostgroups_expr = node.metadata.get("gridengine_hostgroups")

    if not hostgroups_expr:
        hostgroups_expr = node.software_configuration.get("gridengine_hostgroups")

    if not hostgroups_expr:
        return []

    return re.split(",| +", hostgroups_expr)
