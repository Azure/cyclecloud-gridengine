import re
import typing
from typing import Any, Callable, Dict, List, Optional, Union

from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node.bucket import NodeBucket
from hpc.autoscale.node.constraints import (
    And,
    BaseNodeConstraint,
    MinResourcePerNode,
    NodeConstraint,
)
from hpc.autoscale.node.node import Node
from hpc.autoscale.results import Result
from hpc.autoscale.util import partition_single

from gridengine.qbin import QBin
from gridengine.util import get_node_hostgroups, parse_hostgroup_mapping

if typing.TYPE_CHECKING:
    from gridengine.queue import GridEngineQueue
    from gridengine.environment import GridEngineEnvironment


class Complex:
    def __init__(
        self,
        name: str,
        shortcut: str,
        complex_type: str,
        relop: str,
        requestable: bool,
        consumable: bool,
        default: Optional[str],
        urgency: int,
    ) -> None:
        self.name = name
        self.shortcut = shortcut
        self.complex_type = complex_type.upper()
        self.relop = relop
        self.requestable = requestable
        self.consumable = consumable
        self.urgency = urgency
        self.__logged_type_warning = False
        self.__logged_parse_warning = False
        self.__is_numeric = self.complex_type in ["INT", "RSMAP", "DOUBLE", "MEMORY"]

        self.default = None
        self.default = self.parse(default or "NONE")

    def parse(self, value: str) -> Optional[Any]:
        try:
            if value.upper() == "NONE":
                return None
            if value.lower() == "infinity":
                return float("inf")

            if self.complex_type in ["INT", "RSMAP"]:
                return int(value)

            elif self.complex_type == "BOOL":
                try:
                    return bool(float(value))
                except ValueError:
                    if value.lower() in ["true", "false"]:
                        return value.lower() == "true"
                    else:
                        logging.warning(
                            "Could not parse '%s' for complex type %s - treating as string.",
                            value,
                            self.complex_type,
                        )
                    return value
            elif self.complex_type == "DOUBLE":
                return float(value)

            elif self.complex_type in ["RESTRING", "TIME", "STRING", "HOST"]:
                return value

            elif self.complex_type == "CSTRING":
                # TODO test
                return value.lower()  # case insensitve - we will just always lc

            elif self.complex_type == "MEMORY":
                size = value[-1]
                if size.isdigit():
                    mem = ht.Memory(float(value), "b")
                else:
                    mem = ht.Memory(float(value[:-1]), size)
                return mem.convert_to("g")
            else:
                if not self.__logged_type_warning:
                    logging.warning(
                        "Unknown complex type %s - treating as string.",
                        self.complex_type,
                    )
                    self.__logged_type_warning = True
                return value
        except Exception:
            if not self.__logged_parse_warning:
                logging.warning(
                    "Could not parse complex %s with value '%s'. Treating as string",
                    self,
                    value,
                )
                self.__logged_parse_warning = True
            return value

    @property
    def is_numeric(self) -> bool:
        return self.__is_numeric

    def __str__(self) -> str:
        return "Complex(name={}, shortcut={}, type={}, default={})".format(
            self.name, self.shortcut, self.complex_type, self.default
        )

    def __repr__(self) -> str:
        return "Complex(name={}, shortcut={}, type={}, relop='{}', requestable={}, consumable={}, default={}, urgency={})".format(
            self.name,
            self.shortcut,
            self.complex_type,
            self.relop,
            self.requestable,
            self.consumable,
            self.default,
            self.urgency,
        )


def read_complexes(autoscale_config: Dict, qbin: QBin) -> Dict[str, "Complex"]:
    complex_lines = qbin.qconf(["-sc"]).splitlines()
    return _parse_complexes(autoscale_config, complex_lines)


def _parse_complexes(
    autoscale_config: Dict, complex_lines: List[str]
) -> Dict[str, "Complex"]:
    relevant_complexes = None
    if autoscale_config:
        relevant_complexes = autoscale_config.get("gridengine", {}).get(
            "relevant_complexes"
        )
        if relevant_complexes:
            logging.info(
                "Restricting complexes for autoscaling to %s", relevant_complexes
            )

    complexes: List[Complex] = []
    headers = complex_lines[0].lower().replace("#", "").split()

    required = set(["name", "type", "consumable"])
    missing = required - set(headers)
    if missing:
        logging.error(
            "Could not parse complex file as it is missing expected columns: %s."
            + " Autoscale likely will not work.",
            list(missing),
        )
        return {}

    for n, line in enumerate(complex_lines[1:]):
        if line.startswith("#"):
            continue
        toks = line.split()
        if len(toks) != len(headers):
            logging.warning(
                "Could not parse complex at line {} - ignoring: '{}'".format(n, line)
            )
            continue
        c = dict(zip(headers, toks))
        try:

            if (
                relevant_complexes
                and c["name"] not in relevant_complexes
                and c["shortcut"] not in relevant_complexes
            ):
                logging.trace(
                    "Ignoring complex %s because it was not defined in gridengine.relevant_complexes",
                    c["name"],
                )
                continue

            complex = Complex(
                name=c["name"],
                shortcut=c.get("shortcut", c["name"]),
                complex_type=c["type"],
                relop=c.get("relop", "=="),
                requestable=c.get("requestable", "YES").lower() == "yes",
                consumable=c.get("consumable", "YES").lower() == "yes",
                default=c.get("default"),
                urgency=int(c.get("urgency", 0)),
            )

            complexes.append(complex)

        except Exception:
            logging.exception("Could not parse complex %s - %s", line, c)

    # TODO test RDH
    ret = partition_single(complexes, lambda x: x.name)
    shortcut_dict = partition_single(complexes, lambda x: x.shortcut)
    ret.update(shortcut_dict)
    return ret


def parse_queue_complex_values(
    expr: str, complexes: Dict[str, Complex], qname: str
) -> Dict[str, Dict[str, Dict]]:
    raw: Dict[str, List[str]] = parse_hostgroup_mapping(expr)
    ret: Dict[str, Dict] = {}

    for hostgroup, sub_exprs in raw.items():
        if hostgroup not in ret:
            ret[hostgroup] = {}
        d = ret[hostgroup]

        for sub_expr in sub_exprs:
            if sub_expr is None:
                sub_expr = "NONE"

            if "=" not in sub_expr:
                continue

            sub_expr = sub_expr.strip()
            complex_name, value_expr = sub_expr.split("=", 1)

            c = complexes.get(complex_name)
            if not c:
                logging.debug(
                    "Could not find complex %s defined in queue %s", complex_name, qname
                )
                continue
            d[complex_name] = c.parse(value_expr)
    return ret


def process_quotas(
    node: Node,
    complexes: Dict[str, Complex],
    hostgroups: List[str],
    ge_queues: List["GridEngineQueue"],
) -> None:
    for ge_queue in ge_queues:

        for c in complexes.values():

            names = set([c.name, c.shortcut])
            for hg in hostgroups:
                for resource_name in names:
                    _process_quota(c, resource_name, node, ge_queue, hg)


def _process_quota(
    c: Complex,
    resource_name: str,
    node: Node,
    ge_queue: "GridEngineQueue",
    hostgroup: str,
) -> None:
    namespaced_key = "{}@{}".format(ge_queue.qname, resource_name)
    if namespaced_key in node.available:
        return

    host_value = node.available.get(resource_name)
    # if no quota is defined, then use the host_value (which may also be undefined)
    quota = ge_queue.get_quota(c, hostgroup)

    if quota is None:
        quota = host_value

    if quota is None:
        return

    # host value isn't defined, then set it to the quota
    if host_value is None:
        host_value = quota

    if c.is_numeric:
        quota = min(quota, host_value)  # type: ignore
    elif host_value:
        quota = host_value

    quotas_dict = node.metadata.get("quotas")
    if quotas_dict is None:
        quotas_dict = {}
        node.metadata["quotas"] = quotas_dict

    qname_dict = quotas_dict.get(ge_queue.qname)
    if qname_dict is None:
        qname_dict = {}
        quotas_dict[ge_queue.qname] = qname_dict

    hg_dict = qname_dict.get(hostgroup)
    if hg_dict is None:
        hg_dict = {}
        qname_dict[hostgroup] = hg_dict

    hg_dict[resource_name] = quota

    node.available[namespaced_key] = quota
    node._resources[namespaced_key] = quota


def make_node_preprocessor(ge_env: "GridEngineEnvironment") -> Callable[[Node], bool]:
    def node_preprocessor(node: Node) -> bool:
        hostgroups = get_node_hostgroups(node) or ge_env.hostgroups
        for ge_queue in ge_env.queues.values():

            for c in ge_env.complexes.values():

                names = [c.name] if c.name == c.shortcut else [c.name, c.shortcut]
                for hg in hostgroups:
                    for resource_name in names:
                        _process_quota(c, resource_name, node, ge_queue, hg)
        return True

    return node_preprocessor


def make_quota_bound_consumable_constraint(
    resource_name: str,
    value: Union[int, float],
    target_qname: str,
    ge_env: "GridEngineEnvironment",
) -> NodeConstraint:
    process_quota_wrapper = make_node_preprocessor(ge_env)
    qnames = list(ge_env.queues)
    return QuotaConstraint(
        qnames, resource_name, value, target_qname, process_quota_wrapper,
    )


class QuotaConstraint(BaseNodeConstraint):
    """
    Decrements both the total amount and the queue bound amount (complex_name and qname@complex_name).
    It also floors other bound amounts to the total amount available.
    """

    def __init__(
        self,
        qnames: List[str],
        resource_name: str,
        value: Union[float, int],
        target_qname: str,
        bucket_preprocessor: Callable[[Node], bool],
    ) -> None:
        self.resource_name = resource_name
        self.value = value
        self.target_qname = target_qname
        self.pattern = re.compile("([^@]+@)?{}".format(resource_name))
        self.target_resource = "{}@{}".format(target_qname, resource_name)
        self.bucket_preprocessor = bucket_preprocessor

        self.child_constraint = And(
            MinResourcePerNode(resource_name, value),
            MinResourcePerNode(self.target_resource, value),
        )

    def satisfied_by_bucket(self, bucket: NodeBucket) -> Result:
        copy = bucket.example_node.clone()
        self.bucket_preprocessor(copy)
        return self.satisfied_by_node(copy)

    def satisfied_by_node(self, node: Node) -> Result:
        return self.child_constraint.satisfied_by_node(node)

    def do_decrement(self, node: Node) -> bool:
        """
        pcpu = 10
        q1@pcpu=4
        q2@pcpu=8

        if target is pcpu: pcpu=9, q1@pcpu=4, q2@pcpu=8
        if target is q1@ : pcpu=9, q1@pcpu=3, q2@pcpu=8
        if target is q2@ : pcpu=9, q1@pcpu=4, q2@pcpu=7
        """
        matches = []
        for resource_name, value in node.available.items():
            if self.pattern.match(resource_name):
                matches.append((resource_name, value))

        new_floor = node.available[self.resource_name] - self.value
        for match, match_value in matches:
            if "@" in match and match != self.target_resource:
                node.available[match] = min(new_floor, match_value)

        return self.child_constraint.do_decrement(node)

    def get_children(self) -> List[NodeConstraint]:
        return [self.child_constraint]

    def minimum_space(self, node: Node) -> int:
        return self.child_constraint.minimum_space(node)

    def to_dict(self) -> Dict:
        return {
            "quota-constraint": {
                "target-resource": self.target_resource,
                "value": self.value,
                "child-constraint": self.child_constraint.to_dict(),
            }
        }

    def __str__(self) -> str:
        return "QuotaConstraint({}, {})".format(self.target_resource, self.value)
