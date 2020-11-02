import re
import typing
from typing import Any, Callable, Dict, List, Optional, Union

from hpc.autoscale.node import constraints as constraintslib
from hpc.autoscale.node.constraints import (
    And,
    BaseNodeConstraint,
    Constraint,
    Never,
    NodeConstraint,
    get_constraint,
    get_constraints,
)
from hpc.autoscale.node.node import Node
from hpc.autoscale.results import Result

from gridengine.complex import Complex
from gridengine.qbin import QBin
from gridengine.usersandprojects import (
    ProjectConstraint,
    UserConstraint,
    XProjectConstraint,
    XUserConstraint,
)

if typing.TYPE_CHECKING:
    from gridengine.queue import GridEngineQueue
    from gridengine.environment import GridEngineEnvironment


class Hostgroup:
    """
    Represents a hostgroup without the context of a queue, i.e. name,
    members and optionally, constraints imposed on this hostgroup for
    autoscale purposes.
    """

    def __init__(
        self,
        name: str,
        constraints: Optional[List[Constraint]] = None,
        members: Optional[List[str]] = None,
    ) -> None:
        self.name = name
        self.constraints = get_constraints(constraints or [])
        self.members: List[str] = members or []

    def add_member(self, member: str) -> None:
        if member not in self.members:
            self.members.append(member)

    def to_dict(self) -> Dict:
        return {
            "hostgroup": {
                "name": self.name,
                "constraints": [x.to_dict() for x in self.constraints],
            }
        }

    @classmethod
    def from_dict(self, d: Dict) -> "Hostgroup":
        return Hostgroup(d["hostgroup"]["name"], d["hostgroup"]["constraints"])

    def __repr__(self) -> str:
        return "Hostgroup({}, constraints={})".format(self.name, self.constraints)


class BoundHostgroup:
    """
    Represents a hostgroup in the context of a specific queue. i.e. what projects
    are included / excluded etc.
    """

    def __init__(
        self, ge_queue: "GridEngineQueue", hostgroup: Hostgroup, seq_no: int,
    ) -> None:
        self.__queue = ge_queue
        self.__hostgroup = hostgroup
        self.__seq_no = seq_no
        self.__user_list = (self.__queue.user_lists.get(None) or []) + (  # type: ignore
            self.__queue.user_lists.get(self.name) or []
        )
        self.__xuser_list = (self.__queue.xuser_lists.get(None) or []) + (  # type: ignore
            self.__queue.xuser_lists.get(self.name) or []
        )
        self.__projects = (self.__queue.projects.get(None) or []) + (  # type: ignore
            self.__queue.projects.get(self.name) or []
        )
        self.__xprojects = (self.__queue.xprojects.get(None) or []) + (  # type: ignore
            self.__queue.xprojects.get(self.name) or []
        )
        self.__user_list = [x for x in self.__user_list if x is not None]
        self.__xuser_list = [x for x in self.__xuser_list if x is not None]
        self.__projects = [x for x in self.__projects if x is not None]
        self.__xprojects = [x for x in self.__xprojects if x is not None]

    @property
    def name(self) -> str:
        return self.__hostgroup.name

    @property
    def constraints(self) -> List[Constraint]:
        return self.__hostgroup.constraints

    @property
    def queue(self) -> "GridEngineQueue":
        return self.__queue

    @property
    def seq_no(self) -> int:
        return self.__seq_no

    def add_member(self, hostname: str) -> None:
        self.__hostgroup.add_member(hostname)

    @property
    def members(self) -> List[str]:
        return self.__hostgroup.members

    @property
    def user_list(self) -> List[str]:
        return self.__user_list

    @property
    def xuser_list(self) -> List[str]:
        return self.__xuser_list

    @property
    def projects(self) -> List[str]:
        return self.__projects

    @property
    def xprojects(self) -> List[str]:
        return self.__xprojects

    def make_constraint(
        self,
        ge_env: "GridEngineEnvironment",
        user: Optional[str] = None,
        project: Optional[str] = None,
        requested_resources: Optional[Dict[str, Any]] = None,
    ) -> Constraint:
        """
        Creates a constraint that encompasses the user/xuser/project/xproject
        as configured by this queue.
        """
        requested_resources = requested_resources or {}
        cons: List[Constraint] = []
        cons.extend(self.__hostgroup.constraints)

        if self.user_list:
            if user not in self.user_list:
                msg = "User {} is invalid for {} on queue {}".format(
                    user, self.name, self.queue.qname
                )
                return Never(msg)

            cons.append(UserConstraint(user, self.user_list))  # type: ignore

        if self.xuser_list:
            if user in self.xuser_list:
                msg = "User {} is excluded for {} on queue {}".format(
                    user, self.name, self.queue.qname
                )
                return Never(msg)

            cons.append(XUserConstraint(user, self.xuser_list))  # type: ignore

        if self.projects:
            if project not in self.projects:
                msg = "Project {} is invalid for {} on queue {}".format(
                    project, self.name, self.queue.qname
                )
                return Never(msg)

            cons.append(ProjectConstraint(project, self.projects))  # type: ignore

        if self.xprojects:
            if project in self.xprojects:
                msg = "Project {} is excluded for {} on queue {}".format(
                    project, self.name, self.queue.qname
                )
                return Never(msg)

            cons.append(XProjectConstraint(project, self.xprojects))  # type: ignore

        for resource_name, resource_value in requested_resources.items():
            # queue + hostgroup decides what quota you have
            quota_cons = make_quota_bound_consumable_constraint(
                resource_name, resource_value, self.queue, ge_env, [self.name],
            )
            cons.append(quota_cons)

        if not cons:
            return None

        if len(cons) == 1:
            return cons[0]

        return And(*cons)

    def to_dict(self) -> Dict:
        return {
            "hostgroup": self.__hostgroup.to_dict(),
        }

    def __repr__(self) -> str:
        return "{}@{}".format(self.queue.qname, self.__hostgroup)


def read_hostgroups(autoscale_config: Dict, qbin: QBin) -> List[Hostgroup]:
    # map each host (lowercase) to its set of hostgroups
    ret: List[Hostgroup] = []

    hg_config = autoscale_config.get("gridengine", {}).get("hostgroups", {})

    for hg_name in qbin.qconf(["-shgrpl"]).split():
        members = qbin.qconf(["-shgrp_resolved", hg_name]).split()
        members = [h.split(".")[0].lower() for h in members]
        constraints = hg_config.get(hg_name, {}).get("constraints", []) or []

        if not isinstance(constraints, list):
            constraints = [constraints]

        parsed_constraints = constraintslib.get_constraints(constraints)
        hostgroup = Hostgroup(hg_name, parsed_constraints, members)
        ret.append(hostgroup)

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
    if c.is_excl:
        return

    namespaced_key = "{}@{}".format(ge_queue.qname, resource_name)
    if namespaced_key in node.available:
        return

    host_value = node.available.get(resource_name)
    # if no quota is defined, then use the host_value (which may also be undefined)
    quota = ge_queue.get_quota(c, hostgroup, node)

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


def make_node_preprocessor(
    ge_env: "GridEngineEnvironment",
    target_queue: "GridEngineQueue",
    hostgroups: List[str],
) -> Callable[[Node], bool]:
    def node_preprocessor(node: Node) -> bool:
        for c in ge_env.complexes.values():

            names = [c.name] if c.name == c.shortcut else [c.name, c.shortcut]
            for hg in hostgroups:
                for resource_name in names:
                    _process_quota(c, resource_name, node, target_queue, hg)
        return True

    return node_preprocessor


def make_quota_bound_consumable_constraint(
    resource_name: str,
    value: Union[int, float],
    target_queue: "GridEngineQueue",
    ge_env: "GridEngineEnvironment",
    hostgroups: List[str],
) -> "QuotaConstraint":
    process_quota_wrapper = make_node_preprocessor(ge_env, target_queue, hostgroups)
    qnames = list(ge_env.queues)
    return QuotaConstraint(
        qnames, resource_name, value, target_queue.qname, process_quota_wrapper,
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
            get_constraint({resource_name: value}),
            get_constraint({self.target_resource: value}),
        )

    def satisfied_by_node(self, node: Node) -> Result:
        copy = node.clone()
        self.bucket_preprocessor(copy)
        return self.child_constraint.satisfied_by_node(copy)

    def do_decrement(self, node: Node) -> bool:
        """
        pcpu = 10
        q1@pcpu=4
        q2@pcpu=8

        if target is pcpu: pcpu=9, q1@pcpu=4, q2@pcpu=8
        if target is q1@ : pcpu=9, q1@pcpu=3, q2@pcpu=8
        if target is q2@ : pcpu=9, q1@pcpu=4, q2@pcpu=7
        """
        self.bucket_preprocessor(node)
        if isinstance(self.value, (str, bool)):
            return True

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
        # when calculating the min space of an example node
        copy = node.clone()
        self.bucket_preprocessor(copy)
        return self.child_constraint.minimum_space(copy)

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
