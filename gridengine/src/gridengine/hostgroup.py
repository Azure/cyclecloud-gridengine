import typing
from typing import Dict, List, Optional

from hpc.autoscale.node import constraints as constraintslib
from hpc.autoscale.node.constraints import And, Constraint, Never, get_constraints

from gridengine.qbin import QBin
from gridengine.usersandprojects import (
    ProjectConstraint,
    UserConstraint,
    XProjectConstraint,
    XUserConstraint,
)

if typing.TYPE_CHECKING:
    from gridengine.queue import GridEngineQueue


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
                "members": self.members,
                "constraints": self.constraints,
            }
        }

    def __repr__(self) -> str:
        return "Hostgroup({}, constraints={})".format(self.name, self.constraints)


class BoundHostgroup:
    """
    Represents a hostgroup in the context of a specific queue. i.e. what projects
    are included / excluded etc.
    """

    def __init__(
        self, ge_queue: "GridEngineQueue", hostgroup: Hostgroup, seq_no: int
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
        self, user: Optional[str] = None, project: Optional[str] = None
    ) -> Constraint:
        """
        Creates a constraint that encompasses the user/xuser/project/xproject
        as configured by this queue.
        """
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

        if not cons:
            return None

        if len(cons) == 1:
            return cons[0]

        return And(*cons)

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
