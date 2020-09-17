from typing import Dict, List

from hpc.autoscale.node.constraints import BaseNodeConstraint
from hpc.autoscale.node.node import Node
from hpc.autoscale.results import SatisfiedResult


class UserConstraint(BaseNodeConstraint):
    def __init__(self, user: str, user_list: List[str]) -> None:
        self.user = user
        self.user_list = user_list

    def satisfied_by_node(self, node: Node) -> SatisfiedResult:
        user_list = node.resources.get("owner")
        if user_list is None:
            user_list = self.user_list

        if self.user in user_list:
            return SatisfiedResult("success", self, node)
        msg = "{} does not include user {} in its user list {}".format(
            node, self.user, self.user_list
        )
        return SatisfiedResult("InvalidUser", self, node, reasons=[msg],)

    def do_decrement(self, node: Node) -> bool:
        user_list = node.resources.get("owner")
        if user_list is None:
            node.resources["owner"] = user_list = self.user_list

        assert self.user in user_list
        return True

    def to_dict(self) -> Dict:
        return {"gridengine-user-constraint": self.user}

    def __str__(self) -> str:
        return "UserConstraint({} not in [{}])".format(
            self.user, ",".join(self.user_list)
        )


class XUserConstraint(BaseNodeConstraint):
    def __init__(self, user: str, xuser_list: List[str]) -> None:
        self.user = user
        self.xuser_list = xuser_list

    def satisfied_by_node(self, node: Node) -> SatisfiedResult:
        xuser_list = node.resources.get("xowner")
        if xuser_list is None:
            xuser_list = self.xuser_list

        # it is not excluded
        if self.user not in xuser_list:
            return SatisfiedResult("success", self, node)

        msg = "{} has user {} in its xuser list {}".format(node, self.user, xuser_list)
        return SatisfiedResult("ExcludedUser", self, node, reasons=[msg],)

    def do_decrement(self, node: Node) -> bool:
        xuser_list = node.resources.get("xowner")
        if xuser_list is None:
            node.resources["xowner"] = xuser_list = self.xuser_list
        assert self.user not in xuser_list
        return True

    def to_dict(self) -> Dict:
        return {"gridengine-xuser-constraint": self.user}

    def __str__(self) -> str:
        return "XUserConstraint({} not in [{}])".format(
            self.user, ",".join(self.xuser_list)
        )


class ProjectConstraint(BaseNodeConstraint):
    def __init__(self, project: str, project_list: List[str]) -> None:
        self.project = project
        self.project_list = project_list

    def satisfied_by_node(self, node: Node) -> SatisfiedResult:
        project_list = node.resources.get("project")
        if project_list is None:
            project_list = self.project_list

        if self.project in project_list:
            return SatisfiedResult("success", self, node)

        msg = "{} does not include project {} in its project list {}".format(
            node, self.project, project_list
        )
        return SatisfiedResult("InvalidUser", self, node, reasons=[msg],)

    def do_decrement(self, node: Node) -> bool:
        projects = node.resources.get("project")
        if projects is None:
            node.resources["project"] = projects = self.project_list
        assert self.project in projects
        return True

    def to_dict(self) -> Dict:
        return {"gridengine-project-constraint": self.project}

    def __str__(self) -> str:
        return "ProjectConstraint({} in {})".format(
            self.project, ",".join(self.project_list)
        )


class XProjectConstraint(BaseNodeConstraint):
    def __init__(self, project: str, xproject_list: List[str]) -> None:
        self.project = project
        self.xproject_list = xproject_list

    def satisfied_by_node(self, node: Node) -> SatisfiedResult:
        xproject_list = node.resources.get("xproject")
        if xproject_list is None:
            xproject_list = self.xproject_list

        # it is not excluded
        if self.project not in xproject_list:
            return SatisfiedResult("success", self, node)

        msg = "{} has project {} in its xproject list {}".format(
            node, self.project, xproject_list
        )
        return SatisfiedResult("ExcludedProject", self, node, reasons=[msg],)

    def do_decrement(self, node: Node) -> bool:
        xprojects = node.resources.get("xproject")
        if xprojects is None:
            node.resources["xproject"] = xprojects = self.xproject_list
        assert self.project not in xprojects
        return True

    def to_dict(self) -> Dict:
        return {"gridengine-xproject-constraint": self.project}

    def __str__(self) -> str:
        return "XProjectConstraint({} not in [{}])".format(
            self.project, ",".join(self.xproject_list)
        )
