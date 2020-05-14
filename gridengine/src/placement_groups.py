import logging
import math
from typing import Dict, List, Optional

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node.node import Node


class PlacementGroupSet:
    def __init__(self) -> None:
        # placement_group -> PlacementGroup
        self._placement_groups: Dict[ht.PlacementGroup, "PlacementGroup"] = {}

    def query_placement_groups(
        self, slot_type: Optional[str] = None, slots_requested: Optional[int] = None
    ) -> List["PlacementGroup"]:
        pgs: List[PlacementGroup] = []
        for pg in self._placement_groups.values():
            if slot_type is None or pg._slot_type == slot_type:
                if slots_requested is None or pg._slots_total >= slots_requested:
                    pgs.append(pg)
        return pgs

    def get_placement_group(
        self, pg_id: ht.PlacementGroup
    ) -> Optional["PlacementGroup"]:
        for ag in self.query_placement_groups():
            if ag.group_id == pg_id:
                return ag
        return None

    def create_placement_group(
        self, slot_type: str, placement_group: ht.PlacementGroup
    ) -> "PlacementGroup":
        if placement_group not in self._placement_groups:
            self._placement_groups[placement_group] = PlacementGroup(
                placement_group, slot_type
            )
        return self._placement_groups[placement_group]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PlacementGroupSet):
            return self._placement_groups == other._placement_groups
        return False

    def __str__(self) -> str:
        return "PlacementGroupSet(%s)" % self._placement_groups

    def __repr__(self) -> str:
        return str(self)


class PlacementGroup:
    def __init__(
        self,
        group_id: ht.PlacementGroup,
        slot_type: str,
        slots_per_host: int = 0,
        slots_total: int = 0,
        slots_requested: int = 0,
        nodes: Optional[List[Node]] = None,
    ):
        self.group_id = group_id
        self.slots_per_host = slots_per_host
        self._slot_type = slot_type
        self._slots_total = int(float(slots_total))
        self._slots_requested = int(float(slots_requested))
        self.nodes = [] if nodes is None else nodes

    def add_node(self, node: Node) -> None:
        self.nodes.append(node)
        self.add_slots(node.resources["num_procs"])

    def add_slots(self, slots: int) -> None:
        try:
            slots = int(float(slots))
        except Exception:
            logging.error("Could not parse %s as an integer" % slots)
            raise RuntimeError("Could not parse %s as an integer" % slots)
        if not self.slots_per_host:
            self.slots_per_host = slots

        if slots != self.slots_per_host:
            logging.warn("Mixture of # cpus found?")

        self._slots_total += self.slots_per_host

    def request_slots(self, slots: int) -> None:
        self._slots_requested += int(float(slots))

    def queue_depth(self) -> int:
        depth = float(self._slots_requested - self._slots_total) / self.slots_per_host
        if depth < 0:
            return -1
        return int(math.ceil(depth))

    def __str__(self) -> str:
        return (
            "PlacementGroup(group_id=%s, slot_type=%s, slots_total=%d, slots_requested=%d, slots_per_host=%d, queue_depth=%d)"
            % (
                self.group_id[-7:],
                self._slot_type,
                self._slots_total,
                self._slots_requested,
                self._slots_requested,
                self.queue_depth(),
            )
        )

    def __repr__(self) -> str:
        return (
            "PlacementGroup(group_id=%s, slot_type=%s, slots_total=%d, slots_requested=%d, queue_depth=%d, hosts=%s)"
            % (
                self.group_id,
                self._slot_type,
                self._slots_total,
                self._slots_requested,
                self.queue_depth(),
                self.nodes,
            )
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PlacementGroup):
            return False

        return (
            self.group_id == other.group_id
            and self._slot_type == other._slot_type
            and self._slots_total == other._slots_total
            and self._slots_requested == other._slots_requested
            and self.nodes == other.nodes
        )
