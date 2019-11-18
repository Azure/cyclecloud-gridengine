import logging
import math


class PlacementGroupSet:

    def __init__(self):
        # placement_group -> PlacementGroup
        self._placement_groups = {}

    def _query_placement_groups(self, slot_type=None, slots_requested=None):
        for pg in self._placement_groups.values():
            if slot_type is None or pg._slot_type == slot_type:
                if slots_requested is None or pg._slots_total >= slots_requested:
                    yield pg

    def query_placement_groups(self, slot_type=None, slots_requested=None):
        return list(self._query_placement_groups(slot_type=slot_type, slots_requested=slots_requested))
    
    def _placement_group_ids(self):
        for sub_dict in self._placement_groups.values():
            for key in sub_dict:
                yield key

    def placement_group_ids(self):
        return list(self._placement_group_ids())

    def get_placement_group(self, ag_id):
        for ag in self.query_placement_groups():
            if ag.group_id == ag_id:
                return ag
        return None

    def create_placement_group(self, slot_type, placement_group):
        if placement_group not in self._placement_groups:
            self._placement_groups[placement_group] = PlacementGroup(placement_group, slot_type)
        return self._placement_groups[placement_group]

    def __eq__(self, other):
        return self._placement_groups == other._placement_groups

    def __str__(self):
        return "PlacementGroupSet(%s)" % self._placement_groups
    
    def __repr__(self):
        return str(self)


class PlacementGroup:

    def __init__(self, group_id, slot_type, slots_per_host=0, slots_total=0, slots_requested=0, hosts=None):
        self.group_id = group_id
        self.slots_per_host = slots_per_host
        self._slot_type = slot_type
        self._slots_total = int(float(slots_total))
        self._slots_requested = int(float(slots_requested))
        self.hosts = [] if hosts is None else hosts

    def add_host(self, host):
        self.hosts.append(host)

    def add_slots(self, slots):
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

    def request_slots(self, slots):
        self._slots_requested += int(float(slots))

    def queue_depth(self):
        depth = float(self._slots_requested - self._slots_total) / self.slots_per_host
        if depth < 0:
            return -1
        return int(math.ceil(depth))

    def __str__(self):
        return "PlacementGroup(group_id=%s, slot_type=%s, slots_total=%d, slots_requested=%d, slots_per_host=%d, queue_depth=%d)" % (
                    self.group_id[-7:], self._slot_type, self._slots_total, self._slots_requested, self._slots_requested, self.queue_depth())
    
    def __repr__(self):
        return "PlacementGroup(group_id=%s, slot_type=%s, slots_total=%d, slots_requested=%d, queue_depth=%d, hosts=%s)" % (
                    self.group_id,      self._slot_type, self._slots_total, self._slots_requested, self.queue_depth(), self.hosts)

    def __eq__(self, other):
        return self.group_id == other.group_id and \
                self._slot_type == other._slot_type and \
                self._slots_total == other._slots_total and \
                self._slots_requested == other._slots_requested and \
                self.hosts == other.hosts
