import re
import typing
from typing import Dict, List, Optional

from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.util import partition_single

from gridengine.hostgroup import BoundHostgroup, Hostgroup
from gridengine.qbin import QBin
from gridengine.util import parse_ge_config, parse_hostgroup_mapping

if typing.TYPE_CHECKING:
    from gridengine.environment import GridEngineEnvironment
    from gridengine.parallel_environments import ParallelEnvironment  # noqa: F401


class GridEngineQueue:
    def __init__(
        self,
        queue_config: Dict,
        pes: Dict[str, "ParallelEnvironment"],
        unbound_hostgroups: Dict[str, Hostgroup],
        autoscale_enabled: bool = True,
    ) -> None:
        self.queue_config = queue_config

        self.autoscale_enabled = autoscale_enabled
        assert isinstance(self.queue_config["hostlist"], str), self.queue_config[
            "hostlist"
        ]

        self.__hostlist = re.split(",| +", self.queue_config["hostlist"])
        self.__pe_to_hostgroups: Dict[str, List[str]] = {}
        self._pe_keys_cache: Dict[str, List[str]] = {}
        self.__parallel_environments: Dict[str, "ParallelEnvironment"] = {}
        self.__slots = parse_slots(self.queue_config.get("slots", ""))

        pe_list = parse_hostgroup_mapping(queue_config["pe_list"])

        for hostgroup, pes_for_hg in pe_list.items():
            for pe_name in pes_for_hg:
                if not pe_name:
                    continue

                if pe_name not in pes:
                    assert False
                    logging.warning(
                        "Parallel environment %s was not found - %s. Skipping",
                        pe_name,
                        list(pes.keys()),
                    )
                    continue
                self.__parallel_environments[pe_name] = pes[pe_name]

                # common case, and let's avoid nlogn insertion
                if pe_name not in self.__pe_to_hostgroups:
                    self.__pe_to_hostgroups[pe_name] = [hostgroup]
                else:
                    all_hostgroups = self.__pe_to_hostgroups[pe_name]
                    if hostgroup not in all_hostgroups:
                        all_hostgroups.append(hostgroup)

        if queue_config["pe_list"] and queue_config["pe_list"].lower() != "none":
            assert self.__parallel_environments, queue_config["pe_list"]

        all_host_groups = set(self.hostlist)
        for pe in self.__parallel_environments.values():
            if pe.requires_placement_groups:
                all_host_groups = all_host_groups - set(
                    self.get_hostgroups_for_pe(pe.name)
                )

        self.__ht_hostgroups = list(all_host_groups)

        self.user_lists = parse_hostgroup_mapping(
            queue_config.get("user_lists") or "", self.hostlist_groups, filter_none=True
        )
        self.xuser_lists = parse_hostgroup_mapping(
            queue_config.get("xuser_lists") or "",
            self.hostlist_groups,
            filter_none=True,
        )
        self.projects = parse_hostgroup_mapping(
            queue_config.get("projects") or "", self.hostlist_groups, filter_none=True
        )
        self.xprojects = parse_hostgroup_mapping(
            queue_config.get("xprojects") or "", self.hostlist_groups, filter_none=True
        )
        self.__bound_hostgroups: Dict[str, BoundHostgroup] = {}

        for hg_name in self.hostlist_groups:
            self.__bound_hostgroups[hg_name] = BoundHostgroup(
                self, unbound_hostgroups[hg_name]
            )

    @property
    def qname(self) -> str:
        return self.queue_config["qname"]

    @property
    def hostlist(self) -> List[str]:
        return self.__hostlist

    @property
    def hostlist_groups(self) -> List[str]:
        return [x for x in self.hostlist if x.startswith("@")]

    @property
    def slots(self) -> Dict[ht.Hostname, int]:
        return self.__slots

    @property
    def bound_hostgroups(self) -> Dict[str, BoundHostgroup]:
        return self.__bound_hostgroups

    def has_pe(self, pe_name: str) -> bool:
        return bool(self._pe_keys(pe_name))

    def _pe_keys(self, pe_name: str) -> List[str]:
        if "*" not in pe_name:
            return [pe_name]

        if pe_name not in self._pe_keys_cache:
            pe_name_pat = pe_name.replace("*", ".*")
            pe_name_re = re.compile(pe_name_pat)

            ret = []

            for key in self.__parallel_environments:

                if pe_name_re.match(key):
                    ret.append(key)

            self._pe_keys_cache[pe_name] = ret

        return self._pe_keys_cache[pe_name]

    def get_pes(self, pe_name: Optional[str] = None) -> List["ParallelEnvironment"]:
        if pe_name is None:
            return list(self.__parallel_environments.values())

        if not self.has_pe(pe_name):
            raise RuntimeError(
                "Queue {} does not support parallel_environment {}".format(
                    self.qname, pe_name
                )
            )
        return [
            self.__parallel_environments[x]
            for x in self._pe_keys(pe_name)
            if x in self.__parallel_environments
        ]

    def get_hostgroups_for_ht(self) -> List[str]:
        # TODO expensive
        return self.__ht_hostgroups

    def get_hostgroups_for_pe(self, pe_name: str) -> List[str]:
        if not self.has_pe(pe_name):
            raise RuntimeError(
                "Queue {} does not support parallel_environment {}".format(
                    self.qname, pe_name
                )
            )

        return self.__pe_to_hostgroups[pe_name]

    def get_placement_group(self, pe_name: str) -> Optional[str]:
        if pe_name not in self.__parallel_environments:
            raise RuntimeError(
                "Could not find pe {} - {}".format(
                    pe_name, list(self.__parallel_environments.keys())
                )
            )
        pe = self.__parallel_environments[pe_name]

        if not pe.requires_placement_groups:
            return None

        first = self.get_hostgroups_for_pe(pe.name)[0]
        if not first:
            return first

        placement_group = ht.PlacementGroup(first.replace("@", ""))
        placement_group = re.sub("[^a-zA-z0-9-_]", "_", placement_group)
        return placement_group

    def __str__(self) -> str:
        return "GridEnineQueue(qname={}, pes={}, hostlist={})".format(
            self.qname, list(self.__parallel_environments.keys()), self.hostlist
        )

    def __repr__(self) -> str:
        return str(self)


def parse_slots(slots_expr: str) -> Dict[str, int]:
    slots: Dict[str, int] = {}

    mapping = parse_hostgroup_mapping(slots_expr, filter_none=True)
    for host, slots_as_list in mapping.items():
        if len(slots_as_list) != 1:
            raise AssertionError(
                "Expected single int. Got %s. Whole expression\n%s"
                % (slots_as_list, slots_expr),
            )
        slots[host] = int(slots_as_list[0])

    if None in slots:
        slots.pop(None)  # type: ignore

    return slots


def read_queues(
    autoscale_config: Dict,
    pes: Dict[str, "ParallelEnvironment"],
    hostgroups: List[Hostgroup],
    qbin: QBin,
) -> Dict[str, GridEngineQueue]:
    queues = {}
    qnames = qbin.qconf(["-sql"]).split()

    logging.debug("Found %d queues: %s", len(qnames), " ".join(qnames))
    autoscale_queues_config = autoscale_config.get("gridengine", {}).get("queues", {})

    unbound_hostgroups = partition_single(hostgroups, lambda h: h.name)

    for qname in qnames:
        lines = qbin.qconf(["-sq", qname]).splitlines()
        queue_config = parse_ge_config(lines)
        autoscale_enabled = autoscale_queues_config.get(queue_config["qname"], {}).get(
            "autoscale_enabled", True
        )
        queues[qname] = GridEngineQueue(
            queue_config, pes, unbound_hostgroups, autoscale_enabled
        )

    return queues


def new_gequeue(
    qname: str,
    hostlist: str,
    pe_list: str,
    slots_expr: str,
    ge_env: "GridEngineEnvironment",
) -> GridEngineQueue:
    queue_config = {
        "qname": qname,
        "hostlist": hostlist,
        "pe_list": pe_list,
        "slots": slots_expr,
    }

    return GridEngineQueue(queue_config, ge_env.pes, ge_env.hostgroups)
