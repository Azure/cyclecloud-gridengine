import re
import typing
from typing import Any, Dict, List, Optional, Union

from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node.node import Node
from hpc.autoscale.util import partition_single

from gridengine.complex import parse_queue_complex_values
from gridengine.hostgroup import BoundHostgroup, Hostgroup
from gridengine.qbin import QBin
from gridengine.util import parse_ge_config, parse_hostgroup_mapping

if typing.TYPE_CHECKING:
    from gridengine.complex import Complex  # noqa: F401
    from gridengine.environment import GridEngineEnvironment
    from gridengine.parallel_environments import ParallelEnvironment  # noqa: F401


class GridEngineQueue:
    def __init__(
        self,
        queue_config: Dict,
        pes: Dict[str, "ParallelEnvironment"],
        unbound_hostgroups: Dict[str, Hostgroup],
        complex_values: Dict[str, Dict],
        autoscale_enabled: bool = True,
    ) -> None:
        self.queue_config = queue_config
        self.complex_values = complex_values

        self.autoscale_enabled = autoscale_enabled
        assert isinstance(self.queue_config["hostlist"], str), self.queue_config[
            "hostlist"
        ]

        self.__hostlist = re.split(",| +", self.queue_config["hostlist"])
        self.__pe_to_hostgroups: Dict[str, List[str]] = {}
        self._pe_keys_cache: Dict[str, List[str]] = {}
        self.__parallel_environments: Dict[str, "ParallelEnvironment"] = {}
        self.__slots = parse_slots(self.queue_config.get("slots", ""))

        for hg, slots in self.__slots.items():
            if hg is None:
                continue
            if hg not in self.complex_values:
                self.complex_values[hg] = {}
            self.complex_values[hg]["slots"] = slots

        self.__seq_no = parse_seq_no(self.queue_config.get("seq_no", "0"))

        pe_list = parse_hostgroup_mapping(queue_config["pe_list"])

        for hostgroup, pes_for_hg in pe_list.items():
            for pe_name in pes_for_hg:
                if not pe_name:
                    continue

                if pe_name not in pes:
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

        self.__ht_hostgroups = [x for x in list(all_host_groups) if x.startswith("@")]

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
            hg_seq_no = self.seq_no.get(hg_name, self.seq_no.get(None, 0))  # type: ignore
            self.__bound_hostgroups[hg_name] = BoundHostgroup(
                self, unbound_hostgroups[hg_name], hg_seq_no
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
    def seq_no(self) -> Dict[str, int]:
        return self.__seq_no

    @property
    def bound_hostgroups(self) -> Dict[str, BoundHostgroup]:
        return self.__bound_hostgroups

    def get_quota(
        self, node: Node, complex_name: Union[str, "Complex"], hostgroup: str
    ) -> Optional[Any]:
        """
        With quotas, the default quota is not inherited if a hostgroup is defined. i.e.
        > complex_values testbool=true
        > qstat -f -F testbool
        ---------------------------------------------------------------------------------
        testq@ip-0A010009.m1qiv4tr1vqe BIP   0/0/2          0.01     lx-amd64
        qf:testbool=1

        > complex_values testbool=true,[@Standard_A2=pcpu=2]
        > qstat -f -F testbool
        ---------------------------------------------------------------------------------
        testq@ip-0A010009.m1qiv4tr1vqe BIP   0/0/2          0.00     lx-amd64

        So simply defining [@Standard_2=] with anything removes the default
        """
        if not isinstance(complex_name, str):
            complex_name = complex_name.name

        cv_for_hg = self.complex_values.get(hostgroup)
        if cv_for_hg:
            # if the hostgroup is defined, I don't care what the non-hg versions
            return cv_for_hg.get(complex_name)

        default_cv = self.complex_values.get(None)  # type: ignore
        if default_cv and complex_name in default_cv:
            return default_cv[complex_name]

        if complex_name in node.available:
            return node.available[complex_name]

        return None

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
    slots = _parse_int_map(slots_expr)

    # if None in slots:
    #     slots.pop(None)  # type: ignore

    return slots


def parse_seq_no(seq_no_expr: str) -> Dict[str, int]:
    return _parse_int_map(seq_no_expr)


def _parse_int_map(expr: str) -> Dict[str, int]:
    ret: Dict[str, int] = {}

    mapping = parse_hostgroup_mapping(expr)
    for host, as_list in mapping.items():
        if len(as_list) != 1:
            raise AssertionError(
                "Expected single int. Got %s. Whole expression\n%s" % (as_list, expr),
            )
        ret[host] = int(as_list[0])

    return ret


def read_queues(
    autoscale_config: Dict,
    pes: Dict[str, "ParallelEnvironment"],
    hostgroups: List[Hostgroup],
    complexes: Dict[str, "Complex"],
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
        expr = queue_config.get("complex_values", "NONE")
        complex_values = parse_queue_complex_values(expr, complexes, qname)
        queues[qname] = GridEngineQueue(
            queue_config, pes, unbound_hostgroups, complex_values, autoscale_enabled
        )

    return queues


def new_gequeue(
    qname: str,
    hostlist: str,
    pe_list: str,
    slots_expr: str,
    ge_env: "GridEngineEnvironment",
    complexes: Optional[Dict[str, Dict]] = None,
) -> GridEngineQueue:
    complexes = complexes or {}
    queue_config = {
        "qname": qname,
        "hostlist": hostlist,
        "pe_list": pe_list,
        "slots": slots_expr,
    }

    return GridEngineQueue(queue_config, ge_env.pes, ge_env.hostgroups, complexes)
