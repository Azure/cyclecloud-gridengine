import re
import typing
from io import StringIO
from typing import Dict, List, Optional

from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node.constraints import Constraint

from gridengine.util import parse_ge_config

if typing.TYPE_CHECKING:
    from gridengine.environment import GridEngineEnvironment
    from gridengine.parallel_environments import ParallelEnvironment  # noqa: F401
    from gridengine.complex import Complex  # noqa: F401


class GridEngineQueue:
    def __init__(
        self,
        queue_config: Dict,
        ge_env: "GridEngineEnvironment",
        constraints: List[Constraint],
        autoscale_enabled: bool = True,
    ) -> None:
        self.queue_config = queue_config
        self.ge_env = ge_env
        if isinstance(constraints, dict):
            constraints = [constraints]

        if not isinstance(constraints, list):
            logging.error(
                "Ignoring invalid constraints for queue %s! %s", self.qname, constraints
            )
            constraints = [{"node.nodearray": self.qname}]

        self.constraints = constraints
        self.autoscale_enabled = autoscale_enabled
        self.__hostlist = re.split(",| +", self.queue_config["hostlist"])
        self.__pe_to_hostgroups: Dict[str, List[str]] = {}
        self._pe_keys_cache: Dict[str, List[str]] = {}
        self.__parallel_environments: Dict[str, "ParallelEnvironment"] = {}
        self.__slots = {}

        slot_toks = re.split(",| +", self.queue_config.get("slots", ""))
        for tok in slot_toks:
            if "[" not in tok:
                continue
            tok = tok.replace("[", "").replace("]", "")
            fqdn, slot_str = tok.split("=")
            slot = int(slot_str.strip())
            hostname = fqdn.strip().lower().split(".")[0]
            self.__slots[ht.Hostname(hostname)] = slot

        for tok in tokenize_pe_list(queue_config["pe_list"]):
            tok = tok.strip()

            if tok == "NONE":
                continue

            pe_name_expr: str
            hostgroups: List[str]

            if "@" in tok and "=" in tok:
                tok = tok.lstrip("[").rstrip("]")
                hostgroup, pe_name_expr = tok.split("=", 1)

                hostgroups = [hostgroup]
            else:
                pe_name_expr = tok
                hostgroups = [x for x in self.__hostlist if x.startswith("@")]

            pe_names = re.split(",| +", pe_name_expr)

            for pe_name in pe_names:
                if pe_name not in ge_env.pes:
                    logging.warning(
                        "Parallel environment %s was not found - %s. Skipping",
                        pe_name,
                        list(ge_env.pes.keys()),
                    )
                    continue
                assert pe_name not in self.__pe_to_hostgroups
                self.__pe_to_hostgroups[pe_name] = hostgroups
                self.__parallel_environments[pe_name] = ge_env.pes[pe_name]

        if queue_config["pe_list"] and queue_config["pe_list"].lower() != "none":
            assert self.__parallel_environments, queue_config["pe_list"]

        all_host_groups = set(self.hostlist)
        for pe in self.__parallel_environments.values():
            if pe.requires_placement_groups:
                all_host_groups = all_host_groups - set(
                    self.get_hostgroups_for_pe(pe.name)
                )
        self.__ht_hostgroups = list(all_host_groups)

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
    def complexes(self) -> Dict[str, "Complex"]:
        # TODO RDH should not need this here
        return self.ge_env.complexes

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
        return [self.__parallel_environments[x] for x in self._pe_keys(pe_name)]

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
        placement_group = ht.PlacementGroup(first.replace("@", ""))
        placement_group = re.sub("[^a-zA-z0-9-_]", "_", placement_group)
        return placement_group

    def __str__(self) -> str:
        return "GridEnineQueue(qname={}, pes={}, hostlist={})".format(
            self.qname, list(self.__parallel_environments.keys()), self.hostlist
        )

    def __repr__(self) -> str:
        return str(self)


def tokenize_pe_list(expr: str) -> List[str]:
    """ Since GE can use either a comma or space to separate both expressions and
        the list of pes that map to a hostgroup, we have to use a stateful parser.
        NONE,[@hostgroup=pe1 pe2],[@hostgroup2=pe3,pe4]
    """
    buf = StringIO()
    toks = []

    def append_if_token() -> StringIO:
        if buf.getvalue():
            toks.append(buf.getvalue())
        return StringIO()

    in_left_bracket = False

    for c in expr:
        if c.isalnum():
            buf.write(c)
        elif c == "[":
            in_left_bracket = True
        elif c == "]":
            in_left_bracket = False
        elif c in [" ", ","] and not in_left_bracket:
            buf = append_if_token()
        else:
            buf.write(c)

    buf = append_if_token()

    return toks


def read_queue_configs(autoscale_config: Dict, ge_env: "GridEngineEnvironment") -> None:
    qnames = ge_env.qbin.qconf(["-sql"]).split()

    logging.debug("Found %d queues: %s", len(qnames), " ".join(qnames))
    autoscale_queues_config = autoscale_config.get("gridengine", {}).get("queues", {})

    for qname in qnames:
        lines = ge_env.qbin.qconf(["-sq", qname]).splitlines()
        queue_config = parse_ge_config(lines)
        queue_constraints = autoscale_queues_config.get(queue_config["qname"], {}).get(
            "constraints", []
        )
        autoscale_enabled = autoscale_queues_config.get(queue_config["qname"], {}).get(
            "autoscale_enabled", True
        )
        ge_env.queues[qname] = GridEngineQueue(
            queue_config, ge_env, queue_constraints, autoscale_enabled
        )


def new_gequeue(
    qname: str,
    hostlist: List[str],
    pe_list: List[str],
    constraints: List[Constraint],
    slots_expr: str,
    ge_env: "GridEngineEnvironment",
) -> "GridEngineQueue":
    assert isinstance(hostlist, list)
    queue_config = {
        "qname": qname,
        "hostlist": " ".join(hostlist),
        "pe_list": " ".join(pe_list),
        "slots": slots_expr,
    }
    return GridEngineQueue(queue_config, ge_env, constraints)
