# for backwards compatability, this is a global var instead of an argument
# to sge_job_handler.
import re
from abc import ABC, abstractmethod
from shutil import which
from typing import Any, Dict, List, Optional, Union

from gridengine.util import check_output
from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node.constraints import Constraint
from hpc.autoscale.util import partition_single

QCONF_PATH = which("qconf") or ""


_PE_CACHE: Dict[str, "ParallelEnvironment"] = {}
_QUEUE_CACHE: List["GridEngineQueue"] = []
_COMPLEX_CACHE: Dict[str, "Complex"] = {}


def set_pe(pe_name: str, pe: Union[Dict[str, str], "ParallelEnvironment"]) -> None:
    global _PE_CACHE
    if _PE_CACHE is None:
        _PE_CACHE = {}

    if not isinstance(pe, ParallelEnvironment):
        pe = ParallelEnvironment(pe)

    _PE_CACHE[pe_name] = pe


def read_parallel_environments(
    autoscale_config: Dict,
) -> Dict[str, "ParallelEnvironment"]:
    global _PE_CACHE
    if _PE_CACHE:
        return _PE_CACHE

    parallel_envs = {}

    pe_names = check_output([QCONF_PATH, "-spl"]).decode().splitlines()
    for pe_name in pe_names:
        pe_name = pe_name.strip()
        lines = check_output([QCONF_PATH, "-sp", pe_name]).decode().splitlines(False)
        pe = _parse_ge_config(lines)

        parallel_envs[pe_name] = ParallelEnvironment(pe)

    _PE_CACHE = parallel_envs
    return _PE_CACHE


def initialize(
    autoscale_config: Dict[str, str],
    complex_path: str,
    pe_paths: Dict[str, str],
    qconfigs: Dict[str, Dict[str, str]],
) -> None:
    global _COMPLEX_CACHE
    _QUEUE_CACHE.clear()
    _PE_CACHE.clear()

    with open(complex_path) as fr:
        _COMPLEX_CACHE = _parse_complexes(autoscale_config, fr.readlines())

    for pe_name, pe_path in pe_paths.items():
        with open(pe_path) as fr:
            pe_config = _parse_ge_config(fr.readlines())
            pe_config["pe_name"] = pe_name
            pe = ParallelEnvironment(pe_config)
            _PE_CACHE[pe.name] = pe

    for qname, qconfig in qconfigs.items():
        _QUEUE_CACHE.append(GridEngineQueue(qconfig, autoscale_config, []))


def read_queue_configs(autoscale_config: Dict) -> List["GridEngineQueue"]:
    global _QUEUE_CACHE
    if _QUEUE_CACHE:
        return _QUEUE_CACHE

    qnames = check_output([QCONF_PATH, "-sql"]).decode().split()
    ge_queues = []
    logging.debug("Found %d queues: %s", len(qnames), " ".join(qnames))
    autoscale_queues_config = autoscale_config.get("gridengine", {}).get("queues", {})
    for qname in qnames:
        lines = check_output([QCONF_PATH, "-sq", qname]).decode().splitlines()
        queue_config = _parse_ge_config(lines)
        queue_constraints = autoscale_queues_config.get(queue_config["qname"], {}).get(
            "constraints", []
        )
        ge_queues.append(
            GridEngineQueue(queue_config, autoscale_config, queue_constraints)
        )
    _QUEUE_CACHE = ge_queues
    return _QUEUE_CACHE


def set_complexes(autoscale_config: Dict, complex_lines: List[str]) -> None:
    global _COMPLEX_CACHE
    _COMPLEX_CACHE = _parse_complexes(autoscale_config, complex_lines)


def read_complexes(autoscale_config: Dict) -> Dict[str, "Complex"]:
    global _COMPLEX_CACHE
    if _COMPLEX_CACHE:
        return _COMPLEX_CACHE

    complex_lines = check_output([QCONF_PATH, "-sc"]).decode().splitlines()
    _COMPLEX_CACHE = _parse_complexes(autoscale_config, complex_lines)
    return _COMPLEX_CACHE


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
            if relevant_complexes and c["name"] not in relevant_complexes:
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


def _flatten_lines(lines: List[str]) -> List[str]:
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


def _parse_ge_config(lines: List[str]) -> Dict[str, str]:
    config = {}

    for line in _flatten_lines(lines):
        toks = re.split(r"[ \t]+", line, 1)
        if len(toks) != 2:
            continue
        config[toks[0].strip()] = toks[1].strip()

    return config


class GridEngineQueue:
    def __init__(
        self,
        queue_config: Dict[str, str],
        autoscale_config: Dict[str, str],
        constraints: List[Constraint],
    ) -> None:
        self.queue_config = queue_config
        if isinstance(constraints, dict):
            constraints = [constraints]

        if not isinstance(constraints, list):
            logging.error(
                "Ignoring invalid constraints for queue %s! %s", self.qname, constraints
            )
            constraints = [{"node.nodearray": self.qname}]

        self.constraints = constraints
        self.__hostlist = self.queue_config["hostlist"].split(",")
        self.__pe_to_hostgroups: Dict[str, List[str]] = {}
        self.__complexes = read_complexes(autoscale_config)
        self.__parallel_environments: Dict[str, "ParallelEnvironment"] = {}
        self._pe_keys_cache: Dict[str, List[str]] = {}
        all_pes = read_parallel_environments(autoscale_config)
        self.__slots = {}

        for tok in self.queue_config.get("slots", "").split(","):
            if "[" not in tok:
                continue
            tok = tok.replace("[", "").replace("]", "")
            fqdn, slot_str = tok.split("=")
            slot = int(slot_str.strip())
            hostname = fqdn.strip().lower().split(".")[0]
            self.__slots[ht.Hostname(hostname)] = slot

        for tok in re.split(r"[ \t,]+", queue_config["pe_list"]):

            tok = tok.strip()

            if tok == "NONE":
                continue

            pe_name: str
            hostgroups: List[str]

            if not tok.startswith("["):
                pe_name = tok
                hostgroups = [x for x in self.__hostlist if x.startswith("@")]
            else:
                tok = tok[1:-1].strip()
                hostgroup, pe_name = tok.split("=", 1)
                hostgroups = [hostgroup]

            if pe_name not in all_pes:
                logging.warning(
                    "Parallel environment %s was not found - %s. Skipping",
                    pe_name,
                    list(all_pes.keys()),
                )
                continue
            assert pe_name not in self.__pe_to_hostgroups
            self.__pe_to_hostgroups[pe_name] = hostgroups
            self.__parallel_environments[pe_name] = all_pes[pe_name]
        assert self.__parallel_environments, queue_config["pe_list"]

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
        return self.__complexes

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

    def get_pes(self, pe_name: str) -> List["ParallelEnvironment"]:
        if not self.has_pe(pe_name):
            raise RuntimeError(
                "Queue {} does not support parallel_environment {}".format(
                    self.qname, pe_name
                )
            )
        return [self.__parallel_environments[x] for x in self._pe_keys(pe_name)]

    def get_hostgroups_for_pe(self, pe_name: str) -> List[str]:
        if not self.has_pe(pe_name):
            raise RuntimeError(
                "Queue {} does not support parallel_environment {}".format(
                    self.qname, pe_name
                )
            )

        return self.__pe_to_hostgroups[pe_name]

    def __str__(self) -> str:
        return "GridEnineQueue(qname={}, pes={}, hostlist={})".format(
            self.qname, list(self.__parallel_environments.keys()), self.hostlist
        )

    def __repr__(self) -> str:
        return str(self)


class ParallelEnvironment:
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.__allocation_rule = AllocationRule.value_of(config["allocation_rule"])

    @property
    def name(self) -> str:
        return self.config["pe_name"]

    @property
    def slots(self) -> int:
        return int(self.config["slots"])

    @property
    def allocation_rule(self) -> "AllocationRule":
        return self.__allocation_rule

    @property
    def requires_placement_groups(self) -> bool:
        return self.allocation_rule.requires_placement_groups

    @property
    def is_fixed(self) -> bool:
        return self.allocation_rule.is_fixed

    def __repr__(self) -> str:
        return "ParallelEnvironment(name=%s, slots=%s, alloc=%s, pg=%s)" % (
            self.name,
            self.slots,
            self.allocation_rule,
            self.requires_placement_groups,
        )


class AllocationRule(ABC):
    def __init__(self, name: str) -> None:
        self.__name = name

    @property
    @abstractmethod
    def requires_placement_groups(self) -> bool:
        ...

    @property
    def name(self) -> str:
        return self.__name

    @property
    def is_fixed(self) -> bool:
        return False

    @staticmethod
    def value_of(allocation_rule: str) -> "AllocationRule":
        if allocation_rule == "$fill_up":
            return FillUp()
        elif allocation_rule == "$round_robin":
            return RoundRobin()
        elif allocation_rule == "$pe_slots":
            return PESlots()
        elif allocation_rule.isdigit():
            return FixedProcesses(allocation_rule)
        else:
            raise RuntimeError("Unknown allocation rule {}".format(allocation_rule))


class FillUp(AllocationRule):
    def __init__(self) -> None:
        super().__init__("$fill_up")

    @property
    def requires_placement_groups(self) -> bool:
        return True


class RoundRobin(AllocationRule):
    def __init__(self) -> None:
        super().__init__("$round_robin")

    @property
    def requires_placement_groups(self) -> bool:
        return True


class PESlots(AllocationRule):
    def __init__(self) -> None:
        super().__init__("$pe_slots")

    @property
    def requires_placement_groups(self) -> bool:
        return False


class FixedProcesses(AllocationRule):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.__fixed_processes = int(name)

    @property
    def requires_placement_groups(self) -> bool:
        return True

    @property
    def is_fixed(self) -> bool:
        return True

    @property
    def fixed_processes(self) -> int:
        return self.__fixed_processes


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
                return bool(float(value))

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
                return mem.convert_to("m").value
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
