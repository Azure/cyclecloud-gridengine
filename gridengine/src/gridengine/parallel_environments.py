from typing import Dict

from hpc.autoscale import hpclogging as logging

from gridengine.allocation_rules import AllocationRule
from gridengine.qbin import QBin
from gridengine.util import parse_ge_config


def read_parallel_environments(
    autoscale_config: Dict, qbin: QBin,
) -> Dict[str, "ParallelEnvironment"]:
    parallel_envs = {}
    pe_config = autoscale_config.get("gridengine", {}).get("pes", {})
    pe_names = qbin.qconf(["-spl"]).splitlines()

    for pe_name in pe_names:
        pe_name = pe_name.strip()
        lines = qbin.qconf(["-sp", pe_name]).splitlines(False)
        pe = parse_ge_config(lines)

        req_key = "requires_placement_groups"

        if req_key in pe_config.get(pe_name, {}):
            logging.warning(
                "Overriding placement group behavior for PE %s with %s",
                pe_name,
                pe_config[pe_name][req_key],
            )
            pe[req_key] = pe_config[pe_name][req_key]

        parallel_envs[pe_name] = ParallelEnvironment(pe)

    return parallel_envs


def new_parallel_environment(
    pe_name: str, slots: int, allocation_rule: "AllocationRule"
) -> "ParallelEnvironment":
    config = {
        "pe_name": pe_name,
        "slots": str(slots),
        "allocation_rule": allocation_rule.name,
    }
    return ParallelEnvironment(config)


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
        override = self.config.get("requires_placement_groups")
        if override is not None:
            assert isinstance(
                override, bool
            ), "requires_placement_groups must a be a boolean"
            return override
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
