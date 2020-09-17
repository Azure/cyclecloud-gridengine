from typing import Dict, Optional

from gridengine import util
from gridengine.qbin import QBin


class GridEngineScheduler:
    def __init__(self, config: Dict) -> None:
        self.__config = config
        self.__sort_by_seqno = (
            "seqno" == (config.get("queue_sort_method") or "").lower()
        )

    @property
    def queue_sort_method(self) -> Optional[str]:
        return self.__config.get("queue_sort_method")

    def sort_by_seqno(self) -> bool:
        return self.__sort_by_seqno


def read_scheduler(qbin: QBin) -> GridEngineScheduler:
    lines = qbin.qconf(["-ssconf"]).splitlines()
    sched_config = util.parse_ge_config(lines)
    return GridEngineScheduler(sched_config)
