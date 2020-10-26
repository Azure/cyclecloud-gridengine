from typing import Dict

from hpc.autoscale import hpclogging as logging

from gridengine import util
from gridengine.qbin import QBin


class GridEngineScheduler:
    def __init__(self, config: Dict) -> None:
        self.__config = config
        self.__sort_by_seqno = False
        # GE < 8.6 will have this defined
        qsm = self.__config.get("queue_sort_method")
        if qsm:
            self.__sort_by_seqno = qsm == "seqno"
        else:

            def try_parse(k: str, default: float) -> float:
                try:
                    return float(config.get(k, default))
                except ValueError:
                    logging.error(
                        "Could not parse %s as a float", config.get(k),
                    )
                    return default

            weight_queue_seqno = try_parse("weight_queue_seqno", 0.0)
            weight_queue_host_sort = try_parse("weight_queue_host_sort", 1.0)

            self.__sort_by_seqno = weight_queue_host_sort < weight_queue_seqno

    @property
    def sort_by_seqno(self) -> bool:
        return self.__sort_by_seqno


def read_scheduler(qbin: QBin) -> GridEngineScheduler:
    lines = qbin.qconf(["-ssconf"]).splitlines()
    sched_config = util.parse_ge_config(lines)
    return GridEngineScheduler(sched_config)
