from typing import Any, Dict, List, Optional

from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.util import partition_single

from gridengine.qbin import QBin


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
                try:
                    return bool(float(value))
                except ValueError:
                    if value.lower() in ["true", "false"]:
                        return value.lower() == "true"
                    else:
                        logging.warning(
                            "Could not parse '%s' for complex type %s - treating as string.",
                            value,
                            self.complex_type,
                        )
                    return value
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
                return mem.convert_to("g")
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


def read_complexes(autoscale_config: Dict, qbin: QBin) -> Dict[str, "Complex"]:
    complex_lines = qbin.qconf(["-sc"]).splitlines()
    return _parse_complexes(autoscale_config, complex_lines)


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
