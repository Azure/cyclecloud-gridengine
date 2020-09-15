import re
from typing import Dict, List


def flatten_lines(lines: List[str]) -> List[str]:
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


def parse_ge_config(lines: List[str]) -> Dict[str, str]:
    config = {}

    for line in flatten_lines(lines):
        toks = re.split(r"[ \t]+", line, 1)
        if len(toks) != 2:
            continue
        config[toks[0].strip()] = toks[1].strip()

    return config
