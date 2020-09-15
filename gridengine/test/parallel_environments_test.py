from io import StringIO
from typing import List

from gridengine.environment import GridEngineEnvironment
from gridengine.parallel_environments import GridEngineQueue, ParallelEnvironment


def tokenize(expr: str) -> List[str]:
    buf = StringIO()

    in_left_bracket = False

    toks = []

    for c in expr:
        if c.isalnum():
            buf.write(c)
            continue
        if c == "[":
            in_left_bracket = True
            continue
        if c == "]":
            in_left_bracket = False

        if c in [" ", ","] and not in_left_bracket:
            toks.append(buf.getvalue())
            buf = StringIO()
        else:
            buf.write(c)

    if buf.getvalue():
        toks.append(buf.getvalue())
        buf = StringIO()
    return toks


def test_ge_queue_parsing() -> None:
    pes = {}
    pes["smpslots"] = ParallelEnvironment(
        {"name": "smpslots", "allocation_rule": "$pe_slots"}
    )
    pes["smpslots2"] = ParallelEnvironment(
        {"name": "smpslots2", "allocation_rule": "$pe_slots"}
    )

    ge_env = GridEngineEnvironment([], [], [], pes, {})
    GridEngineQueue(
        {
            "hostlist": "@allhosts @otherhosts",
            "qname": "tempy",
            "pe_list": "[@allhosts=smpslots,smpslots2]",
        },
        ge_env,
        [],
    )


# print(tokenize("NONE,[@allhosts=mpi,mpislots]"))
test_ge_queue_parsing()
