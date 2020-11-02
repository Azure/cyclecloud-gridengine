import json
import re
import sys
import typing
from io import StringIO
from typing import Any, Dict, List, Optional, Union

from hpc.autoscale.node.node import Node

if typing.TYPE_CHECKING:
    from gridengine.hostgroup import BoundHostgroup, Hostgroup  # noqa: F401


def flatten_lines(lines: List[str]) -> List[str]:
    """
    Combines lines that were split with line continuations
    i.e.
    this is \
       one line
    => this is        one line
    """
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
    """
    Parses GE config files as a dictionary.
    e.g.
    name   all.q
    slots  5,\
            [@hostgroup=123]
    =>
    {"name": "all.q", "slots", "5, [@hostgroup=123]"}
    """
    config = {}

    for line in flatten_lines(lines):
        toks = re.split(r"[ \t]+", line, 1)
        if len(toks) != 2:
            continue
        config[toks[0].strip()] = toks[1].strip()

    return config


def get_node_hostgroups(node: Node) -> List[str]:
    hostgroups_expr = node.metadata.get("gridengine_hostgroups")

    if not hostgroups_expr:
        hostgroups_expr = node.software_configuration.get("gridengine_hostgroups")

    if not hostgroups_expr:
        return []

    return re.split(",| +", hostgroups_expr)


def add_node_to_hostgroup(
    node: Node, hostgroup: Union["BoundHostgroup", "Hostgroup"]
) -> None:
    def _node_override(node: Node) -> Dict:
        c = node.node_attribute_overrides.get("Configuration", {})
        node.node_attribute_overrides["Configuration"] = c
        return c

    def _metadata(node: Node) -> Dict:
        return node.metadata

    for attr in [_metadata] if node.exists else [_metadata, _node_override]:
        d = attr(node)
        hostgroups_expr = d.get("gridengine_hostgroups")
        if not hostgroups_expr:
            d["gridengine_hostgroups"] = hostgroup.name
        elif hostgroup.name not in hostgroups_expr:
            d["gridengine_hostgroups"] = "{} {}".format(hostgroups_expr, hostgroup.name)

    hostgroup.add_member(node.hostname_or_uuid.lower())


_SPLIT_PATTERN = re.compile("[, \t]+")


def split_ge_list(expr: str) -> List[str]:
    """
    Tokenizes expressions like
    a,b c
    => "a", "b", "c"
    """
    ret: List[str] = []
    for x in _SPLIT_PATTERN.split(expr):
        if not x:
            continue
        if x.lower() == "none":
            ret.append(None)  # type: ignore
        else:
            ret.append(x)
    return ret


def parse_hostgroup_mapping(
    expr: str, default_hostgroups: Optional[List[str]] = None, filter_none: bool = False
) -> Dict[str, List[str]]:
    """
        parses expressions like
        NONE,[@hostgroup=list delimited by spaces or commas],[@hostgroup2=a,b,c]
    """
    default_hostgroups = [None] if default_hostgroups is None else default_hostgroups  # type: ignore
    mapping: Dict[str, List[str]] = {}

    toks = _tokenize_ge_list(expr)

    def _add_default_mapping(tok: str) -> None:
        sub_toks = split_ge_list(tok)
        # the type checker does not like default_hostgroups, since it is looking at the type
        # of the argument
        for hg in default_hostgroups:  # type: ignore
            if hg not in mapping:
                mapping[hg] = []
            mapping[hg].extend(sub_toks)

    for tok in toks:
        tok = tok.strip()

        if not tok:
            continue

        if "[" not in tok:
            _add_default_mapping(tok)
            continue

        tok = tok.replace("[", "").replace("]", "")
        if filter_none and tok.lower() == "none":
            continue

        if "=" not in tok:
            _add_default_mapping(tok)
            continue

        fqdn, sub_expr = tok.split("=", 1)

        if not fqdn.startswith("@"):
            hostgroup = fqdn.strip().lower().split(".")[0]
        else:
            hostgroup = fqdn

        mapping[hostgroup] = split_ge_list(sub_expr)

    return mapping


def _tokenize_ge_list(expr: str) -> List[str]:
    """ Since GE can use either a comma or space to separate both expressions and
        the lists that map a hostgroup to various things, we have to use a stateful parser.
        e.g. NONE,[@hostgroup=pe1 pe2],[@hostgroup2=pe3,pe4]
        A simple re.split is fine for the subexprs.
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
            buf.write(c)
        elif c == "]":
            in_left_bracket = False
            buf.write(c)
        elif c in [" ", ","] and not in_left_bracket:
            buf = append_if_token()
        else:
            buf.write(c)

    buf = append_if_token()
    return toks


def _json_default(x: Any) -> Any:
    if hasattr(x, "to_dict"):
        return getattr(x, "to_dict")()
    return str(x)


def json_dump(obj: Any, stream: Any = sys.stdout) -> None:
    json.dump(obj, stream, indent=2, default=_json_default)
