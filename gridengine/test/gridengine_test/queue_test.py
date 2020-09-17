from gridengine.queue import parse_slots


def test_parse_slots() -> None:
    slots_with_0 = parse_slots("0, [@allhosts=0], [tux=4], [@onprem=16]")
    slots_without_0 = parse_slots("[@allhosts=0], [tux=4], [@onprem=16]")
    assert slots_with_0 == slots_without_0
    assert slots_with_0 == {"@allhosts": 0, "tux": 4, "@onprem": 16}
