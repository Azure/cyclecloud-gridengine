from abc import ABC, abstractmethod


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

    def __repr__(self) -> str:
        cname = self.__class__.__name__
        return "{}({})".format(cname, self.name)


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
