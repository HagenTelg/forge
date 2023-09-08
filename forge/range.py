import typing
from abc import ABC, abstractmethod


def intersects(a_start: typing.Union[int, float], a_end: typing.Union[int, float],
               b_start: typing.Union[int, float], b_end: typing.Union[int, float]) -> bool:
    if a_start >= b_end:
        return False
    if b_start >= a_end:
        return False
    return True


class Subtractor(ABC):
    @property
    def canonical(self) -> bool:
        return True

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __delitem__(self, key: int) -> None:
        pass

    @abstractmethod
    def get_start(self, index: int) -> typing.Union[int, float]:
        pass

    @abstractmethod
    def get_end(self, index: int) -> typing.Union[int, float]:
        pass

    @abstractmethod
    def set_start(self, index: int, value: typing.Union[int, float]) -> None:
        pass

    @abstractmethod
    def set_end(self, index: int, value: typing.Union[int, float]) -> None:
        pass

    @abstractmethod
    def duplicate_after(self, source: int, start: typing.Union[int, float], end: typing.Union[int, float]) -> None:
        pass

    def subtract(self, sub_start: typing.Union[int, float], sub_end: typing.Union[int, float]):
        canonical = self.canonical

        if canonical:
            existing_index = 0
            end_index = len(self)
            while existing_index < end_index:
                mid = (existing_index + end_index) // 2
                if self.get_start(mid) < sub_start:
                    existing_index = mid + 1
                else:
                    end_index = mid
            existing_index = max(existing_index-1, 0)
        else:
            existing_index = 0

        while existing_index < len(self):
            inspect_start = self.get_start(existing_index)
            inspect_end = self.get_end(existing_index)
            # No intersection, so nothing to change
            if not intersects(sub_start, sub_end, inspect_start, inspect_end):
                if canonical and inspect_start >= sub_end:
                    break
                existing_index += 1
                continue

            # Entirely within the subtraction, so removed
            if inspect_start >= sub_start and inspect_end <= sub_end:
                del self[existing_index]
                continue

            if sub_start > inspect_start:
                # Since there's an intersection, if the subtraction starts after the existing, then the existing
                # must end at the subtraction start
                self.set_end(existing_index, sub_start)

                # If the subtraction ends after the existing, then we're done with just the truncation
                if sub_end >= inspect_end:
                    existing_index += 1
                    continue

                # Otherwise the subtraction is punching a hole, so make a second half
                self.duplicate_after(existing_index, sub_end, inspect_end)
                existing_index += 2
                continue

            # Subtraction starts before the existing, so with a known intersection, then the start is the subtraction
            self.set_start(existing_index, sub_end)
            existing_index += 1


def subtract_tuple(existing: typing.List[typing.Union[typing.Tuple[int, int], typing.Tuple[float, float]]],
                   sub_start: typing.Union[int, float], sub_end: typing.Union[int, float],
                   canonical: bool = True):
    class TupleSubtract(Subtractor):
        @property
        def canonical(self) -> bool:
            return canonical

        def __len__(self) -> int:
            return len(existing)

        def __delitem__(self, key: int) -> None:
            del existing[key]

        def get_start(self, index: int) -> typing.Union[int, float]:
            return existing[index][0]

        def get_end(self, index: int) -> typing.Union[int, float]:
            return existing[index][1]

        def set_start(self, index: int, value: typing.Union[int, float]) -> None:
            existing[index] = (value, existing[index][1])

        def set_end(self, index: int, value: typing.Union[int, float]) -> None:
            existing[index] = (existing[index][0], value)

        def duplicate_after(self, source: int, start: typing.Union[int, float], end: typing.Union[int, float]) -> None:
            existing.insert(source+1, (start, end))

    TupleSubtract().subtract(sub_start, sub_end)
