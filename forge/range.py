import typing
from abc import ABC, abstractmethod


def intersects(a_start: typing.Union[int, float], a_end: typing.Union[int, float],
               b_start: typing.Union[int, float], b_end: typing.Union[int, float]) -> bool:
    if a_start >= b_end:
        return False
    if b_start >= a_end:
        return False
    return True


def contains(out_start: typing.Union[int, float], out_end: typing.Union[int, float],
             in_start: typing.Union[int, float], in_end: typing.Union[int, float]) -> bool:
    if in_start < out_start:
        return False
    if in_end > out_end:
        return False
    return True


class _Search(ABC):
    @property
    def canonical(self) -> bool:
        return True

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def get_start(self, index: int) -> typing.Union[int, float]:
        pass

    @abstractmethod
    def get_end(self, index: int) -> typing.Union[int, float]:
        pass

    def _find_before_start(self, search_start: typing.Union[int, float]) -> int:
        if self.canonical:
            existing_index = 0
            end_index = len(self)
            while existing_index < end_index:
                mid = (existing_index + end_index) // 2
                if self.get_start(mid) < search_start:
                    existing_index = mid + 1
                else:
                    end_index = mid
            return max(existing_index-1, 0)
        else:
            return 0


class Insertion(_Search):
    def before(self, start: typing.Union[int, float]) -> int:
        if not self.canonical:
            return len(self)
        existing_index = 0
        end_index = len(self)
        while existing_index < end_index:
            mid = (existing_index + end_index) // 2
            if self.get_start(mid) < start:
                existing_index = mid + 1
            else:
                end_index = mid
        return existing_index

    __call__ = before


def insertion_tuple(existing: typing.List[typing.Union[typing.Tuple[int, int], typing.Tuple[float, float]]],
                    find_start: typing.Union[int, float], canonical: bool = True) -> int:
    class TupleInsertion(Insertion):
        @property
        def canonical(self) -> bool:
            return canonical

        def __len__(self) -> int:
            return len(existing)

        def get_start(self, index: int) -> typing.Union[int, float]:
            return existing[index][0]

        def get_end(self, index: int) -> typing.Union[int, float]:
            return existing[index][1]

    return TupleInsertion()(find_start)


class Subtractor(_Search):
    @abstractmethod
    def __delitem__(self, key: int) -> None:
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

    def __call__(self, sub_start: typing.Union[int, float], sub_end: typing.Union[int, float]) -> None:
        canonical = self.canonical
        existing_index = self._find_before_start(sub_start)

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
                   canonical: bool = True) -> None:
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

    TupleSubtract()(sub_start, sub_end)


class FindIntersecting(_Search):
    def __call__(self, find_start: int, find_end: int) -> typing.Union[typing.List[int], range]:
        if self.canonical:
            begin_index = self._find_before_start(find_start)
            intersection_begin: typing.Optional[int] = None
            intersection_end: typing.Optional[int] = None
            for inspect_index in range(begin_index, len(self)):
                inspect_start = self.get_start(inspect_index)
                inspect_end = self.get_end(inspect_index)
                if not intersects(find_start, find_end, inspect_start, inspect_end):
                    if inspect_start >= find_end:
                        break
                    continue
                if intersection_begin is None:
                    intersection_begin = inspect_index
                intersection_end = inspect_index
            if intersection_begin is not None:
                return range(intersection_begin, intersection_end+1)
            return range(0)

        result: typing.List[int] = list()
        for inspect_index in range(len(self)):
            inspect_start = self.get_start(inspect_index)
            inspect_end = self.get_end(inspect_index)
            if not intersects(find_start, find_end, inspect_start, inspect_end):
                continue
            result.append(inspect_index)
        return result


def intersecting_tuple(existing: typing.List[typing.Union[typing.Tuple[int, int], typing.Tuple[float, float]]],
                       find_start: typing.Union[int, float], find_end: typing.Union[int, float],
                       canonical: bool = True) -> typing.Union[typing.List[int], range]:
    class TupleIntersecting(FindIntersecting):
        @property
        def canonical(self) -> bool:
            return canonical

        def __len__(self) -> int:
            return len(existing)

        def get_start(self, index: int) -> typing.Union[int, float]:
            return existing[index][0]

        def get_end(self, index: int) -> typing.Union[int, float]:
            return existing[index][1]

    return TupleIntersecting()(find_start, find_end)
