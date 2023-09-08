import typing
import logging
from bisect import bisect_left, bisect_right
from forge.range import intersects

_LOGGER = logging.getLogger(__name__)


class LockDenied(Exception):
    def __init__(self, blocked: typing.Any):
        self.blocked = blocked


class ArchiveLocker:
    def __init__(self):
        self._held: typing.Dict[str, typing.Tuple[typing.List[int], typing.List["ArchiveLocker.Lock"]]] = dict()

    class Lock:
        def __init__(self, locker: "ArchiveLocker", origin: typing.Any,
                     generation: int, key: str, start: int, end: int, write: bool):
            self.locker = locker
            self.origin = origin
            self.generation = generation
            self.key = key
            self.start = start
            self.end = end
            self.write = write
            self._was_released = False

        def release(self) -> None:
            self.locker._remove_lock(self)
            self._was_released = True

        def __del__(self):
            if not self._was_released:
                _LOGGER.error("Leaked lock for generation %d", self.generation)
                self._was_released = True

        def __enter__(self) -> "ArchiveLocker.Lock":
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            self.release()

    class _IntersectingIterator:
        def __init__(self, start: int, end: int, possible: typing.Iterable["ArchiveLocker.Lock"]):
            self.start = start
            self.end = end
            self.possible = iter(possible)

        def __iter__(self):
            return self

        def __next__(self):
            while True:
                n = next(self.possible)
                if not intersects(self.start, self.end, n.start, n.end):
                    continue
                return n

    def _add_lock(self, lock: "ArchiveLocker.Lock") -> None:
        destination = self._held.get(lock.key)
        if destination is None:
            destination = (list(), list())
            self._held[lock.key] = destination
        target_index = bisect_right(destination[0], lock.generation)
        destination[0].insert(target_index, lock.generation)
        destination[1].insert(target_index, lock)

    def _remove_lock(self, lock: "ArchiveLocker.Lock"):
        all_locks = self._held[lock.key]
        generation = lock.generation
        begin_check = bisect_left(all_locks[0], generation)
        for i in range(begin_check, len(all_locks[1])):
            check = all_locks[1][i]
            if check == lock:
                del all_locks[0][i]
                del all_locks[1][i]
                break
            if all_locks[0][i] != generation:
                raise KeyError
        else:
            raise KeyError

    def _intersecting_locks(self, generation: typing.Optional[int], key: str, start: int, end: int) -> typing.Optional["ArchiveLocker._IntersectingIterator"]:
        all_locks = self._held.get(key)
        if not all_locks:
            return None
        if generation is None:
            return self._IntersectingIterator(start, end, all_locks[1])
        # Storage writes commit at transaction generation plus one, so the write lock only needs to intersect
        # if the write generation is greater than (not equal to) the read generation.  That is, same generation
        # locks don't see each other, since changes are made at the NEXT generation.
        end_check = bisect_left(all_locks[0], generation)
        if end_check == 0:
            return None
        return self._IntersectingIterator(start, end, all_locks[1][:end_check])

    def acquire_read(self, generation: int, origin: typing.Any, key: str, start: int, end: int) -> "ArchiveLocker.Lock":
        locks = self._intersecting_locks(generation, key, start, end)
        if locks:
            for check in locks:
                if check.origin == origin:
                    continue
                if not check.write:
                    # Any combination of overlapping reads is fine, but if there's a write before us (see below),
                    # then we have to fail, since we might see a partial view.
                    continue
                _LOGGER.debug("Read %s lock conflict (%d) with %s (%d)", key, generation,
                              check.origin, check.generation)
                raise LockDenied(check.origin)
        add = self.Lock(self, origin, generation, key, start, end, False)
        self._add_lock(add)
        return add

    def acquire_write(self, generation: int, origin: typing.Any, key: str, start: int, end: int) -> "ArchiveLocker.Lock":
        locks = self._intersecting_locks(None, key, start, end)
        if locks:
            for check in locks:
                if check.origin == origin:
                    continue
                # Any overlapping writes conflict.
                if not check.write:
                    # A read lock behind us is fine (it'll see the redirections), one ahead can happen if another
                    # write transaction advanced the generation, then the read starts, before we actually acquire
                    # the lock.  In that case, we have to abort, since that read might see a partial view.
                    if check.generation <= generation:
                        continue
                _LOGGER.debug("Write %s lock conflict (%d) with %s (%d)", key, generation,
                              check.origin, check.generation)
                raise LockDenied(check.origin)
        add = self.Lock(self, origin, generation, key, start, end, True)
        self._add_lock(add)
        return add
