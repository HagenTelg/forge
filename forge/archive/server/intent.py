import typing
import asyncio
import logging
from forge.range import intersects

_LOGGER = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from .connection import Connection


class IntentTracker:
    def __init__(self):
        self._key_held: typing.Dict[str, typing.Set["IntentTracker.Intent"]] = dict()
        self._origin_held: typing.Dict["Connection", typing.Dict[int, "IntentTracker.Intent"]] = dict()

    class Intent:
        def __init__(self, origin: "Connection", uid: int, key: str, start: int, end: int):
            self.origin = origin
            self.uid = uid
            self.key = key
            self.start = start
            self.end = end

    def _release_intent(self, to_release: "IntentTracker.Intent") -> None:
        intents = self._key_held[to_release.key]
        intents.remove(to_release)
        if not intents:
            del self._key_held[to_release.key]

    def disconnect(self, connection: "Connection") -> None:
        targets = self._origin_held.pop(connection, None)
        if targets:
            for to_release in targets.values():
                self._release_intent(to_release)

    def acquire(self, origin: "Connection", uid: int, key: str, start: int, end: int) -> None:
        # if self.get_conflicting(origin, key, start, end):
        #     raise KeyError("Overlapping conflicting intents do not make sense")

        intent = self.Intent(origin, uid, key, start, end)

        target = self._key_held.get(key)
        if not target:
            target = set()
            self._key_held[key] = target
        target.add(intent)

        target = self._origin_held.get(origin)
        if not target:
            target = dict()
            self._origin_held[origin] = target
        target[uid] = intent

    def release(self, origin: "Connection", uid: int) -> None:
        connection_intents = self._origin_held[origin]
        to_release = connection_intents.pop(uid)
        self._release_intent(to_release)
        if not connection_intents:
            del self._origin_held[origin]

    def get_conflicting(self, origin: typing.Optional["Connection"],
                        key: str, start: int, end: int) -> typing.Set["Connection"]:
        intersecting = set()

        possible = self._key_held.get(key)
        if not possible:
            return intersecting

        for check in possible:
            if not intersects(start, end, check.start, check.end):
                continue
            if check.origin == origin:
                continue
            intersecting.add(check.origin)

        return intersecting

    def get_held(self, origin: "Connection") -> typing.Dict[int, "IntentTracker.Intent"]:
        connection_intents = self._origin_held.get(origin)
        if not connection_intents:
            return dict()
        return connection_intents
