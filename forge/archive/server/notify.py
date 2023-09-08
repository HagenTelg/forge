import typing
import asyncio
import logging

_LOGGER = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from .connection import Connection


class NotificationDispatch:
    def __init__(self):
        self._listen_targets: typing.Dict[str, typing.Set["Connection"]] = dict()
        self._connection_attached: typing.Dict["Connection", typing.Set[str]] = dict()
        self._awaiting_send: typing.Dict["Connection", typing.Set["NotificationDispatch.Queued"]] = dict()
        self._awaiting_acknowledge: typing.Dict["Connection", typing.Dict[int, "NotificationDispatch.Queued"]] = dict()

    async def acknowledge(self, connection: "Connection", uid: int) -> None:
        waiting = self._awaiting_acknowledge.get(connection)
        if not waiting:
            return
        waiting = waiting.get(uid)
        if not waiting:
            return
        await waiting.acknowledged_received(connection)

    def listen(self, connection: "Connection", key: str) -> None:
        target = self._listen_targets.get(key)
        if target is None:
            target = set()
            self._listen_targets[key] = target
        target.add(connection)
        target = self._connection_attached.get(connection)
        if not target:
            target = set()
            self._connection_attached[connection] = target
        target.add(key)

    async def disconnect(self, connection: "Connection") -> None:
        targets = self._connection_attached.pop(connection, None)
        if targets:
            for key in targets:
                listeners = self._listen_targets[key]
                listeners.discard(connection)
                if not listeners:
                    del self._listen_targets[key]

        waiting = self._awaiting_send.pop(connection, None)
        if waiting:
            for queued in waiting:
                await queued.disconnected(connection)

        waiting = self._awaiting_acknowledge.pop(connection, None)
        if waiting:
            for queued in waiting.values():
                await queued.disconnected(connection)

    class Queued:
        def __init__(self, dispatch: "NotificationDispatch", key: str, start: int, end: int):
            self.dispatch = dispatch
            self.key = key
            self.start = start
            self.end = end
            self._wait_changed = asyncio.Condition()
            self._waiting_send: typing.Set["Connection"] = set()
            self._waiting_for: typing.Set["Connection"] = set()

        async def _send_notification(self, connection: "Connection") -> None:
            async with self._wait_changed:
                self._waiting_send.discard(connection)
                self.dispatch._awaiting_send[connection].discard(self)

                uid = connection.write_notification(self.key, self.start, self.end)
                self._waiting_for.add(connection)
                waiting = self.dispatch._awaiting_acknowledge.get(connection)
                if waiting is None:
                    waiting = dict()
                    self.dispatch._awaiting_acknowledge[connection] = waiting
                waiting[uid] = self

        def send(self) -> None:
            targets = self.dispatch._listen_targets.get(self.key)
            if not targets:
                return

            for connection in targets:
                self._waiting_send.add(connection)

                waiting = self.dispatch._awaiting_send.get(connection)
                if waiting is None:
                    waiting = set()
                    self.dispatch._awaiting_send[connection] = waiting
                waiting.add(self)

                connection.queue_unsolicited(self._send_notification, connection)

        async def disconnected(self, connection: "Connection") -> None:
            async with self._wait_changed:
                self._waiting_send.discard(connection)
                self._waiting_for.discard(connection)
                self._wait_changed.notify_all()

        async def wait_acknowledged(self) -> None:
            async with self._wait_changed:
                while True:
                    if not self._waiting_send and not self._waiting_for:
                        return
                    await self._wait_changed.wait()

        async def acknowledged_received(self, connection: "Connection") -> None:
            async with self._wait_changed:
                self._waiting_for.discard(connection)
                self._wait_changed.notify_all()

    def queue_notification(self, key: str, start: int, end: int) -> "NotificationDispatch.Queued":
        return self.Queued(self, key, start, end)

    @staticmethod
    async def dispatch(queued: typing.Iterable["NotificationDispatch.Queued"]) -> None:
        if not queued:
            return

        for n in queued:
            n.send()
        await asyncio.wait([
            asyncio.create_task(n.wait_acknowledged()) for n in queued
        ])

    def get_listening(self, connection: "Connection") -> typing.Set[str]:
        return self._connection_attached.get(connection, set())

    def get_awaiting_send(self, connection: "Connection") -> typing.Set["NotificationDispatch.Queued"]:
        return self._awaiting_send.get(connection, set())

    def get_awaiting_acknowledge(self, connection: "Connection") -> typing.Dict[int, "NotificationDispatch.Queued"]:
        return self._awaiting_acknowledge.get(connection, dict())
