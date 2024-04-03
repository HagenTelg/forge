import typing
import asyncio
import logging
import numpy as np
from math import floor, ceil
from tempfile import NamedTemporaryFile
from netCDF4 import Dataset
from forge.archive.client import event_log_lock_key, event_log_file_name
from forge.vis.data.stream import DataStream, ArchiveReadStream

_LOGGER = logging.getLogger(__name__)


class _EventLogStream(ArchiveReadStream):
    def __init__(self, station: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station
        self.start_epoch_ms = start_epoch_ms
        self.end_epoch_ms = end_epoch_ms

    @property
    def connection_name(self) -> str:
        return "read event log"

    async def acquire_locks(self) -> None:
        lock_start = int(floor(self.start_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60 * 1000
        lock_end = int(ceil(self.end_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60 * 1000
        await self.connection.lock_read(event_log_lock_key(self.station), lock_start, lock_end)

    async def _send_file(self, root: Dataset) -> None:
        log_group = root.groups.get("log")
        if log_group is None:
            _LOGGER.warning("Invalid event log file")
            return
        event_time = log_group.variables.get("time")
        if event_time is None:
            _LOGGER.warning("Invalid event log file")
            return
        if len(event_time.shape) != 1:
            _LOGGER.warning("Invalid event log file")
            return
        if event_time.shape[0] == 0:
            return

        if self.start_epoch_ms <= event_time[0]:
            start_idx = 0
        else:
            start_idx = np.searchsorted(event_time, self.start_epoch_ms)
        if self.end_epoch_ms > event_time[-1]:
            end_idx = event_time.shape[0]
        else:
            end_idx = np.searchsorted(event_time, self.end_epoch_ms)
        if start_idx >= end_idx:
            return

        event_type = log_group.variables["type"]
        event_source = log_group.variables["source"]
        event_message = log_group.variables["message"]

        event_type_map: typing.Dict[int, str] = {
            event_type.datatype.enum_dict.get('User'): "User",
            event_type.datatype.enum_dict.get('Info'): "Instrument",
            event_type.datatype.enum_dict.get('Error'): "Instrument",
            event_type.datatype.enum_dict.get('CommunicationsEstablished'): "Communications",
            event_type.datatype.enum_dict.get('CommunicationsLost'): "Communications",
        }
        event_acquisition: typing.Set[int] = {
            event_type.datatype.enum_dict.get('Info'),
            event_type.datatype.enum_dict.get('Error'),
            event_type.datatype.enum_dict.get('CommunicationsEstablished'),
            event_type.datatype.enum_dict.get('CommunicationsLost'),
        }
        event_error: typing.Set[int] = {
            event_type.datatype.enum_dict.get('Error'),
            event_type.datatype.enum_dict.get('CommunicationsLost'),
        }

        for event_idx in range(start_idx, end_idx):
            type_code = int(event_type[event_idx])

            author = str(event_source[event_idx])
            send_event: typing.Dict[str, typing.Any] = {
                'epoch_ms': int(event_time[event_idx]),
                'message': str(event_message[event_idx]),
                'author': author,
                'type': event_type_map.get(type_code, "Instrument"),
            }
            if type_code in event_acquisition:
                send_event['acquisition'] = True
                if not author:
                    send_event['author'] = "SYSTEM"
            if type_code in event_error:
                send_event['error'] = True

            await self.send(send_event)

    async def with_locks_held(self) -> None:
        day_start = int(floor(self.start_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60
        day_end = int(ceil(self.end_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60
        if day_end * (24 * 60 * 60 * 1000) <= self.end_epoch_ms:
            day_end += 1
        for day_begin in range(day_start, day_end, 24 * 60 * 60):
            with NamedTemporaryFile(suffix=".nc") as data_file:
                try:
                    await self.connection.read_file(event_log_file_name(self.station, day_begin), data_file)
                    data_file.flush()
                except FileNotFoundError:
                    continue
                data = Dataset(data_file.name, 'r')
                try:
                    await self._send_file(data)
                finally:
                    data.close()


def read_eventlog(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                  send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return _EventLogStream(station, start_epoch_ms, end_epoch_ms, send)
