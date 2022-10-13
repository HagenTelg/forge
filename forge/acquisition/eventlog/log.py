import typing
import asyncio
import time
import logging
import shutil
import numpy as np
import forge.data.structure.timeseries as netcdf_timeseries
from enum import IntEnum
from math import floor
from json import dumps as to_json
from pathlib import Path
from secrets import token_bytes
from base64 import b32encode
from netCDF4 import Dataset
from forge.tasks import wait_cancelable
from forge.formattime import format_iso8601_time
from forge.data.structure import event_log
from forge.data.structure.history import append_history
from forge.acquisition import LayeredConfiguration
from forge.acquisition.util import parse_interval, write_replace_file


_LOGGER = logging.getLogger(__name__)


class _Type(IntEnum):
    User = 0,
    Info = 1,
    CommunicationsEstablished = 2,
    CommunicationsLost = 3,
    Error = 4,


class _Event:
    def __init__(self, message_time: float, event_type: _Type, source: str, message: str,
                 data: typing.Optional[str] = None):
        self.time = message_time
        self.type = event_type
        self.source = source
        self.message = message
        self.data = data


class Log:
    def __init__(self, station: str, config: LayeredConfiguration,
                 working_directory: Path = None,
                 completed_directory: Path = None,):
        self.station = station
        self.first_time: typing.Optional[float] = time.time()
        self._file_start = self.first_time
        self._file_end: typing.Optional[float] = None

        self._data_updated: typing.Optional[asyncio.Event] = None
        self._events: typing.List[_Event] = list()

        self._override_config = config.section("METADATA")

        if not working_directory:
            working_directory = Path('.')
        self._working_directory: Path = working_directory
        if not completed_directory:
            completed_directory = Path('.')
        self._completed_directory: Path = completed_directory

        self._file_duration: float = parse_interval(config.get("DURATION"), 60 * 60)
        if self._file_duration <= 0.0:
            raise ValueError(f"invalid data file duration {self._file_duration}")

        self._active_output_file: typing.Optional[Path] = None

        self._events.append(_Event(self.first_time, _Type.Info, "", "Acquisition system startup"))

    def add_message(self, source: str, message: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(message, dict):
            _LOGGER.debug("Invalid message content received")
            return

        text = message.get("message")
        if not text or not isinstance(text, str):
            _LOGGER.debug("Empty message received")
            return

        message_type = message.get('type')
        message_source = source
        if message_type == 'info':
            message_type = _Type.Info
        elif message_type == 'communications_established':
            message_type = _Type.CommunicationsEstablished
        elif message_type == 'communications_lost':
            message_type = _Type.CommunicationsLost
        elif message_type == 'error':
            message_type = _Type.Error
        else:
            message_type = _Type.User
            author = message.get('author')
            if author and isinstance(author, str):
                message_source = author

        serialized_data: typing.Optional[str] = None
        data = message.get('auxiliary')
        if data and isinstance(data, dict) and len(data) > 0:
            serialized_data = to_json(data)

        self._events.append(_Event(time.time(), message_type, message_source, text, serialized_data))

        if self._data_updated:
            self._data_updated.set()

    @staticmethod
    def _declare_type_enum(target: Dataset):
        d: typing.Dict[str, int] = dict()
        for v in _Type:
            d[v.name] = v.value
        return target.createEnumType(np.uint8, "event_t", d)

    def _write_events(self, target: Dataset) -> None:
        type_enum = self._declare_type_enum(target)

        time_var = netcdf_timeseries.time_coordinate(target)
        time_var.long_name = "time of event"

        type_var = target.createVariable("type", type_enum, ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(target, type_var)
        type_var.long_name = "event type"

        source_var = target.createVariable("source", str, ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(target, source_var)
        source_var.long_name = "source name"
        source_var.text_encoding = "UTF-8"

        message_var = target.createVariable("message", str, ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(target, message_var)
        message_var.long_name = "text description of the event"
        message_var.text_encoding = "UTF-8"

        auxiliary_var = target.createVariable("auxiliary_data", str, ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(target, auxiliary_var)
        auxiliary_var.long_name = "JSON encoded auxiliary data"
        auxiliary_var.text_encoding = "UTF-8"

        time_var[:] = [round(e.time * 1000.0) for e in self._events]
        type_var[:] = [e.type.value for e in self._events]
        for i in range(len(self._events)):
            e = self._events[i]
            source_var[i] = e.source
            message_var[i] = e.message
            auxiliary_var[i] = e.data or ""

    def _query_override(self, key: str) -> typing.Any:
        return self._override_config.get(key)

    def write_file(self, filename: str) -> None:
        root = Dataset(filename, 'w', format='NETCDF4')

        self._events.sort(key=lambda e: e.time)

        start_epoch: float = self._file_start
        end_epoch: typing.Optional[float] = self._file_end

        if len(self._events) > 0:
            if not start_epoch or start_epoch > self._events[0].time:
                start_epoch = self._events[0].time
            if not end_epoch or end_epoch < self._events[-1].time:
                end_epoch = self._events[-1].time

        event_log(root, self.station, start_epoch, end_epoch, override=self._query_override)

        if self.first_time:
            root.setncattr("acquisition_start_time", format_iso8601_time(self.first_time))

        append_history(root, "forge.acquisition.eventlog")

        self._write_events(root.createGroup("log"))

        root.close()

    def _flush_file(self):
        if self._data_updated:
            self._data_updated.clear()
        write_replace_file(str(self._active_output_file), str(self._working_directory), self.write_file)
        _LOGGER.debug("Event log flush completed")

    def _set_target_name(self):
        filetime = format_iso8601_time(time.time(), delimited=False)
        uid = b32encode(token_bytes(10)).decode('ascii')
        self._active_output_file = self._working_directory / f"{self.station.upper()}-LOG_a{filetime}_u{uid}.nc"
        _LOGGER.info(f"Event log output file set to {str(self._active_output_file)}")

    async def _advance_file(self):
        self._flush_file()

        source_file = self._active_output_file
        target_file = self._completed_directory / self._active_output_file.name

        self._set_target_name()
        self._events.clear()

        try:
            asyncio.get_event_loop().run_in_executor(None, shutil.move,
                                                     str(source_file), str(target_file))
            _LOGGER.debug(f"Moved completed event log file {source_file} to {target_file}")
        except OSError:
            _LOGGER.warning(f"Failed to relocate completed event log file {source_file} to {target_file}",
                            exc_info=True)

    async def run(self) -> None:
        self._data_updated = asyncio.Event()
        self._set_target_name()

        def next_interval(now: float, interval: float) -> float:
            return floor(now / interval) * interval + interval

        now = time.time()
        next_immediate_write = now + 60.0
        next_file = next_interval(now, self._file_duration)
        while True:
            if next_immediate_write <= now:
                maximum_sleep = next_file - now
                if maximum_sleep < 0.001:
                    maximum_sleep = 0.001
                try:
                    await wait_cancelable(self._data_updated.wait(), maximum_sleep)
                except asyncio.TimeoutError:
                    pass
            else:
                maximum_sleep = min(next_file, next_immediate_write) - now
                if maximum_sleep < 0.001:
                    maximum_sleep = 0.001
                await asyncio.sleep(maximum_sleep)

            now = time.time()
            if now >= next_file:
                self._file_end = now
                await asyncio.shield(self._advance_file())
                now = time.time()
                next_file = next_interval(now, self._file_duration)
                if next_immediate_write > now:
                    next_immediate_write = now + 60.0

                self._file_start = self._file_end
                self._file_end = now
            elif self._data_updated.is_set():
                self._file_end = now
                self._flush_file()
                now = time.time()
                next_immediate_write = now + 60.0

    async def shutdown(self):
        self._data_updated = None
        self._file_end = time.time()
        self._events.append(_Event(self._file_end, _Type.Info, "", "Acquisition system shutdown"))
        self._flush_file()


if __name__ == '__main__':
    import sys
    target_file = sys.argv[1]
    print("Writing data to", target_file)

    data = Log('bos', LayeredConfiguration())

    data.add_message("N61", {
        "type": "communications_established",
        "message": "Communications established",
    })
    data.add_message("_USER", {
        "type": "user",
        "author": "DCH",
        "message": "User message",
        "auxiliary": {"user_id": 1},
    })

    data.write_file(target_file)
