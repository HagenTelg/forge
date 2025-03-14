import typing
import asyncio
import logging
import enum
import re
import time
import numpy as np
import forge.data.structure.eventlog as netcdf_eventlog
from math import floor
from concurrent.futures import Executor
from netCDF4 import Dataset
from tempfile import NamedTemporaryFile
from forge.cpd3.identity import Identity
from forge.cpd3.variant import to_json
from forge.archive.client import event_log_file_name, event_log_lock_key
from forge.archive.client.connection import Connection, LockBackoff, LockDenied
from forge.data.structure import event_log
from forge.data.structure.history import append_history
from forge.data.merge.eventlog import MergeEventLog
from forge.data.structure.timeseries import file_id, date_created
from forge.formattime import format_iso8601_time

_LOGGER = logging.getLogger(__name__)


class Event:
    class Type(enum.IntEnum):
        User = 0,
        Info = 1,
        CommunicationsEstablished = 2,
        CommunicationsLost = 3,
        Error = 4,

        @classmethod
        def enum_dict(cls) -> typing.Dict[str, int]:
            result: typing.Dict[str, int] = dict()
            for v in cls:
                result[v.name] = v.value
            return result

    COMMUNICATIONS_LOST = re.compile(r".*comm.*(?:(?:dropped)|(?:lost)).*", re.IGNORECASE)
    COMMUNICATIONS_ESTABLISHED = re.compile(r".*comm.*(?:(?:established)|(?:gained)|(?:restore)).*", re.IGNORECASE)

    MESSAGE_LOG_MATCH: typing.Dict[re.Pattern, "Event.Type"] = {
        re.compile(r".*filter change.*", re.IGNORECASE): Type.Info,
        re.compile(r".*Lamp current too high.*", re.IGNORECASE): Type.Error,
        re.compile(r".*Impactor pressure.*", re.IGNORECASE): Type.Error,
        re.compile(r".*Pressure across the impactor.*", re.IGNORECASE): Type.Info,
        re.compile(r".*does not appear to be a white filter.*", re.IGNORECASE): Type.Error,
    }

    def __init__(self, identity: Identity, info: typing.Dict[str, typing.Any],
                 modified: typing.Optional[float] = None):
        self.time: float = identity.start
        assert self.time is not None
        self.modified: typing.Optional[float] = modified
        self.message: str = str(info.get("Text", ""))
        self.source = info.get("Source", "")
        if self.source == "EXTERNAL":
            self.source = info.get("Author", identity.station.upper())
            self.type = self.Type.User
        else:
            if self.COMMUNICATIONS_LOST.fullmatch(self.message):
                self.type = self.Type.CommunicationsLost
            elif self.COMMUNICATIONS_ESTABLISHED.fullmatch(self.message):
                self.type = self.Type.CommunicationsEstablished
            else:
                if info.get("ShowRealtime", False):
                    self.type = self.Type.Error
                else:
                    self.type = self.Type.Info
                for pattern, type in self.MESSAGE_LOG_MATCH.items():
                    if pattern.fullmatch(self.message):
                        self.type = type

        info = info.get("Information")
        if info:
            self.auxiliary_data: str = to_json(info, sort_keys=True)
        else:
            self.auxiliary_data: str = ""


def make_file(
        events: typing.List[Event], destination: str,
        station: str, start_epoch: float, end_epoch: float
) -> None:
    events.sort(key=lambda x: x.time)

    root = Dataset(destination, 'w', format='NETCDF4')
    try:
        event_log(root, station, start_epoch, end_epoch)
        append_history(root, "forge.cpd3.convert.eventlog")

        target = root.createGroup("log")

        type_enum = target.createEnumType(np.uint8, "event_t", Event.Type.enum_dict())

        time_var = netcdf_eventlog.event_time(target)
        type_var = netcdf_eventlog.event_type(target, type_enum)
        source_var = netcdf_eventlog.event_source(target)
        message_var = netcdf_eventlog.event_message(target)
        auxiliary_var = netcdf_eventlog.event_auxiliary(target)

        time_var[:] = [round(e.time * 1000.0) for e in events]
        type_var[:] = [e.type.value for e in events]
        for i in range(len(events)):
            e = events[i]
            source_var[i] = e.source
            message_var[i] = e.message
            auxiliary_var[i] = e.auxiliary_data
    finally:
        root.close()


async def write_day(
        connection: Connection,
        converted_events: typing.List[Event],
        station: str,
        start_of_day: float,
        end_of_day: float,
        incomplete_day: bool = False,
        netcdf_executor: Executor = None,
) -> None:
    assert int(floor(start_of_day / (24 * 60 * 60))) * 24 * 60 * 60 == start_of_day
    assert end_of_day > start_of_day
    assert (end_of_day - start_of_day) <= 24 * 60 * 60

    with NamedTemporaryFile(suffix=".nc") as incoming_events:
        if netcdf_executor is not None:
            await asyncio.get_event_loop().run_in_executor(
                netcdf_executor, make_file,
                converted_events, incoming_events.name, station, start_of_day, end_of_day
            )
        else:
            make_file(converted_events, incoming_events.name, station, start_of_day, end_of_day)

        backoff = LockBackoff()
        while True:
            try:
                async with connection.transaction(True):
                    archive_file_start = int(floor(start_of_day * 1000))
                    archive_file_end = archive_file_start + 24 * 60 * 60 * 1000
                    archive_file_name = event_log_file_name(station, start_of_day)

                    await connection.lock_write(event_log_lock_key(station), archive_file_start, archive_file_end)

                    file = Dataset(incoming_events.name, 'r')
                    with NamedTemporaryFile(suffix=".nc") as existing_file, NamedTemporaryFile(suffix=".nc") as merged_file:
                        merge = MergeEventLog()

                        if incomplete_day:
                            try:
                                await connection.read_file(archive_file_name, existing_file)
                                existing_file.flush()
                                existing_data = Dataset(existing_file.name, 'r')
                                merge.overlay(existing_data, int(floor(end_of_day * 1000)), archive_file_end)
                                _LOGGER.debug("Using existing data file %s", archive_file_name)
                            except FileNotFoundError:
                                _LOGGER.debug("No existing data for %s", archive_file_name)
                                existing_data = None
                        else:
                            existing_data = None

                        merge.overlay(file, archive_file_start, archive_file_end)

                        if netcdf_executor is not None:
                            result = await asyncio.get_event_loop().run_in_executor(
                                netcdf_executor, merge.execute, merged_file.name
                            )
                        else:
                            result = merge.execute(merged_file.name)
                        if existing_data is not None:
                            existing_data.close()
                            existing_data = None
                        file.close()

                        if result is None:
                            _LOGGER.debug("Removing empty event log %s", archive_file_name)
                            try:
                                await connection.remove_file(archive_file_name)
                            except FileNotFoundError:
                                pass
                            break

                        now = time.time()

                        latest_modified: typing.Optional[float] = None
                        for e in converted_events:
                            if not latest_modified or (e.modified and e.modified > latest_modified):
                                latest_modified = e.modified
                        date_created(result, latest_modified if latest_modified else now)

                        file_id(result, "LOG", archive_file_start / 1000.0, archive_file_end / 1000.0, now)
                        result.time_coverage_start = format_iso8601_time(archive_file_start / 1000.0)
                        result.time_coverage_end = format_iso8601_time(archive_file_end / 1000.0)

                        result.close()
                        result = None

                        merged_file.seek(0)
                        merged_file.flush()
                        await connection.write_file(archive_file_name, merged_file)
                        _LOGGER.debug("Sent updated file %s", archive_file_name)
                break
            except LockDenied as ld:
                _LOGGER.debug("Archive busy: %s", ld.status)
                await backoff()

