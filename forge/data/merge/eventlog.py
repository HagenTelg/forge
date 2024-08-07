import typing
import logging
import netCDF4
import numpy as np
import forge.data.structure.eventlog as netcdf_eventlog
from math import nan
from .enum import MergeEnum

if typing.TYPE_CHECKING:
    from pathlib import Path

_LOGGER = logging.getLogger(__name__)


def _merge_attrs(source: typing.Union[netCDF4.Dataset, netCDF4.Variable],
                 destination: typing.Union[netCDF4.Dataset, netCDF4.Variable]) -> None:
    for attr in source.ncattrs():
        if attr.startswith('_'):
            continue
        if attr in destination.ncattrs():
            continue
        destination.setncattr(attr, source.getncattr(attr))


class _EventArrays:
    def __init__(self, time: netCDF4.Variable, event_type: netCDF4.Variable, source: typing.Optional[netCDF4.Variable],
                 message: typing.Optional[netCDF4.Variable], auxiliary: typing.Optional[netCDF4.Variable],
                 not_before_ms: typing.Optional[int] = None,
                 not_after_ms: typing.Optional[int] = None):
        self.time: np.ndarray = time[:]
        self.event_type: np.ndarray = event_type[:]
        if source is not None and source.dimensions == ("time", ):
            self.source: np.ndarray = source[:]
        else:
            self.source: np.ndarray = np.full(self.time.shape, "", dtype=str)
        if message is not None and message.dimensions == ("time", ):
            self.message: np.ndarray = message[:]
        else:
            self.message: np.ndarray = np.full(self.time.shape, "", dtype=str)
        if auxiliary is not None and auxiliary.dimensions == ("time", ):
            self.auxiliary: np.ndarray = auxiliary[:]
        else:
            self.auxiliary: np.ndarray = np.full(self.time.shape, "", dtype=str)

        filter_selection = None
        if not_before_ms and not_after_ms:
            filter_selection = np.all((self.time >= not_before_ms, self.time < not_after_ms), axis=0)
        elif not_before_ms:
            filter_selection = self.time >= not_before_ms
        elif not_after_ms:
            filter_selection = self.time < not_after_ms

        if filter_selection is not None:
            self.time = self.time[filter_selection]
            self.event_type = self.event_type[filter_selection]
            self.source = self.source[filter_selection]
            self.message = self.message[filter_selection]
            self.auxiliary = self.auxiliary[filter_selection]


class MergeEventLog:
    class _Source:
        def __init__(self, contents: netCDF4.Dataset,
                     not_before_ms: typing.Optional[int] = None,
                     not_after_ms: typing.Optional[int] = None):
            self.root = contents
            self.not_before_ms: typing.Optional[int] = not_before_ms
            self.not_after_ms: typing.Optional[int] = not_after_ms

    def __init__(self):
        self._sources: typing.List["MergeEventLog._Source"] = list()

    def overlay(self, contents: netCDF4.Dataset,
                not_before_ms: typing.Optional[int] = None,
                not_after_ms: typing.Optional[int] = None) -> None:
        source = self._Source(contents, not_before_ms, not_after_ms)
        self._sources.append(source)

    def append(self, contents: netCDF4.Dataset) -> None:
        self.overlay(contents)

    def execute(self, output: typing.Union[str, "Path"]) -> typing.Optional[netCDF4.Dataset]:
        event_t = MergeEnum("event_t")
        streams: typing.List[typing.Tuple[MergeEventLog._Source, _EventArrays]] = list()
        for source in reversed(self._sources):
            source_group = source.root.groups.get("log")
            if source_group is None:
                _LOGGER.warning("No log group present in input file")
                continue
            if len(source.root.groups) != 1:
                _LOGGER.warning("Additional groups ignored in input file")

            source_time = source_group.variables.get("time")
            if source_time is None or source_time.dimensions != ("time", ):
                _LOGGER.warning("No time present in log group")
                continue
            source_type = source_group.variables.get("type")
            if source_type is None or source_type.dimensions != ("time", ):
                _LOGGER.warning("No event type present in log group")
                continue

            event_arrays = _EventArrays(
                source_time,
                source_type,
                source_group.variables.get("source"),
                source_group.variables.get("message"),
                source_group.variables.get("auxiliary_data"),
                not_before_ms=source.not_before_ms,
                not_after_ms=source.not_after_ms,
            )
            if event_arrays.time.shape == (0,):
                continue

            source_event_t = source_group.enumtypes.get("event_t")
            if source_event_t is None:
                _LOGGER.warning("No event type definition in inpout file")
            else:
                event_t.incorporate_structure(source_event_t)

            streams.append((source, event_arrays))

        if not streams:
            return None

        output = netCDF4.Dataset(str(output), 'w', format='NETCDF4')

        for source, data in streams:
            _merge_attrs(source.root, output)

            for name, invar in source.root.variables.items():
                outvar = output.variables.get(name)
                if outvar is None:
                    fill_value = False
                    try:
                        fill_value = invar._FillValue
                    except AttributeError:
                        pass
                    outvar = output.createVariable(name, invar.dtype, (), fill_value=fill_value)
                    outvar[0] = invar[0]

                _merge_attrs(invar, outvar)

        log_group: netCDF4.Group = output.createGroup("log")
        event_t.declare_structure(log_group)

        events_record = np.rec.fromarrays((
            np.concatenate([
                s[1].time.astype(np.int64, casting='unsafe', copy=False) for s in streams
            ]),
            np.concatenate([
                event_t
                    .apply(s[0].root.groups["log"].variables["type"], s[1].event_type, copy=False)
                    .astype(event_t.storage_dtype, casting='unsafe', copy=False)
                for s in streams
            ]),
            np.concatenate([
                s[1].source.astype(str, casting='unsafe', copy=False) for s in streams
            ]),
            np.concatenate([
                s[1].message.astype(str, casting='unsafe', copy=False) for s in streams
            ]),
            np.concatenate([
                s[1].auxiliary.astype(str, casting='unsafe', copy=False) for s in streams
            ]),
        ), names=("time", "type", "source", "message", "auxiliary"))

        # De-duplicate and sort
        events_record = np.unique(events_record)

        time_var = netcdf_eventlog.event_time(log_group)
        time_var[:] = events_record.time
        type_var = netcdf_eventlog.event_type(log_group, event_t.dtype)
        type_var[:] = events_record.type
        source_var = netcdf_eventlog.event_source(log_group)
        source_var[:] = events_record.source
        message_var = netcdf_eventlog.event_message(log_group)
        message_var[:] = events_record.message
        auxiliary_var = netcdf_eventlog.event_auxiliary(log_group)
        auxiliary_var[:] = events_record.auxiliary

        return output
