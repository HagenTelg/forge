import typing
import enum
from math import isfinite
from json import loads as from_json
from netCDF4 import Dataset
from forge.processing.instrument.lookup import instrument_data
from forge.dashboard.report.action import DashboardAction


class _EventType(enum.IntEnum):
    User = enum.auto()
    Info = enum.auto()
    CommunicationsEstablished = enum.auto()
    CommunicationsLost = enum.auto()
    Error = enum.auto()

    @staticmethod
    def assemble_lookup(datatype: typing.Dict[str, int]) -> typing.Dict[int, "_EventType"]:
        result: typing.Dict[int, "_EventType"] = dict()
        for name, value in datatype.items():
            result[value] = _EventType[name]
        return result


def _analyze_event_log(root: Dataset, target: DashboardAction) -> None:
    log = root.groups["log"]
    times = log.variables["time"]
    raw_types = log.variables["type"]

    event_type_convert = _EventType.assemble_lookup(raw_types.datatype.enum_dict)
    event_source = log.variables["source"]
    event_message = log.variables["message"]
    raw_auxiliary = log.variables["auxiliary_data"]

    for i in range(len(times)):
        time = float(times[i]) / 1000.0
        event_type = event_type_convert[int(raw_types[i])]
        source = str(event_source[i])
        message = str(event_message[i])
        data = str(raw_auxiliary[i])
        if data:
            data = from_json(data)
        else:
            data = None

        if event_type == _EventType.User:
            target.events.append(target.Event(
                'message-log',
                target.Severity.INFO,
                f"{source.replace(',', ' ')},{message}",
                occurred_at=time,
            ))
        elif event_type == _EventType.CommunicationsLost:
            target.events.append(target.Event(
                f'{source}-communications-lost',
                target.Severity.INFO,
                message,
                occurred_at=time,
            ))
        elif event_type == _EventType.CommunicationsEstablished:
            pass
        else:
            severity = target.Severity.INFO
            if event_type == _EventType.Error:
                severity = target.Severity.ERROR
            target.events.append(target.Event(
                'message-log',
                severity,
                f"{source.replace(',', ' ')},{message}",
                occurred_at=time,
            ))


def analyze_acquisition(station: str, root: Dataset, target: DashboardAction) -> None:
    tags = getattr(root, 'forge_tags', None)
    if tags == 'eventlog':
        return _analyze_event_log(root, target)

    instrument = root.instrument
    if not instrument:
        from forge.processing.instrument.default.dashboard import analyze_acquisition
        return analyze_acquisition(station, root, target)

    return instrument_data(root.instrument, 'dashboard', 'analyze_acquisition')(station, root, target)
