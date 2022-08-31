import typing
import enum
import re
from json import loads as from_json
from netCDF4 import Dataset
from forge.cpd3.identity import Identity, Name


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


_communications_lost_message = re.compile(r".*comm.*(?:(?:dropped)|(?:lost)).*", re.IGNORECASE)


def convert_event_log(station: str, root: Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    result_events: typing.List[typing.Tuple[Identity, typing.Any]] = list()
    result_name = Name(station, "events", "acquisition")

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

        output_event: typing.Dict[str, typing.Any] = dict()
        if data:
            output_event["Information"] = data

        if event_type == _EventType.User:
            output_event["Text"] = message
            output_event["Author"] = source
            output_event["Source"] = "EXTERNAL"
        elif event_type == _EventType.CommunicationsEstablished:
            output_event["Text"] = message
            if source:
                output_event["Source"] = source
        elif event_type == _EventType.CommunicationsLost:
            output_event["ShowRealtime"] = True
            if not _communications_lost_message.search(message):
                message = "Communications lost: " + message
            output_event["Text"] = message
            if source:
                output_event["Source"] = source
        else:
            output_event["Text"] = message
            if source:
                output_event["Source"] = source
            if event_type == _EventType.Error:
                output_event["ShowRealtime"] = True

        result_events.append((Identity(name=result_name, start=time, end=time), output_event))

    return result_events
