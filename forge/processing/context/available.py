import typing
import datetime
import numpy as np
from abc import ABC, abstractmethod
from math import floor, ceil
from forge.timeparse import parse_time_argument
from forge.const import MAX_I64
from .data import SelectedData
from .selection import InstrumentSelection


class AvailableData(ABC):
    @staticmethod
    def _to_bounds_ms(
            start: typing.Optional[typing.Union[str, float, int, datetime.datetime]],
            end: typing.Optional[typing.Union[str, float, int, datetime.datetime]],
    ) -> typing.Tuple[int, int]:
        if start:
            if isinstance(start, str):
                start = parse_time_argument(start).timestamp()
            elif isinstance(start, int):
                start = int(start)
            else:
                try:
                    start = float(start)
                except (TypeError, ValueError):
                    start = start.timestamp()            
            start = int(floor(start * 1000))
        else:
            start = -MAX_I64
        if end:
            if isinstance(end, str):
                end = parse_time_argument(end).timestamp()
            elif isinstance(end, int):
                end = int(end)
            else:
                try:
                    end = float(end)
                except (TypeError, ValueError):
                    end = end.timestamp()            
            end = int(ceil(end * 1000))
        else:
            end = MAX_I64
        return start, end

    @staticmethod
    def _derive_output_times(
            selected: typing.Iterable[SelectedData],
            average_interval: typing.Optional[float],
            start: int, end: int,
            peer_times: bool = False,
    ) -> typing.Optional[np.ndarray]:
        output_times = None
        if peer_times or average_interval is None:
            from forge.data.merge.timealign import peer_output_time

            combine_times = []
            for i in selected:
                times = i.times
                if times.shape[0] == 0:
                    continue
                combine_times.append(times)

            if combine_times:
                output_times = peer_output_time(*combine_times)

        if output_times is None and average_interval:
            output_times = np.arange(
                start, end,
                round(average_interval * 1000.0),
                dtype=np.int64
            )

        return output_times

    @abstractmethod
    def select_instrument(
            self,
            instrument: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            *auxiliary: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            start: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            end: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            always_tuple: bool = False,
    ) -> typing.Iterator[typing.Union[SelectedData, typing.Tuple[SelectedData, ...]]]:
        pass

    @abstractmethod
    def select_multiple(
            self,
            *selected: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            start: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            end: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            always_tuple: bool = False,
    ) -> typing.Iterator[typing.Union[SelectedData, typing.Tuple[SelectedData, ...]]]:
        pass

    @abstractmethod
    def derive_output(
            self,
            instrument_id: str,
            *inputs: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            tags: typing.Optional[typing.Union[str, typing.Iterable[str]]] = None,
            start: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            end: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            peer_times: bool = False,
    ) -> typing.Iterator[typing.Tuple[SelectedData, ...]]:
        pass
