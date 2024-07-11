import typing
import numpy as np
from abc import ABC, abstractmethod
from json import loads as from_json
from netCDF4 import Dataset
from .selection import Selection


class Condition(ABC):
    def __init__(self, parameters: str):
        if parameters:
            self.parameters: typing.Dict[str, typing.Any] = from_json(parameters)
        else:
            self.parameters: typing.Dict[str, typing.Any] = dict()

    @staticmethod
    def from_code(code: str) -> typing.Callable[[str], "Condition"]:
        code = code.lower()
        if code == 'periodic':
            return Periodic
        elif code == 'threshold':
            return Threshold
        return Always

    @property
    def needs_prepare(self) -> bool:
        return False

    def prepare(self, root: Dataset, data: Dataset) -> None:
        pass

    @staticmethod
    def _time_selection(destination_times: np.ndarray,
                        edit_start: int, edit_end: int) -> typing.Optional[slice]:
        begin_index = int(np.searchsorted(destination_times, edit_start, side='left'))
        if begin_index >= destination_times.shape[0]:
            return None
        end_index = int(np.searchsorted(destination_times, edit_end, side='left'))
        if end_index <= begin_index:
            return None
        if begin_index == 0 and end_index == destination_times.shape[0]:
            return slice(None)
        return slice(begin_index, end_index)

    @abstractmethod
    def evaluate(self, destination_times: np.ndarray,
                 edit_start: int, edit_end: int) -> typing.Optional[typing.Union[slice, np.ndarray]]:
        pass


class Always(Condition):
    def evaluate(self, destination_times: np.ndarray,
                 edit_start: int, edit_end: int) -> typing.Optional[typing.Union[slice, np.ndarray]]:
        return self._time_selection(destination_times, edit_start, edit_end)


class Periodic(Condition):
    def __init__(self, parameters: str):
        super().__init__(parameters)

        interval = str(self.parameters['interval']).lower()
        if interval == 'day':
            self.interval = 24 * 60 * 60 * 1000
        else:
            self.interval = 60 * 60 * 1000

        division = str(self.parameters['division']).lower()
        if division == 'hour':
            self.division = 60 * 60 * 1000
        else:
            self.division = 60 * 1000

        if self.interval <= self.division:
            raise ValueError("Interval greater than division")

        self.moments = np.array([int(i) for i in self.parameters['moments']], dtype=np.int64)

    def evaluate(self, destination_times: np.ndarray,
                 edit_start: int, edit_end: int) -> typing.Optional[typing.Union[slice, np.ndarray]]:
        time_selection = self._time_selection(destination_times, edit_start, edit_end)
        if time_selection is None:
            return None

        moment_numbers = np.empty_like(destination_times, dtype=np.int64)
        np.floor((destination_times % self.interval) / self.division, out=moment_numbers, casting='unsafe')
        hit = np.full(destination_times.shape, False, dtype=bool)
        hit[time_selection] = np.isin(moment_numbers[time_selection], self.moments)
        return hit


class Threshold(Condition):
    def __init__(self, parameters: str):
        super().__init__(parameters)
        self.threshold_value = Selection(self.parameters["selection"])
        self.lower = self.parameters.get('lower')
        if self.lower is not None:
            self.lower = float(self.lower)
        self.upper = self.parameters.get('upper')
        if self.upper is not None:
            self.upper = float(self.upper)

        self._active: typing.List[typing.Tuple[np.ndarray, np.ndarray]] = list()

    @property
    def needs_prepare(self) -> bool:
        return True

    def prepare(self, root: Dataset, data: Dataset) -> None:
        times = None
        total_hit = None
        for var, value_select in self.threshold_value.select_single(root, data):
            if times is None:
                from .directives import data_times
                times = data_times(data)
                if times is None:
                    return

            values: np.ndarray = var[:].data[(slice(None), *value_select)]
            if self.lower is not None:
                if self.upper is not None:
                    hit = np.all((
                        values > self.lower,
                        values < self.upper
                    ), axis=0)
                else:
                    hit = values > self.lower
            elif self.upper is not None:
                hit = values < self.upper
            else:
                hit = np.isfinite(values)

            if not np.any(hit):
                continue

            if total_hit is None:
                total_hit = hit
            else:
                total_hit &= hit

        if total_hit is not None:
            self._active.append((times, total_hit))

    def evaluate(self, destination_times: np.ndarray,
                 edit_start: int, edit_end: int) -> typing.Optional[typing.Union[slice, np.ndarray]]:
        if not self._active:
            return None
        time_selection = self._time_selection(destination_times, edit_start, edit_end)
        if time_selection is None:
            return None
        selected_times = destination_times[time_selection]

        hit = np.full(destination_times.shape, False, dtype=bool)

        from forge.data.merge.timealign import align_latest
        for trigger_times, trigger_hit in self._active:
            trigger_bounds = self._time_selection(trigger_times, edit_start, edit_end)
            if trigger_bounds is None:
                continue
            trigger_aligned = align_latest(selected_times, trigger_times[time_selection], trigger_hit[time_selection])
            hit[time_selection] = np.any((
                hit[time_selection],
                trigger_aligned
            ), axis=0)

        if not np.any(hit):
            return None

        return hit
