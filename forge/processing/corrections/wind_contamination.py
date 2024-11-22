import logging
import typing
import numpy as np
from math import isfinite, nan
from forge.data.flags import declare_flag
from ..context import SelectedData

_LOGGER = logging.getLogger(__name__)


def _find_wind_contaminated(
        speed: np.ndarray, direction: np.ndarray,
        contaminated_sector: typing.List[typing.Union[float, typing.Tuple[float, float]]] = None,
        contaminated_minimum_speed: float = 0.0,
) -> np.ndarray:
    assert speed.shape == direction.shape
    direction = direction % 360.0
    result = np.full(speed.shape, False, dtype=np.bool_)

    result[speed < contaminated_minimum_speed] = True

    def apply_sector(start: float, end: float) -> None:
        if isfinite(start):
            if isfinite(end):
                result[np.logical_and(direction >= start, direction <= end)] = True
            else:
                result[direction >= start] = True
        elif isfinite(end):
            result[direction <= end] = True
        else:
            result[:] = True

    i = 0
    while i < len(contaminated_sector):
        if isinstance(contaminated_sector[i], float) or isinstance(contaminated_sector[i], int):
            sector_start = contaminated_sector[i]
            i += 1
            if i < len(contaminated_sector):
                sector_end = contaminated_sector[i]
            else:
                sector_end = nan
            i += 1
        else:
            sector_start, sector_end = contaminated_sector[i]
            i += 1
        sector_start = float(sector_start)
        sector_end = float(sector_end)
        if sector_start < sector_end:
            apply_sector(sector_start, sector_end)
        else:
            apply_sector(sector_start, nan)
            apply_sector(nan, sector_end)

    return result


def _extend_contaminated(
        hit: np.ndarray,
        times: np.ndarray,
        extend_before_ms: int,
        extend_after_ms: int,
) -> np.ndarray:
    assert times.shape == hit.shape

    if extend_before_ms <= 0 and extend_after_ms <= 0:
        return hit

    origin_times = times[hit]
    begin_times = origin_times - extend_before_ms
    end_times = origin_times + extend_after_ms

    compare = times[:, None]
    return np.any((compare >= begin_times) & (compare <= end_times), axis=1)


def wind_sector_contamination(
        data,
        wind,
        contaminated_sector: typing.Iterable[typing.Union[float, typing.Tuple[float, float]]] = None,
        contaminated_minimum_speed: float = 0.0,
        extend_before_ms: int = 0,
        extend_after_ms: int = 0,
) -> None:
    data = SelectedData.ensure_data(data)
    wind = SelectedData.ensure_data(wind)
    data.append_history("forge.correction.windsectorcontamination")

    contaminated_sector = list(contaminated_sector) if contaminated_sector else []

    for system_flags in data.system_flags():
        bit = declare_flag(system_flags.variable, "data_contamination_wind_sector", 0x08)

        speed = wind.get_input(system_flags, {"variable_id": "WS1?"}, error_when_missing=False)
        direction = wind.get_input(system_flags, {"variable_id": "WD1?"}, error_when_missing=False)
        apply_bits = _find_wind_contaminated(speed.values, direction.values,
                                             contaminated_sector, contaminated_minimum_speed)
        apply_bits = _extend_contaminated(apply_bits, system_flags.times, extend_before_ms, extend_after_ms)

        system_flags[apply_bits] = np.bitwise_or(system_flags[apply_bits], bit)
