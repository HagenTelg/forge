import logging
import typing
import numpy as np
from math import isfinite, nan
from forge.data.flags import declare_flag
from forge.data.merge.extend import extend_selected
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


def wind_sector_contamination(
        data,
        *wind,
        contaminated_sector: typing.Iterable[typing.Union[float, typing.Tuple[float, float]]] = None,
        contaminated_minimum_speed: float = 0.0,
        extend_before_ms: int = 0,
        extend_after_ms: int = 0,
) -> None:
    data = SelectedData.ensure_data(data)
    wind = [SelectedData.ensure_data(w) for w in wind]
    data.append_history("forge.correction.windsectorcontamination")

    contaminated_sector = list(contaminated_sector) if contaminated_sector else []

    for system_flags in data.system_flags():
        bit = declare_flag(system_flags.variable, "data_contamination_wind_sector", 0x08)

        speed = np.full((system_flags.shape[0], ), nan, dtype=np.float64)
        for w in wind:
            add_values = w.get_input(system_flags, {"variable_id": "WS1?"}, error_when_missing=False).values
            apply = np.invert(np.isfinite(speed))
            speed[apply] = add_values[apply]

        direction = np.full((system_flags.shape[0], ), nan, dtype=np.float64)
        for w in wind:
            add_values = w.get_input(system_flags, {"variable_id": "WD1?"}, error_when_missing=False).values
            apply = np.invert(np.isfinite(direction))
            direction[apply] = add_values[apply]

        apply_bits = _find_wind_contaminated(speed, direction,
                                             contaminated_sector, contaminated_minimum_speed)
        apply_bits = extend_selected(apply_bits, system_flags.times, extend_before_ms, extend_after_ms)

        system_flags[apply_bits] = np.bitwise_or(system_flags[apply_bits], bit)
