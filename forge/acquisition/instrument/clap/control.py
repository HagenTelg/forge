import typing
import asyncio
import logging
import enum
import time
from collections import deque
from math import isfinite, nan
from statistics import mean, stdev, StatisticsError
from bisect import bisect_left
from forge.tasks import wait_cancelable
from ..base import BaseBusInterface

if typing.TYPE_CHECKING:
    from .instrument import Instrument


_LOGGER = logging.getLogger(__name__)


@enum.unique
class MeasurementState(enum.IntEnum):
    Normal = 0
    BypassDiscard = 1
    SpotNormalize = 2
    SpotDiscard = 3
    FilterChange = 4
    FilterBaseline = 5
    WhiteFilterChange = 6
    WhiteFilterBaseline = 7
    NeedFilterChange = 8


class ArrayAverage:
    def __init__(self):
        self.sum: typing.List[float] = list()
        self.count: typing.List[int] = list()

    def clear(self) -> None:
        self.sum.clear()
        self.count.clear()

    def mean(self) -> typing.List[float]:
        result: typing.List[float] = list()
        for i in range(len(self.sum)):
            s = self.sum[i]
            c = self.count[i]
            if not c:
                result.append(nan)
            else:
                result.append(s / c)
        return result

    def add_index(self, index: int, value: float) -> None:
        while index >= len(self.sum):
            self.sum.append(0)
            self.count.append(0)
        if isfinite(value):
            self.sum[index] += value
            self.count[index] += 1

    def add(self, values: typing.List) -> None:
        for i in range(len(values)):
            v = float(values[i])
            self.add_index(i, v)


class BoxcarArrayAverage:
    def __init__(self):
        self.values: typing.List[typing.Deque[float]] = list()
        self.times: typing.Deque[float] = deque()

    def clear(self) -> None:
        self.values.clear()
        self.times.clear()

    def mean(self) -> typing.List[float]:
        result: typing.List[float] = list()
        for values in self.values:
            try:
                m = mean(filter(lambda v: isfinite(v), values))
            except StatisticsError:
                m = nan
            result.append(m)
        return result

    def stddev(self) -> typing.List[float]:
        result: typing.List[float] = list()
        for values in self.values:
            try:
                m = stdev(filter(lambda v: isfinite(v), values))
            except StatisticsError:
                m = nan
            result.append(m)
        return result

    def rsd(self) -> typing.List[float]:
        m = self.mean()
        s = self.stddev()
        result: typing.List[float] = list()
        for i in range(len(m)):
            mi = m[i]
            si = s[i]
            if not isfinite(mi) or not isfinite(si):
                result.append(0.0)
            elif mi == 0.0:
                result.append(abs(si))
            else:
                result.append(abs(si / mi))
        return result

    def add(self, values: typing.List, now: float = None) -> None:
        if now is None:
            now = time.monotonic()
        self.times.append(now)
        for i in range(len(values)):
            v = float(values[i])
            while i >= len(self.values):
                self.values.append(deque())
            if isfinite(v):
                self.values[i].append(v)
            else:
                self.values[i].append(nan)

    def discard_old(self, age: float, now: float = None) -> None:
        if len(self.times) <= 1:
            return
        if now is None:
            now = time.monotonic()
        discard_time = now - age
        discard_n = bisect_left(self.times, discard_time)
        if not discard_n:
            return
        del self.times[:discard_n]
        for v in self.values:
            del v[:discard_n]

    @property
    def total_seconds(self) -> float:
        if len(self.times) < 2:
            return 0.0
        return self.times[-1] - self.times[0]


class Control:
    def __init__(self, instrument: "Instrument"):
        self.instrument = instrument

        self._bypass_discard: float = float(instrument.context.config.get('BYPASS_DISCARD', default=30.0))
        self._advance_transmittance: typing.List[float] = instrument.context.config.get('ADVANCE_TRANSMITTANCE',
                                                                                        default=[0.5, 0.7, 0.5])

        self._filter_baseline_seconds: float = float(instrument.context.config.get('CHANGE.FILTER.TIME', default=90.0))
        self._spot_normalize_seconds: float = float(instrument.context.config.get('CHANGE.SPOT.TIME', default=30.0))
        self._spot_normalize_discard: float = float(instrument.context.config.get('CHANGE.SPOT.DISCARD', default=8.0))
        self._filter_change_timeout: float = float(instrument.context.config.get('CHANGE.FILTER.END_TIMEOUT',
                                                                                 default=30 * 60.0))

        self._enable_autodetect_start: bool = True
        self._enable_autodetect_end: bool = True
        autodetect = instrument.context.config.get('AUTODETECT')
        if autodetect is not None:
            if isinstance(autodetect, str):
                autodetect = autodetect.lower()
                if autodetect == 'start':
                    self._enable_autodetect_end = False
                elif autodetect == 'end':
                    self._enable_autodetect_start = False
                elif autodetect != 'both':
                    self._enable_autodetect_end = False
                    self._enable_autodetect_start = False
            elif not bool(autodetect):
                self._enable_autodetect_end = False
                self._enable_autodetect_start = False

        self._autodetect_minimum_seconds: float = float(instrument.context.config.get(
            'CHANGE.FILTER.MINIMUM_TIME', default=min(self._filter_baseline_seconds * 0.5, 30.0)))
        self._autodetect_rsd: float = float(instrument.context.config.get('CHANGE.FILTER.RSD', default=0.001))
        self._autodetect_dark_limit: float = float(instrument.context.config.get('CHANGE.FILTER.DARK',
                                                                                 default=1000.0))
        self._autodetect_intensity_limit: float = float(instrument.context.config.get(
            'CHANGE.FILTER.INTENSITY', default=self._autodetect_dark_limit))
        self._verify_white_band: float = float(instrument.context.config.get('CHANGE.FILTER.WHITE_BAND', default=0.9))
        self._autodetect_white_band: float = float(instrument.context.config.get('CHANGE.FILTER.AUTODETECT_WHITE_BAND',
                                                                                 default=self._verify_white_band))
        self._autodetect_start_required_triggers: int = int(instrument.context.config.get(
            'CHANGE.FILTER.START_TRIGGERS', default=2))

        self._command_filter_change: typing.Optional[bool] = None
        self._command_filter_change_white: bool = False

        self._spot_normalization_In = ArrayAverage()
        self._filter_baseline_ID = BoxcarArrayAverage()
        self._filter_baseline_I: typing.List[BoxcarArrayAverage] = list()
        self._filter_baseline_In: typing.List[BoxcarArrayAverage] = list()
        for _ in range(len(self.instrument.data_I_wavelength)):
            self._filter_baseline_I.append(BoxcarArrayAverage())
            self._filter_baseline_In.append(BoxcarArrayAverage())

        self._state: MeasurementState = MeasurementState.NeedFilterChange
        self._state_begin_time: float = time.time()
        self._serial_number: typing.Optional[str] = None
        self._filter_id: typing.Optional[int] = None
        self._filter_was_white: bool = True
        self._elapsed_seconds: typing.Optional[int] = None
        self._measurement_spot: int = 1
        self._shutdown_time: float = time.time()

        existing, _ = instrument.context.persistent.load('control_state')
        if isinstance(existing, dict):
            self._state = MeasurementState(existing['state'])
            self._state_begin_time = float(existing['state_begin_time_ms']) / 1000.0
            self._serial_number = existing.get('serial_number')
            self._filter_id = existing.get('filter_id')
            if self._filter_id:
                self._filter_id = int(self._filter_id)
            self._filter_was_white = existing.get('filter_was_white')
            self._elapsed_seconds = existing.get('elapsed_seconds')
            if self._elapsed_seconds:
                self._elapsed_seconds = int(self._elapsed_seconds)
            self._measurement_spot = int(existing['measurement_spot'])
            self._shutdown_time = float(existing['shutdown_time_ms']) / 1000.0

        self.instrument.context.bus.connect_command('spot_advance', self._command_spot_advance)
        self.instrument.context.bus.connect_command('filter_change_start', self._command_filter_change_start)
        self.instrument.context.bus.connect_command('filter_change_end', self._command_filter_change_end)
        self.instrument.context.bus.connect_command('white_filter_change', self._command_white_filter_change)

    def _serialize_state(self) -> typing.Dict[str, typing.Any]:
        state = {
            'state': self._state.value,
            'state_begin_time_ms': int(self._state_begin_time * 1000.0),
            'filter_was_white': self._filter_was_white,
            'measurement_spot': self._measurement_spot,
            'shutdown_time_ms': int(self._shutdown_time * 1000.0),
        }
        if self._serial_number:
            state['serial_number'] = self._serial_number
        if self._filter_id:
            state['filter_id'] = self._filter_id
        if self._elapsed_seconds:
            state['elapsed_seconds'] = self._elapsed_seconds
        return state

    async def _save_state(self) -> None:
        await self.instrument.context.persistent.save('control_state', self._serialize_state(), None)

    def advance_spot(self):
        if self._state not in (MeasurementState.Normal, MeasurementState.BypassDiscard,
                               MeasurementState.SpotNormalize, MeasurementState.SpotDiscard):
            _LOGGER.debug("Not sampling a spot, ignored spot advance")
            return
        if self._measurement_spot >= 8:
            _LOGGER.debug("On final spot, ignored spot advance")
            return

        self._measurement_spot += 1
        self._state = MeasurementState.SpotDiscard
        self._state_begin_time = time.time()
        self._clear_spot_normalization_accumulator()

    def _command_spot_advance(self, _) -> None:
        _LOGGER.debug("Received spot advance command")
        self.advance_spot()

    def _command_filter_change_start(self, _) -> None:
        _LOGGER.debug("Received filter change start command")
        self._command_filter_change = True
        self._command_filter_change_white = False

    def _command_filter_change_end(self, _) -> None:
        _LOGGER.debug("Received filter change end command")
        self._command_filter_change = False

    def _command_white_filter_change(self, _) -> None:
        _LOGGER.debug("Received white filter change command")
        self._command_filter_change = True
        self._command_filter_change_white = True

    @property
    def is_changing(self) -> bool:
        return self._state in (MeasurementState.FilterChange, MeasurementState.WhiteFilterChange,
                               MeasurementState.NeedFilterChange)

    @property
    def is_air_flow_enabled(self) -> bool:
        if self.instrument.context.bus.bypassed:
            return False
        return self._state in (MeasurementState.Normal, MeasurementState.BypassDiscard,
                               MeasurementState.SpotNormalize, MeasurementState.SpotDiscard)

    @property
    def have_white_filter(self) -> bool:
        for c in self.instrument.data_Iinw0_wavelength:
            if not c.value:
                return False
            for wl in c.value:
                if wl is None or not isfinite(wl):
                    return False
        return True

    @property
    def active_spot_number(self) -> int:
        return self._measurement_spot

    @property
    def intensities_valid(self) -> bool:
        return self._state in (MeasurementState.Normal, MeasurementState.BypassDiscard,
                               MeasurementState.SpotNormalize, MeasurementState.SpotDiscard)

    @property
    def path_length_valid(self) -> bool:
        if not self.is_air_flow_enabled:
            return False
        return self._state in (MeasurementState.Normal, MeasurementState.BypassDiscard)

    @property
    def transmittance_valid(self) -> bool:
        return self._state in (MeasurementState.Normal, MeasurementState.BypassDiscard)

    @property
    def absorption_valid(self) -> bool:
        return self._state == MeasurementState.Normal

    @property
    def absorption_logged(self) -> bool:
        return not self.instrument.context.bus.bypassed

    def apply_serial_number(self, serial_number: str) -> None:
        prior_serial_number = self._serial_number
        self._serial_number = serial_number
        if not prior_serial_number or prior_serial_number == serial_number:
            return

        self._state = MeasurementState.NeedFilterChange
        for c in self.instrument.data_Iin0_wavelength:
            c(None, oneshot=True)
        for c in self.instrument.data_Iinw0_wavelength:
            c(None, oneshot=True)

        self.instrument.context.bus.log("Serial number changed, a white filter change is required", {
            "old_serial_number": prior_serial_number,
            "new_serial_number": serial_number,
        }, type=BaseBusInterface.LogType.INFO)
        _LOGGER.info("Serial number changed, filter state discarded")

    async def communications_established(self,  state: "Instrument.InstrumentState") -> "Instrument.InstrumentState":
        if self._state in (MeasurementState.WhiteFilterChange, MeasurementState.WhiteFilterBaseline):
            for c in self.instrument.data_Iin0_wavelength:
                c(None, oneshot=True)
            for c in self.instrument.data_Iinw0_wavelength:
                c(None, oneshot=True)
        elif self._state in (MeasurementState.FilterChange, MeasurementState.FilterBaseline):
            for c in self.instrument.data_Iin0_wavelength:
                c(None, oneshot=True)

        # We do not take any action in these cases, because the most common cause is just that the CLAP experienced
        # a power glitch and reset, so we just force it back to the correct state.
        if self._filter_id and state.Ff != self._filter_id:
            _LOGGER.info(f"Filter ID changed from expected {self._filter_id} to {state.Ff}")
            self._filter_id = state.Ff
        elif self._elapsed_seconds and state.elapsed_seconds < self._elapsed_seconds:
            _LOGGER.info(f"Filter elapsed seconds went backwards from {self._elapsed_seconds} to {state.elapsed_seconds}")
            self._elapsed_seconds = state.elapsed_seconds

        if self.is_changing:
            if not state.is_changing and self.instrument.writer:
                state = await wait_cancelable(self.instrument.apply_instrument_command(
                    b"stop\r", lambda s: s.is_changing), 10.0)
                _LOGGER.debug("Putting instrument into filter change mode due to startup changing")
        elif not self.is_air_flow_enabled:
            if state.Fn != 0 and self.instrument.writer:
                state = await wait_cancelable(self.instrument.apply_instrument_command(
                    b"spot=0\r", lambda s: s.Fn == 0), 10.0)
                _LOGGER.debug("Changing to spot zero due to no flow mode")
        else:
            current_spot = state.Fn
            target_spot = self.active_spot_number
            if current_spot != target_spot and self.instrument.writer:
                state = await wait_cancelable(self.instrument.apply_instrument_command(
                    b"spot=%d\r" % target_spot, lambda s: s.Fn == target_spot), 10.0)
                _LOGGER.debug(f"Changed to spot {target_spot} from {current_spot} during start communications")

        self._filter_id = state.Ff
        self._elapsed_seconds = state.elapsed_seconds
        return state

    def _clear_spot_normalization_accumulator(self) -> None:
        self._spot_normalization_In.clear()

    def _spot_normalization_accumulate(self) -> None:
        sample_index, reference_index = self.instrument.active_spot_index(self.active_spot_number)

        for widx in range(len(self.instrument.data_Ip_wavelength)):
            Ip = float(self.instrument.data_I_wavelength[widx][sample_index])
            If = float(self.instrument.data_I_wavelength[widx][reference_index])

            if isfinite(Ip) and isfinite(If) and If != 0.0:
                In = Ip / If
            else:
                In = nan
            self._spot_normalization_In.add_index(widx, In)

    def _spot_normalization_complete(self) -> None:
        self.instrument.data_In0(self._spot_normalization_In.mean(), oneshot=True)
        self._clear_spot_normalization_accumulator()

    def _clear_filter_baseline_accumulator(self) -> None:
        self._filter_baseline_ID.clear()
        for c in self._filter_baseline_I:
            c.clear()
        for c in self._filter_baseline_In:
            c.clear()

    def _filter_baseline_accumulate(self) -> None:
        age = self._filter_baseline_seconds
        now = time.monotonic()
        self._filter_baseline_ID.discard_old(age, now)
        for c in self._filter_baseline_I:
            c.discard_old(age, now)
        for c in self._filter_baseline_In:
            c.discard_old(age, now)

        self._filter_baseline_ID.add(self.instrument.data_ID.value, now)
        for widx in range(len(self.instrument.data_I_wavelength)):
            I = self.instrument.data_I_wavelength[widx].value
            self._filter_baseline_I[widx].add(I, now)

            In: typing.List[float] = list()
            for i in range(8):
                sample_index, reference_index = self.instrument.active_spot_index(i+1)

                Ip = float(I[sample_index])
                If = float(I[reference_index])
                if isfinite(Ip) and isfinite(If) and If != 0.0:
                    In.append(Ip / If)
                else:
                    In.append(nan)
            self._filter_baseline_In[widx].add(In, now)

    @staticmethod
    def _in_band(base: float, check: float, band_scale: float) -> bool:
        if not isfinite(base) or not isfinite(check) or not isfinite(band_scale):
            return True
        if base <= 0.0 or check <= 0.0 or band_scale <= 0.0:
            return True
        band_scale += 1.0
        if check < base / band_scale:
            return False
        if check > base * band_scale:
            return False
        return True

    def _filter_baseline_complete(self, state: "Instrument.InstrumentState", white: bool = False) -> None:
        for widx in range(len(self.instrument.data_Ip_wavelength)):
            Iin = self._filter_baseline_In[widx].mean()
            self.instrument.data_Iin0_wavelength[widx](Iin, oneshot=True)
            if white:
                self.instrument.data_Iinw0_wavelength[widx](Iin, oneshot=True)
        self._clear_filter_baseline_accumulator()

        if white or not self.have_white_filter:
            self._filter_was_white = True
        else:
            def was_white() -> bool:
                if self._verify_white_band <= 0.0:
                    return True
                for widx in range(len(self.instrument.data_Iin0_wavelength)):
                    In = self.instrument.data_Iin0_wavelength[widx].value
                    Inw = self.instrument.data_Iinw0_wavelength[widx].value
                    if not In or not Inw:
                        return True
                    for cidx in range(min(len(In), len(Inw))):
                        if not self._in_band(Inw[cidx], In[cidx], self._verify_white_band):
                            _LOGGER.info(f"Filter baseline channel {widx}:{cidx} is not white")
                            return False
                return True
            self._filter_was_white = was_white()
            if not self._filter_was_white:
                self.instrument.context.bus.log("Filter baseline established on what does not appear to be a white filter",
                                                self._filter_change_end_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.ERROR)

    def _autodetect_filter_change_end(self) -> bool:
        if not self._enable_autodetect_end:
            return False
        required_seconds = max(self._autodetect_minimum_seconds, 0.0)

        if self._filter_baseline_ID.total_seconds <= required_seconds:
            return False
        if self._autodetect_dark_limit > 0.0:
            # Require dark channels to be dark (lid on)
            ID = self._filter_baseline_ID.mean()
            for v in ID:
                if abs(v) > self._autodetect_dark_limit:
                    return False

        for check in self._filter_baseline_I:
            if check.total_seconds <= required_seconds:
                return False
            if self._autodetect_intensity_limit > 0.0:
                # Require illuminated channels to be lit (LEDs on)
                I = check.mean()
                for v in I:
                    if v < self._autodetect_intensity_limit:
                        return False

        for widx in range(len(self._filter_baseline_In)):
            check = self._filter_baseline_In[widx]
            if check.total_seconds <= required_seconds:
                return False
            if self._autodetect_rsd > 0.0:
                # Require normalized intensities to be stable (lid on for a while)
                rsd = check.rsd()
                for v in rsd:
                    if v > self._autodetect_rsd:
                        return False
            if self._autodetect_white_band > 0.0:
                white = self.instrument.data_Iinw0_wavelength[widx].value
                if white:
                    In_current = check.mean()
                    for cidx in range(min(len(white), len(In_current))):
                        if not self._in_band(white[cidx], In_current[cidx], self._autodetect_white_band):
                            return False

        return True

    def _autodetect_filter_change_start(self) -> bool:
        if not self._enable_autodetect_start:
            return False
        if self._autodetect_start_required_triggers <= 0.0:
            return False
        detected_triggers = 0

        if self._autodetect_dark_limit > 0.0 and self.instrument.data_ID.value:
            # Dark channels too high (lid off)
            for v in self.instrument.data_ID.value:
                if abs(v) > self._autodetect_dark_limit:
                    detected_triggers += 1

        if self._autodetect_intensity_limit > 0.0:
            # Illuminated channel too low (lid not secure)
            for wl in self.instrument.data_I_wavelength:
                if not wl.value:
                    continue
                for v in wl.value:
                    if v < self._autodetect_dark_limit:
                        detected_triggers += 1

        return detected_triggers > self._autodetect_start_required_triggers

    def _filter_change_start_auxiliary_data(self, state: "Instrument.InstrumentState") -> typing.Dict[str, typing.Any]:
        result: typing.Dict[str, typing.Any] = {
            'required_triggers': self._autodetect_start_required_triggers,
            'dark_limit': self._autodetect_dark_limit,
            'intensity_limit': self._autodetect_intensity_limit,
            'ID': self.instrument.data_ID.value,
            'is_changing': state.is_changing,
            'Ff': state.Ff,
        }
        for widx in range(len(self.instrument.WAVELENGTHS)):
            code = self.instrument.WAVELENGTHS[widx][1]
            result['I' + code] = self.instrument.data_I_wavelength[widx].value
        return result

    def _filter_change_end_auxiliary_data(self, state: "Instrument.InstrumentState") -> typing.Dict[str, typing.Any]:
        result: typing.Dict[str, typing.Any] = {
            'required_seconds': self._autodetect_minimum_seconds,
            'available_seconds': self._filter_baseline_ID.total_seconds,
            'dark_limit': self._autodetect_dark_limit,
            'intensity_limit': self._autodetect_intensity_limit,
            'rsd': self._autodetect_rsd,
            'white_band': self._autodetect_white_band,
            'ID': self._filter_baseline_ID.mean(),
            'is_changing': state.is_changing,
            'Ff': state.Ff,
        }
        for widx in range(len(self.instrument.WAVELENGTHS)):
            code = self.instrument.WAVELENGTHS[widx][1]
            result['I' + code] = self._filter_baseline_I[widx].mean()
            result['In' + code] = self._filter_baseline_In[widx].mean()
            result['Ing' + code] = self._filter_baseline_I[widx].rsd()
            result['Inw' + code] = self.instrument.data_Iinw0_wavelength[widx].value
        return result

    async def process(self, state: "Instrument.InstrumentState") -> "Instrument.InstrumentState":
        async def handle_filter_change_start() -> bool:
            nonlocal state
            command_change = self._command_filter_change
            self._command_filter_change = None

            if state.is_changing:
                self.instrument.context.bus.log("Instrument initiated filter change started",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.INFO)
                _LOGGER.info("Detected instrument initiated filter change start")
            elif self._autodetect_filter_change_start():
                self.instrument.context.bus.log("Auto-detected filter change started",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.INFO)
                _LOGGER.info("Auto-detected filter change start")
            elif command_change is not None and command_change:
                self.instrument.context.bus.log(f"Manual {self._command_filter_change_white and 'white ' or ''}filter change started",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.INFO)
                _LOGGER.info("Processed manual filter change command")
            else:
                return False

            if command_change is not None and command_change and self._command_filter_change_white:
                self._state = MeasurementState.WhiteFilterChange
            else:
                self._state = MeasurementState.FilterChange
            self._state_begin_time = time.time()
            if not state.is_changing and self.instrument.writer:
                state = await wait_cancelable(self.instrument.apply_instrument_command(
                    b"stop\r", lambda s: s.is_changing), 10.0)
                _LOGGER.debug("Putting instrument into filter change mode")

            return True

        async def _exit_instrument_filter_change():
            nonlocal state

            if state.is_changing and self.instrument.writer:
                state = await wait_cancelable(self.instrument.apply_instrument_command(
                    b"go\r", lambda s: not s.is_changing), 10.0)
                _LOGGER.debug("Exiting instrument filter change mode")

        if self._state == MeasurementState.Normal:
            if await handle_filter_change_start():
                _LOGGER.debug("Entering filter change from normal sampling")
            elif self.instrument.context.bus.bypassed:
                self._state = MeasurementState.BypassDiscard
                self._state_begin_time = time.time()
                _LOGGER.debug("Entering bypass discard state from normal sampling")
            elif state.Fn != self._measurement_spot:
                if not self.instrument.writer and state.Fn != 0:
                    self._measurement_spot = state.Fn
                    self._state = MeasurementState.SpotDiscard
                    self._state_begin_time = time.time()
                    self._clear_spot_normalization_accumulator()
                    _LOGGER.debug(f"Entering spot discard mode due to passive acquisition on spot {state.Fn}")
                else:
                    self._state = MeasurementState.BypassDiscard
                    self._state_begin_time = time.time()
                    _LOGGER.debug(f"Returning to bypass discard due to spot disagreement {state.Fn} vs {self._measurement_spot}")
        elif self._state == MeasurementState.BypassDiscard:
            if await handle_filter_change_start():
                _LOGGER.debug("Entering filter change from bypass discard")
            elif self.instrument.context.bus.bypassed:
                self._state_begin_time = time.time()
            elif state.Fn != self._measurement_spot:
                if not self.instrument.writer and state.Fn != 0:
                    self._measurement_spot = state.Fn
                    self._state = MeasurementState.SpotDiscard
                    self._state_begin_time = time.time()
                    self._clear_spot_normalization_accumulator()
                    _LOGGER.debug(f"Entering spot discard mode due to passive acquisition on spot {state.Fn}")
                else:
                    self._state_begin_time = time.time()
            elif (time.time() - self._state_begin_time) >= self._bypass_discard:
                self._state = MeasurementState.Normal
                self._state_begin_time = time.time()
                _LOGGER.debug("Returning to normal sampling from bypass discard")
        elif self._state == MeasurementState.SpotNormalize:
            if await handle_filter_change_start():
                _LOGGER.debug("Entering filter change from spot normalization")
            elif self.instrument.context.bus.bypassed:
                self._state = MeasurementState.SpotDiscard
                self._state_begin_time = time.time()
                self._clear_spot_normalization_accumulator()
                _LOGGER.debug("Returning to discard state from spot normalization due to bypass")
            elif state.Fn != self._measurement_spot:
                self._state = MeasurementState.SpotDiscard
                self._state_begin_time = time.time()
                self._clear_spot_normalization_accumulator()
                _LOGGER.debug(f"Returning to spot discard due to spot disagreement {state.Fn} vs {self._measurement_spot}")
                if not self.instrument.writer and state.Fn != 0:
                    self._measurement_spot = state.Fn
            else:
                self._spot_normalization_accumulate()
                if (time.time() - self._state_begin_time) >= self._spot_normalize_seconds:
                    self._state = MeasurementState.Normal
                    self._state_begin_time = time.time()
                    _LOGGER.debug("Spot normalization complete, starting normal sampling")
                    self._spot_normalization_complete()
                    await self._save_state()
        elif self._state == MeasurementState.SpotDiscard:
            if await handle_filter_change_start():
                _LOGGER.debug("Entering filter change from spot normalization discard")
            elif self.instrument.context.bus.bypassed:
                self._state_begin_time = time.time()
            elif state.Fn != self._measurement_spot:
                self._state_begin_time = time.time()
                if not self.instrument.writer and state.Fn != 0:
                    self._measurement_spot = state.Fn
            elif (time.time() - self._state_begin_time) >= self._spot_normalize_discard:
                self._state = MeasurementState.SpotNormalize
                self._state_begin_time = time.time()
                self._clear_spot_normalization_accumulator()
                _LOGGER.debug("Spot discard complete, starting normalization measurement")
        elif self._state == MeasurementState.FilterBaseline:
            if await handle_filter_change_start():
                _LOGGER.debug("Entering filter change from filter baseline")
            else:
                self._filter_baseline_accumulate()
                if (time.time() - self._state_begin_time) >= self._filter_baseline_seconds:
                    self._state = MeasurementState.SpotDiscard
                    self._state_begin_time = time.time()
                    self._measurement_spot = 1
                    _LOGGER.debug("Filter baseline complete, starting spot normalization")
                    self._filter_baseline_complete(state)
                    await self._save_state()
        elif self._state == MeasurementState.FilterChange:
            command_change = self._command_filter_change
            self._command_filter_change = None

            if not state.is_changing:
                self.instrument.context.bus.log("Instrument initiated filter change end",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.INFO)
                _LOGGER.info("Detected instrument initiated filter change end")

                self._state = MeasurementState.FilterBaseline
                self._state_begin_time = time.time()
                self._clear_filter_baseline_accumulator()
            elif command_change is not None and not command_change:
                await _exit_instrument_filter_change()

                self.instrument.context.bus.log("Manual filter change end",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.INFO)
                _LOGGER.info("Manual filter change end")

                self._state = MeasurementState.FilterBaseline
                self._state_begin_time = time.time()
                self._clear_filter_baseline_accumulator()
            elif self._autodetect_filter_change_end():
                await _exit_instrument_filter_change()

                self.instrument.context.bus.log("Auto-detected filter change end",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.INFO)
                _LOGGER.info("Auto-detected filter change end")

                # The baseline accumulator is used for auto-detection, so it is already available and we can go
                # directly to spot normalization
                self._state = MeasurementState.SpotDiscard
                self._state_begin_time = time.time()
                self._measurement_spot = 1
                self._filter_baseline_complete(state)
                await self._save_state()
            elif self._filter_change_timeout > 0.0 and (time.time() - self._state_begin_time) >= self._filter_change_timeout:
                await _exit_instrument_filter_change()

                self.instrument.context.bus.log("Filter change took too long and was forced to end",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.ERROR)
                _LOGGER.info("Timeout waiting for filter change end")

                self._state = MeasurementState.FilterBaseline
                self._state_begin_time = time.time()
                self._clear_filter_baseline_accumulator()
            elif command_change is not None and command_change and self._command_filter_change_white:
                _LOGGER.info("Switching to white filter change mode")
                self._state = MeasurementState.WhiteFilterChange
                self._state_begin_time = time.time()
                self._clear_filter_baseline_accumulator()
        elif self._state == MeasurementState.WhiteFilterBaseline:
            if await handle_filter_change_start():
                _LOGGER.debug("Entering filter change from white filter baseline")
            else:
                self._filter_baseline_accumulate()
                if (time.time() - self._state_begin_time) >= self._filter_baseline_seconds:
                    self._state = MeasurementState.SpotDiscard
                    self._state_begin_time = time.time()
                    self._measurement_spot = 1
                    _LOGGER.debug("White filter baseline complete, starting spot normalization")
                    self._filter_baseline_complete(state, white=True)
                    await self._save_state()
        elif self._state == MeasurementState.WhiteFilterChange:
            command_change = self._command_filter_change
            self._command_filter_change = None

            if not state.is_changing:
                self.instrument.context.bus.log("Instrument initiated white filter change end",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.INFO)
                _LOGGER.info("Detected instrument initiated white filter change end")

                self._state = MeasurementState.WhiteFilterBaseline
                self._state_begin_time = time.time()
                self._clear_filter_baseline_accumulator()
            elif command_change is not None and not command_change:
                self.instrument.context.bus.log("Manual white filter change end",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.INFO)
                _LOGGER.info("Manual white filter change end")

                self._state = MeasurementState.WhiteFilterBaseline
                self._state_begin_time = time.time()
                self._clear_filter_baseline_accumulator()

                await _exit_instrument_filter_change()
            elif self._filter_change_timeout > 0.0 and (time.time() - self._state_begin_time) >= self._filter_change_timeout:
                await _exit_instrument_filter_change()

                self.instrument.context.bus.log("White filter change took too long and was forced to end",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.ERROR)
                _LOGGER.info("Timeout waiting for white filter change end")

                self._state = MeasurementState.WhiteFilterBaseline
                self._state_begin_time = time.time()
                self._clear_filter_baseline_accumulator()
        elif self._state == MeasurementState.NeedFilterChange:
            command_change = self._command_filter_change
            self._command_filter_change = None

            if not state.is_changing:
                self.instrument.context.bus.log("Instrument initiated white filter change end",
                                                self._filter_change_start_auxiliary_data(state),
                                                type=BaseBusInterface.LogType.INFO)
                _LOGGER.info("Detected instrument initiated filter change end from required change mode")

                self._state = MeasurementState.FilterBaseline
                self._state_begin_time = time.time()
                self._clear_filter_baseline_accumulator()
            elif command_change is not None:
                if command_change and self._command_filter_change_white:
                    self.instrument.context.bus.log("Switching to white filter change mode",
                                                    self._filter_change_start_auxiliary_data(state),
                                                    type=BaseBusInterface.LogType.INFO)
                    _LOGGER.info("Received white filter change command")

                    self._state = MeasurementState.WhiteFilterChange
                    self._state_begin_time = time.time()
                    self._clear_filter_baseline_accumulator()
                elif not command_change:
                    await _exit_instrument_filter_change()

                    self.instrument.context.bus.log("Manual filter change end",
                                                    self._filter_change_start_auxiliary_data(state),
                                                    type=BaseBusInterface.LogType.INFO)
                    _LOGGER.info("Manual filter change end from required change mode")

                    self._state = MeasurementState.FilterBaseline
                    self._state_begin_time = time.time()
                    self._clear_filter_baseline_accumulator()
                else:
                    _LOGGER.info("Switching to normal filter change mode from required change")

                    self._state = MeasurementState.FilterChange
                    self._state_begin_time = time.time()
                    self._clear_filter_baseline_accumulator()

        if not self.is_changing and self.instrument.writer:
            if self.is_air_flow_enabled:
                target_spot = self.active_spot_number
                if state.Fn != target_spot:
                    _LOGGER.debug(f"Changing instrument spot from {state.Fn} to {target_spot}")
                    state = await wait_cancelable(self.instrument.apply_instrument_command(
                        b"spot=%d\r" % target_spot, lambda s: s.Fn == target_spot), 10.0)
            else:
                if state.Fn != 0:
                    _LOGGER.debug(f"Disabling flow from spot {state.Fn}")
                    state = await wait_cancelable(self.instrument.apply_instrument_command(
                        b"spot=0\r", lambda s: s.Fn == 0), 10.0)

        if not self.instrument.context.bus.bypassed:
            self.instrument.notify_wait_spot_stability(
                self._state in (MeasurementState.SpotNormalize, MeasurementState.SpotDiscard)
            )
            self.instrument.notify_bypass_wait_spot_stability(False)
        else:
            self.instrument.notify_bypass_wait_spot_stability(
                self._state in (MeasurementState.SpotNormalize, MeasurementState.SpotDiscard)
            )
            self.instrument.notify_wait_spot_stability(False)

        self.instrument.notify_filter_baseline(
            self._state in (MeasurementState.FilterBaseline, MeasurementState.WhiteFilterBaseline)
        )
        self.instrument.notify_filter_change(self._state == MeasurementState.FilterChange)
        self.instrument.notify_white_filter_change(self._state == MeasurementState.WhiteFilterChange)

        if self._state == MeasurementState.NeedFilterChange:
            have_white = self.have_white_filter
            self.instrument.notify_need_filter_change(have_white)
            self.instrument.notify_need_white_filter_change(not have_white)
        else:
            self.instrument.notify_need_filter_change(False)
            self.instrument.notify_need_white_filter_change(False)

        if not self.is_changing:
            self.instrument.notify_filter_was_not_white(not self._filter_was_white)
        else:
            self.instrument.notify_filter_was_not_white(False)

        self._filter_id = state.Ff
        self._elapsed_seconds = state.elapsed_seconds
        return state

    def handle_advance_spot(self, Ir: typing.List[float]) -> None:
        if self._state not in (MeasurementState.Normal, MeasurementState.BypassDiscard):
            return
        if self._measurement_spot >= 8:
            return
        if not self._advance_transmittance:
            return
        if not self.transmittance_valid:
            return
        for widx in range(min(len(Ir), len(self._advance_transmittance))):
            threshold = self._advance_transmittance[widx]
            if not threshold or not isfinite(threshold) or threshold <= 0.0:
                continue
            level = Ir[widx]
            if level < threshold:
                _LOGGER.debug(f"Advancing due to transmittance {widx} {level} less than {threshold}")
                self.advance_spot()
                return

    async def shutdown(self) -> None:
        await self._save_state()
        if self.instrument.writer:
            if not self.is_changing:
                _LOGGER.debug("Changing to spot zero on shutdown")
                self.instrument.writer.write(b"spot=0\r")
                await self.instrument.writer.drain()
