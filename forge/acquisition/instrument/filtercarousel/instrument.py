import typing
import asyncio
import logging
import time
import enum
from collections import deque
from math import nan, isfinite, floor, ceil
from forge.units import flow_lpm_to_m3s
from forge.acquisition import LayeredConfiguration
from forge.acquisition.util import parse_interval
from ..standard import StandardInstrument
from ..base import BaseContext, BaseBusInterface
from ..businterface import BusInterface
from ..state import Persistent
from ..variable import Input

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StandardInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    DISPLAY_LETTER = "F"
    TAGS = frozenset({"aerosol"})

    @enum.unique
    class Mode(enum.IntEnum):
        Filter = 0
        InitialBlank = 1
        Complete = 2
        Changing = 3
        Bypass = 4

    class _DigitalOutput:
        def __init__(self, config: typing.Union[LayeredConfiguration, dict, list, str],
                     default_target: typing.Optional[str], default_invert: bool = False):
            self._outputs: typing.List[typing.Tuple[str, typing.Optional[str], bool]] = list()
            if isinstance(config, str):
                self._outputs.append((config, default_target, default_invert))
            elif isinstance(config, LayeredConfiguration):
                output: str = config.constant()
                if not output:
                    self._outputs.append((
                        str(config["OUTPUT"]),
                        config.get("TARGET", default=default_target),
                        bool(config.get("INVERT", default=default_invert)),
                    ))
                elif isinstance(output, list):
                    for o in output:
                        if isinstance(o, dict):
                            self._outputs.append((
                                str(o["OUTPUT"]),
                                o.get("TARGET", default=default_target),
                                bool(o.get("INVERT", default=default_invert)),
                            ))
                        else:
                            self._outputs.append((str(o), default_target, default_invert))
                else:
                    self._outputs.append((str(output), default_target, default_invert))
            elif isinstance(config, list):
                for o in config:
                    if isinstance(o, dict):
                        self._outputs.append((
                            str(o["OUTPUT"]),
                            o.get("TARGET", default=default_target),
                            bool(o.get("INVERT", default=default_invert)),
                        ))
                    else:
                        self._outputs.append((str(o), default_target, default_invert))
            else:
                self._outputs.append((
                    str(config["OUTPUT"]),
                    config.get("TARGET", default=default_target),
                    bool(config.get("INVERT", default=default_invert)),
                ))

        def set(self, bus: BusInterface, value: bool) -> None:
            for output, target, invert in self._outputs:
                data: typing.Dict[str, typing.Any] = {
                    'command': 'set_digital',
                    'data': {
                        'output': output,
                        'value': value ^ invert,
                    },
                }
                if target:
                    data['target'] = target
                bus.client.send_data('command', data)

    class _Baseline:
        def __init__(self, total_time: float, minimum_time: float, discard_time: float):
            self.total_time = total_time
            self.minimum_time = minimum_time
            self.points: typing.Deque[typing.Tuple[float, float]] = deque()
            self._begin_time: float = time.monotonic() + discard_time
            self._sum: float = 0.0

        def __call__(self, value: float) -> None:
            now = time.monotonic()
            if now < self._begin_time:
                return
            cutoff = now - self.total_time
            while len(self.points) != 0 and self.points[0][0] < cutoff:
                self._sum -= self.points[0][1]
                self.points.popleft()

            if not isfinite(value):
                return

            self._sum += value
            self.points.append((now, value))

        def __float__(self) -> float:
            n = len(self.points)
            if n == 0:
                return nan
            if self.points[-1][0] - self.points[0][0] < self.minimum_time:
                return nan
            return self._sum / float(n)

    class _ConditionHold:
        def __init__(self, hold_time: float):
            self.hold_time = hold_time
            self._expire_at: typing.Optional[float] = None

        def __call__(self, value: bool) -> bool:
            if value:
                self._expire_at = time.monotonic() + self.hold_time
                return True
            if self._expire_at is None:
                return False
            if self._expire_at > time.monotonic():
                return True
            self._expire_at = None
            return False

    def __init__(self, context: BaseContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._carousel_size: int = int(context.config.get('CAROUSEL_SIZE', default=8))
        self._filter_time = parse_interval(context.config.get('SAMPLE_TIME', default=24 * 60 * 60))
        if self._filter_time <= 0:
            self._filter_time = 24 * 60 * 60
        self._initial_blank_time = parse_interval(context.config.get('INITIAL_BLANK', default=10.0))
        if self._initial_blank_time <= 0:
            self._initial_blank_time = 10.0

        self._cnc_high_threshold: float = context.config.get('CNC_HIGH.THRESHOLD', default=4000.0)
        self._cnc_high_hold = self._ConditionHold(
            parse_interval(context.config.get('CNC_HIGH.HOLD', default=15.0)))

        self._cnc_spike_data = self._Baseline(
            parse_interval(context.config.get('CNC_SPIKE.TOTAL_TIME', default=1800.0)),
            parse_interval(context.config.get('CNC_SPIKE.MINIMUM_TIME', default=30.0)),
            parse_interval(context.config.get('CNC_SPIKE.DISCARD_TIME', default=30.0)),
        )
        self._cnc_spike_threshold: float = context.config.get('CNC_SPIKE.THRESHOLD', default=2.5)
        self._cnc_spike_minimum: float = float(context.config.get('CNC_SPIKE.MINIMUM', default=500.0))
        self._cnc_spike_hold = self._ConditionHold(
            parse_interval(context.config.get('CNC_SPIKE.HOLD', default=15.0)))

        self._wind_speed_low_threshold: float = context.config.get('WIND_SPEED.THRESHOLD', default=0.5)
        self._wind_speed_low_hold = self._ConditionHold(
            parse_interval(context.config.get('WIND_SPEED.HOLD', default=90.0)))

        self._wind_out_of_sector_start: float = context.config.get('WIND_SECTOR.START')
        self._wind_out_of_sector_end: float = context.config.get('WIND_SECTOR.END')
        self._wind_out_of_sector_hold = self._ConditionHold(
            parse_interval(context.config.get('WIND_SPEED.HOLD', default=90.0)))

        digital_target = context.config.get('DIGITAL')
        valves = context.config.section_or_constant('VALVES')
        self._valve_outputs: typing.List[Instrument._DigitalOutput] = list()
        if valves is None:
            self._valve_outputs.append(self._DigitalOutput("FilterBypass", digital_target, True))
            for i in range(0, self._carousel_size):
                self._valve_outputs.append(self._DigitalOutput(f"FilterValve{i+1}", digital_target))
        elif isinstance(valves, dict) or isinstance(valves, LayeredConfiguration):
            self._valve_outputs.append(self._DigitalOutput(valves.get("BYPASS", default="FilterBypass"),
                                                           digital_target, True))
            for i in range(0, self._carousel_size):
                self._valve_outputs.append(self._DigitalOutput(
                    valves.get(f"CAROUSEL{i+1}", default=f"FilterValve{i+1}"), digital_target))
        else:
            if len(valves) == 0:
                self._valve_outputs.append(self._DigitalOutput("FilterBypass", digital_target, True))
                for i in range(0, self._carousel_size):
                    self._valve_outputs.append(self._DigitalOutput(f"FilterValve{i + 1}", digital_target))
            else:
                for i in range(0, self._carousel_size+1):
                    if i >= len(valves):
                        self._valve_outputs.append(self._DigitalOutput(f"FilterValve{i}", digital_target))
                    else:
                        self._valve_outputs.append(self._DigitalOutput(valves[i], digital_target, i == 0))

        self._prior_time: float = time.monotonic()

        self.data_Q = self.input("Q")
        self.data_Ff = self.persistent("Ff", send_to_bus=False)
        self.data_Fn = self.persistent("Fn")
        self.data_Fp = self.persistent("Fp")
        self._saved_advance_time = self.persistent("advance_time_ms", send_to_bus=False)

        if self.data_Ff.value is None:
            self.data_Ff(0)
        if self.data_Fn.value is None:
            self.data_Fn(0)
        if self.data_Fp.value is None:
            self.data_Fp(0)

        self.data_mode = self.persistent_enum("mode", self.Mode, send_to_bus=False)
        if self.data_mode.value is None:
            self.data_mode(self.Mode.Complete)

        self.data_PD_input: typing.List[Instrument.Input] = list()
        self.data_Qt_input: typing.List[Persistent] = list()
        self.data_St_input: typing.List[Persistent] = list()
        # Separate from the state, so that the state doesn't spam the bus constantly as
        # it accumulates.
        self.data_Qt_display: typing.List[Input] = list()
        self.data_St_display: typing.List[Input] = list()
        for i in range(0, self._carousel_size+1):
            if i > 0:
                self.data_PD_input.append(self.input(f"PD{i}"))
            Qt = self.persistent(f"Qt{i}", send_to_bus=False)
            Qt.autosave = False
            self.data_Qt_input.append(Qt)
            self.data_Qt_display.append(self.input(f"Qt{i}"))
            St = self.persistent(f"St{i}", send_to_bus=False)
            St.autosave = False
            self.data_St_input.append(St)
            self.data_St_display.append(self.input(f"St{i}"))

        self.data_display_aux: typing.List[Instrument.Input] = list()
        for var in ("Usample", "Tsample", "Track"):
            self.data_display_aux.append(self.input(var))

        self.data_N = self.input("N", send_to_bus=False)
        self.data_WS = self.input("WS", send_to_bus=False)
        self.data_WD = self.input("WD", send_to_bus=False)

        self.notify_carousel_complete = self.notification("carousel_complete")
        self.notify_carousel_change = self.notification("carousel_change")
        self.notify_initial_blank = self.notification("initial_blank")
        self.notify_cnc_high = self.notification("cnc_high", is_warning=True)
        self.notify_cnc_spike = self.notification("cnc_spike", is_warning=True)
        self.notify_wind_out_of_sector = self.notification("wind_out_of_sector", is_warning=True)
        self.notify_wind_speed_low = self.notification("wind_speed_low", is_warning=True)

        self.data_Qt = self.input_array("Qt", send_to_bus=False)
        self.data_St = self.input_array("St", send_to_bus=False)
        self.data_PD = self.input_array("PD", send_to_bus=False)
        self.measurement_report = self.report(
            self.variable_sample_flow(self.data_Q, code="Q", attributes={
                'long_name': "flow through the active filter",
            }).at_stp(),

            self.variable_array_last_valid(self.data_Qt, name="total_volume", code="Qt", attributes={
                'long_name': "total volume through each filter with the first (zero) as the bypass line",
                'units': "m3",
                'C_format': "%10.5f",
            }).at_stp(),

            self.variable_array(self.data_PD, name="filter_pressure_drop", code="Pd", attributes={
                'long_name': "pressure drop across each filter in the carousel",
                'units': "hPa",
                'C_format': "%5.1f",
            }),

            flags=[
                self.flag(self.notify_carousel_complete),
                self.flag(self.notify_carousel_change),
                self.flag(self.notify_initial_blank),
                self.flag(self.notify_cnc_high),
                self.flag(self.notify_cnc_spike),
                self.flag(self.notify_wind_out_of_sector),
                self.flag(self.notify_wind_speed_low),
            ]
        )

        self.state_report = self.change_event(
            self.state_unsigned_integer(self.data_Ff, "carousel_start_time", code="Ff", attributes={
                'long_name': "start time of the carousel",
                'units': "milliseconds since 1970-01-01 00:00:00",
            }),
            self.state_unsigned_integer(self.data_Fn, "active_filter", code="Fn", attributes={
                'long_name': "currently accumulating filter number or zero for the bypass",
                'C_format': "%2llu",
            }),
            self.state_unsigned_integer(self.data_Fp, "measurement_filter", code="Fp", attributes={
                'long_name': "desired measurement carousel filter number or zero when complete",
                'C_format': "%2llu",
            }),
            self.state_enum(self.data_mode, attributes={
                'long_name': "sampling mode",
            }),
        )

        self.data_Ff_complete = self.persistent("FfComplete", send_to_bus=False)
        self.data_Qt_complete = self.persistent("QtComplete", send_to_bus=False)
        self.data_St_complete = self.persistent("StComplete", send_to_bus=False)
        self.carousel_report = self.change_event(
            self.state_unsigned_integer(self.data_Ff_complete, "completed_start_time", attributes={
                'long_name': "start time of the completed carousel",
                'units': "milliseconds since 1970-01-01 00:00:00",
            }),
            self.state_measurement_array(self.data_Qt_complete, name="final_volume", attributes={
                'long_name': "final volume through each filter in the completed carousel with the first (zero) as the bypass line",
                'units': "m3",
                'C_format': "%10.5f",
            }).at_stp(),
            self.state_measurement_array(self.data_St_complete, name="final_accumulated_time", attributes={
                'long_name': "final amount of sampling time on the completed carousel with the first (zero) as the bypass line",
                'units': "seconds",
                'C_format': "%7.0f",
            }),

            name="completed_carousel",
        )

        self.context.bus.connect_command('start_change', self.start_change)
        self.context.bus.connect_command('end_change', self.end_change)
        self.context.bus.connect_command('advance_filter', self.advance_filter)

        self._request_start_change: bool = False
        self._request_end_change: bool = False
        self._request_advance_filter: bool = False

    def start_change(self, _) -> None:
        self._request_start_change = True

    def end_change(self, _) -> None:
        self._request_end_change = True

    def advance_filter(self, _) -> None:
        self._request_advance_filter = True

    @property
    def _advance_time(self) -> typing.Optional[float]:
        if self.data_mode.value in (self.Mode.Complete, self.Mode.Changing):
            return None
        v = self._saved_advance_time.value
        if not v:
            return None
        return v / 1000.0

    @_advance_time.setter
    def _advance_time(self, value: typing.Optional[float]) -> None:
        if not value:
            self._saved_advance_time(None)
            return
        self._saved_advance_time(int(ceil(value * 1000.0)))

    async def _process_advance(self) -> None:
        now = time.time()
        if self._request_advance_filter:
            self._request_advance_filter = False
            if self.data_mode.value in (self.Mode.Complete, self.Mode.Changing):
                _LOGGER.debug("Ignored advance request with no active filter")
                return
            _LOGGER.debug("Advancing active filter on request")
            advance_at = now
        else:
            advance_at = self._advance_time
            if advance_at is None:
                return
            if now < advance_at:
                return

        next_index = int(self.data_Fp.value or 1) + 1
        if next_index > self._carousel_size:
            _LOGGER.debug("Carousel complete")
            self.data_mode(self.Mode.Complete)
            self.data_Fp(0)
            await self._save_all()
            await self._update_next()
            return

        self.data_Fp(next_index)

        if self._filter_time >= 24 * 60 * 60:
            rounded_start = floor(advance_at / (24 * 60 * 60)) * (24 * 60 * 60)
            minimum_time = 6 * 60 * 60
        elif self._filter_time >= 60 * 60:
            rounded_start = floor(advance_at / (60 * 60)) * (60 * 60)
            minimum_time = 15 * 60
        elif self._filter_time >= 60:
            rounded_start = floor(advance_at / 60) * 60
            minimum_time = 15
        else:
            rounded_start = advance_at
            minimum_time = 0

        next_advance = rounded_start + self._filter_time
        if minimum_time > 0:
            while next_advance <= now + minimum_time:
                next_advance += self._filter_time

        if self.data_mode.value == self.Mode.InitialBlank:
            _LOGGER.debug(f"Exiting initial blank to filter {next_index} (advance {advance_at} to {next_advance} with {self._filter_time})")
            self.data_mode(self.Mode.Filter)
        else:
            _LOGGER.debug(f"Changed to filter {next_index} (advance {advance_at} to {next_advance} with {self._filter_time})")

        self._advance_time = next_advance
        await self._update_next()

    async def _process_data(self):
        now = time.monotonic()
        elapsed = now - self._prior_time
        self._prior_time = now

        for external in (self.data_Q, self.data_N, self.data_WS, self.data_WD,
                         *self.data_PD_input, *self.data_display_aux):
            external(nan)
        self.data_PD([float(p) for p in self.data_PD_input])

        add_volume = flow_lpm_to_m3s(float(self.data_Q)) * elapsed
        pos = self.data_Fn.value
        if self.data_mode.value != self.Mode.Changing and isfinite(add_volume) and pos < len(self.data_Qt_input):
            v = self.data_Qt_input[pos].value
            if v is None or not isfinite(v):
                self.data_Qt_input[pos](add_volume)
            else:
                self.data_Qt_input[pos](v + add_volume)

            v = self.data_St_input[pos].value
            if v is None or not isfinite(v):
                self.data_St_input[pos](elapsed)
            else:
                self.data_St_input[pos](v + elapsed)
        self.data_Qt([float(v.value) if v.value is not None else nan for v in self.data_Qt_input])
        self.data_St([float(v.value) if v.value is not None else nan for v in self.data_St_input])
        for i in range(len(self.data_Qt_input)):
            self.data_Qt_display[i](self.data_Qt_input[i].value)
        for i in range(len(self.data_St_input)):
            self.data_St_display[i](self.data_St_input[i].value)

        self._cnc_spike_data(float(self.data_N))

    @property
    def _cnc_high(self) -> bool:
        if self._cnc_high_threshold is None or not isfinite(self._cnc_high_threshold) or self._cnc_high_threshold <= 0.0:
            return False
        cnc = float(self.data_N)
        if not isfinite(cnc):
            return self._cnc_high_hold(False)
        return self._cnc_high_hold(cnc >= self._cnc_high_threshold)

    @property
    def _cnc_spike(self) -> bool:
        if self._cnc_spike_threshold is None or not isfinite(self._cnc_spike_threshold) or self._cnc_spike_threshold <= 0.0:
            return False
        cnc = float(self.data_N)
        if not isfinite(cnc):
            return self._cnc_spike_hold(False)
        baseline = float(self._cnc_spike_data)
        if not isfinite(baseline):
            return self._cnc_spike_hold(False)

        threshold = baseline * self._cnc_spike_threshold
        return self._cnc_spike_hold(cnc >= threshold and cnc >= self._cnc_spike_minimum)

    @property
    def _wind_speed_low(self) -> bool:
        if self._wind_speed_low_threshold is None or not isfinite(self._wind_speed_low_threshold) or self._wind_speed_low_threshold <= 0.0:
            return False
        ws = float(self.data_WS)
        if not isfinite(ws):
            return self._wind_speed_low_hold(False)
        return self._wind_speed_low_hold(ws < self._wind_speed_low_threshold)

    @property
    def _wind_out_of_sector(self) -> bool:
        if self._wind_out_of_sector_start is None or not isfinite(self._wind_out_of_sector_start):
            return False
        if self._wind_out_of_sector_end is None or not isfinite(self._wind_out_of_sector_end):
            return False
        wd = float(self.data_WD)
        if not isfinite(wd):
            return self._wind_out_of_sector_hold(False)
        while wd < 0.0:
            wd += 360.0
        while wd >= 360.0:
            wd -= 360.0

        if self._wind_out_of_sector_start < self._wind_out_of_sector_end:
            out_of_sector = self._wind_out_of_sector_start <= wd <= self._wind_out_of_sector_end
        else:
            out_of_sector = wd <= self._wind_out_of_sector_end or wd >= self._wind_out_of_sector_start
        return self._wind_out_of_sector_hold(out_of_sector)

    @property
    def _should_bypass_filter(self) -> bool:
        return bool(self.notify_cnc_high.value) or bool(self.notify_cnc_spike) or bool(self.notify_wind_speed_low) or bool(self.notify_wind_out_of_sector)

    async def _process_state(self) -> None:
        self.notify_cnc_high(self._cnc_high)
        self.notify_cnc_spike(self._cnc_spike)
        self.notify_wind_speed_low(self._wind_speed_low)
        self.notify_wind_out_of_sector(self._wind_out_of_sector)

        if self._request_start_change:
            self._request_start_change = False
            if self.data_mode.value != self.Mode.Changing:
                _LOGGER.debug("Filter carousel change starting")
                self.context.bus.log("Filter carousel change started.",
                                     type=BaseBusInterface.LogType.INFO)
                self.data_mode(self.Mode.Changing)
                await self._update_next()
        if self._request_end_change:
            self._request_end_change = False
            if self.data_mode.value == self.Mode.Changing:
                _LOGGER.debug("Filter carousel change ending")
                self.context.bus.log("Filter carousel change ended.", {
                    "filter_time": self._filter_time,
                    "initial_blank_time": self._initial_blank_time,
                    "carousel_size": self._carousel_size,
                }, type=BaseBusInterface.LogType.INFO)

                self.data_Qt_complete(list(self.data_Qt.value), oneshot=True)
                self.data_St_complete(list(self.data_St.value), oneshot=True)
                self.data_Ff_complete(self.data_Ff.value, oneshot=True)

                self.data_Ff(round(time.time() * 1000), oneshot=True)
                for reset in self.data_Qt_input:
                    reset(nan)
                self.data_Qt([nan for _ in self.data_Qt_input])
                for reset in self.data_St_input:
                    reset(nan)
                self.data_St([nan for _ in self.data_St_input])
                self.data_mode(self.Mode.InitialBlank)
                self.data_Fp(1)
                self._advance_time = time.time() + self._initial_blank_time

                await self._save_all()
                await self._update_next()

        mode = self.data_mode.value

        if mode == self.Mode.Filter:
            if self._should_bypass_filter:
                _LOGGER.debug("Bypassing filter sampling")
                mode = self.data_mode(self.Mode.Bypass)
        elif mode == self.Mode.Bypass:
            if not self._should_bypass_filter:
                _LOGGER.debug("Resuming filter sampling")
                mode = self.data_mode(self.Mode.Filter)

        if mode == self.Mode.Filter:
            self.data_Fn(self.data_Fp.value)
        elif mode == self.Mode.InitialBlank:
            self.data_Fn(1)
        elif mode == self.Mode.Complete:
            self.data_Fn(0)
        elif mode == self.Mode.Changing:
            self.data_Fn(0)
        else:
            self.data_Fn(0)

        self.notify_initial_blank(mode == self.Mode.InitialBlank)
        self.notify_carousel_complete(mode == self.Mode.Complete)
        self.notify_carousel_change(mode == self.Mode.Changing)

    async def _update(self) -> None:
        await self._process_data()
        await self._process_advance()

        prior_Fn = self.data_Fn.value
        await self._process_state()
        if prior_Fn != self.data_Fn.value:
            _LOGGER.debug(f"Changing valve from {prior_Fn} to {self.data_Fn.value}")
            self._activate_valve(self.data_Fn.value)

        self.measurement_report()

    async def _update_next(self) -> None:
        advance_time = self._advance_time
        await self.context.bus.set_state_value("next", {
            'epoch_ms': round(advance_time * 1000) if advance_time is not None else None,
        })

    def _activate_valve(self, index: int) -> None:
        if len(self._valve_outputs) == 0:
            _LOGGER.warning(f"Valve activation for {index} skipped since no valves are defined")
            return

        if index == 0:
            # Open bypass, close all other valves
            self._valve_outputs[0].set(self.context.bus, True)
            for i in range(1, len(self._valve_outputs)):
                self._valve_outputs[i].set(self.context.bus, False)
            return

        any_open = False
        for i in range(len(self._valve_outputs)):
            if i == index:
                any_open = True
                self._valve_outputs[i].set(self.context.bus, True)
            else:
                self._valve_outputs[i].set(self.context.bus, False)

        if any_open:
            # Disable bypass last after a valve has been opened
            self._valve_outputs[0].set(self.context.bus, False)
        else:
            # No filter valve open so bypass to make sure there's a flow path
            _LOGGER.warning(f"Invalid valve index {index}, bypass enabled")
            self._valve_outputs[0].set(self.context.bus, True)

    async def _save_all(self) -> None:
        _LOGGER.debug("Saving persistent data")
        now = time.time()
        for Qt in self.data_Qt_input:
            await Qt.save(now)
        for St in self.data_St_input:
            await St.save(now)

    async def run(self) -> typing.NoReturn:
        # Initial delay to make sure everything is started
        await asyncio.sleep(5)

        try:
            await self.emit()
            await self._process_data()
            await self._process_advance()
            self._activate_valve(self.data_Fn.value)
            await self._update_next()
            await asyncio.sleep(self._report_interval)

            self.is_communicating = True

            next_save = time.monotonic() + 10 * 60
            while True:
                await self.emit()
                await self._update()

                now = time.monotonic()
                if next_save < now:
                    await self._save_all()
                    next_save = now + 10 * 60

                await asyncio.sleep(self._report_interval)
        finally:
            # This should be set by the shutdown of the uMAC, but do it here anyway
            self._activate_valve(0)

            self.data_Fn(0)
            if self.data_mode.value in (self.Mode.Filter, self.Mode.InitialBlank):
                self.data_mode(self.Mode.Bypass)
            await self.emit()

            await self._save_all()
