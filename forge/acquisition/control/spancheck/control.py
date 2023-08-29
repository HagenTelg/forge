import typing
import asyncio
import logging
import time
import enum
from math import isfinite
from forge.tasks import wait_cancelable
from forge.acquisition import CONFIGURATION
from forge.rayleigh import RAYLEIGH_FACTOR, CO2
from ..base import BaseControl
from ...bus.client import PersistenceLevel

_LOGGER = logging.getLogger(__name__)


class Control(BaseControl):
    CONTROL_TYPE = "spancheck"

    GAS_SAMPLE_SECONDS: float = 300.0
    GAS_FLUSH_SECONDS: float = 600.0
    AIR_SAMPLE_SECONDS: float = 600.0
    AIR_FLUSH_SECONDS: float = 180.0

    class _State(enum.Enum):
        GasAirFlush = 'gas_air_flush'
        GasFlush = 'gas_flush'
        GasSample = 'gas_sample'
        AirFlush = 'air_flush'
        AirSample = 'air_sample'
        Inactive = 'inactive'

    class _Timing:
        def __init__(self,
                     gas_sample_seconds: typing.Optional[float],
                     gas_flush_seconds: typing.Optional[float],
                     air_sample_seconds: typing.Optional[float],
                     air_flush_seconds: typing.Optional[float]):
            self._gas_sample_seconds = gas_sample_seconds
            self._gas_flush_seconds = gas_flush_seconds
            self._air_sample_seconds = air_sample_seconds
            self._air_flush_seconds = air_flush_seconds

        @property
        def gas_sample_seconds(self) -> float:
            return self._gas_sample_seconds

        @property
        def gas_flush_seconds(self) -> float:
            return self._gas_flush_seconds

        @property
        def air_sample_seconds(self) -> float:
            return self._air_sample_seconds

        @property
        def air_flush_seconds(self) -> float:
            return self._air_flush_seconds

        def combine(self, other: typing.Optional["Control._Timing"]) -> "Control._Timing":
            gas_sample_seconds = self.gas_sample_seconds
            if not gas_sample_seconds or (other.gas_sample_seconds and gas_sample_seconds < other.gas_flush_seconds):
                gas_sample_seconds = other.gas_sample_seconds
            gas_flush_seconds = self.gas_flush_seconds
            if not gas_flush_seconds or (other.gas_flush_seconds and gas_flush_seconds < other.gas_flush_seconds):
                gas_flush_seconds = other.gas_flush_seconds
            air_sample_seconds = self.air_sample_seconds
            if not air_sample_seconds or (other.air_sample_seconds and air_sample_seconds < other.air_flush_seconds):
                air_sample_seconds = other.air_sample_seconds
            air_flush_seconds = self.air_flush_seconds
            if not air_flush_seconds or (other.air_flush_seconds and air_flush_seconds < other.air_flush_seconds):
                air_flush_seconds = other.air_flush_seconds
            return Control._Timing(gas_sample_seconds, gas_flush_seconds, air_sample_seconds, air_flush_seconds)

        def override(self, other: "Control._Timing") -> "Control._Timing":
            return Control._Timing(
                other.gas_sample_seconds or self.gas_sample_seconds,
                other.gas_flush_seconds or self.gas_flush_seconds,
                other.air_sample_seconds or self.air_sample_seconds,
                other.air_flush_seconds or self.air_flush_seconds,
            )

    INSTRUMENT_TIMING = {
        'tsi3563nephelometer': _Timing(GAS_SAMPLE_SECONDS, GAS_FLUSH_SECONDS, AIR_SAMPLE_SECONDS, AIR_FLUSH_SECONDS),
        'ecotechnephelometer': _Timing(600.0, 480.0, AIR_SAMPLE_SECONDS, 480.0),
    }

    def __init__(self):
        super().__init__()

        self._override_timing = self._Timing(
            CONFIGURATION.get("ACQUISITION.SPANCHECK.GAS.SAMPLE"),
            CONFIGURATION.get("ACQUISITION.SPANCHECK.GAS.FLUSH"),
            CONFIGURATION.get("ACQUISITION.SPANCHECK.AIR.SAMPLE"),
            CONFIGURATION.get("ACQUISITION.SPANCHECK.AIR.FLUSH"),
        )
        self._default_timing = self._Timing(
            self.GAS_SAMPLE_SECONDS,
            self.GAS_FLUSH_SECONDS,
            self.AIR_SAMPLE_SECONDS,
            self.AIR_FLUSH_SECONDS,
        ).override(self._override_timing)

        self._gas_sample_seconds: float = self._default_timing.gas_sample_seconds
        self._gas_flush_seconds: float = self._default_timing.gas_flush_seconds
        self._air_sample_seconds: float = self._default_timing.air_sample_seconds
        self._air_flush_seconds: float = self._default_timing.air_flush_seconds

        self._source_timing: typing.Dict[str, Control._Timing] = dict()

        gas_factor = CONFIGURATION.get("ACQUISITION.SPANCHECK.GAS.TYPE", CO2)
        if isinstance(gas_factor, str):
            gas_factor = RAYLEIGH_FACTOR[gas_factor.upper()]
        self._gas_factor: float = float(gas_factor)

        self._event: asyncio.Event = None

        self._current_state: Control._State = Control._State.Inactive
        self._advance_time: typing.Optional[float] = None
        self._command_target: typing.Optional[str] = None

        self._percent_error: typing.Dict[str, float] = dict()

    def _broadcast_state(self) -> None:
        state: typing.Dict[str, typing.Any] = {
            'id': self._current_state.value,
        }
        if self._advance_time:
            seconds_remaining = self._advance_time - time.monotonic()
            real_time = time.time() + seconds_remaining
            state['next_epoch_ms'] = round(real_time * 1000.0)
        self.bus.send_message(PersistenceLevel.SOURCE, 'state', state)

    def _broadcast_result(self) -> None:
        self.bus.send_message(PersistenceLevel.SOURCE, 'percent_error', self._percent_error)

    def _control_command(self, command: str) -> None:
        data: typing.Dict[str, typing.Any] = {
            'command': '_spancheck_control',
            'data': command,
        }
        if self._command_target:
            data['target'] = self._command_target
        self.bus.send_message(PersistenceLevel.DATA, 'command', data)

    def _calculate(self) -> None:
        data: typing.Dict[str, typing.Any] = {
            'command': '_spancheck_calculate',
            'data': {
                'gas_factor': self._gas_factor,
            },
        }
        if self._command_target:
            data['target'] = self._command_target
        self.bus.send_message(PersistenceLevel.DATA, 'command', data)

    async def initialize(self):
        self._event = asyncio.Event()

    def _process_result(self, source: str, message: typing.Dict[str, typing.Any]) -> None:
        if self._command_target and source != self._command_target:
            return
        if not isinstance(message, dict):
            return
        percent_error = message.get('percent_error')
        if not isinstance(percent_error, dict):
            return
        percent_error = percent_error.get('average')
        if not isinstance(percent_error, float) or not isfinite(percent_error):
            return

        self._percent_error[source] = percent_error
        self._broadcast_result()

    def _update_instrument(self, source: str, message: typing.Dict[str, typing.Any]) -> None:
        self._source_timing.pop(source, None)
        if not isinstance(message, dict):
            return
        instrument_type = message.get('type')
        if not instrument_type:
            return
        timing = self.INSTRUMENT_TIMING.get(instrument_type)
        if not timing:
            return
        self._source_timing[source] = timing

    async def bus_message(self, source: str, record: str, message: typing.Any) -> None:
        if record == 'spancheck_result':
            self._process_result(source, message)
            return
        elif record == 'instrument':
            self._update_instrument(source, message)
            return

        if record != 'command':
            return
        if not isinstance(message, dict):
            return
        command = message.get('command')
        if command == 'start_spancheck':
            if self._current_state != Control._State.Inactive:
                _LOGGER.debug("Ignoring spancheck start request since one is ongoing")
                return
            self._command_target = message.get('target')
            self._percent_error.clear()

            if not self._command_target:
                timing = self._default_timing
                for merge in self._source_timing.values():
                    timing = timing.combine(merge)
            else:
                timing = self._source_timing.get(self._command_target)
                if not timing:
                    timing = self._default_timing
            timing = timing.override(self._override_timing)
            self._gas_sample_seconds = timing.gas_sample_seconds
            self._gas_flush_seconds = timing.gas_flush_seconds
            self._air_sample_seconds = timing.air_sample_seconds
            self._air_flush_seconds = timing.air_flush_seconds

            self._current_state = Control._State.GasAirFlush
            self._advance_time = time.monotonic() + self._air_flush_seconds
            self._control_command('initialize')
            self._control_command('air_flush')
            self._broadcast_state()
            self._event.set()

            self.bus.send_message(PersistenceLevel.DATA, 'command', {
                'command': 'disable_humidograph',
                'target': 'humidograph',
            })

            _LOGGER.debug("Spancheck starting initial air flush before gas sampling")

            self.log("Spancheck initiated", {
                'target': self._command_target,
                'gas_sample_seconds': self._gas_sample_seconds,
                'gas_flush_seconds': self._gas_flush_seconds,
                'air_sample_seconds': self._air_sample_seconds,
                'air_flush_seconds': self._air_flush_seconds,
                'gas_rayleigh_factor': self._gas_factor,
            })
        elif command == 'stop_spancheck':
            if self._current_state == Control._State.Inactive:
                _LOGGER.debug("Ignoring spancheck stop request since there is none active")
                return

            _LOGGER.debug(f"Spancheck aborted from state {self._current_state.value}")
            self.log("Spancheck aborted", {
                'state': self._current_state.value,
            })

            self._current_state = Control._State.Inactive
            self._advance_time = None
            self._control_command('abort')
            self._broadcast_state()
            self._event.set()

            self.bus.send_message(PersistenceLevel.DATA, 'command', {
                'command': 'enable_humidograph',
                'target': 'humidograph',
            })

    async def _advance_state(self, now: float) -> None:
        if self._current_state == Control._State.GasAirFlush:
            _LOGGER.debug("Initial air flush completed, starting gas flush")
            self._current_state = Control._State.GasFlush
            self._advance_time = now + self._gas_flush_seconds
            self._control_command('gas_flush')
            self._broadcast_state()
        elif self._current_state == Control._State.GasFlush:
            _LOGGER.debug("Gas flush completed, starting gas measurement")
            self._current_state = Control._State.GasSample
            self._advance_time = now + self._gas_sample_seconds
            self._control_command('gas_sample')
            self._broadcast_state()
        elif self._current_state == Control._State.GasSample:
            _LOGGER.debug("Gas sampling completed, starting air flush")
            self._current_state = Control._State.AirFlush
            self._advance_time = now + self._air_flush_seconds
            self._control_command('air_flush')
            self._broadcast_state()
        elif self._current_state == Control._State.AirFlush:
            _LOGGER.debug("Air flush completed, starting air measurement")
            self._current_state = Control._State.AirSample
            self._advance_time = now + self._air_sample_seconds
            self._control_command('air_sample')
            self._broadcast_state()
        elif self._current_state == Control._State.AirSample:
            _LOGGER.debug("Air sampling completed, ending spancheck")
            self._current_state = Control._State.Inactive
            self._advance_time = None
            self._control_command('complete')
            self._calculate()
            self._broadcast_state()
            self.bus.send_message(PersistenceLevel.DATA, 'command', {
                'command': 'enable_humidograph',
                'target': 'humidograph',
            })

    async def run(self) -> typing.NoReturn:
        while True:
            if not self._advance_time:
                await self._event.wait()
                self._event.clear()
            else:
                now = time.monotonic()
                if self._advance_time <= now:
                    await self._advance_state(now)
                    if not self._advance_time:
                        continue

                maximum_sleep = self._advance_time - now
                if maximum_sleep < 0.001:
                    maximum_sleep = 0.001
                try:
                    await wait_cancelable(self._event.wait(), maximum_sleep)
                except asyncio.TimeoutError:
                    pass