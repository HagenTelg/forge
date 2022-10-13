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

    def __init__(self):
        super().__init__()

        self._gas_sample_seconds: float = CONFIGURATION.get("ACQUISITION.SPANCHECK.GAS.SAMPLE", self.GAS_SAMPLE_SECONDS)
        self._gas_flush_seconds: float = CONFIGURATION.get("ACQUISITION.SPANCHECK.GAS.FLUSH", self.GAS_FLUSH_SECONDS)
        self._air_sample_seconds: float = CONFIGURATION.get("ACQUISITION.SPANCHECK.AIR.SAMPLE", self.AIR_SAMPLE_SECONDS)
        self._air_flush_seconds: float = CONFIGURATION.get("ACQUISITION.SPANCHECK.AIR.FLUSH", self.AIR_FLUSH_SECONDS)

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

    async def bus_message(self, source: str, record: str, message: typing.Any) -> None:
        if record == 'spancheck_result':
            self._process_result(source, message)
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

            self._current_state = Control._State.GasAirFlush
            self._advance_time = time.monotonic() + self._air_flush_seconds
            self._control_command('initialize')
            self._control_command('air_flush')
            self._broadcast_state()
            self._event.set()

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