import typing
import asyncio
import logging
import time
from math import isfinite
from forge.acquisition import CONFIGURATION, LayeredConfiguration
from forge.acquisition.util import parse_interval
from ..base import BaseControl
from ...bus.client import PersistenceLevel
from ...schedule import Schedule

_LOGGER = logging.getLogger(__name__)


class Control(BaseControl):
    CONTROL_TYPE = "humidograph"

    class _Cycle(Schedule):
        class Active(Schedule.Active):
            def __init__(self, config: LayeredConfiguration):
                super().__init__(config)
                self.control: Control = None

                if isinstance(config, LayeredConfiguration):
                    constant_config = config.constant()
                    if constant_config is not None:
                        self.setpoint = float(constant_config)
                    else:
                        self.setpoint = float(config.get("SETPOINT"))
                elif isinstance(config, dict):
                    self.setpoint = float(config.get("SETPOINT"))
                else:
                    self.setpoint = float(config)

            def __repr__(self) -> str:
                return f"Humidograph.Active({self.describe_offset()}={str(self.setpoint)})"

            def apply(self, activation_time: float) -> None:
                self.control.bus.send_message(PersistenceLevel.STATE, 'active', {
                    'setpoint': self.setpoint,
                    'epoch_ms': round(activation_time * 1000.0) if activation_time else None,
                })
                self.control._set_target(self.setpoint)

            async def automatic_activation(self, now: float = None) -> bool:
                if not await Schedule.Active.automatic_activation(self, now):
                    return False
                if not self.control.enabled:
                    return True

                self.apply(self.last_time)
                self.control._update_next(now)
                return True

    def __init__(self):
        super().__init__()
        self.cycle = self._Cycle(LayeredConfiguration(CONFIGURATION.get("ACQUISITION.HUMIDOGRAPH")))
        for a in self.cycle.active:
            a.control = self

        self.enabled = True

        self._setpoint_target = CONFIGURATION.get("ACQUISITION.HUMIDOGRAPH.SETPOINT.TARGET")
        self._setpoint_output = CONFIGURATION.get("ACQUISITION.HUMIDOGRAPH.SETPOINT.OUTPUT", "U_V12")
        self._setpoint_idle = CONFIGURATION.get("ACQUISITION.HUMIDOGRAPH.SETPOINT.IDLE")

        self._flow_source = CONFIGURATION.get("ACQUISITION.HUMIDOGRAPH.FLOW.SOURCE")
        self._flow_variable = CONFIGURATION.get("ACQUISITION.HUMIDOGRAPH.FLOW.VARIABLE", "Q_Q11")
        self._flow_minimum = float(CONFIGURATION.get("ACQUISITION.HUMIDOGRAPH.FLOW.MINIMUM", 10.0))

    async def initialize(self) -> None:
        self.bus.send_message(PersistenceLevel.SOURCE, 'instrument', {
            'type': self.CONTROL_TYPE,
        })
        self.bus.send_message(PersistenceLevel.STATE, 'enabled', 1 if self.enabled else 0)

    def _set_target(self, setpoint: float) -> None:
        data: typing.Dict[str, typing.Any] = {
            'command': 'set_analog',
            'data': {
                'output': self._setpoint_output,
                'value': setpoint,
            },
        }
        if self._setpoint_target:
            data['target'] = self._setpoint_target
        # Persist this so it immediately sets on any new instruments
        self.bus.send_message(PersistenceLevel.STATE, 'command', data)

    def _update_next(self, now: float = None) -> None:
        if not self.enabled:
            self.bus.send_message(PersistenceLevel.STATE, 'next', None)
            return

        next = self.cycle.next(now)
        if next:
            self.bus.send_message(PersistenceLevel.STATE, 'next', {
                'setpoint': next.setpoint,
                'epoch_ms': round(next.next_time * 1000.0) if next.next_time else None,
            })
        else:
            self.bus.send_message(PersistenceLevel.STATE, 'next', None)

    def _disable_cycle(self) -> None:
        self.enabled = False
        self.bus.send_message(PersistenceLevel.STATE, 'active', None)
        self.bus.send_message(PersistenceLevel.STATE, 'enabled', 0)
        self._update_next()

        if self._setpoint_idle is not None:
            sp = float(self._setpoint_idle)
            self._set_target(sp)

    def _process_data(self, source: str, message: typing.Any) -> None:
        if not isinstance(message, dict):
            return
        if self._flow_source is not None and source != self._flow_source:
            return
        if not self._flow_variable or not self._flow_minimum:
            return

        value = message.get(self._flow_variable)
        if value is None:
            return
        try:
            value = float(value)
        except (TypeError, ValueError, OverflowError):
            return
        if not isfinite(value):
            return

        if self.enabled and value < self._flow_minimum:
            self.enabled = False
            _LOGGER.debug(f"Disabling humidograph: {self._flow_variable} at {value} less than {self._flow_minimum}")
            self.log("Flow through humidifier is too low.  Humidograph scan disabled.", {
                'flow': value,
                'threshold': self._flow_minimum,
            }, is_error=True)

    async def bus_message(self, source: str, record: str, message: typing.Any) -> None:
        if record == 'data':
            self._process_data(source, message)
            return

        if record != 'command':
            return
        if not isinstance(message, dict):
            return
        command_target = message.get('target')
        if command_target and command_target != self.bus.source:
            return

        command = message.get('command')
        if command == 'enable_humidograph':
            _LOGGER.debug("Humidograph enable command received")
            self.enabled = True
            self.bus.send_message(PersistenceLevel.STATE, 'enabled', 1)
            now = time.time()
            current = self.cycle.current(now)
            if current:
                current.apply(now)
            self._update_next(now)
        elif command == 'disable_humidograph':
            _LOGGER.debug("Humidograph disable command received")
            self._disable_cycle()

    async def run(self):
        await self.cycle.automatic_activation()
