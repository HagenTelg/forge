import typing
import asyncio
import logging
from math import isfinite
from forge.acquisition import CONFIGURATION, LayeredConfiguration
from forge.tasks import background_task
from ..base import BaseControl, PersistenceLevel

_LOGGER = logging.getLogger(__name__)


class Control(BaseControl):
    CONTROL_TYPE = "pressure_bypass"

    def __init__(self):
        super().__init__()
        self._source = CONFIGURATION.get("ACQUISITION.PRESSURE_BYPASS.SOURCE")
        self._variable = CONFIGURATION.get("ACQUISITION.PRESSURE_BYPASS.VARIABLE", "Pd_P11")
        self._threshold_trigger = float(CONFIGURATION.get("ACQUISITION.PRESSURE_BYPASS.TRIGGER", 100.0))
        self._threshold_release = float(CONFIGURATION.get("ACQUISITION.PRESSURE_BYPASS.RELEASE", self._threshold_trigger * 0.9))
        self._hold_time = float(CONFIGURATION.get("ACQUISITION.PRESSURE_BYPASS.HOLD_SECONDS", 30.0))

        self._active: bool = False
        self._release_task: typing.Optional[asyncio.Task] = None

    async def _cancel_release(self) -> None:
        t = self._release_task
        self._release_task = None
        if not t:
            return
        try:
            t.cancel()
        except:
            pass
        try:
            await t
        except:
            pass

    async def _release_after_hold(self) -> None:
        if self._hold_time > 0.0:
            await asyncio.sleep(self._hold_time)
        self._release_task = None
        self._active = False

        _LOGGER.debug(f"Releasing bypass lock")

        self.bus.set_state('bypass_held', False)
        self.log("Pressure across the impactor returned to normal.  The automatic bypass has been released but a manual release is required before sampling resumes.",
                 is_error=True)

    async def bus_message(self, source: str, record: str, message: typing.Any) -> None:
        if record != 'data':
            return
        if self._source is not None and source != self._source:
            return
        if not isinstance(message, dict):
            return

        value = message.get(self._variable)
        if value is None:
            return
        try:
            value = float(value)
        except (TypeError, ValueError, OverflowError):
            return
        if not isfinite(value):
            return

        if value > self._threshold_trigger:
            await self._cancel_release()
            if not self._active:
                self._active = True
                self.bus.set_state('bypass_held', True)
                self.bus.send_message(PersistenceLevel.SYSTEM, 'bypass_user', 1)

                _LOGGER.debug(f"Set pressure bypass: {self._variable} at {value} greater than {self._threshold_trigger}")
                self.log("Pressure across the impactor is too high.  System bypassed.  Please check the manual ball valve.", {
                    'pressure': value,
                    'threshold': self._threshold_trigger,
                }, is_error=True)
        elif value < self._threshold_release:
            if self._active and self._release_task is None:
                _LOGGER.debug(f"Pressure returned to normal: {self._variable} at {value} less than {self._threshold_release}")
                self._release_task = background_task(self._release_after_hold())

    async def run(self) -> typing.NoReturn:
        try:
            e = asyncio.Event()
            await e.wait()
        finally:
            await self._cancel_release()
