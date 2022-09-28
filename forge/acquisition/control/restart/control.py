import typing
import asyncio
import logging
from forge.acquisition import CONFIGURATION
from ..base import BaseControl

_LOGGER = logging.getLogger(__name__)


class Control(BaseControl):
    CONTROL_TYPE = "restart"

    def __init__(self):
        super().__init__()
        self.restart_issued: typing.Optional[asyncio.Future] = None
        self.restart_command = CONFIGURATION.get(
            "ACQUISITION.RESTART_COMMAND",
            "systemctl --no-block restart forge-acquisition.service"
        )

    async def initialize(self):
        self.restart_issued = asyncio.get_event_loop().create_future()

    async def bus_message(self, source: str, record: str, message: typing.Any) -> None:
        if record != 'restart_acquisition':
            return
        if self.restart_issued is None:
            return

        notify = self.restart_issued
        self.restart_issued = None
        notify.set_result(True)

        if not self.restart_command:
            _LOGGER.warning("No restart command available")
            notify.set_result(False)
            return

        _LOGGER.info("Issuing restart command")

        process = await asyncio.create_subprocess_shell(self.restart_command,
                                                        stdin=asyncio.subprocess.DEVNULL)
        await process.wait()

        notify.set_result(True)

    async def run(self):
        if self.restart_issued is None:
            return
        wait = self.restart_issued
        await wait
