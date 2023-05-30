import typing
import asyncio
from forge.acquisition import CONFIGURATION, LayeredConfiguration
from ..base import BaseControl
from ...bus.client import PersistenceLevel
from ...cutsize import CutSize


class Control(BaseControl):
    CONTROL_TYPE = "impactor_cycle"

    class _Cycle(CutSize):
        class Active(CutSize.Active):
            def __init__(self, config: LayeredConfiguration):
                super().__init__(config)
                self.control: Control = None

            async def automatic_activation(self, now: float = None) -> bool:
                if not await CutSize.Active.automatic_activation(self, now):
                    return False

                self.control.bus.send_message(PersistenceLevel.STATE, 'active', {
                    'size': str(self.size),
                    'epoch_ms': round(self.last_time * 1000.0) if self.last_time else None,
                })

                next = self.control.cycle.next(now)
                if next:
                    self.control.bus.send_message(PersistenceLevel.STATE, 'next', {
                        'size': str(next.size),
                        'epoch_ms': round(next.next_time * 1000.0) if next.next_time else None,
                    })
                else:
                    self.control.bus.send_message(PersistenceLevel.STATE, 'next', None)

                return True

    def __init__(self):
        super().__init__()
        cutsize = CONFIGURATION.get("ACQUISITION.CUT_SIZE")
        if not cutsize:
            self.cycle = self._Cycle(None, True)
        else:
            self.cycle = self._Cycle(LayeredConfiguration(cutsize))

        for a in self.cycle.active:
            a.control = self

    async def initialize(self):
        self.bus.send_message(PersistenceLevel.SOURCE, 'instrument', {
            'type': self.CONTROL_TYPE,
        })

    async def run(self):
        await self.cycle.automatic_activation()
