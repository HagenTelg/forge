import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument, BusInterface
from forge.acquisition.instrument.tech2b205.simulator import Simulator
from forge.acquisition.instrument.tech2b205.instrument import Instrument


@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 240)

    assert await bus.value('X') == simulator.data_X
    assert await bus.value('T') == simulator.data_T
    assert await bus.value('P') == simulator.data_P
    assert await bus.value('Q') == simulator.data_Q

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)
