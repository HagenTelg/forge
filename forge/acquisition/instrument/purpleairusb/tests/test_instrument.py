import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.purpleairusb.simulator import Simulator
from forge.acquisition.instrument.purpleairusb.instrument import Instrument


@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    simulator.unpolled_interval = 1.0
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Xa') == simulator.data_Xa
    assert await bus.value('Xb') == simulator.data_Xb
    assert await bus.value('IBsa') == simulator.data_IBsa
    assert await bus.value('IBsb') == simulator.data_IBsb
    assert await bus.value('T') == pytest.approx(simulator.data_T, abs=0.1)
    assert await bus.value('U') == simulator.data_U
    assert await bus.value('P') == simulator.data_P

    instrument_run.cancel()
    simulator_run.cancel()
    try:
        await instrument_run
    except asyncio.CancelledError:
        pass
    try:
        await simulator_run
    except asyncio.CancelledError:
        pass


