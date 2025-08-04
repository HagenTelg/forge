import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_http_instrument, cleanup_http_instrument, BusInterface
from forge.acquisition.instrument.purpleairwifi.simulator import Simulator
from forge.acquisition.instrument.purpleairwifi.instrument import Instrument


@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_http_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Xa') == simulator.data_Xa
    assert await bus.value('Xb') == simulator.data_Xb
    assert await bus.value('IBsa') == simulator.data_IBsa
    assert await bus.value('IBsb') == simulator.data_IBsb
    assert await bus.value('T') == simulator.data_T
    assert await bus.value('U') == simulator.data_U
    assert await bus.value('P') == simulator.data_P

    await cleanup_http_instrument(simulator, instrument, instrument_run, simulator_run)

