import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument, BusInterface
from forge.acquisition.instrument.tsi3010cpc.simulator import Simulator
from forge.acquisition.instrument.tsi3010cpc.instrument import Instrument


@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('N') == simulator.data_N
    assert await bus.value('C') == simulator.data_C
    assert await bus.value('Tsaturator') == simulator.data_Tsaturator
    assert await bus.value('Tcondenser') == simulator.data_Tcondenser

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_flow_configuration():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'DATA': {
            'Q': 0.5,
        },
    })
    simulator.data_Q = 0.5
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('N') == simulator.data_N
    assert await bus.value('C') == simulator.data_C
    assert await bus.value('Tsaturator') == simulator.data_Tsaturator
    assert await bus.value('Tcondenser') == simulator.data_Tcondenser

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


