import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument, BusInterface
from forge.acquisition.instrument.tsi375xcpc.simulator import Simulator
from forge.acquisition.instrument.tsi375xcpc.instrument import Instrument


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
    assert await bus.value('P') == simulator.data_P
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('Qinlet') == simulator.data_Qinlet
    assert await bus.value('PDnozzle') == simulator.data_PDnozzle
    assert await bus.value('PDorifice') == simulator.data_PDorifice
    assert await bus.value('Tsaturator') == simulator.data_Tsaturator
    assert await bus.value('Tcondenser') == simulator.data_Tcondenser
    assert await bus.value('Toptics') == simulator.data_Toptics
    assert await bus.value('Tcabinet') == simulator.data_Tcabinet
    assert await bus.value('Alaser') == simulator.data_Alaser
    assert await bus.value('liquid_level') == simulator.data_liquid_level

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_3752():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    simulator.model_number = "3752"
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('N') == simulator.data_N
    assert await bus.value('Vphoto') == simulator.data_Vphoto

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
    simulator.data_Q = 0.25
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q') == 0.5
    assert await bus.value('Qinstrument') == simulator.data_Q
    assert await bus.value('N') == simulator.data_N / 2.0

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


