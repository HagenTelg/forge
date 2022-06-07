import asyncio
import typing
import pytest
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.admagiccpc.simulator import Simulator
from forge.acquisition.instrument.admagiccpc.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await asyncio.wait_for(bus.wait_for_communicating(), 30)

    assert await bus.value('N') == simulator.data_N
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('Qinstrument') == simulator.data_Q
    assert await bus.value('Clower') == simulator.data_Clower
    assert await bus.value('Cupper') == simulator.data_Cupper
    assert await bus.value('P') == simulator.data_P
    assert await bus.value('PD') == simulator.data_PD
    assert await bus.value('V') == simulator.data_V

    assert await bus.value('Tinlet') == simulator.data_Tinlet
    assert await bus.value('Tconditioner') == simulator.data_Tconditioner
    assert await bus.value('Tinitiator') == simulator.data_Tinitiator
    assert await bus.value('Tmoderator') == simulator.data_Tmoderator
    assert await bus.value('Toptics') == simulator.data_Toptics
    assert await bus.value('Theatsink') == simulator.data_Theatsink
    assert await bus.value('Tpcb') == simulator.data_Tpcb
    assert await bus.value('Tcabinet') == simulator.data_Tcabinet
    assert await bus.value('Uinlet') == simulator.data_Uinlet
    assert await bus.value('TDinlet') == simulator.data_TDinlet

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

    await asyncio.wait_for(bus.wait_for_communicating(), 30)

    assert await bus.value('Q') == 0.5
    assert await bus.value('Qinstrument') == simulator.data_Q
    assert await bus.value('N') == simulator.data_N / 2.0

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

