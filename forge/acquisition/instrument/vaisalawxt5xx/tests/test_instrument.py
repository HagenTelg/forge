import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.vaisalawxt5xx.simulator import Simulator
from forge.acquisition.instrument.vaisalawxt5xx.instrument import Instrument


@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('WS') == simulator.data_WS
    assert await bus.value('WD') == simulator.data_WD
    assert await bus.value('WSgust') == simulator.data_WSgust
    assert await bus.value('WI') == simulator.data_WI
    assert await bus.value('P') == simulator.data_P
    assert await bus.value('Uambient') == simulator.data_Uambient
    assert await bus.value('Tambient') == simulator.data_Tambient
    assert await bus.value('Tinternal') == simulator.data_Tinternal
    assert await bus.value('Theater') == simulator.data_Theater
    assert await bus.value('Vsupply') == simulator.data_Vsupply
    assert await bus.value('Vreference') == simulator.data_Vreference
    assert await bus.value('Vheater') == simulator.data_Vheater

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
async def test_auxiliary():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus
    simulator.data_R = 15.0
    simulator.data_Ld = 5.0

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('WS') == simulator.data_WS
    assert await bus.value('R') == simulator.data_R
    assert await bus.value('Ld') == simulator.data_Ld

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
