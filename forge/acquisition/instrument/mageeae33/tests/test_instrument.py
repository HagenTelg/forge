import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.mageeae33.simulator import Simulator
from forge.acquisition.instrument.mageeae33.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q1') == simulator.data_Q1
    assert await bus.value('Q2') == simulator.data_Q2
    assert await bus.value('Tcontroller') == simulator.data_Tcontroller
    assert await bus.value('Tsupply') == simulator.data_Tsupply
    assert await bus.value('Tled') == simulator.data_Tled
    assert await bus.state('Fn') == simulator.data_Fn

    assert await bus.value('X1') == simulator.data_X1
    assert await bus.value('Xa1') == simulator.data_Xa1
    assert await bus.value('Xb1') == simulator.data_Xb1
    assert await bus.value('k1') == simulator.data_k1
    assert await bus.value('If1') == simulator.data_If1
    assert await bus.value('Ip1') == simulator.data_Ip1
    assert await bus.value('Ips1') == simulator.data_Ips1

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
async def test_spot_advance():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus
    simulator.data_Fn = 1
    simulator.parameters_data = simulator.parameters_data.strip()

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q1') == simulator.data_Q1
    assert await bus.state('Fn') == 1

    bus.command('spot_advance')
    bus.state_records.pop('Fn')
    assert await bus.state('Fn') == 2
    await bus.wait_for_notification('spot_advancing', is_set=False)

    assert await bus.value('Ir1') == 1.0
    assert await bus.value('Irs1') == 1.0

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


