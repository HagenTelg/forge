import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.bmi1720cpc.simulator import Simulator
from forge.acquisition.instrument.bmi1720cpc.instrument import Instrument



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
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('Qsaturator') == simulator.data_Qsaturator
    assert await bus.value('Tinlet') == simulator.data_Tinlet
    assert await bus.value('Tsaturatorbottom') == simulator.data_Tsaturatorbottom
    assert await bus.value('Tsaturatortop') == simulator.data_Tsaturatortop
    assert await bus.value('Tcondenser') == simulator.data_Tcondenser
    assert await bus.value('Toptics') == simulator.data_Toptics
    assert await bus.value('PCTsaturatorbottom') == simulator.data_PCTsaturatorbottom
    assert await bus.value('PCTsaturatortop') == simulator.data_PCTsaturatortop
    assert await bus.value('PCTcondenser') == simulator.data_PCTcondenser
    assert await bus.value('PCToptics') == simulator.data_PCToptics
    assert await bus.value('PCTsaturatorpump') == simulator.data_PCTsaturatorpump

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

    await wait_cancelable(bus.wait_for_communicating(), 30)

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

