import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.dmtccn.simulator import Simulator
from forge.acquisition.instrument.dmtccn.instrument import Instrument



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
    assert await bus.value('Ttec1') == simulator.data_Ttec1
    assert await bus.value('Ttec2') == simulator.data_Ttec2
    assert await bus.value('Ttec3') == simulator.data_Ttec3
    assert await bus.value('Tsample') == simulator.data_Tsample
    assert await bus.value('Topc') == simulator.data_Topc
    assert await bus.value('Tinlet') == simulator.data_Tinlet
    assert await bus.value('Tnafion') == simulator.data_Tnafion
    assert await bus.value('DTsetpoint') == simulator.data_DTsetpoint
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('Qsheath') == simulator.data_Qsheath
    assert await bus.value('SSset') == simulator.data_SSset
    assert await bus.value('P') == simulator.data_P
    assert await bus.value('Vmonitor') == simulator.data_Vmonitor
    assert await bus.value('Vvalve') == simulator.data_Vvalve
    assert await bus.value('Alaser') == simulator.data_Alaser
    assert await bus.value('minimum_bin_number') == simulator.data_minimum_bin_number
    assert await bus.value('dN') == simulator.data_dN

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
async def test_single_line():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    simulator.record_join = b","
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('N') == simulator.data_N
    assert await bus.value('Ttec1') == simulator.data_Ttec1
    assert await bus.value('Ttec2') == simulator.data_Ttec2
    assert await bus.value('Ttec3') == simulator.data_Ttec3
    assert await bus.value('Tsample') == simulator.data_Tsample
    assert await bus.value('Topc') == simulator.data_Topc
    assert await bus.value('Tinlet') == simulator.data_Tinlet
    assert await bus.value('Tnafion') == simulator.data_Tnafion
    assert await bus.value('DTsetpoint') == simulator.data_DTsetpoint
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('Qsheath') == simulator.data_Qsheath
    assert await bus.value('SSset') == simulator.data_SSset
    assert await bus.value('P') == simulator.data_P
    assert await bus.value('Vmonitor') == simulator.data_Vmonitor
    assert await bus.value('Vvalve') == simulator.data_Vvalve
    assert await bus.value('Alaser') == simulator.data_Alaser
    assert await bus.value('minimum_bin_number') == simulator.data_minimum_bin_number
    assert await bus.value('dN') == simulator.data_dN

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
            'Q': 0.3,
        },
    })
    simulator.data_Q = 0.15
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q') == 0.3
    assert await bus.value('N') == simulator.data_N / 2.0
    assert await bus.value('dN') == [n / 2.0 for n in simulator.data_dN]

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

