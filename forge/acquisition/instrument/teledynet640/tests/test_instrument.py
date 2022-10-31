import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.modbus import ModbusProtocol
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.teledynet640.simulator import Simulator
from forge.acquisition.instrument.teledynet640.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'MODBUS': {
            'PROTOCOL': 'TCP',
        },
    })
    simulator.protocol = ModbusProtocol.TCP
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('X1') == simulator.data_X1
    assert await bus.value('X25') == simulator.data_X25
    assert await bus.value('X10') == simulator.data_X10
    assert await bus.value('Pambient') == simulator.data_Pambient
    assert await bus.value('Tsample') == simulator.data_Tsample
    assert await bus.value('Tambient') == simulator.data_Tambient
    assert await bus.value('Tasc') == simulator.data_Tasc
    assert await bus.value('Tled') == simulator.data_Tled
    assert await bus.value('Tbox') == simulator.data_Tbox
    assert await bus.value('Usample') == simulator.data_Usample
    assert await bus.value('Qsample') == simulator.data_Qsample
    assert await bus.value('Qbypass') == simulator.data_Qbypass
    assert await bus.value('spandev') == simulator.data_spandev
    assert await bus.value('PCTpump') == simulator.data_PCTpump
    assert await bus.value('PCTvalve') == simulator.data_PCTvalve
    assert await bus.value('PCTasc') == simulator.data_PCTasc
    assert await bus.value('spandev') == simulator.data_spandev

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
async def test_no_pm1():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    simulator.data_X1 = None
    simulator.flags = 0b1
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('X25') == simulator.data_X25
    assert await bus.value('X10') == simulator.data_X10
    await bus.wait_for_notification('box_temperature_out_of_range')
    assert 'X1' not in bus.data_values

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
