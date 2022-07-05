import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.modbus import ModbusProtocol
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument, BusInterface
from forge.acquisition.instrument.teledynen500.simulator import Simulator
from forge.acquisition.instrument.teledynen500.instrument import Instrument



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

    assert await bus.value('XNO2') == simulator.data_XNO2
    assert await bus.value('XNO') == simulator.data_XNO
    assert await bus.value('XNOx') == simulator.data_XNOx
    assert await bus.value('Tmanifold') == simulator.data_Tmanifold
    assert await bus.value('Toven') == simulator.data_Toven
    assert await bus.value('Tbox') == simulator.data_Tbox
    assert await bus.value('Psample') == pytest.approx(simulator.data_Psample)
    assert await bus.value('PCTmanifold') == simulator.data_PCTmanifold
    assert await bus.value('PCToven') == simulator.data_PCToven
    assert await bus.value('Bax') == simulator.data_Bax

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)
