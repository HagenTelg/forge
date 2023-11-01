import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.modbus import ModbusProtocol
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument, BusInterface
from forge.acquisition.instrument.thermo49iq.simulator import Simulator
from forge.acquisition.instrument.thermo49iq.instrument import Instrument



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

    assert await bus.value('X') == simulator.data_X
    assert await bus.value('Tsample') == simulator.data_Tsample
    assert await bus.value('Tlamp') == simulator.data_Tlamp
    assert await bus.value('Psample') == pytest.approx(simulator.data_Psample)
    assert await bus.value('Ppump') == pytest.approx(simulator.data_Ppump)
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('Ca') == simulator.data_Ca
    assert await bus.value('Cag') == simulator.data_Cag
    assert await bus.value('Cb') == simulator.data_Cb
    assert await bus.value('Cbg') == simulator.data_Cbg
    assert await bus.value('Alamp') == simulator.data_Alamp
    assert await bus.value('Aheater') == simulator.data_Aheater

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)
