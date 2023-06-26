import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.vaisalapwdx2.simulator import Simulator
from forge.acquisition.instrument.vaisalapwdx2.instrument import Instrument


@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('WZ') == simulator.data_WZ
    assert await bus.value('WZ10Min') == simulator.data_WZ10Min
    assert await bus.value('WI') == simulator.data_WI
    assert await bus.value('Tambient') == simulator.data_Tambient
    assert await bus.value('Tinternal') == simulator.data_Tinternal
    assert await bus.value('Tdrd') == simulator.data_Tdrd
    assert await bus.value('Csignal') == simulator.data_Csignal
    assert await bus.value('Coffset') == simulator.data_Coffset
    assert await bus.value('Cdrift') == simulator.data_Cdrift
    assert await bus.value('Cdrd') == simulator.data_Cdrd
    assert await bus.value('I') == simulator.data_I
    assert await bus.value('BsTx') == simulator.data_BsTx
    assert await bus.value('BsTxChange') == simulator.data_BsTxChange
    assert await bus.value('BsRx') == simulator.data_BsRx
    assert await bus.value('BsRxChange') == simulator.data_BsRxChange
    assert await bus.value('Vsupply') == simulator.data_Vsupply
    assert await bus.value('Vpositive') == simulator.data_Vpositive
    assert await bus.value('Vnegative') == simulator.data_Vnegative
    assert await bus.value('Vled') == simulator.data_Vled
    assert await bus.value('Vambient') == simulator.data_Vambient

    assert await bus.value('WX15Min') == simulator.data_WX15Min
    assert await bus.value('WX1Hour') == simulator.data_WX1Hour
    assert await bus.state('WX') == simulator.data_WX
    assert await bus.state('nws_code') == simulator.data_nws_code


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
