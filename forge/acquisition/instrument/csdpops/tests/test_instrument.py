import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.csdpops.simulator import Simulator
from forge.acquisition.instrument.csdpops.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    dP = [0.1, 0.2, 0.3, 0.4, 0.5,
          0.6, 0.7, 0.8, 0.9, 1.0]
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'DIAMETER': dP,
    })
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('N') == simulator.data_N
    assert await bus.value('C') == simulator.data_C
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('P') == simulator.data_P
    assert await bus.value('Tpressure') == simulator.data_Tpressure
    assert await bus.value('Tinternal') == simulator.data_Tinternal
    assert await bus.value('Tlaser') == simulator.data_Tlaser
    assert await bus.value('Vsupply') == simulator.data_Vsupply
    assert await bus.value('Alaser') == simulator.data_Alaser
    assert await bus.value('peak_width') == simulator.data_peak_width
    assert await bus.value('laser_monitor') == simulator.data_laser_monitor
    assert await bus.value('laser_feedback') == simulator.data_laser_feedback
    assert await bus.value('baseline') == simulator.data_baseline
    assert await bus.value('baseline_stddev') == simulator.data_baseline_stddev
    assert await bus.value('baseline_threshold') == simulator.data_baseline_threshold
    assert await bus.value('baseline_stddevmax') == simulator.data_baseline_stddevmax
    assert await bus.value('pump_on_time') == simulator.data_pump_on_time
    assert await bus.value('pump_feedback') == simulator.data_pump_feedback
    assert await bus.value('dN') == simulator.data_dN
    assert bus.state_records['Dp'] == dP

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

