import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'liquid_low': CPD3Flag("LiquidLow", "Instrument reporting low water level", 0x20000),
    'not_ready': CPD3Flag("InstrumentNotReady", "Instrument reporting a not-ready condition (laser, temperature, flow or liquid out of expected range)", 0x10000),
}
