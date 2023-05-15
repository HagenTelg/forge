import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'liquid_low': CPD3Flag("LiquidLow", "Instrument reporting low butanol level", 0x20000),
    'not_ready': CPD3Flag("InstrumentNotReady", "Instrument reporting a not-ready condition (laser, temperature, flow or liquid out of expected range)", 0x10000),
    'vacuum_low': CPD3Flag("LowVacuum", "Instrument reporting low vacuum pressure (less than 12 inHg)", 0x40000),
}
