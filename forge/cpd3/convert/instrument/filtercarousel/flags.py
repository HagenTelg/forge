import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'cnc_high': CPD3Flag("ContaminateCPCHigh", "Data contaminated due to high particle concentration", 0x1),
    'cnc_spike': CPD3Flag("ContaminateCPCSpike", "Data contaminated due to a spike in particle concentration", 0x1),
    'wind_out_of_sector': CPD3Flag("ContaminateRealtimeWindDirection", "Data contaminated due to winds out of sector", 0x5),
    'wind_speed_low': CPD3Flag("ContaminateRealtimeWindSpeed", "Data contaminated due to low wind speed", 0x5),
    'carousel_change': CPD3Flag("AccumulatorChanging", "Accumulator change in progress"),
    'initial_blank': CPD3Flag("Blank", "Accumulated quanties added to blank"),
}
