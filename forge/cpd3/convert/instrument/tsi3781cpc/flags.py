import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'concentration_out_of_range': CPD3Flag("ConcentrationOutOfRange", "Concentration out of range", 0x00010000),
    'sample_flow_error': CPD3Flag("SampleFlowError", "Sample flow out of range", 0x00020000),
    'nozzle_flow_error': CPD3Flag("NozzleFlowError", "Nozzle flow error present", 0x00040000),
    'pressure_out_of_range': CPD3Flag("PressureError", "Nozzle flow error present", 0x00080000),
    'temperature_out_of_range': CPD3Flag("TemperatureError", "Nozzle flow error present", 0x00100000),
    'warmup': CPD3Flag("WarmUpInProgress", "Warm-up period in progress", 0x00200000),
    'tilt_error': CPD3Flag("TiltError", "Instrument tilted beyond 45 degrees", 0x00400000),
    'laser_current_error': CPD3Flag("LaserCurrentError", "Laser current error", 0x00800000),
    'water_valve_open': CPD3Flag("LiquidValveOpen", "Water fill valve open", 0x01000000),
    'liquid_low': CPD3Flag("LiquidLow", "Out of water condition present", 0x02000000),
}
