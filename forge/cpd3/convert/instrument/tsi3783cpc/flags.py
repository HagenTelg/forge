import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'saturator_temperature_out_of_range': CPD3Flag("SaturatorTemperatureError", "Saturator temperature error (referenced as conditioner in the manual)", 0x00010000),
    'growth_tube_temperature_out_of_range': CPD3Flag("GrowthTubeTemperatureError", "Growth tube temperature error", 0x00020000),
    'optics_temperature_out_of_range': CPD3Flag("OpticsTemperatureError", "Optics temperature error", 0x00040000),
    'vacuum_error': CPD3Flag("VacuumError", "Vacuum level error", 0x00080000),
    'laser_error': CPD3Flag("LaserError", "Laser status error", 0x00200000),
    'liquid_low': CPD3Flag("LiquidLow", "Out of water condition present", 0x00400000),
    'concentration_out_of_range': CPD3Flag("ConcentrationOutOfRange", "Concentration out of range", 0x00800000),
    'pulse_height_error': CPD3Flag("PulseHeightFault", "Pulse height fault condition present", 0x01000000),
    'pressure_out_of_range': CPD3Flag("AbsolutePressureError", "Absolute pressure error", 0x02000000),
    'nozzle_pressure_error': CPD3Flag("NozzlePressureError", "Nozzle pressure error", 0x04000000),
    'seperator_temperature_out_of_range': CPD3Flag("WaterSeparatorTemperatureError", "Water separator temperature error error", 0x08000000),
    'warmup': CPD3Flag("WarmUpInProgress", "Warm-up period in progress", 0x10000000),
    'service_reminder': CPD3Flag("ServiceReminder", "Service reminder", 0x40000000),
}
