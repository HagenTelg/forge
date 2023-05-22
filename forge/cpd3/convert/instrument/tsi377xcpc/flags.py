import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'saturator_temperature_out_of_range': CPD3Flag("SaturatorTemperatureError", "Saturator temperature error detected", 0x00010000),
    'condenser_temperature_out_of_range': CPD3Flag("CondenserTemperatureError", "Condenser temperature error detected", 0x00020000),
    'optics_temperature_error': CPD3Flag("OpticsTemperatureError", "Optics temperature error detected", 0x00040000),
    'inlet_flow_error': CPD3Flag("InletFlowError", "Inlet flow error detected", 0x00080000),
    'sample_flow_error': CPD3Flag("SampleFlowError", "Sample flow error detected", 0x00100000),
    'laser_power_error': CPD3Flag("LaserPowerError", "Laser power error detected", 0x00200000),
    'liquid_low': CPD3Flag("LiquidLow", "Water level low", 0x00400000),
    'concentration_out_of_range': CPD3Flag("ConcentrationOutOfRange", "Concentration out of range", 0x00800000),
}
