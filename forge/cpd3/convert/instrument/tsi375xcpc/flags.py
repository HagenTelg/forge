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

    'saturator_temperature_warning': CPD3Flag("SaturatorTemperatureWarning", "Saturator temperature warning detected"),
    'condenser_temperature_warning': CPD3Flag("CondenserTemperatureWarning", "Condenser temperature warning detected"),
    'optics_temperature_warning': CPD3Flag("OpticsTemperatureWarning", "Optics temperature warning detected"),
    'water_trap_temperature_error': CPD3Flag("WaterTrapTemperatureError", "Water trap temperature error detected"),
    'water_trap_temperature_warning': CPD3Flag("WaterTrapTemperatureWarning", "Water trap temperature warning detected"),
    'orifice_pressure_drop_error': CPD3Flag("OrificePressureDropError", "Orifice pressure drop error detected"),
    'orifice_pressure_drop_warning': CPD3Flag("OrificePressureDropWarning", "Orifice pressure drop warning detected"),
    'nozzle_pressure_drop_error': CPD3Flag("NozzlePressureDropError", "Nozzle pressure drop error detected"),
    'nozzle_pressure_drop_warning': CPD3Flag("NozzlePressureDropWarning", "Nozzle pressure drop warning detected"),
    'inlet_pressure_drop_error': CPD3Flag("InletPressureDropError", "Inlet pressure drop error detected"),
    'inlet_pressure_drop_warning': CPD3Flag("InletPressureDropWarning", "Inlet pressure drop warning detected"),
    'sample_flow_warning': CPD3Flag("SampleFlowWarning", "Sample flow warning detected"),
    'pulse_height_error': CPD3Flag("PulseHeightError", "Pulse height error detected"),
    'pulse_height_warning': CPD3Flag("PulseHeightWarning", "Pulse height warning detected"),
    'case_temperature_error': CPD3Flag("CaseTemperatureError", "Case temperature error detected"),
    'absolute_pressure_error': CPD3Flag("AbsolutePressureError", "Absolute pressure error detected"),
    'instrument_tilt_detected': CPD3Flag("InstrumentTiltDetected", "Instrument tilt angle error detected"),
}
