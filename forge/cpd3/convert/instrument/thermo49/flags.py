import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'temperature_compensation': CPD3Flag("TemperatureCompensation", "Temperature compensation enabled", 0x080000000000),
    'pressure_compensation': CPD3Flag("PressureCompensation", "Pressure compensation enabled", 0x040000000000),
    'ozonator_on': CPD3Flag("OzonatorOn", "Ozonator lamp turned on", 0x001000000000),
    'alarm_sample_temperature_low': CPD3Flag("SampleTemperatureLowAlarm", "Sample temperature exceeded lower bound alarm threshold", 0x000000010000),
    'alarm_sample_temperature_high': CPD3Flag("SampleTemperatureHighAlarm", "Sample temperature exceeded upper bound alarm threshold", 0x000000020000),
    'alarm_lamp_temperature_low': CPD3Flag("LampTemperatureLowAlarm", "Lamp temperature exceeded lower bound alarm threshold", 0x000000040000),
    'alarm_lamp_temperature_high': CPD3Flag("LampTemperatureHighAlarm", "Lamp temperature exceeded upper bound alarm threshold", 0x000000080000),
    'alarm_ozonator_temperature_low': CPD3Flag("OzonatorTemperatureLowAlarm", "Ozonator lamp temperature exceeded lower bound alarm threshold", 0x000000100000),
    'alarm_ozonator_temperature_high': CPD3Flag("OzonatorTemperatureHighAlarm", "Ozonator lamp temperature exceeded upper bound alarm threshold", 0x000000200000),
    'alarm_pressure_low': CPD3Flag("PressureLowAlarm", "Sample pressure exceeded lower bound alarm threshold", 0x000000400000),
    'alarm_pressure_high': CPD3Flag("PressureHighAlarm", "Sample pressure exceeded upper bound alarm threshold", 0x000000800000),
    'alarm_flow_a_low': CPD3Flag("FlowALowAlarm", "Cell A flow rate exceeded lower bound alarm threshold", 0x000001000000),
    'alarm_flow_a_high': CPD3Flag("FlowAHighAlarm", "Cell A flow rate exceeded upper bound alarm threshold", 0x000002000000),
    'alarm_flow_b_low': CPD3Flag("FlowBLowAlarm", "Cell B flow rate exceeded lower bound alarm threshold", 0x000004000000),
    'alarm_flow_b_high': CPD3Flag("FlowBHighAlarm", "Cell B flow rate exceeded upper bound alarm threshold", 0x000008000000),
    'alarm_intensity_a_low': CPD3Flag("IntensityALowAlarm", "Cell A intensity exceeded lower bound alarm threshold", 0x000010000000),
    'alarm_intensity_a_high': CPD3Flag("IntensityAHighAlarm", "Cell A intensity exceeded upper bound alarm threshold", 0x000020000000),
    'alarm_intensity_b_low': CPD3Flag("IntensityBLowAlarm", "Cell B intensity exceeded lower bound alarm threshold", 0x000040000000),
    'alarm_intensity_b_high': CPD3Flag("IntensityBHighAlarm", "Cell B intensity exceeded upper bound alarm threshold", 0x000080000000),
    'alarm_ozone_low': CPD3Flag("OzoneLowAlarm", "Ozone concentration exceeded lower bound alarm threshold", 0x400000000000),
    'alarm_ozone_high': CPD3Flag("OzoneHighAlarm", "Ozone concentration exceeded upper bound alarm threshold", 0x800000000000),
    'service_mode': CPD3Flag("ServiceMode", "Service mode enabled", 0x200000000000),
}
