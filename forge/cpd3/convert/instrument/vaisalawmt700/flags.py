import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'temperature_sensor_1_failure': CPD3Flag("TemperatureSensor1Failure", "Temperature sensor 1 failed", 0x00010000),
    'temperature_sensor_2_failure': CPD3Flag("TemperatureSensor2Failure", "Temperature sensor 2 failed", 0x00020000),
    'temperature_sensor_3_failure': CPD3Flag("TemperatureSensor3Failure", "Temperature sensor 3 failed", 0x00040000),
    'heater_failure': CPD3Flag("HeaterFailure", "Heater failure", 0x00080000),
    'supply_voltage_high': CPD3Flag("HighSupplyVoltage", "Supply voltage too high", 0x00100000),
    'supply_voltage_low': CPD3Flag("LowSupplyVoltage", "Supply voltage too low", 0x00200000),
    'wind_speed_high': CPD3Flag("WindSpeedHigh", "Wind speed too high", 0x00400000),
    'sonic_temperature_out_of_range': CPD3Flag("SonicTemperatureOutOfRange", "Sonic temperature out of range", 0x00800000),
    'low_wind_validity': CPD3Flag("WindMeasurementSuspect", "Low wind measurement validity", 0x01000000),
    'blocked_sensor': CPD3Flag("BlockedSensor", "Blocked sensor", 0x04000000),
    'high_noise_level': CPD3Flag("HighNoise", "High noise level", 0x10000000),
}
