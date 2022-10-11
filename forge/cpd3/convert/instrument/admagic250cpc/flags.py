import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'conditioner_temperature_out_of_range': CPD3Flag("ConditionerTemperatureOutOfRange", "Conditioner (1st stage) temperature out of range", 0x00010000),
    'initiator_temperature_out_of_range': CPD3Flag("InitiatorTemperatureOutOfRange", "Initiator (2nd stage) temperature out of range", 0x00020000),
    'moderator_temperature_out_of_range': CPD3Flag("ModeratorTemperatureOutOfRange", "Moderator (3rd stage) temperature out of range", 0x00040000),
    'optics_temperature_out_of_range': CPD3Flag("OpticsTemperatureOutOfRange", "Optics head temperature out of range", 0x00080000),
    'laser_off': CPD3Flag("LaserOff", "Laser off", 0x00100000),
    'pump_off': CPD3Flag("PumpOff", "Pump off", 0x00200000),
    'rh_data_stale': CPD3Flag("RHDataStale", "RH data not updated since the last read", 0x00400000),
    'i2c_communication_error': CPD3Flag("I2CCommunicationFailure", "I²C communication error", 0x00800000),
    'rh_sensor_error': CPD3Flag("RHSensorError", "RH sensor error", 0x01000000),
    'overheat': CPD3Flag("Overheat", "Overheating detected, TECs and optics turned off", 0x02000000),
    'dry_wick': CPD3Flag("DryWick", "Dry wick, wick recovery in progresss, data is invalid", 0x04000000),
    'fallback_humidifier_dewpoint': CPD3Flag("FallbackHumidifierDewpoint", "Fallback calculation being used for humidifer dew point", 0x08000000),
    'dewpoint_calculation_error': CPD3Flag("DewpointCalculationError", "Dewpoint calculation error", 0x10000000),
    'wick_sensor_out_of_range': CPD3Flag("WickSensorOutOfRange", "Wick sensor out of range", 0x20000000),
    'flash_full': CPD3Flag("FlashFull", "Flash memory full", 0x40000000),
    'fram_data_invalid': CPD3Flag("FRAMDataInvalid", "FRAM data invalid", 0x80000000),
    'thermistor_fault': CPD3Flag("BadThermistor", "Bad thermistor reading", 0x200000000),
    'sample_flow_out_of_range': CPD3Flag("SampleFlowOutOfRange", "Sample flow out of range", 0x400000000),
    'ic2_multiplexer_error': CPD3Flag("I2CMultiplexerError", "I²C multiplexer error", 0x1000000000),
    'low_clock_battery': CPD3Flag("LowClockBattery", "Low clock battery", 0x2000000000),
    'clock_stopped': CPD3Flag("ClockStopped", "Instrument clock stopped", 0x4000000000),
}
