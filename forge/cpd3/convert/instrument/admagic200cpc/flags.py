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
    'moderator_in_absolute_mode': CPD3Flag("ModeratorInAbsoluteMode", "Moderator switched to absolute mode due to RH sensor failure", 0x08000000),
    'water_pump_activated': CPD3Flag("WaterPumpActivated", "Water extraction pump activated", 0x10000000),
    'invalid_flash_record': CPD3Flag("InvalidFlashRecord", "Invalid flash record", 0x20000000),
    'flash_full': CPD3Flag("FlashFull", "Flash memory full", 0x40000000),
    'fram_data_invalid': CPD3Flag("FRAMDataInvalid", "FRAM data invalid", 0x80000000),
}
