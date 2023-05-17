import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'eeprom_error': CPD3Flag("EEPROMError", "Instrument unable to read or write to EEPROM", 0x00010000),
    'configuration_error': CPD3Flag("ConfigurationError", "Instrument saved configuration value out of range", 0x00020000),
    'rtc_reset': CPD3Flag("RTCReset", "Instrument internal RTC time has been reset (check RTC battery)", 0x00040000),
    'rtc_error': CPD3Flag("RTCError", "Instrument unable to communicate with RTC chip", 0x00080000),
    'sdcard_error': CPD3Flag("SDCardError", "Instrument SD card error", 0x00100000),
    'sdcard_format_error': CPD3Flag("SDCardFormatError", "Instrument SD card filesystem is not FAT32", 0x00200000),
    'sdcard_full': CPD3Flag("SDCardFull", "Instrument SD card is full", 0x00400000),
    'saturator_pump_warning': CPD3Flag("SaturatorPumpWarning", "Saturator pump at maximum power", 0x00800000),
    'liquid_low': CPD3Flag("LiquidLow", "Instrument reporting low butanol level", 0x01000000),
    'temperature_control_error': CPD3Flag("TemperatureControlError", "Condenser temperature greater than inlet temperature (check condenser fan)", 0x02000000),
    'overheating': CPD3Flag("Overheating", "Condenser temperature greater than 45°C", 0x04000000),
    'optics_thermistor_error': CPD3Flag("OpticsThermistorError", "Optics thermistor malfunctioning or out of range", 0x08000000),
    'condenser_thermistor_error': CPD3Flag("CondenserThermistorError", "Condenser thermistor malfunctioning or out of range", 0x10000000),
    'saturator_top_thermistor_error': CPD3Flag("SaturatorTopThermistorError", "Saturator top thermistor malfunctioning or out of range", 0x20000000),
    'saturator_bottom_thermistor_error': CPD3Flag("SaturatorBottomThermistorError", "Saturator bottom thermistor malfunctioning or out of range", 0x40000000),
    'inlet_thermistor_error': CPD3Flag("InletThermistorError", "Inlet thermistor malfunctioning or out of range", 0x80000000),
}
