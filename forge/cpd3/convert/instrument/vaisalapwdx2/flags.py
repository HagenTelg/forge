import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'hardware_error': CPD3Flag("HardwareError", "Hardware error or warning condition present", 0x00010000),
    'backscatter_range': CPD3Flag("BackscatterAlarm", "Receiver or transmitter contamination signal exceeded alarm or warning threshold", 0x00040000),
    'transmitter_range': CPD3Flag("TransmitterError", "LED control signal it outside the range of -8V to 7V", 0x00080000),
    'power_range': CPD3Flag("PowerError", "Receiver or transmitter power supply is outside of 10V to 14V", 0x00100000),
    'offset_range': CPD3Flag("OffsetError", "Offset frequency is outside of 80Hz to 170Hz", 0x00200000),
    'signal_error': CPD3Flag("SignalError", "Signal and offset combine to an invalid value", 0x00400000),
    'receiver_range': CPD3Flag("ReceiverError", "Receiver backscatter signal too low or saturated", 0x00800000),
    'data_ram_error': CPD3Flag("DataRAMError", "Error in RAM read/write check", 0x01000000),
    'eeprom_error': CPD3Flag("EEPROMError", "EEPROM checksum error", 0x02000000),
    'temperature_range': CPD3Flag("TemperatureError", "Temperature out of range", 0x04000000),
    'rain_range': CPD3Flag("RainError", "Rain sensor too close to zero", 0x08000000),
    'luminance_range': CPD3Flag("LuminanceError", "PWL111 luminance signal out of range", 0x10000000),
    'offset_drift': CPD3Flag("OffsetDrifted", "Offset drifted"),
    'visibility_not_calibrated': CPD3Flag("VisiblityNotCalibrated", "Visibility calibration coefficient has not been changed from the factory default", 0x80000000),
}
