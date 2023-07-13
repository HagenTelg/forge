import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'spot_advancing': CPD3Flag("FilterChanging", "Changing filter", 0x0000010000),
    'zero': CPD3Flag("Zero", "Zero in progress", 0x0000020000),
    'pump_off': CPD3Flag("PumpOff", "Pump turned off", 0x0000080000),
    'manual_operation': CPD3Flag("ManualOperation", "Manual operation, offline with keyboard enabled", 0x0000100000),
    'calibration_enabled': CPD3Flag("CalibrationEnabled", "Calibration enabled", 0x0000200000),
    'mains_on': CPD3Flag("MainsOn", "Mains on", 0x0000800000),
    'led_too_weak': CPD3Flag("LEDWeak", "LED signal too weak warning"),
    'memory_error': CPD3Flag("MemoryError", "Memory system error", 0x0100000000),
    'mechanical_error': CPD3Flag("MechanicalError", "Mechanical system error", 0x0200000000),
    'pressure_error': CPD3Flag("PressureError", "Pressure measurement error", 0x0400000000),
    'flow_error': CPD3Flag("FlowError", "Flow measurement error", 0x0800000000),
    'detector_error': CPD3Flag("DetectorError", "Photodetector error", 0x1000000000),
    'temperature_error': CPD3Flag("TemperatureError", "Temperature measurement error", 0x2000000000),
}
