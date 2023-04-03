import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'blank': CPD3Flag("Blank", "Data removed in blank period", 0x40000000),
    'zero': CPD3Flag("Zero", "Zero in progress", 0x20000000),
    'spancheck': CPD3Flag("Spancheck", "Spancheck in progress"),
    'backscatter_disabled': CPD3Flag("BackscatterDisabled", "Backscatter shutter disabled"),
    'lamp_power_error': CPD3Flag("LampPowerError", "Zero in progress", 0x00010000),
    'valve_fault': CPD3Flag("ValveFault", "Valve fault detected", 0x00020000),
    'chopper_fault': CPD3Flag("ChopperFault", "Chopper fault detected", 0x00040000),
    'shutter_fault': CPD3Flag("ShutterFault", "Backscatter shutter fault detected", 0x00080000),
    'heater_unstable': CPD3Flag("HeaterUnstable", "Heater active but not stable", 0x00100000),
    'pressure_out_of_range': CPD3Flag("PressureOutOfRange", "Pressure out of range", 0x00200000),
    'sample_temperature_out_of_range': CPD3Flag("TemperatureOutOfRange", "Sample temperature out of range", 0x00400000),
    'inlet_temperature_out_of_range': CPD3Flag("InletTemperatureOutOfRange", "Inlet temperature out of range", 0x00800000),
    'rh_out_of_range': CPD3Flag("RHOutOfRange", "Relative humidity out of range", 0x01000000),
}
