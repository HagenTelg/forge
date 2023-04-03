import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'blank': CPD3Flag("Blank", "Data removed in blank period"),
    'zero': CPD3Flag("Zero", "Zero in progress", 0x20000000),
    'spancheck': CPD3Flag("Spancheck", "Spancheck in progress"),
    'calibration': CPD3Flag("Calibration", "Instrument calibration in progress"),
    'inconsistent_zero': CPD3Flag("InconsistentZero", "Inconsistent zeroing mode and data filter settings"),
    'backscatter_fault': CPD3Flag("BackscatterFault", "Inconsistent zeroing mode and data filter settings", 0x00010000),
    'backscatter_digital_fault': CPD3Flag("BackscatterDigitalFault", "Instrument reporting a backscatter digital IO fault condition", 0x00020000),
    'shutter_fault': CPD3Flag("ShutterFault", "Instrument reporting a shutter fault condition", 0x00040000),
    'light_source_fault': CPD3Flag("LightSourceFault", "Instrument reporting a light source fault condition", 0x00080000),
    'pressure_sensor_fault': CPD3Flag("PressureFault", "Instrument reporting a pressure sensor fault condition", 0x00100000),
    'enclosure_temperature_fault': CPD3Flag("EnclosureTemperatureFault", "Instrument reporting a enclosure temperature sensor fault condition", 0x00200000),
    'sample_temperature_fault': CPD3Flag("SampleTemperatureFault", "Instrument reporting a sample temperature sensor fault condition", 0x00400000),
    'rh_fault': CPD3Flag("RHFault", "Instrument reporting a RH sensor fault condition", 0x00800000),
    'pmt_fault': CPD3Flag("PMTFault", "Instrument reporting a PMT fault condition", 0x01000000),
    'warmup_fault': CPD3Flag("WarmupFault", "Instrument reporting a failure during warm up", 0x02000000),
    'backscatter_high_warning': CPD3Flag("BackscattterHighWarning", "Instrument reporting a high backscatter warning condition", 0x04000000),
    'system_fault': CPD3Flag("SystemFault", "Instrument reporting a system fault condition", 0x80000000),
}
