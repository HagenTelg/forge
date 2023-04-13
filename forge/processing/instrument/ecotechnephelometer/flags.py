import typing
from ..default.flags import DashboardFlag, Severity


dashboard_flags: typing.Dict[str, DashboardFlag] = {
    'backscatter_fault': DashboardFlag(Severity.ERROR, "Backscatter fault", "Scatterings suspect"),
    'backscatter_digital_fault': DashboardFlag(Severity.ERROR, "Backscatter digital IO fault", "Scatterings suspect"),
    'shutter_fault': DashboardFlag(Severity.ERROR, "Shutter fault", "Scatterings suspect"),
    'light_source_fault': DashboardFlag(Severity.ERROR, "Light source fault", "Scatterings suspect"),
    'pmt_fault': DashboardFlag(Severity.ERROR, "PMT fault", "Scatterings suspect"),
    'system_fault': DashboardFlag(Severity.ERROR, "Unclassified system fault"),
    'pressure_sensor_fault': DashboardFlag(Severity.WARNING, "Pressure sensor fault"),
    'enclosure_temperature_fault': DashboardFlag(Severity.WARNING, "Enclosure temperature sensor fault"),
    'sample_temperature_fault': DashboardFlag(Severity.WARNING, "Sample temperature sensor fault"),
    'rh_fault': DashboardFlag(Severity.WARNING, "RH sensor fault"),
    'warmup_fault': DashboardFlag(Severity.WARNING, "Warm up fault"),
    'backscatter_high_warning': DashboardFlag(Severity.WARNING, "Backscatter high warning"),
    'inconsistent_zero': DashboardFlag(Severity.WARNING, "Inconsistent zero and data filter settings selected"),

    'rh_suspect': DashboardFlag(Severity.ERROR, "Internal relative humidity suspect",
                                "Check for condensation and sensor integrity", instrument_flag=False),
}

