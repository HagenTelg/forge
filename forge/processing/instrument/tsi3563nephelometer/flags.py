import typing
from ..default.flags import DashboardFlag, Severity


dashboard_flags: typing.Dict[str, DashboardFlag] = {
    'lamp_power_error': DashboardFlag(Severity.WARNING, "Lamp power not within 10 percent of the setpoint"),
    'heater_unstable': DashboardFlag(Severity.WARNING, "Heater instability"),
    'valve_fault': DashboardFlag(Severity.ERROR, "Valve fault"),
    'pressure_out_of_range': DashboardFlag(Severity.ERROR, "Pressure out of range",
                                           "Check the sensor and connections"),
    'sample_temperature_out_of_range': DashboardFlag(Severity.ERROR, "Internal temperature out of range",
                                                     "Check the sensor and connections"),
    'inlet_temperature_out_of_range': DashboardFlag(Severity.ERROR, "Inlet temperature out of range",
                                                    "Check the sensor and connections"),
    'rh_suspect': DashboardFlag(Severity.ERROR, "Internal relative humidity suspect",
                                "Check for condensation and sensor integrity", instrument_flag=False),
    'lamp_voltage_high': DashboardFlag(Severity.ERROR, "Lamp voltage too high",
                                       "Lamp is likely burned out", instrument_flag=False),
    'shutter_fault': DashboardFlag(Severity.ERROR, "Shutter fault",
                                   "Scatterings suspect, motor service may be required", instrument_flag=False),
    'chopper_fault': DashboardFlag(Severity.ERROR, "Chopper fault",
                                   "Scatterings suspect, motor service may be required", instrument_flag=False),
}
