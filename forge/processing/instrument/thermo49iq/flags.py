import typing
from ..default.flags import DashboardFlag, Severity


dashboard_flags: typing.Dict[str, DashboardFlag] = {
    'alarm_intensity_a_high': DashboardFlag(Severity.WARNING, "Cell A count rate too high"),
    'alarm_intensity_b_high': DashboardFlag(Severity.WARNING, "Cell B count rate too high"),
    'lamp_temperature_short': DashboardFlag(Severity.WARNING, "Lamp temperature sensor short circuit"),
    'lamp_temperature_open': DashboardFlag(Severity.WARNING, "Lamp temperature sensor open circuit"),
    'sample_temperature_short': DashboardFlag(Severity.WARNING, "Sample temperature sensor short circuit"),
    'sample_temperature_open': DashboardFlag(Severity.WARNING, "Sample temperature sensor open circuit"),
    'lamp_connection_alarm': DashboardFlag(Severity.ERROR, "Lamp connection alarm"),
    'lamp_short': DashboardFlag(Severity.ERROR, "Lamp short circuit"),
    'communications_alarm': DashboardFlag(Severity.WARNING, "Communications alarm"),
    'power_supply_alarm': DashboardFlag(Severity.WARNING, "Power supply alarm"),
    'lamp_current_alarm': DashboardFlag(Severity.WARNING, "Lamp current out of range"),
    'lamp_temperature_alarm': DashboardFlag(Severity.WARNING, "Lamp temperature out of range"),
    'sample_temperature_alarm': DashboardFlag(Severity.WARNING, "Sample temperature out of range"),
}
