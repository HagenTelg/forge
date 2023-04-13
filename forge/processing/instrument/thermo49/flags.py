import typing
from ..default.flags import DashboardFlag, Severity


dashboard_flags: typing.Dict[str, DashboardFlag] = {
    'alarm_sample_temperature_low': DashboardFlag(Severity.WARNING, "Sample temperature too low"),
    'alarm_sample_temperature_high': DashboardFlag(Severity.WARNING, "Sample temperature too high"),
    'alarm_lamp_temperature_low': DashboardFlag(Severity.WARNING, "Lamp temperature too low"),
    'alarm_lamp_temperature_high': DashboardFlag(Severity.WARNING, "Lamp temperature too high"),
    'alarm_ozonator_temperature_low': DashboardFlag(Severity.WARNING, "Ozonator temperature too low"),
    'alarm_ozonator_temperature_high': DashboardFlag(Severity.WARNING, "Ozonator temperature too high"),
    'alarm_pressure_low': DashboardFlag(Severity.WARNING, "Pressure too low"),
    'alarm_pressure_high': DashboardFlag(Severity.WARNING, "Pressure too high"),
    'alarm_flow_a_low': DashboardFlag(Severity.WARNING, "Cell A flow too low"),
    'alarm_flow_a_high': DashboardFlag(Severity.WARNING, "Cell A flow too high"),
    'alarm_intensity_a_low': DashboardFlag(Severity.WARNING, "Cell A count rate too low"),
    'alarm_intensity_a_high': DashboardFlag(Severity.WARNING, "Cell A count rate too high"),
    'alarm_flow_b_low': DashboardFlag(Severity.WARNING, "Cell B flow too low"),
    'alarm_flow_b_high': DashboardFlag(Severity.WARNING, "Cell B flow too high"),
    'alarm_intensity_b_low': DashboardFlag(Severity.WARNING, "Cell B count rate too low"),
    'alarm_intensity_b_high': DashboardFlag(Severity.WARNING, "Cell B count rate too high"),
    'alarm_ozone_low': DashboardFlag(Severity.WARNING, "Ozone concentration too low"),
    'alarm_ozone_high': DashboardFlag(Severity.WARNING, "Ozone concentration too high"),
}
