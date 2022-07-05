import typing
from ..default.flags import DashboardFlag, Severity


dashboard_flags: typing.Dict[str, DashboardFlag] = {
    'caps_board_communication_error': DashboardFlag(Severity.ERROR, "Error communicating with CAPS board"),
    'cell_pressure_out_of_range': DashboardFlag(Severity.WARNING, "Measurement cell pressure out of range"),
    'reference_out_of_range': DashboardFlag(Severity.WARNING, "Reference measurement too high"),
    'ozone_pressure_out_of_range': DashboardFlag(Severity.WARNING, "Ozone pressure out of range"),
    'ozone_tower_communications_error': DashboardFlag(Severity.ERROR, "Error communicating with Ozone tower board"),
    'sample_flow_out_of_range': DashboardFlag(Severity.WARNING, "Sample flow out of range"),
    'sample_pressure_out_of_range': DashboardFlag(Severity.WARNING, "Sample pressure out of range"),
    'sample_temperature_out_of_range': DashboardFlag(Severity.WARNING, "Sample temperature out of range"),
}
