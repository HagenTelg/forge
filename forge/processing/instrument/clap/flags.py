import typing
from ..default.flags import DashboardFlag, Severity


dashboard_flags: typing.Dict[str, DashboardFlag] = {
    'flow_error': DashboardFlag(Severity.ERROR, "Flow error", "Check valve and pump status"),
    'led_error': DashboardFlag(Severity.ERROR, "LED error"),
    'filter_was_not_white': DashboardFlag(Severity.ERROR, "Initial filter parameters where not close to white",
                                          "Change the filter and verify normal sampling if ongoing"),
    'temperature_out_of_range': DashboardFlag(Severity.ERROR, "Temperature out of range"),
}
