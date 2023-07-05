import typing
from ..default.flags import DashboardFlag, Severity


dashboard_flags: typing.Dict[str, DashboardFlag] = {
    'pulse_height_low': DashboardFlag(Severity.WARNING, "Pulse height low (less than 400mV)",
                                      "Wick wetting may be required", instrument_flag=False),
}
