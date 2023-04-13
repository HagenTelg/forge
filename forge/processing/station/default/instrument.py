import typing
from forge.processing.instrument.lookup import instrument_data

if typing.TYPE_CHECKING:
    from forge.processing.instrument.default.flags import DashboardFlag


def dashboard_flag(station: str, instrument_id: str, instrument_type: str, flag: str) -> typing.Optional["DashboardFlag"]:
    return instrument_data(instrument_type, 'flags', 'dashboard_flags').get(flag)
