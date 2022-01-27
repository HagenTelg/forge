import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes


station_modes = detach(aerosol_modes)


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-cpcstatus2', "Second CPC Status"),
                                    'aerosol-raw-cpcstatus')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
