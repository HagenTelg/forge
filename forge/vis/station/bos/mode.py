import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes


station_modes = detach(aerosol_modes, ozone_modes)

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-dmpsstatus', "DMPS Status"),
                                    'aerosol-raw-cpcstatus')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-popsstatus', "POPS Status"),
                                    'aerosol-raw-dmpsstatus')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
