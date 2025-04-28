import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, aerosol_public, ozone_modes, ozone_public, met_modes, radiation_modes


station_modes = detach(aerosol_modes, aerosol_public, ozone_modes, ozone_public, met_modes, radiation_modes)

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-cpcstatus2', "MAGIC CPC Status N42"),
                                    'aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-cpcstatus2', "MAGIC CPC Status N42"),
                                         'aerosol-realtime-cpcstatus')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-cpcstatus3', "MAGIC CPC Status N44"),
                                    'aerosol-raw-cpcstatus2')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-cpcstatus3', "MAGIC CPC Status N44"),
                                         'aerosol-realtime-cpcstatus2')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-dmps', "DMPS"),
                                    'aerosol-raw-aethalometer')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-dmps', "DMPS"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-dmps', "DMPS"),
                                      'aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-dmps', "DMPS"),
                                     'aerosol-avgh-aethalometer')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-dmpsstatus', "DMPS Status"),
                                    'aerosol-raw-umacstatus')

station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')
station_modes['ozone-realtime'].remove('ozone-realtime-wind')

station_modes['met-raw'].insert(ViewList.Entry('met-raw-tower', "Tower dT"))
station_modes['met-editing'].insert(ViewList.Entry('met-editing-tower', "Tower dT"))


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
