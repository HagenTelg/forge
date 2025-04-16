import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, aerosol_public


station_modes = detach(aerosol_modes, aerosol_public)

station_modes.pop("public-aerosolshort", None)
station_modes.pop("public-aerosollong", None)


station_modes['aerosol-raw'].remove('aerosol-raw-aethalometer')
station_modes['aerosol-raw'].remove('aerosol-raw-aethalometerstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometer')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometerstatus')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometer')
station_modes['aerosol-clean'].remove('aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].remove('aerosol-avgh-aethalometer')

station_modes['aerosol-raw'].remove('aerosol-raw-flow')
station_modes['aerosol-raw'].remove('aerosol-raw-umacstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-flow')
station_modes['aerosol-realtime'].remove('aerosol-realtime-umacstatus')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-smps', "SMPS"),
                                    'aerosol-raw-green')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-smps', "SMPS"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-smps', "SMPS"),
                                      'aerosol-clean-green')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-smps', "SMPS"),
                                     'aerosol-avgh-green')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
