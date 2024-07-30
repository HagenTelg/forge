import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes, met_modes, radiation_modes


station_modes = detach(aerosol_modes, ozone_modes, met_modes, radiation_modes)


station_modes['aerosol-raw'].remove('aerosol-raw-umacstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-umacstatus')

station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')
station_modes['ozone-realtime'].remove('ozone-realtime-wind')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticalscattering2', "Ecotech Optical"),
                                    'aerosol-raw-optical')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-opticalscattering2', "Ecotech Optical"),
                                         'aerosol-realtime-optical')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticalscattering2', "Ecotech Optical"),
                                      'aerosol-clean-optical')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticalscattering2', "Ecotech Optical"),
                                     'aerosol-avgh-optical')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-nephelometerzero2', "Ecotech Nephelometer Zero"),
                                    'aerosol-raw-nephelometerstatus')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-nephelometerstatus2', "Ecotech Nephelometer Status"),
                                    'aerosol-raw-nephelometerzero2')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-nephelometerzero2', "Ecotech Nephelometer Zero"),
                                    'aerosol-realtime-nephelometerstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-nephelometerstatus2', "Ecotech Nephelometer Status"),
                                    'aerosol-realtime-nephelometerzero2')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-scattering2', "Ecotech Scattering"),
                                        'aerosol-editing-backscattering')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-backscattering2', "Ecotech Back Scattering"),
                                        'aerosol-editing-scattering2')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
