import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes


station_modes = detach(aerosol_modes)


station_modes['aerosol-raw'].remove('aerosol-raw-wind')
station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')
station_modes['aerosol-editing'].remove('aerosol-editing-wind')
station_modes['aerosol-clean'].remove('aerosol-clean-wind')
station_modes['aerosol-avgh'].remove('aerosol-avgh-wind')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticalscattering2', "Humidified Optical"),
                                    'aerosol-raw-optical')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-opticalscattering2', "Humidified Optical"),
                                         'aerosol-realtime-optical')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-scattering2', "Humidified Scattering"),
                                        'aerosol-editing-backscattering')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-backscattering2', "Humidified Back Scattering"),
                                        'aerosol-editing-scattering2')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticalscattering2', "Humidified Optical"),
                                      'aerosol-clean-optical')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticalscattering2', "Humidified Optical"),
                                     'aerosol-avgh-optical')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-nephelometerzero2', "Wet Nephelometer Zero"),
                                    'aerosol-raw-nephelometerstatus')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-nephelometerstatus2', "Wet Nephelometer Status"),
                                    'aerosol-raw-nephelometerzero2')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-nephelometerzero2', "Wet Nephelometer Zero"),
                                         'aerosol-realtime-nephelometerstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-nephelometerstatus2', "Wet Nephelometer Status"),
                                         'aerosol-realtime-nephelometerzero2')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-humidograph', "Humidograph"),
                                    'aerosol-raw-green')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-humidograph', "Humidograph"),
                                         'aerosol-realtime-green')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-humidograph', "Humidograph"),
                                      'aerosol-clean-green')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-humidograph', "Humidograph"),
                                     'aerosol-avgh-green')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
