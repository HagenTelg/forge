import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes


station_modes = detach(aerosol_modes)

station_modes['aerosol-raw'].remove('aerosol-raw-wind')
station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')
station_modes['aerosol-editing'].remove('aerosol-editing-wind')
station_modes['aerosol-clean'].remove('aerosol-clean-wind')
station_modes['aerosol-avgh'].remove('aerosol-avgh-wind')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticalcosmos', "COSMOS CLAP Optical"),
                                    'aerosol-raw-optical')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-opticalcosmos', "COSMOS CLAP Optical"),
                                         'aerosol-realtime-optical')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-cosmosclap', "COSMOS CLAP"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticalcosmos', "COSMOS CLAP Optical"),
                                      'aerosol-clean-optical')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticalcosmos', "COSMOS CLAP Optical"),
                                     'aerosol-avgh-optical')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-clapstatuscosmos', "COSMOS CLAP Status"),
                                    'aerosol-raw-clapstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-clapstatuscosmos', "COSMOS CLAP Status"),
                                         'aerosol-realtime-clapstatus')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-tca', "TCA"),
                                    'aerosol-raw-opticalcosmos')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-tca', "TCA"),
                                         'aerosol-realtime-opticalcosmos')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-tca', "TCA"),
                                        'aerosol-editing-cosmosclap')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-tca', "TCA"),
                                      'aerosol-clean-opticalcosmos')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-tca', "TCA"),
                                     'aerosol-avgh-opticalcosmos')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-tcastatus', "TCA Status"),
                                    'aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-tcastatus', "TCA Status"),
                                         'aerosol-realtime-cpcstatus')



def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
