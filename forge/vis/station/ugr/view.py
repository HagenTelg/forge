import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.maap import MAAP5012Optical, MAAP5012Status
from ..default.aerosol.editing.maap import EditingMAA5012P
from ..default.aerosol.ccn import CCNStatus
from .pressure import Pressure


station_views = detach(aerosol_views)


station_views['aerosol-raw-maap'] = MAAP5012Optical('aerosol-raw')
station_views['aerosol-realtime-maap'] = MAAP5012Optical('aerosol-realtime', realtime=True)
station_views['aerosol-editing-maap'] = EditingMAA5012P('aerosol')
station_views['aerosol-clean-maap'] = MAAP5012Optical('aerosol-clean')
station_views['aerosol-avgh-maap'] = MAAP5012Optical('aerosol-avgh')
station_views['aerosol-raw-maapstatus'] = MAAP5012Status('aerosol-raw')
station_views['aerosol-realtime-maapstatus'] = MAAP5012Status('aerosol-realtime', realtime=True)

measurements = OrderedDict([
    ('{code}nephinlet', '{code}u_S11 (dry inlet)'),
    ('{code}neph', '{code}_S11 (dry sample)'),
    ('{code}dryneph', '{code}_V11 (dry neph internal)'),
    ('{code}humidiferinlet', '{code}_V12 (humidifier inlet)'),
    ('{code}humidiferoutlet', '{code}_V13 (humidifier outlet)'),
    ('{code}nephinlet2', '{code}u_S12 (wet inlet)'),
    ('{code}neph2', '{code}_S12 (wet sample)'),
    ('{code}wetneph', '{code}_V14 (wet neph internal)'),
])
omit_traces = {'TDnephinlet', 'TDnephinlet2'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=measurements, 
                                                       omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', measurements=measurements, 
                                                            omit_traces=omit_traces, realtime=True)

station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = Pressure('aerosol-realtime', realtime=True)

station_views['aerosol-raw-ccnstatus'] = CCNStatus('aerosol-raw')
station_views['aerosol-realtime-ccnstatus'] = CCNStatus('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
