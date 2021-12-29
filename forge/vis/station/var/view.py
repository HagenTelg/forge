import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.pressure import BasicPressure
from ..default.aerosol.ecotechnephelometer import NephelometerStatus
from ..default.aerosol.psap import PSAPStatus
from .optical import OpticalPSAP, EditingPSAP
from .green import Green


station_views = detach(aerosol_views)


station_views['aerosol-raw-opticalpsap'] = OpticalPSAP('aerosol-raw')
station_views['aerosol-realtime-opticalpsap'] = OpticalPSAP('aerosol-realtime', realtime=True)
station_views['aerosol-editing-psap'] = EditingPSAP()
station_views['aerosol-clean-opticalpsap'] = OpticalPSAP('aerosol-clean')
station_views['aerosol-avgh-opticalpsap'] = OpticalPSAP('aerosol-avgh')
station_views['aerosol-raw-psapstatus'] = PSAPStatus('aerosol-raw')
station_views['aerosol-realtime-psapstatus'] = PSAPStatus('aerosol-realtime', realtime=True)

station_views['aerosol-raw-green'] = Green('aerosol-raw')
station_views['aerosol-realtime-green'] = Green('aerosol-realtime', realtime=True)
station_views['aerosol-clean-green'] = Green('aerosol-clean')
station_views['aerosol-avgh-green'] = Green('aerosol-avgh')

measurements = OrderedDict([
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}nephcell', '{code}x_S11 (neph cell)'),
])
omit_traces = {'TDnephcell', 'Unephcell'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=measurements, 
                                                       omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', measurements=measurements, 
                                                            omit_traces=omit_traces, realtime=True)

station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = BasicPressure('aerosol-realtime', realtime=True)

station_views['aerosol-raw-nephelometerstatus'] = NephelometerStatus('aerosol-raw')
station_views['aerosol-realtime-nephelometerstatus'] = NephelometerStatus('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
