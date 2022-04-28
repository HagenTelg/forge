import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.maap import MAAP5012Optical, MAAP5012Status
from ..default.aerosol.editing.maap import EditingMAA5012
from .optical import OpticalScatteringSecondary, EditingScatteringSecondary, EditingBackScatteringSecondary
from .green import Green
from .ecotechnephelometer import NephelometerZeroSecondary, NephelometerStatusSecondary
from .pressure import Pressure


station_views = detach(aerosol_views)


station_views['aerosol-raw-opticalscattering2'] = OpticalScatteringSecondary('aerosol-raw')
station_views['aerosol-realtime-opticalscattering2'] = OpticalScatteringSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-editing-scattering2'] = EditingScatteringSecondary()
station_views['aerosol-editing-backscattering2'] = EditingBackScatteringSecondary()
station_views['aerosol-clean-opticalscattering2'] = OpticalScatteringSecondary('aerosol-clean')
station_views['aerosol-avgh-opticalscattering2'] = OpticalScatteringSecondary('aerosol-avgh')
station_views['aerosol-raw-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-realtime', realtime=True)

station_views['aerosol-raw-maap'] = MAAP5012Optical('aerosol-raw')
station_views['aerosol-realtime-maap'] = MAAP5012Optical('aerosol-realtime', realtime=True)
station_views['aerosol-editing-maap'] = EditingMAA5012('aerosol')
station_views['aerosol-clean-maap'] = MAAP5012Optical('aerosol-clean')
station_views['aerosol-avgh-maap'] = MAAP5012Optical('aerosol-avgh')
station_views['aerosol-raw-maapstatus'] = MAAP5012Status('aerosol-raw')
station_views['aerosol-realtime-maapstatus'] = MAAP5012Status('aerosol-realtime', realtime=True)

station_views['aerosol-raw-green'] = Green('aerosol-raw')
station_views['aerosol-realtime-green'] = Green('aerosol-realtime', realtime=True)
station_views['aerosol-clean-green'] = Green('aerosol-clean')
station_views['aerosol-avgh-green'] = Green('aerosol-avgh')

measurements = OrderedDict([
    ('{code}nephinlet', '{code}u_S41 (TSI inlet)'),
    ('{code}neph', '{code}_S41 (TSI sample)'),
    ('{code}neph2', '{code}_S13 (Ecotech sample)'),
    ('{code}nephcell2', '{code}x_S13 (Ecotech cell)'),
])
omit_traces = {'TDnephinlet', 'TDnephcell2', 'Unephcell2'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=measurements, 
                                                       omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', measurements=measurements, 
                                                            omit_traces=omit_traces, realtime=True)

station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = Pressure('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
