import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from .optical import OpticalScatteringSecondary, EditingScatteringSecondary, EditingBackScatteringSecondary
from .green import Green
from .humidograph import WetDryRatio
from .pressure import Pressure
from .tsi3563nephelometer import NephelometerZeroSecondary, NephelometerStatusSecondary


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

station_views['aerosol-raw-green'] = Green('aerosol-raw')
station_views['aerosol-realtime-green'] = Green('aerosol-realtime', realtime=True)
station_views['aerosol-clean-green'] = Green('aerosol-clean')
station_views['aerosol-avgh-green'] = Green('aerosol-avgh')

station_views['aerosol-raw-humidograph'] = WetDryRatio('aerosol-raw')
station_views['aerosol-realtime-humidograph'] = WetDryRatio('aerosol-realtime', realtime=True)
station_views['aerosol-clean-humidograph'] = WetDryRatio('aerosol-clean')
station_views['aerosol-avgh-humidograph'] = WetDryRatio('aerosol-avgh')

measurements = OrderedDict([
    ('{code}inlet', '{code}_V51 (inlet)'),
    ('{code}sample', '{code}_V11 (sample)'),
    ('{code}nephinlet', '{code}u_S11 (dry inlet)'),
    ('{code}neph', '{code}_S11 (dry sample)'),
    ('{code}nephinlet2', '{code}u_S12 (wet inlet)'),
    ('{code}neph2', '{code}_S12 (wet sample)'),
    ('{code}outlet', '{code}_V12 (outlet)'),
])
omit_traces={'TDnephinlet', 'TDnephinlet2'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements, omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-raw', measurements, omit_traces=omit_traces,
                                                            realtime=True)

station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = Pressure('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
