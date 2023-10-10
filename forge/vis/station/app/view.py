import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.ccn import CCNStatus
from .optical import OpticalScatteringSecondary, EditingScatteringSecondary, EditingBackScatteringSecondary, EditingScattering3, EditingBackScattering3, EditingScattering4, EditingBackScattering4, AllScattering
from .green import Green
from .humidograph import WetDryRatio
from .pressure import Pressure
from .tsi3563nephelometer import NephelometerZeroSecondary, NephelometerStatusSecondary
from .ecotechnephelometer import NephelometerZero3, NephelometerStatus3, NephelometerZero4, NephelometerStatus4
from .counts import RealtimeParticleConcentration, ParticleConcentration, EditingParticleConcentration, SMPSDistribution


station_views = detach(aerosol_views)


station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-realtime-counts'] = RealtimeParticleConcentration('aerosol-realtime')
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')

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


station_views['aerosol-raw-allscattering'] = AllScattering('aerosol-raw')
station_views['aerosol-realtime-allscattering'] = AllScattering('aerosol-realtime', realtime=True)
station_views['aerosol-clean-allscattering'] = AllScattering('aerosol-clean')
station_views['aerosol-avgh-allscattering'] = AllScattering('aerosol-avgh')

station_views['aerosol-editing-scattering3'] = EditingScattering3()
station_views['aerosol-editing-backscattering3'] = EditingBackScattering3()
station_views['aerosol-raw-nephelometerzero3'] = NephelometerZero3('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus3'] = NephelometerStatus3('aerosol-raw')
station_views['aerosol-realtime-nephelometerzero3'] = NephelometerZero3('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-nephelometerstatus3'] = NephelometerStatus3('aerosol-realtime', realtime=True)

station_views['aerosol-editing-scattering4'] = EditingScattering4()
station_views['aerosol-editing-backscattering4'] = EditingBackScattering4()
station_views['aerosol-raw-nephelometerzero4'] = NephelometerZero4('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus4'] = NephelometerStatus4('aerosol-raw')
station_views['aerosol-realtime-nephelometerzero4'] = NephelometerZero4('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-nephelometerstatus4'] = NephelometerStatus4('aerosol-realtime', realtime=True)


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
    ('{code}neph3', '{code}_S13 (sample)'),
    ('{code}neph4', '{code}_S14 (sample)'),
])
omit_traces={'TDnephinlet', 'TDnephinlet2'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements, omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-raw', measurements, omit_traces=omit_traces,
                                                            realtime=True)

station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = Pressure('aerosol-realtime', realtime=True)


station_views['aerosol-raw-ccnstatus'] = CCNStatus('aerosol-raw')
station_views['aerosol-realtime-ccnstatus'] = CCNStatus('aerosol-raw', realtime=True)


station_views['aerosol-raw-smps'] = SMPSDistribution('aerosol-raw')
station_views['aerosol-editing-smps'] = SMPSDistribution('aerosol-editing')
station_views['aerosol-clean-smps'] = SMPSDistribution('aerosol-clean')
station_views['aerosol-avgh-smps'] = SMPSDistribution('aerosol-avgh')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
