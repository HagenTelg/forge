import typing
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.maap import MAAP5012Status
from ..default.aerosol.tsi375Xcpc import TSI375xCPCStatus
from .aethalometer import AE33, AE33Status, AE33OpticalStatus, EditingAE33
from .maap import MAAP5012Optical, EditingMAAP5012
from .green import Green
from .pressure import Pressure
from .ne300nephelometer import NephelometerStatusSecondary, NephelometerZeroSecondary
from .optical import OpticalScatteringSecondary, EditingScatteringSecondary, EditingBackScatteringSecondary


station_views = detach(aerosol_views)


station_views['aerosol-raw-maap'] = MAAP5012Optical('aerosol-raw')
station_views['aerosol-realtime-maap'] = MAAP5012Optical('aerosol-realtime', realtime=True)
station_views['aerosol-editing-maap'] = EditingMAAP5012('aerosol-editing')
station_views['aerosol-clean-maap'] = MAAP5012Optical('aerosol-clean')
station_views['aerosol-avgh-maap'] = MAAP5012Optical('aerosol-avgh')
station_views['aerosol-raw-maapstatus'] = MAAP5012Status('aerosol-raw')
station_views['aerosol-realtime-maapstatus'] = MAAP5012Status('aerosol-realtime', realtime=True)

station_views['aerosol-raw-aethalometer'] = AE33('aerosol-raw')
station_views['aerosol-raw-aethalometerstatus'] = AE33Status('aerosol-raw')
station_views['aerosol-realtime-aethalometer'] = AE33('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-aethalometerstatus'] = AE33Status('aerosol-realtime', realtime=True)
station_views['aerosol-editing-aethalometer'] = EditingAE33()
station_views['aerosol-editing-aethalometerstatus'] = AE33OpticalStatus('aerosol-editing')
station_views['aerosol-clean-aethalometer'] = AE33('aerosol-clean')
station_views['aerosol-avgh-aethalometer'] = AE33('aerosol-avgh')

station_views['aerosol-raw-green'] = Green('aerosol-raw')
station_views['aerosol-realtime-green'] = Green('aerosol-realtime', realtime=True)
station_views['aerosol-clean-green'] = Green('aerosol-clean')
station_views['aerosol-avgh-green'] = Green('aerosol-avgh')

station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = Pressure('aerosol-realtime', realtime=True)

station_views['aerosol-raw-cpcstatus'] = TSI375xCPCStatus('aerosol-raw')
station_views['aerosol-realtime-cpcstatus'] = TSI375xCPCStatus('aerosol-realtime', realtime=True)


station_views['aerosol-raw-opticalscattering2'] = OpticalScatteringSecondary('aerosol-raw')
station_views['aerosol-realtime-opticalscattering2'] = OpticalScatteringSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-editing-scattering2'] = EditingScatteringSecondary()
station_views['aerosol-editing-backscattering2'] = EditingBackScatteringSecondary()
station_views['aerosol-clean-opticalscattering2'] = OpticalScatteringSecondary('aerosol-clean')
station_views['aerosol-avgh-opticalscattering2'] = OpticalScatteringSecondary('aerosol-avgh')
station_views['aerosol-raw-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
