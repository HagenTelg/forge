import typing
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.tsi3010cpc import TSI3010CPCStatus
from ..default.aerosol.clap import CLAPStatus
from ..default.aerosol.psap import PSAPStatus
from ..default.aerosol.aethalometer import AE31, AE31Status, AE31OpticalStatus
from .optical import OpticalPSAP, EditingPSAP, OpticalCLAP, EditingCLAP, OpticalCOSMOS, EditingCOSMOS
from .clap import CLAPStatusCOSMOS
from .green import Green
from .tca import TCA08Mass, TCA08Status, EditingTCA


station_views = detach(aerosol_views)


station_views['aerosol-raw-cpcstatus'] = TSI3010CPCStatus('aerosol-raw')

station_views['aerosol-raw-optical'] = OpticalPSAP('aerosol-raw')
station_views['aerosol-editing-absorption'] = EditingPSAP()
station_views['aerosol-clean-optical'] = OpticalPSAP('aerosol-clean')
station_views['aerosol-avgh-optical'] = OpticalPSAP('aerosol-avgh')
station_views['aerosol-raw-psapstatus'] = PSAPStatus('aerosol-raw')

station_views['aerosol-raw-opticalclap'] = OpticalCLAP('aerosol-raw')
station_views['aerosol-editing-clap'] = EditingCLAP()
station_views['aerosol-clean-opticalclap'] = OpticalCLAP('aerosol-clean')
station_views['aerosol-avgh-opticalclap'] = OpticalCLAP('aerosol-avgh')
station_views['aerosol-raw-clapstatus'] = CLAPStatus('aerosol-raw')

station_views['aerosol-raw-opticalcosmos'] = OpticalCOSMOS('aerosol-raw')
station_views['aerosol-editing-cosmosclap'] = EditingCOSMOS()
station_views['aerosol-clean-opticalcosmos'] = OpticalCOSMOS('aerosol-clean')
station_views['aerosol-avgh-opticalcosmos'] = OpticalCOSMOS('aerosol-avgh')
station_views['aerosol-raw-clapstatuscosmos'] = CLAPStatusCOSMOS('aerosol-raw')

station_views['aerosol-raw-aethalometer'] = AE31('aerosol-raw')
station_views['aerosol-raw-aethalometerstatus'] = AE31Status('aerosol-raw')
station_views['aerosol-editing-aethalometerstatus'] = AE31OpticalStatus('aerosol-editing')

station_views['aerosol-raw-green'] = Green('aerosol-raw')
station_views['aerosol-clean-green'] = Green('aerosol-clean')
station_views['aerosol-avgh-green'] = Green('aerosol-avgh')

station_views['aerosol-raw-tca'] = TCA08Mass('aerosol-raw')
station_views['aerosol-editing-tca'] = EditingTCA()
station_views['aerosol-clean-tca'] = TCA08Mass('aerosol-clean')
station_views['aerosol-avgh-tca'] = TCA08Mass('aerosol-avgh')
station_views['aerosol-raw-tcastatus'] = TCA08Status('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
