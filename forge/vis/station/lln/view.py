import typing
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.admagiccpc import ADMagicCPC200Status
from ..default.aerosol.clap import CLAPStatus
from ..default.aerosol.psap import PSAPStatus
from .optical import OpticalCLAP, EditingCLAP, OpticalCOSMOS, EditingCOSMOS
from .clap import CLAPStatusCOSMOS
from .tca import TCA08Mass, TCA08Status, EditingTCA


station_views = detach(aerosol_views)


station_views['aerosol-raw-cpcstatus'] = ADMagicCPC200Status('aerosol-raw')
station_views['aerosol-realtime-cpcstatus'] = ADMagicCPC200Status('aerosol-realtime', realtime=True)

station_views['aerosol-raw-psapstatus'] = PSAPStatus('aerosol-raw')
station_views['aerosol-realtime-psapstatus'] = PSAPStatus('aerosol-realtime', realtime=True)

station_views['aerosol-raw-opticalclap'] = OpticalCLAP('aerosol-raw')
station_views['aerosol-realtime-opticalclap'] = OpticalCLAP('aerosol-realtime', realtime=True)
station_views['aerosol-editing-clap'] = EditingCLAP()
station_views['aerosol-clean-opticalclap'] = OpticalCLAP('aerosol-clean')
station_views['aerosol-avgh-opticalclap'] = OpticalCLAP('aerosol-avgh')
station_views['aerosol-raw-clapstatus'] = CLAPStatus('aerosol-raw')
station_views['aerosol-realtime-clapstatus'] = CLAPStatus('aerosol-realtime', realtime=True)

station_views['aerosol-raw-opticalcosmos'] = OpticalCOSMOS('aerosol-raw')
station_views['aerosol-realtime-opticalcosmos'] = OpticalCOSMOS('aerosol-realtime', realtime=True)
station_views['aerosol-editing-cosmosclap'] = EditingCOSMOS()
station_views['aerosol-clean-opticalcosmos'] = OpticalCOSMOS('aerosol-clean')
station_views['aerosol-avgh-opticalcosmos'] = OpticalCOSMOS('aerosol-avgh')
station_views['aerosol-raw-clapstatuscosmos'] = CLAPStatusCOSMOS('aerosol-raw')
station_views['aerosol-realtime-clapstatuscosmos'] = CLAPStatusCOSMOS('aerosol-realtime', realtime=True)

station_views['aerosol-raw-tca'] = TCA08Mass('aerosol-raw')
station_views['aerosol-realtime-tca'] = TCA08Mass('aerosol-realtime', realtime=True)
station_views['aerosol-editing-tca'] = EditingTCA()
station_views['aerosol-clean-tca'] = TCA08Mass('aerosol-clean')
station_views['aerosol-avgh-tca'] = TCA08Mass('aerosol-avgh')
station_views['aerosol-raw-tcastatus'] = TCA08Status('aerosol-raw')
station_views['aerosol-realtime-tcastatus'] = TCA08Status('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
