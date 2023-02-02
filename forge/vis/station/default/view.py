import typing
from forge.vis.view import View
from forge.vis.view.solar import SolarPosition
from forge.processing.station.lookup import station_data

from .aerosol.counts import ParticleConcentration
from .aerosol.optical import Optical
from .aerosol.green import Green
from .aerosol.aethalometer import AethalometerOptical, AE33, AE33Status, AE33OpticalStatus
from .aerosol.intensive import Intensive
from .aerosol.extensive import Extensive
from .aerosol.flow import Flow
from .aerosol.wind import Wind
from .aerosol.temperature import Temperature
from .aerosol.pressure import Pressure
from .aerosol.tsi3563nephelometer import NephelometerZero, NephelometerStatus
from .aerosol.clap import CLAPStatus
from .aerosol.cpcgeneric import SystemCPCFlow
from .aerosol.umac import UMACStatus
from .aerosol.editing.counts import EditingParticleConcentration
from .aerosol.editing.optical import EditingScattering, EditingBackScattering, EditingAbsorption
from .aerosol.editing.aethalometer import EditingAethalometer

from .met.wind import Wind as MetWind
from .met.temperature import Temperature as MetTemperature
from .met.pressure import Pressure as MetPressure
from .met.editing.wind import EditingWindSpeed as MetEditingWindSpeed
from .met.editing.wind import EditingWindDirection as MetEditingWindDirection
from .met.editing.temperature import EditingTemperature as MetEditingTemperature
from .met.editing.temperature import EditingDewpoint as MetEditingDewpoint
from .met.editing.temperature import EditingRH as MetEditingRH
from .met.editing.pressure import EditingPressure as MetEditingPressure

from .ozone.concentration import OzoneConcentration
from .ozone.thermo49 import Thermo49Status, Thermo49Cells
from .ozone.wind import Wind as OzoneWind
from .ozone.editing.concentration import EditingOzoneConcentration

from .radiation.ambient import Ambient as RadiationAmbient
from .radiation.editing.albedo import EditingAlbedo as RadiationEditingAlbedo
from .radiation.editing.infrared import EditingInfrared as RadiationEditingInfrared
from .radiation.editing.infrared import EditingPyrgeometerTemperature as RadiationEditingPyrgeometerTemperature
from .radiation.editing.ratio import EditingTotalRatio as RadiationEditingEditingTotalRatio
from .radiation.editing.solar import EditingSolar as RadiationEditingSolar


aerosol_views: typing.Dict[str, View] = {
    'aerosol-raw-counts': ParticleConcentration('aerosol-raw'),
    'aerosol-raw-optical': Optical('aerosol-raw'),
    'aerosol-raw-green': Green('aerosol-raw'),
    'aerosol-raw-aethalometer': AE33('aerosol-raw'),
    'aerosol-raw-intensive': Intensive('aerosol-raw'),
    'aerosol-raw-wind': Wind('aerosol-raw'),
    'aerosol-raw-flow': Flow('aerosol-raw'),
    'aerosol-raw-temperature': Temperature('aerosol-raw'),
    'aerosol-raw-pressure': Pressure('aerosol-raw'),
    'aerosol-raw-nephelometerzero': NephelometerZero('aerosol-raw'),
    'aerosol-raw-nephelometerstatus': NephelometerStatus('aerosol-raw'),
    'aerosol-raw-clapstatus': CLAPStatus('aerosol-raw'),
    'aerosol-raw-aethalometerstatus': AE33Status('aerosol-raw'),
    'aerosol-raw-cpcstatus': SystemCPCFlow('aerosol-raw'),
    'aerosol-raw-umacstatus': UMACStatus('aerosol-raw'),

    'aerosol-editing-counts': EditingParticleConcentration(),
    'aerosol-editing-scattering': EditingScattering(),
    'aerosol-editing-backscattering': EditingBackScattering(),
    'aerosol-editing-absorption': EditingAbsorption(),
    'aerosol-editing-aethalometer': EditingAethalometer(),
    'aerosol-editing-aethalometerstatus': AE33OpticalStatus('aerosol-editing'),
    'aerosol-editing-wind': Wind('aerosol-editing'),
    'aerosol-editing-intensive': Intensive('aerosol-editing'),
    'aerosol-editing-extensive': Extensive('aerosol-editing'),

    'aerosol-clean-counts': ParticleConcentration('aerosol-clean'),
    'aerosol-clean-optical': Optical('aerosol-clean'),
    'aerosol-clean-green': Green('aerosol-clean'),
    'aerosol-clean-aethalometer': AethalometerOptical('aerosol-clean'),
    'aerosol-clean-intensive': Intensive('aerosol-clean'),
    'aerosol-clean-extensive': Extensive('aerosol-clean'),
    'aerosol-clean-wind': Wind('aerosol-clean'),

    'aerosol-avgh-counts': ParticleConcentration('aerosol-avgh'),
    'aerosol-avgh-optical': Optical('aerosol-avgh'),
    'aerosol-avgh-green': Green('aerosol-avgh'),
    'aerosol-avgh-aethalometer': AethalometerOptical('aerosol-avgh'),
    'aerosol-avgh-intensive': Intensive('aerosol-avgh'),
    'aerosol-avgh-extensive': Extensive('aerosol-avgh'),
    'aerosol-avgh-wind': Wind('aerosol-avgh'),
    
    'aerosol-realtime-counts': ParticleConcentration('aerosol-realtime', realtime=True),
    'aerosol-realtime-optical': Optical('aerosol-realtime', realtime=True),
    'aerosol-realtime-green': Green('aerosol-realtime', realtime=True),
    'aerosol-realtime-aethalometer': AE33('aerosol-realtime', realtime=True),
    'aerosol-realtime-intensive': Intensive('aerosol-realtime', realtime=True),
    'aerosol-realtime-wind': Wind('aerosol-realtime', realtime=True),
    'aerosol-realtime-flow': Flow('aerosol-realtime', realtime=True),
    'aerosol-realtime-temperature': Temperature('aerosol-realtime', realtime=True),
    'aerosol-realtime-pressure': Pressure('aerosol-realtime', realtime=True),
    'aerosol-realtime-nephelometerzero': NephelometerZero('aerosol-realtime', realtime=True),
    'aerosol-realtime-nephelometerstatus': NephelometerStatus('aerosol-realtime', realtime=True),
    'aerosol-realtime-clapstatus': CLAPStatus('aerosol-realtime', realtime=True),
    'aerosol-realtime-aethalometerstatus': AE33Status('aerosol-realtime', realtime=True),
    'aerosol-realtime-cpcstatus': SystemCPCFlow('aerosol-realtime', realtime=True),
    'aerosol-realtime-umacstatus': UMACStatus('aerosol-realtime', realtime=True),
}
ozone_views: typing.Dict[str, View] = {
    'ozone-raw-concentration': OzoneConcentration('ozone-raw'),
    'ozone-raw-status': Thermo49Status('ozone-raw'),
    'ozone-raw-cells': Thermo49Cells('ozone-raw'),
    'ozone-raw-wind': OzoneWind('ozone-raw'),

    'ozone-editing-concentration': EditingOzoneConcentration(),
    'ozone-editing-wind': OzoneWind('ozone-editing'),
    
    'ozone-clean-concentration': OzoneConcentration('ozone-clean'),
    'ozone-clean-wind': OzoneWind('ozone-clean'),
    
    'ozone-avgh-concentration': OzoneConcentration('ozone-avgh'),
    'ozone-avgh-wind': OzoneWind('ozone-avgh'),
    
    'ozone-realtime-concentration': OzoneConcentration('ozone-realtime', realtime=True),
    'ozone-realtime-status': Thermo49Status('ozone-realtime', realtime=True),
    'ozone-realtime-cells': Thermo49Cells('ozone-realtime', realtime=True),
    'ozone-realtime-wind': OzoneWind('ozone-realtime', realtime=True),
}
met_views: typing.Dict[str, View] = {
    'met-raw-wind': MetWind('met-raw-wind'),
    'met-raw-temperature': MetTemperature('met-raw-temperature'),
    'met-raw-pressure': MetPressure('met-raw'),

    'met-editing-windspeed': MetEditingWindSpeed(),
    'met-editing-winddirection': MetEditingWindDirection(),
    'met-editing-temperature': MetEditingTemperature(),
    'met-editing-dewpoint': MetEditingDewpoint(),
    'met-editing-rh': MetEditingRH(),
    'met-editing-pressure': MetEditingPressure(),

    'met-clean-wind': MetWind('met-clean-wind'),
    'met-clean-temperature': MetTemperature('met-clean-temperature'),
    'met-clean-pressure': MetPressure('met-clean'),

    'met-avgh-wind': MetWind('met-avgh-wind'),
    'met-avgh-temperature': MetTemperature('met-avgh-temperature'),
    'met-avgh-pressure': MetPressure('met-avgh'),
}
radiation_views: typing.Dict[str, View] = {
    'radiation-editing-solar': RadiationEditingSolar(),
    'radiation-editing-ir': RadiationEditingInfrared(),
    'radiation-editing-pyranometertemperature': RadiationEditingPyrgeometerTemperature(),
    'radiation-editing-albedo': RadiationEditingAlbedo(),
    'radiation-editing-totalratio': RadiationEditingEditingTotalRatio(),
    'radiation-editing-ambient': RadiationAmbient('radiation-raw-ambient'),
    'radiation-editing-solarposition': SolarPosition(),
}


def detach(*views: typing.Dict[str, View]) -> typing.Dict[str, View]:
    result: typing.Dict[str, View] = dict()
    for add in views:
        result.update(add)
    return result


def get(station: str, view_name: str) -> typing.Optional[View]:
    return aerosol_views.get(view_name)


def modes(station: str, view_name: str) -> typing.List[str]:
    # Just assume the same naming hierarchy
    return ['-'.join(view_name.split('-')[0:2])]
