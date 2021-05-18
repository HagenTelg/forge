import typing
from forge.vis.view import View

from .aerosol.counts import ParticleConcentration
from .aerosol.optical import Optical
from .aerosol.green import Green
from .aerosol.aethalometer import AethalometerOptical, AE33, AE33Status, AE33OpticalStatus
from .aerosol.intensive import Intensive
from .aerosol.extensive import Extensive
from .aerosol.flow import Flow
from .aerosol.temperature import Temperature
from .aerosol.pressure import Pressure
from .aerosol.tsi3563nephelometer import NephelometerZero, NephelometerStatus
from .aerosol.clap import CLAPStatus
from .aerosol.cpcgeneric import SystemCPCFlow
from .aerosol.umac import UMACStatus
from .aerosol.editing.counts import EditingParticleConcentration
from .aerosol.editing.optical import EditingScattering, EditingBackScattering, EditingAbsorption
from .aerosol.editing.aethalometer import EditingAethalometer

from .met.wind import Wind


aerosol_views: typing.Dict[str, View] = {
    'aerosol-raw-counts': ParticleConcentration('aerosol-raw'),
    'aerosol-raw-optical': Optical('aerosol-raw'),
    'aerosol-raw-green': Green('aerosol-raw'),
    'aerosol-raw-aethalometer': AE33('aerosol-raw'),
    'aerosol-raw-intensive': Intensive('aerosol-raw'),
    'aerosol-raw-wind': Wind('aerosol-raw-wind'),
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
    'aerosol-editing-wind': Wind('aerosol-editing-wind'),
    'aerosol-editing-intensive': Intensive('aerosol-editing'),
    'aerosol-editing-extensive': Extensive('aerosol-editing'),

    'aerosol-clean-counts': ParticleConcentration('aerosol-clean'),
    'aerosol-clean-optical': Optical('aerosol-clean'),
    'aerosol-clean-green': Green('aerosol-clean'),
    'aerosol-clean-aethalometer': AethalometerOptical('aerosol-clean'),
    'aerosol-clean-intensive': Intensive('aerosol-clean'),
    'aerosol-clean-extensive': Extensive('aerosol-clean'),
    'aerosol-clean-wind': Wind('aerosol-clean-wind'),
}
ozone_views: typing.Dict[str, View] = {
}
met_views: typing.Dict[str, View] = {
}


def get(station: str, view_name: str) -> typing.Optional[View]:
    return aerosol_views.get(view_name)


def modes(station: str, view_name: str) -> typing.List[str]:
    # Just assume the same naming hierarchy
    return ['-'.join(view_name.split('-')[0:2])]
