import typing
from forge.vis.view import View
from forge.vis.view.solar import SolarPosition, BSRNQC

from .aerosol.counts import ParticleConcentration
from .aerosol.optical import Optical
from .aerosol.green import Green
from .aerosol.aethalometer import AethalometerOptical, AE33, AE33Status
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

from .aerosol.public.overview import PublicOverviewShort, PublicOverviewLong
from .aerosol.public.counts import PublicCountsShort, PublicCountsLong
from .aerosol.public.clap import PublicCLAPShort, PublicCLAPLong
from .aerosol.public.nephelometer import PublicTSI3563Short, PublicTSI3563Long
from .aerosol.public.housekeeping import PublicHousekeepingShort, PublicHousekeepingLong
from .aerosol.statistics.counts import StatisticsParticleConcentration
from .aerosol.statistics.extensive import StatisticsScattering, StatisticsAbsorption
from .aerosol.statistics.intensive import StatisticsBackscatterFraction, StatisticsAngstromExponent, StatisticsSingleScatteringAlbedo, StatisticsSubumFraction

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

from .ozone.public.concentration import PublicConcentrationShort as PublicOzoneConcentrationShort, PublicConcentrationLong as PublicOzoneConcentrationLong
from .ozone.public.housekeeping import PublicHousekeepingShort as PublicOzoneHousekeepingShort, PublicHousekeepingLong as PublicOzoneHousekeepingLong
from .ozone.statistics.concentration import StatisticsOzoneConcentration

from .radiation.shortwave import Shortwave, ShortwaveSimplified
from .radiation.longwave import Longwave, LongwaveSimplified, PyrgeometerTemperature
from .radiation.ratio import Ratios as RadiationRatios
from .radiation.status import Status as RadiationStatus
from .radiation.compare import ShortwaveCompare
from .radiation.ambient import Ambient as RadiationAmbient


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
    'radiation-raw-shortwave': Shortwave('radiation-raw'),
    'radiation-raw-longwave': Longwave('radiation-raw'),
    'radiation-raw-ratio': RadiationRatios('radiation-raw'),
    'radiation-raw-bsrnqc': BSRNQC('radiation-raw'),
    'radiation-raw-pyranometertemperature': PyrgeometerTemperature('radiation-raw'),
    'radiation-raw-status': RadiationStatus('radiation-raw'),
    'radiation-raw-shortwavecompare': ShortwaveCompare('radiation-raw'),
    'radiation-raw-ambient': RadiationAmbient('radiation-raw'),
    'radiation-raw-solarposition': SolarPosition(),

    'radiation-editing-ratio': RadiationRatios('radiation-editing'),
    'radiation-editing-bsrnqc': BSRNQC('radiation-editing'),
    'radiation-editing-shortwave': ShortwaveSimplified('radiation-editing'),
    'radiation-editing-longwave': LongwaveSimplified('radiation-editing'),
    'radiation-editing-pyranometertemperature': PyrgeometerTemperature('radiation-editing'),
    'radiation-editing-shortwavecompare': ShortwaveCompare('radiation-editing'),
    'radiation-editing-ambient': RadiationAmbient('radiation-editing'),
    'radiation-editing-solarposition': SolarPosition(),
    
    'radiation-clean-shortwave': Shortwave('radiation-clean'),
    'radiation-clean-longwave': Longwave('radiation-clean'),
    'radiation-clean-ratio': RadiationRatios('radiation-clean'),
    'radiation-clean-bsrnqc': BSRNQC('radiation-clean'),
    'radiation-clean-pyranometertemperature': PyrgeometerTemperature('radiation-clean'),
    'radiation-clean-status': RadiationStatus('radiation-clean'),
    'radiation-clean-shortwavecompare': ShortwaveCompare('radiation-clean'),
    'radiation-clean-ambient': RadiationAmbient('radiation-editing'),
    'radiation-clean-solarposition': SolarPosition(),
}

aerosol_public: typing.Dict[str, View] = {
    'public-aerosolshort-overview': PublicOverviewShort(),
    'public-aerosolshort-counts': PublicCountsShort(),
    'public-aerosolshort-nephelometer': PublicTSI3563Short(),
    'public-aerosolshort-clap': PublicCLAPShort(),
    'public-aerosolshort-housekeeping': PublicHousekeepingShort(),

    'public-aerosollong-overview': PublicOverviewLong(),
    'public-aerosollong-counts': PublicCountsLong(),
    'public-aerosollong-nephelometer': PublicTSI3563Long(),
    'public-aerosollong-clap': PublicCLAPLong(),
    'public-aerosollong-housekeeping': PublicHousekeepingLong(),

    'public-aerosolstats-counts': StatisticsParticleConcentration('cnc'),
    'public-aerosolstats-scattering-blue-total': StatisticsScattering.with_title('bs-b0', "Total Scattering at 450nm"),
    'public-aerosolstats-scattering-blue-subum': StatisticsScattering.with_title('bs-b1', "Sub-μm Scattering at 450nm"),
    'public-aerosolstats-scattering-green-total': StatisticsScattering.with_title('bs-g0', "Total Scattering at 550nm"),
    'public-aerosolstats-scattering-green-subum': StatisticsScattering.with_title('bs-g1', "Sub-μm Scattering at 550nm"),
    'public-aerosolstats-scattering-red-total': StatisticsScattering.with_title('bs-r0', "Total Scattering at 700nm"),
    'public-aerosolstats-scattering-red-subum': StatisticsScattering.with_title('bs-r1', "Sub-μm Scattering at 700nm"),
    'public-aerosolstats-absorption-blue-total': StatisticsAbsorption.with_title('ba-b0', "Total Absorption at 450nm"),
    'public-aerosolstats-absorption-blue-subum': StatisticsAbsorption.with_title('ba-b1', "Sub-μm Absorption at 450nm"),
    'public-aerosolstats-absorption-green-total': StatisticsAbsorption.with_title('ba-g0', "Total Absorption at 550nm"),
    'public-aerosolstats-absorption-green-subum': StatisticsAbsorption.with_title('ba-g1', "Sub-μm Absorption at 550nm"),
    'public-aerosolstats-absorption-red-total': StatisticsAbsorption.with_title('ba-r0', "Total Absorption at 700nm"),
    'public-aerosolstats-absorption-red-subum': StatisticsAbsorption.with_title('ba-r1', "Sub-μm Absorption at 700nm"),
    'public-aerosolstats-bfr-blue-total': StatisticsBackscatterFraction.with_title('bfr-b0', "Total Backscatter Fraction at 450nm (σbsp/σsp)"),
    'public-aerosolstats-bfr-blue-subum': StatisticsBackscatterFraction.with_title('bfr-b1', "Sub-μm Backscatter Fraction at 450nm (σbsp/σsp)"),
    'public-aerosolstats-bfr-green-total': StatisticsBackscatterFraction.with_title('bfr-g0', "Total Backscatter Fraction at 550nm (σbsp/σsp)"),
    'public-aerosolstats-bfr-green-subum': StatisticsBackscatterFraction.with_title('bfr-g1', "Sub-μm Backscatter Fraction at 550nm (σbsp/σsp)"),
    'public-aerosolstats-bfr-red-total': StatisticsBackscatterFraction.with_title('bfr-r0', "Total Backscatter Fraction at 700nm (σbsp/σsp)"),
    'public-aerosolstats-bfr-red-subum': StatisticsBackscatterFraction.with_title('bfr-r1', "Sub-μm Backscatter Fraction at 700nm (σbsp/σsp)"),
    'public-aerosolstats-sae-green-total': StatisticsAngstromExponent.with_title('sae-g0', "Total Scattering Ångström Exponent at 450nm/700nm"),
    'public-aerosolstats-sae-green-subum': StatisticsAngstromExponent.with_title('sae-g1', "Sub-μm Scattering Ångström Exponent at 450nm/700nm"),
    'public-aerosolstats-aae-green-total': StatisticsAngstromExponent.with_title('aae-g0', "Total Absorption Ångström Exponent at 450nm/700nm"),
    'public-aerosolstats-aae-green-subum': StatisticsAngstromExponent.with_title('aae-g1', "Sub-μm Absorption Ångström Exponent at 450nm/700nm"),
    'public-aerosolstats-ssa-green-total': StatisticsSingleScatteringAlbedo.with_title('ssa-g0', "Total Single Scattering Albedo (ω 550nm)"),
    'public-aerosolstats-ssa-green-subum': StatisticsSingleScatteringAlbedo.with_title('ssa-g1', "Sub-μm Single Scattering Albedo (ω 550nm)"),
    'public-aerosolstats-bsf-blue': StatisticsSubumFraction.with_title('bsf-b', "Scattering Sub-μm Fraction 450nm"),
    'public-aerosolstats-bsf-green': StatisticsSubumFraction.with_title('bsf-g', "Scattering Sub-μm Fraction 550nm"),
    'public-aerosolstats-bsf-red': StatisticsSubumFraction.with_title('bsf-r', "Scattering Sub-μm Fraction 700nm"),
    'public-aerosolstats-baf-blue': StatisticsSubumFraction.with_title('baf-b', "Absorption Sub-μm Fraction 450nm"),
    'public-aerosolstats-baf-green': StatisticsSubumFraction.with_title('baf-g', "Absorption Sub-μm Fraction 550nm"),
    'public-aerosolstats-baf-red': StatisticsSubumFraction.with_title('baf-r', "Absorption Sub-μm Fraction 700nm"),
}
ozone_public: typing.Dict[str, View] = {
    'public-ozoneshort-concentration': PublicOzoneConcentrationShort(),
    'public-ozoneshort-housekeeping': PublicOzoneHousekeepingShort(),

    'public-ozonelong-concentration': PublicOzoneConcentrationLong(),
    'public-ozonelong-housekeeping': PublicOzoneHousekeepingLong(),

    'public-ozonestats-concentration': StatisticsOzoneConcentration(),
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
