import typing
from math import nan
from forge.vis.export import Export, ExportList
from forge.vis.export.archive import ArchiveExportEntry, ExportCSV, ExportNetCDF, ExportCompleteRawNetCDF, ExportEBAS, InstrumentSelection, Selection
from .data import STANDARD_THREE_WAVELENGTHS


STANDARD_CUT_SIZE_SPLIT: typing.Iterable[typing.Tuple[str, typing.Union[float, typing.Tuple[float, float]]]] = (
    ("", ()),
    ("0", (10, nan)),
    ("2", (2.5, 10)),
    ("1", (nan, 2.5)),
)


def find_key(exports: typing.List[ArchiveExportEntry], key: str) -> typing.Optional[ArchiveExportEntry]:
    for export in exports:
        if export.key == key:
            return export
    return None


aerosol_exports: typing.Dict[str, typing.List[ArchiveExportEntry]] = dict()
for archive in ("raw", "clean", "avgh"):
    aerosol_exports[archive] = list()
for archive in ("raw",):
    aerosol_exports[archive].append(ExportCSV("extensive", "Extensive", [
        ExportCSV.Column([Selection(variable_name="number_concentration",
                                    require_tags={"cpc"}, exclude_tags={"secondary"})],
                         default_header="N", always_present=True)
    ] + [
        ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="Bs" + code + "_{instrument_id}", default_header=f"Bs{code}", always_present=True)
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="Bbs" + code + "_{instrument_id}", default_header=f"Bbs{code}", always_present=True)
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                    require_tags={"absorption"}, exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                         header="Ba" + code + "_{instrument_id}", default_header=f"Ba{code}", always_present=True)
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="sample_temperature",
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         default_header="T", always_present=True),
        ExportCSV.Column([Selection(variable_name="sample_humidity",
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         default_header="U", always_present=True),
        ExportCSV.Column([Selection(variable_name="sample_pressure",
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         default_header="P", always_present=True),
    ]))
for archive in ("clean",):
    aerosol_exports[archive].append(ExportCSV("intensive", "Intensive", [
        ExportCSV.Column([Selection(variable_name="number_concentration",
                                    instrument_id="XI")],
                         header="N_XI", always_present=True),
    ] + [
        ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength,
                                    instrument_id="XI")],
                         header=f"Bs{code}_XI", always_present=True)
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                    instrument_id="XI")],
                         header=f"Ba{code}_XI", always_present=True)
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="single_scattering_albedo", wavelength=(500, 600),
                                    instrument_id="XI")],
                         header="ZSSAG_XI", always_present=True),
        ExportCSV.Column([Selection(variable_name="backscatter_fraction", wavelength=(500, 600),
                                    instrument_id="XI")],
                         header="ZBfrG_XI", always_present=True),
        ExportCSV.Column([Selection(variable_name="scattering_angstrom_exponent", wavelength=(500, 600),
                                    instrument_id="XI")],
                         header="ZAngBsG_XI", always_present=True),
        ExportCSV.Column([Selection(variable_name="radiative_forcing_efficiency", wavelength=(500, 600),
                                    instrument_id="XI")],
                         header="ZRFEG_XI", always_present=True),
        ExportCSV.Column([Selection(variable_name="asymmetry_parameter", wavelength=(500, 600),
                                    instrument_id="XI")],
                         header="ZGG_XI", always_present=True),
    ]))
for archive in ("raw", "clean",):
    aerosol_exports[archive].append(ExportCSV("scattering", "Scattering", [
        ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="Bs" + code + "_{instrument_id}", default_header=f"Bs{code}", always_present=True)
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="Bbs" + code + "_{instrument_id}")
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="sample_temperature",
                                    require_tags={"scattering"}, exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="sample_humidity",
                                    require_tags={"scattering"}, exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="sample_pressure",
                                    require_tags={"scattering"}, exclude_tags={"secondary"})]),
    ]))
    aerosol_exports[archive].append(ExportCSV("absorption", "Absorption", [
        ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                    require_tags={"absorption"},
                                    exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                         header="Ba" + code + "_{instrument_id}", default_header=f"Ba{code}", always_present=True)
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="sample_flow",
                                    require_tags={"absorption"},
                                    exclude_tags={"secondary", "aethalometer", "thermomaap"})]),
        ExportCSV.Column([Selection(variable_name="path_length_change",
                                    require_tags={"absorption"},
                                    exclude_tags={"secondary", "aethalometer", "thermomaap"})]),
        ExportCSV.Column([Selection(variable_name="spot_number",
                                    require_tags={"absorption"},
                                    exclude_tags={"secondary", "aethalometer", "thermomaap"})]),
    ]))
    aerosol_exports[archive].append(ExportCSV("counts", "Counts", [
        ExportCSV.Column([Selection(variable_name="number_concentration",
                                    require_tags={"cpc"}, exclude_tags={"secondary"})],
                         default_header="N", always_present=True)
    ]))
    aerosol_exports[archive].append(ExportCSV("aethalometer", "Aethalometer", [
        ExportCSV.Column([Selection(variable_id="Ba", wavelength_number=wl,
                                    require_tags={"aethalometer"}, exclude_tags={"secondary"})],
                         header="Ba" + str(wl+1) + "_{instrument_id}", default_header=f"Ba{wl+1}", always_present=True)
        for wl in range(7)
    ] + [
        ExportCSV.Column([Selection(variable_name="equivalent_black_carbon", wavelength_number=wl,
                                    require_tags={"aethalometer"}, exclude_tags={"secondary"})],
                         header="X" + str(wl+1) + "_{instrument_id}", default_header=f"X{wl+1}", always_present=True)
        for wl in range(7)
    ] + [
        ExportCSV.Column([Selection(variable_id="Ir", wavelength_number=wl,
                                    require_tags={"aethalometer"}, exclude_tags={"secondary"})],
                         header="Ir" + str(wl+1) + "_{instrument_id}", default_header=f"Ir{wl+1}")
        for wl in range(7)
    ] + [
        ExportCSV.Column([Selection(variable_name="correction_factor", wavelength_number=wl,
                                    require_tags={"aethalometer", "mageeae33"}, exclude_tags={"secondary"})],
                         header="ZFACTOR" + str(wl+1) + "_{instrument_id}", default_header=f"ZFACTOR{wl+1}")
        for wl in range(7)
    ]))
for archive in ("avgh",):
    aerosol_exports[archive].append(ExportCSV("intensive", "Intensive", [
        ExportCSV.Column([Selection(variable_name="number_concentration", cut_size=cut_size,
                                    instrument_id="XI")],
                         header="N" + record + "_XI")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ] + [
        ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                    instrument_id="XI")],
                         header=f"Bs{code}{record}_XI")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                    instrument_id="XI")],
                         header=f"Ba{code}{record}_XI")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="single_scattering_albedo", wavelength=(500, 600), cut_size=cut_size,
                                    instrument_id="XI")],
                         header=f"ZSSAG{record}_XI")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ] + [
        ExportCSV.Column([Selection(variable_name="backscatter_fraction", wavelength=(500, 600), cut_size=cut_size,
                                    instrument_id="XI")],
                         header=f"ZBfrG{record}_XI")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ] + [
        ExportCSV.Column([Selection(variable_name="scattering_angstrom_exponent", wavelength=(500, 600), cut_size=cut_size,
                                    instrument_id="XI")],
                         header=f"ZAngBsG{record}_XI")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ] + [
        ExportCSV.Column([Selection(variable_name="radiative_forcing_efficiency", wavelength=(500, 600), cut_size=cut_size,
                                    instrument_id="XI")],
                         header=f"ZRFEG{record}_XI")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ] + [
        ExportCSV.Column([Selection(variable_name="asymmetry_parameter", wavelength=(500, 600), cut_size=cut_size,
                                    instrument_id="XI")],
                         header=f"ZGG{record}_XI")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ]))
    aerosol_exports[archive].append(ExportCSV("scattering", "Scattering", [
        ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="Bs" + code + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="Bbs" + code + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="sample_temperature", cut_size=cut_size,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="T" + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ] + [
        ExportCSV.Column([Selection(variable_name="sample_humidity", cut_size=cut_size,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="U" + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ] + [
        ExportCSV.Column([Selection(variable_name="sample_pressure", cut_size=cut_size,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="P" + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ]))
    aerosol_exports[archive].append(ExportCSV("absorption", "Absorption", [
        ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                    require_tags={"absorption"},
                                    exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                         header="Ba" + code + record + "_{instrument_id}", default_header=f"Ba{code}", always_present=True)
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="sample_flow", cut_size=cut_size,
                                    require_tags={"absorption"},
                                    exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                         header="Q" + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ] + [
        ExportCSV.Column([Selection(variable_name="path_length_change", cut_size=cut_size,
                                    require_tags={"absorption"},
                                    exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                         header="Ld" + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ] + [
        ExportCSV.Column([Selection(variable_name="spot_number", cut_size=cut_size,
                                    require_tags={"absorption"},
                                    exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                         header="Fn" + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ]))
    aerosol_exports[archive].append(ExportCSV("counts", "Counts", [
        ExportCSV.Column([Selection(variable_name="number_concentration", cut_size=cut_size,
                                    require_tags={"cpc"}, exclude_tags={"secondary"})],
                         header="N" + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ]))
    aerosol_exports[archive].append(ExportCSV("aethalometer", "Aethalometer", [
        ExportCSV.Column([Selection(variable_id="Ba", wavelength_number=wl, cut_size=cut_size,
                                    require_tags={"aethalometer"}, exclude_tags={"secondary"})],
                         header="Ba" + str(wl+1) + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        for wl in range(7)
    ] + [
        ExportCSV.Column([Selection(variable_name="equivalent_black_carbon", wavelength_number=wl, cut_size=cut_size,
                                    require_tags={"aethalometer"}, exclude_tags={"secondary"})],
                         header="X" + str(wl+1) + record + "_{instrument_id}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        for wl in range(7)
    ] + [
        ExportCSV.Column([Selection(variable_id="Ir", wavelength_number=wl, cut_size=cut_size,
                                    require_tags={"aethalometer"}, exclude_tags={"secondary"})],
                         header="Ir" + str(wl+1) + record + "_{instrument_id}", default_header=f"Ir{wl+1}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        for wl in range(7)
    ] + [
        ExportCSV.Column([Selection(variable_name="correction_factor", wavelength_number=wl, cut_size=cut_size,
                                    require_tags={"aethalometer", "mageeae33"}, exclude_tags={"secondary"})],
                         header="ZFACTOR" + str(wl+1) + record + "_{instrument_id}", default_header=f"ZFACTOR{wl+1}")
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        for wl in range(7)
    ]))

aerosol_exports["raw"].append(ExportEBAS(
    display="EBAS Level 0",
    ebas=["absorption_lev0", "scattering_lev0", "cpc_lev0"],
))
aerosol_exports["clean"].append(ExportEBAS(
    display="EBAS Level 1",
    ebas=["absorption_lev1", "scattering_lev1", "cpc_lev1"],
))
aerosol_exports["avgh"].append(ExportEBAS(
    display="EBAS Level 2",
    ebas=["absorption_lev2", "scattering_lev2", "cpc_lev2"],
))
aerosol_exports["raw"].append(ExportCompleteRawNetCDF())
for archive in ("clean", "avgh"):
    aerosol_exports[archive].append(ExportNetCDF())


ozone_exports: typing.Dict[str, typing.List[ArchiveExportEntry]] = dict()
for archive in ("raw", "clean", "avgh"):
    ozone_exports[archive] = list()
    ozone_exports[archive].append(ExportCSV("basic", "Basic", [
        ExportCSV.Column([Selection(standard_name="mole_fraction_of_ozone_in_air",
                                    require_tags={"ozone"}, exclude_tags={"secondary"})],
                         default_header="X", always_present=True),
        ExportCSV.Column([Selection(variable_name="sample_temperature",
                                    require_tags={"ozone"}, exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="lamp_temperature",
                                    require_tags={"ozone"}, exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="sample_pressure",
                                    require_tags={"ozone"}, exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="cell_a_flow",
                                    require_tags={"ozone"}, exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="cell_b_flow",
                                    require_tags={"ozone"}, exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="cell_a_count_rate",
                                    require_tags={"ozone"}, exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="cell_b_count_rate",
                                    require_tags={"ozone"}, exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="wind_speed", exclude_tags={"secondary"})]),
        ExportCSV.Column([Selection(variable_name="wind_direction", exclude_tags={"secondary"})]),
    ], format=ExportCSV.Format(cut_size=False)))
    ozone_exports[archive].append(ExportNetCDF(selections=[InstrumentSelection(require_tags={"ozone"})]))


met_exports: typing.Dict[str, typing.List[ArchiveExportEntry]] = dict()
for archive in ("raw", "clean", "avgh"):
    met_exports[archive] = list()
    met_exports[archive].append(ExportCSV("ambient", "Ambient", [
        ExportCSV.Column([Selection(variable_id="WS1", instrument_id="XM1")],
                         default_header="WS1", always_present=True),
        ExportCSV.Column([Selection(variable_id="WD1", instrument_id="XM1")],
                         default_header="WD1", always_present=True),
        ExportCSV.Column([Selection(variable_id="WS2", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="WD2", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="WS3", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="WD3", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="T1", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="U1", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="TD1", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="T2", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="U2", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="TD2", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="T3", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="U3", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="TD3", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="P", instrument_id="XM1")]),
        ExportCSV.Column([Selection(variable_id="WI", instrument_id="XM1")]),
    ], format=ExportCSV.Format(cut_size=False)))
    met_exports[archive].append(ExportNetCDF(selections=[InstrumentSelection(require_tags={"met"})]))


export_entries: {typing.Dict[str, typing.Dict[str, typing.List[ArchiveExportEntry]]]} = dict()
export_entries["aerosol"] = aerosol_exports
export_entries["ozone"] = ozone_exports
export_entries["met"] = met_exports


def export_get(station: str, mode_name: str, export_key: str,
               start_epoch_ms: int, end_epoch_ms: int, directory: str,
               exports: typing.Dict[str, typing.Dict[str, typing.List[ArchiveExportEntry]]]) -> typing.Optional[Export]:
    components = mode_name.split('-', 2)
    if len(components) < 2:
        return None
    profile = components[0]
    profile = exports.get(profile)
    if not profile:
        return None
    archive = components[1]
    archive = profile.get(archive)
    if not archive:
        return None
    for entry in archive:
        if entry.key == export_key:
            return entry(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory)
    return None


def export_visible(station: str, mode_name: str,
                   exports: typing.Dict[str, typing.Dict[str, typing.List[ArchiveExportEntry]]]) -> typing.Optional[ExportList]:
    components = mode_name.split('-', 2)
    if len(components) < 2:
        return None
    profile = components[0]
    profile = exports.get(profile)
    if not profile:
        return None
    archive = components[1]
    archive = profile.get(archive)
    if not archive:
        return None
    return ExportList(archive)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    from forge.vis.station.cpd3 import use_cpd3, export_get as cpd3_get
    if use_cpd3(station):
        return cpd3_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory)
    return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    from forge.vis.station.cpd3 import use_cpd3, export_available as cpd3_available
    if use_cpd3(station):
        return cpd3_available(station, mode_name)
    return export_visible(station, mode_name, export_entries)
