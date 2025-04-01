import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("mbo"):
    from ..cpd3 import Name, DataExport, DataExportList, detach, profile_export, export_profile_get, export_profile_lookup

    station_profile_export = detach(profile_export)

    station_profile_export['aerosol']['raw']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'raw', 'T_S11'),
            Name(station, 'raw', 'P_S11'),
            Name(station, 'raw', 'U_S11'),
            Name(station, 'raw', 'T_S12'),
            Name(station, 'raw', 'P_S12'),
            Name(station, 'raw', 'U_S12'),
            Name(station, 'raw', 'BsB_S11'),
            Name(station, 'raw', 'BsG_S11'),
            Name(station, 'raw', 'BsR_S11'),
            Name(station, 'raw', 'BbsB_S11'),
            Name(station, 'raw', 'BbsG_S11'),
            Name(station, 'raw', 'BbsR_S11'),
            Name(station, 'raw', 'BsB_S12'),
            Name(station, 'raw', 'BsG_S12'),
            Name(station, 'raw', 'BsR_S12'),
            Name(station, 'raw', 'BbsB_S12'),
            Name(station, 'raw', 'BbsG_S12'),
            Name(station, 'raw', 'BbsR_S12'),
        },
    )
    station_profile_export['aerosol']['clean']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'clean', 'T_S11'),
            Name(station, 'clean', 'P_S11'),
            Name(station, 'clean', 'U_S11'),
            Name(station, 'clean', 'T_S12'),
            Name(station, 'clean', 'P_S12'),
            Name(station, 'clean', 'U_S12'),
            Name(station, 'clean', 'BsB_S11'),
            Name(station, 'clean', 'BsG_S11'),
            Name(station, 'clean', 'BsR_S11'),
            Name(station, 'clean', 'BbsB_S11'),
            Name(station, 'clean', 'BbsG_S11'),
            Name(station, 'clean', 'BbsR_S11'),
            Name(station, 'clean', 'BsB_S12'),
            Name(station, 'clean', 'BsG_S12'),
            Name(station, 'clean', 'BsR_S12'),
            Name(station, 'clean', 'BbsB_S12'),
            Name(station, 'clean', 'BbsG_S12'),
            Name(station, 'clean', 'BbsR_S12'),
        },
    )
    station_profile_export['aerosol']['avgh']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'avgh', 'T_S11'),
            Name(station, 'avgh', 'P_S11'),
            Name(station, 'avgh', 'U_S11'),
            Name(station, 'avgh', 'T_S12'),
            Name(station, 'avgh', 'P_S12'),
            Name(station, 'avgh', 'U_S12'),
            Name(station, 'avgh', 'BsB_S11'),
            Name(station, 'avgh', 'BsG_S11'),
            Name(station, 'avgh', 'BsR_S11'),
            Name(station, 'avgh', 'BbsB_S11'),
            Name(station, 'avgh', 'BbsG_S11'),
            Name(station, 'avgh', 'BbsR_S11'),
            Name(station, 'avgh', 'BsB_S12'),
            Name(station, 'avgh', 'BsG_S12'),
            Name(station, 'avgh', 'BsR_S12'),
            Name(station, 'avgh', 'BbsB_S12'),
            Name(station, 'avgh', 'BbsG_S12'),
            Name(station, 'avgh', 'BbsR_S12'),
        },
    )

    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('met', "Meteorological", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'WS1_XM1'), Name(station, 'raw', 'WD1_XM1'),
                Name(station, 'raw', 'WS2_XM1'), Name(station, 'raw', 'WD2_XM1'),
                Name(station, 'raw', 'WS3_XM1'), Name(station, 'raw', 'WD3_XM1'),
                Name(station, 'raw', 'T1_XM1'), Name(station, 'raw', 'U1_XM1'), Name(station, 'raw', 'TD1_XM1'),
                Name(station, 'raw', 'T2_XM1'), Name(station, 'raw', 'U2_XM1'), Name(station, 'raw', 'TD2_XM1'),
                Name(station, 'raw', 'T3_XM1'), Name(station, 'raw', 'U3_XM1'), Name(station, 'raw', 'TD3_XM1'),
                Name(station, 'raw', 'P_XM1'),
                Name(station, 'raw', 'WI_XM1'),
            },
        )),
    )
    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('gas', "Gas Measurements", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'X_G81'),
                Name(station, 'raw', 'X_G82'),
                Name(station, 'raw', 'X1_G71'),
                Name(station, 'raw', 'X2_G71'),
            },
        )),
    )
    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('combined', "Combined Summary", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'raw', 'BsG_S11'),
                Name(station, 'raw', 'BaG_A11'),
                Name(station, 'raw', 'T1_XM1'),
                Name(station, 'raw', 'U1_XM1'),
                Name(station, 'raw', 'X_G81'),
                Name(station, 'raw', 'X_G82'),
                Name(station, 'raw', 'X1_G71'),
                Name(station, 'raw', 'X2_G71'),
            },
        )),
    )
    station_profile_export['aerosol']['clean'].insert(
        DataExportList.Entry('combined', "Combined Summary", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'clean', 'BsG_S11'),
                Name(station, 'clean', 'BaG_A11'),
                Name(station, 'raw', 'T1_XM1'),
                Name(station, 'raw', 'U1_XM1'),
                Name(station, 'raw', 'X_G81'),
                Name(station, 'raw', 'X_G82'),
                Name(station, 'raw', 'X1_G71'),
                Name(station, 'raw', 'X2_G71'),
            },
        )),
    )


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_profile_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, station_profile_export)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_profile_lookup(station, mode_name, station_profile_export)

else:
    from ..default.export import aerosol_exports, ozone_exports, export_get, export_visible, ExportCSV, Selection, STANDARD_CUT_SIZE_SPLIT, STANDARD_THREE_WAVELENGTHS
    from copy import deepcopy

    export_entries = dict()
    export_entries["aerosol"] = deepcopy(aerosol_exports)
    export_entries["ozone"] = ozone_exports

    for archive in ("raw", "clean",):
        for entry in export_entries["aerosol"][archive]:
            if entry.key != "scattering":
                continue
            entry.columns.extend([
                ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength,
                                            require_tags={"scattering"}, instrument_id="S12")],
                                 header="Bs" + code + "_{instrument_id}", default_header=f"Bs{code}", always_present=True)
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
            entry.columns.extend([
                ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength,
                                            require_tags={"scattering"}, instrument_id="S12")],
                                 header="Bbs" + code + "_{instrument_id}")
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
            entry.columns.extend([
                ExportCSV.Column([Selection(variable_name="sample_temperature",
                                            require_tags={"scattering"}, instrument_id="S12")]),
                ExportCSV.Column([Selection(variable_name="sample_humidity",
                                            require_tags={"scattering"}, instrument_id="S12")]),
                ExportCSV.Column([Selection(variable_name="sample_pressure",
                                            require_tags={"scattering"}, instrument_id="S12")]),
            ])
    for archive in ("avgh",):
        for entry in export_entries["aerosol"][archive]:
            if entry.key != "scattering":
                continue
            entry.columns.extend([
                ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                            require_tags={"scattering"}, instrument_id="S12")],
                                 header="Bs" + code + record + "_{instrument_id}")
                for record, cut_size in STANDARD_CUT_SIZE_SPLIT
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
            entry.columns.extend([
                ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                            require_tags={"scattering"}, instrument_id="S12")],
                                 header="Bbs" + code + record + "_{instrument_id}")
                for record, cut_size in STANDARD_CUT_SIZE_SPLIT
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
            entry.columns.extend([
                ExportCSV.Column([Selection(variable_name="sample_temperature", cut_size=cut_size,
                                            require_tags={"scattering"}, instrument_id="S12")],
                                 header="T" + record + "_{instrument_id}")
                for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            ])
            entry.columns.extend([
                ExportCSV.Column([Selection(variable_name="sample_humidity", cut_size=cut_size,
                                            require_tags={"scattering"}, instrument_id="S12")],
                                 header="U" + record + "_{instrument_id}")
                for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            ])
            entry.columns.extend([
                ExportCSV.Column([Selection(variable_name="sample_pressure", cut_size=cut_size,
                                            require_tags={"scattering"}, instrument_id="S12")],
                                 header="P" + record + "_{instrument_id}")
                for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            ])

    for archive in ("raw", "clean", "avgh"):
        export_entries["aerosol"][archive].append(ExportCSV("met", "Meteorological", [
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

    for archive in ("raw", "clean"):
        export_entries["aerosol"][archive].append(ExportCSV("gas", "Gas Measurements", [
            ExportCSV.Column([Selection(variable_id="X", instrument_id="G81")]),
            ExportCSV.Column([Selection(variable_id="X", instrument_id="G82")]),
            ExportCSV.Column([Selection(variable_id="X1", instrument_id="G71")]),
            ExportCSV.Column([Selection(variable_id="X2", instrument_id="G71")]),
        ], format=ExportCSV.Format(cut_size=False, uniform_time=60 * 1000)))
        export_entries["aerosol"][archive].append(ExportCSV("combined", "Combined Summary", [
            ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=(500, 600),
                                        require_tags={"scattering"}, exclude_tags={"secondary"})],
                             header="BsG_S11", always_present=True),
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=(500, 600),
                                        require_tags={"absorption"},
                                        exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                             header="BaG_A11", always_present=True),
            ExportCSV.Column([Selection(variable_id="T1", instrument_id="XM1")],
                             header="T1_XM1", always_present=True),
            ExportCSV.Column([Selection(variable_id="U1", instrument_id="XM1")],
                             header="U1_XM1", always_present=True),
            ExportCSV.Column([Selection(variable_id="X", instrument_id="G81")],
                             header="X_G81", always_present=True),
            ExportCSV.Column([Selection(variable_id="X", instrument_id="G82")],
                             header="X_G82", always_present=True),
            ExportCSV.Column([Selection(variable_id="X1", instrument_id="G71")],
                             header="X1_G71", always_present=True),
            ExportCSV.Column([Selection(variable_id="X2", instrument_id="G71")],
                             header="X2_G71", always_present=True),
        ], format=ExportCSV.Format(uniform_time=60 * 1000)))
    for archive in ("avgh",):
        export_entries["aerosol"][archive].append(ExportCSV("gas", "Gas Measurements", [
            ExportCSV.Column([Selection(variable_id="X", instrument_id="G81")]),
            ExportCSV.Column([Selection(variable_id="X", instrument_id="G82")]),
            ExportCSV.Column([Selection(variable_id="X1", instrument_id="G71")]),
            ExportCSV.Column([Selection(variable_id="X2", instrument_id="G71")]),
        ], format=ExportCSV.Format(cut_size=False)))
        export_entries["aerosol"][archive].append(ExportCSV("combined", "Combined Summary", [
            ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=(500, 600), cut_size=cut_size,
                                        require_tags={"scattering"}, exclude_tags={"secondary"})],
                             header="BsG" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=(500, 600), cut_size=cut_size,
                                        require_tags={"absorption"},
                                        exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                             header="BaG" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="T1", instrument_id="XM1")],
                             header="T1_XM1", always_present=True),
            ExportCSV.Column([Selection(variable_id="U1", instrument_id="XM1")],
                             header="U1_XM1", always_present=True),
            ExportCSV.Column([Selection(variable_id="X", instrument_id="G81")],
                             header="X_G81", always_present=True),
            ExportCSV.Column([Selection(variable_id="X", instrument_id="G82")],
                             header="X_G82", always_present=True),
            ExportCSV.Column([Selection(variable_id="X1", instrument_id="G71")],
                             header="X1_G71", always_present=True),
            ExportCSV.Column([Selection(variable_id="X2", instrument_id="G71")],
                             header="X2_G71", always_present=True),
        ]))

    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)
