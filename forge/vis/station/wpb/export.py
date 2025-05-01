import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("wpb"):
    from ..cpd3 import Name, DataExport, DataExportList, detach, profile_export, export_profile_get, export_profile_lookup

    station_profile_export = detach(profile_export)

    station_profile_export['aerosol']['raw']['absorption'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'raw', 'Q_A11'),
            Name(station, 'raw', 'L_A11'),
            Name(station, 'raw', 'Fn_A11'),
            Name(station, 'raw', 'BaB_A11'),
            Name(station, 'raw', 'BaG_A11'),
            Name(station, 'raw', 'BaR_A11'),
            Name(station, 'raw', 'IrB_A11'),
            Name(station, 'raw', 'IrG_A11'),
            Name(station, 'raw', 'IrR_A11'),
            Name(station, 'raw', 'Q_A12'),
            Name(station, 'raw', 'L_A12'),
            Name(station, 'raw', 'Fn_A12'),
            Name(station, 'raw', 'BaB_A12'),
            Name(station, 'raw', 'BaG_A12'),
            Name(station, 'raw', 'BaR_A12'),
            Name(station, 'raw', 'IrB_A12'),
            Name(station, 'raw', 'IrG_A12'),
            Name(station, 'raw', 'IrR_A12'),
        },
    )
    station_profile_export['aerosol']['clean']['absorption'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'clean', 'Q_A11'),
            Name(station, 'clean', 'L_A11'),
            Name(station, 'clean', 'Fn_A11'),
            Name(station, 'clean', 'BaB_A11'),
            Name(station, 'clean', 'BaG_A11'),
            Name(station, 'clean', 'BaR_A11'),
            Name(station, 'clean', 'IrB_A11'),
            Name(station, 'clean', 'IrG_A11'),
            Name(station, 'clean', 'IrR_A11'),
            Name(station, 'clean', 'Q_A12'),
            Name(station, 'clean', 'L_A12'),
            Name(station, 'clean', 'Fn_A12'),
            Name(station, 'clean', 'BaB_A12'),
            Name(station, 'clean', 'BaG_A12'),
            Name(station, 'clean', 'BaR_A12'),
            Name(station, 'clean', 'IrB_A12'),
            Name(station, 'clean', 'IrG_A12'),
            Name(station, 'clean', 'IrR_A12'),
        },
    )
    station_profile_export['aerosol']['avgh']['absorption'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'avgh', 'Q_A11'),
            Name(station, 'avgh', 'L_A11'),
            Name(station, 'avgh', 'Fn_A11'),
            Name(station, 'avgh', 'BaB_A11'),
            Name(station, 'avgh', 'BaG_A11'),
            Name(station, 'avgh', 'BaR_A11'),
            Name(station, 'avgh', 'IrB_A11'),
            Name(station, 'avgh', 'IrG_A11'),
            Name(station, 'avgh', 'IrR_A11'),
            Name(station, 'avgh', 'Q_A12'),
            Name(station, 'avgh', 'L_A12'),
            Name(station, 'avgh', 'Fn_A12'),
            Name(station, 'avgh', 'BaB_A12'),
            Name(station, 'avgh', 'BaG_A12'),
            Name(station, 'avgh', 'BaR_A12'),
            Name(station, 'avgh', 'IrB_A12'),
            Name(station, 'avgh', 'IrG_A12'),
            Name(station, 'avgh', 'IrR_A12'),
        },
    )

    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('ambient', "Ambient Meteorological", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'WS_XM1'), Name(station, 'raw', 'WD_XM1'),
                Name(station, 'raw', 'WI_XM1'),
                Name(station, 'raw', 'T1_XM1'), Name(station, 'raw', 'U1_XM1'), Name(station, 'raw', 'TD1_XM1'),
                Name(station, 'raw', 'P_XM1'),
            },
        ))
    )
    station_profile_export['aerosol']['clean'].insert(
        DataExportList.Entry('ambient', "Ambient Meteorological", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'clean', 'WS_XM1'), Name(station, 'clean', 'WD_XM1'),
                Name(station, 'clean', 'WI_XM1'),
                Name(station, 'clean', 'T1_XM1'), Name(station, 'clean', 'U1_XM1'), Name(station, 'clean', 'TD1_XM1'),
                Name(station, 'clean', 'P_XM1'),
            },
        ))
    )
    station_profile_export['aerosol']['avgh'].insert(
        DataExportList.Entry('ambient', "Ambient Meteorological", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'avgh', 'WS_XM1'), Name(station, 'avgh', 'WD_XM1'),
                Name(station, 'avgh', 'WI_XM1'),
                Name(station, 'avgh', 'T1_XM1'), Name(station, 'avgh', 'U1_XM1'), Name(station, 'avgh', 'TD1_XM1'),
                Name(station, 'avgh', 'P_XM1'),
            },
        ))
    )


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_profile_get(station, mode_name, export_key,
                                  start_epoch_ms, end_epoch_ms, directory, station_profile_export)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_profile_lookup(station, mode_name, station_profile_export)

else:
    from ..default.export import aerosol_exports, export_get, export_visible, ExportCSV, Selection, STANDARD_CUT_SIZE_SPLIT, STANDARD_THREE_WAVELENGTHS
    from copy import deepcopy

    export_entries = dict()
    export_entries["aerosol"] = deepcopy(aerosol_exports)

    for archive in ("raw", "clean",):
        export_entries[archive].append(ExportCSV("absorption", "Absorption", [
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                        require_tags={"absorption"},
                                        exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                             header="Ba" + code + "_{instrument_id}", default_header=f"Ba{code}", always_present=True)
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})],
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
            ExportCSV.Column([Selection(variable_name="sample_flow",
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})]),
            ExportCSV.Column([Selection(variable_name="path_length_change",
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"secondary", "aethalometer", "thermomaap"})]),
            ExportCSV.Column([Selection(variable_name="spot_number",
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})]),
        ]))
    for archive in ("avgh",):
        aerosol_exports[archive].append(ExportCSV("absorption", "Absorption", [
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                        require_tags={"absorption"},
                                        exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                             header="Ba" + code + record + "_{instrument_id}", default_header=f"Ba{code}", always_present=True)
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] +  [
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})],
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
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_flow", cut_size=cut_size,
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Q" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="path_length_change", cut_size=cut_size,
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Ld" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="spot_number", cut_size=cut_size,
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Fn" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ]))


    for archive in ("raw", "clean", "avgh"):
        export_entries[archive].append(ExportCSV("ambient", "Ambient Meteorological", [
            ExportCSV.Column([Selection(variable_id="WS", instrument_id="XM1")]),
            ExportCSV.Column([Selection(variable_id="WD", instrument_id="XM1")]),
            ExportCSV.Column([Selection(variable_id="T1", instrument_id="XM1")]),
            ExportCSV.Column([Selection(variable_id="U1", instrument_id="XM1")]),
            ExportCSV.Column([Selection(variable_id="TD1", instrument_id="XM1")]),
            ExportCSV.Column([Selection(variable_id="P", instrument_id="XM1")]),
        ], format=ExportCSV.Format(cut_size=False)))


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)