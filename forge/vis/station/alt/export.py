import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("alt"):
    from ..cpd3 import Name, DataExport, DataExportList, detach, profile_export, export_profile_get, export_profile_lookup

    station_profile_export = detach(profile_export)

    station_profile_export['aerosol']['raw']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'raw', 'N_N61'),
            Name(station, 'raw', 'N_N62'),
        },
    )
    station_profile_export['aerosol']['clean']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'clean', 'N_N61'),
            Name(station, 'clean', 'N_N62'),
        },
    )
    station_profile_export['aerosol']['avgh']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', {
            Name(station, 'avgh', 'N_N61'),
            Name(station, 'avgh', 'N_N62'),
        },
    )

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
            Name(station, 'raw', 'Q_A13'),
            Name(station, 'raw', 'L_A13'),
            Name(station, 'raw', 'Fn_A13'),
            Name(station, 'raw', 'BaB_A13'),
            Name(station, 'raw', 'BaG_A13'),
            Name(station, 'raw', 'BaR_A13'),
            Name(station, 'raw', 'IrB_A13'),
            Name(station, 'raw', 'IrG_A13'),
            Name(station, 'raw', 'IrR_A13'),
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
            Name(station, 'clean', 'Q_A13'),
            Name(station, 'clean', 'L_A13'),
            Name(station, 'clean', 'Fn_A13'),
            Name(station, 'clean', 'BaB_A13'),
            Name(station, 'clean', 'BaG_A13'),
            Name(station, 'clean', 'BaR_A13'),
            Name(station, 'clean', 'IrB_A13'),
            Name(station, 'clean', 'IrG_A13'),
            Name(station, 'clean', 'IrR_A13'),
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
            Name(station, 'avgh', 'Q_A13'),
            Name(station, 'avgh', 'L_A13'),
            Name(station, 'avgh', 'Fn_A13'),
            Name(station, 'avgh', 'BaB_A13'),
            Name(station, 'avgh', 'BaG_A13'),
            Name(station, 'avgh', 'BaR_A13'),
            Name(station, 'avgh', 'IrB_A13'),
            Name(station, 'avgh', 'IrG_A13'),
            Name(station, 'avgh', 'IrR_A13'),
        },
    )

    station_profile_export['aerosol']['raw']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'raw', f'Ba{i + 1}_A81') for i in range(7)] +
            [Name(station, 'raw', f'X{i + 1}_A81') for i in range(7)] +
            [Name(station, 'raw', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
            [Name(station, 'raw', f'Ir{i + 1}_A81') for i in range(7)] +
            [Name(station, 'raw', f'Ba{i + 1}_A82') for i in range(7)] +
            [Name(station, 'raw', f'X{i + 1}_A82') for i in range(7)] +
            [Name(station, 'raw', f'ZFACTOR{i + 1}_A82') for i in range(7)] +
            [Name(station, 'raw', f'Ir{i + 1}_A82') for i in range(7)]
        )
    )
    station_profile_export['aerosol']['clean']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'clean', f'Ba{i + 1}_A81') for i in range(7)] +
            [Name(station, 'clean', f'X{i + 1}_A81') for i in range(7)] +
            [Name(station, 'clean', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
            [Name(station, 'clean', f'Ir{i + 1}_A81') for i in range(7)] +
            [Name(station, 'clean', f'Ba{i + 1}_A82') for i in range(7)] +
            [Name(station, 'clean', f'X{i + 1}_A82') for i in range(7)] +
            [Name(station, 'clean', f'ZFACTOR{i + 1}_A82') for i in range(7)] +
            [Name(station, 'clean', f'Ir{i + 1}_A82') for i in range(7)]
        )
    )
    station_profile_export['aerosol']['avgh']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', set(
            [Name(station, 'avgh', f'Ba{i + 1}_A81') for i in range(7)] +
            [Name(station, 'avgh', f'X{i + 1}_A81') for i in range(7)] +
            [Name(station, 'avgh', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
            [Name(station, 'avgh', f'Ir{i + 1}_A81') for i in range(7)] +
            [Name(station, 'avgh', f'Ba{i + 1}_A82') for i in range(7)] +
            [Name(station, 'avgh', f'X{i + 1}_A82') for i in range(7)] +
            [Name(station, 'avgh', f'ZFACTOR{i + 1}_A82') for i in range(7)] +
            [Name(station, 'avgh', f'Ir{i + 1}_A82') for i in range(7)]
        )
    )


    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('smps', "SMPS", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'Ns_N12'),
                Name(station, 'raw', 'Nn_N12'),
            },
        ))
    )
    station_profile_export['aerosol']['clean'].insert(
        DataExportList.Entry('smps', "SMPS", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'clean', 'Ns_N12'),
                Name(station, 'clean', 'Nn_N12'),
            },
        ))
    )
    station_profile_export['aerosol']['avgh'].insert(
        DataExportList.Entry('smps', "SMPS", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'avgh', 'Ns_N12'),
                Name(station, 'avgh', 'Nn_N12'),
            },
        ))
    )

    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('grimm', "Grimm", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'Ns_N11'),
                Name(station, 'raw', 'Nn_N11'),
                Name(station, 'raw', 'Nb_N11'),
                Name(station, 'raw', 'N_N11'),
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
    from ..default.export import aerosol_exports, find_key, export_get, export_visible, ExportCSV, Selection, STANDARD_CUT_SIZE_SPLIT, STANDARD_THREE_WAVELENGTHS
    from copy import deepcopy

    export_entries = dict()
    export_entries["aerosol"] = deepcopy(aerosol_exports)

    for archive in ("raw", "clean",):
        find_key(export_entries["aerosol"][archive], "absorption").columns.extend([
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Ba" + code + "_{instrument_id}", default_header=f"Ba{code}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_flow",
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})]),
            ExportCSV.Column([Selection(variable_name="path_length_change",
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"secondary", "aethalometer", "thermomaap"})]),
            ExportCSV.Column([Selection(variable_name="spot_number",
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})]),
        ])
    for archive in ("avgh",):
        find_key(export_entries["aerosol"][archive], "absorption").columns.extend([
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Ba" + code + record + "_{instrument_id}", default_header=f"Ba{code}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
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
        ])


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)
