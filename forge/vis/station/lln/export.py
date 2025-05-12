import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("lln"):
    from ..cpd3 import Name, DataExport, detach, profile_export, export_profile_get, export_profile_lookup

    station_profile_export = detach(profile_export)

    station_profile_export['aerosol']['raw']['extensive'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'raw', 'T_S11'),
            Name(station, 'raw', 'P_S11'),
            Name(station, 'raw', 'U_S11'),
            Name(station, 'raw', 'BsB_S11'),
            Name(station, 'raw', 'BsG_S11'),
            Name(station, 'raw', 'BsR_S11'),
            Name(station, 'raw', 'BbsB_S11'),
            Name(station, 'raw', 'BbsG_S11'),
            Name(station, 'raw', 'BbsR_S11'),
            Name(station, 'raw', 'BaB_A11'),
            Name(station, 'raw', 'BaG_A11'),
            Name(station, 'raw', 'BaR_A11'),
            Name(station, 'raw', 'BaB_A12'),
            Name(station, 'raw', 'BaG_A12'),
            Name(station, 'raw', 'BaR_A12'),
            Name(station, 'raw', 'BaB_A91'),
            Name(station, 'raw', 'BaG_A91'),
            Name(station, 'raw', 'BaR_A91'),
            Name(station, 'raw', 'N_N71'),
            Name(station, 'raw', 'N_N61'),
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
            Name(station, 'raw', 'Q_A91'),
            Name(station, 'raw', 'L_A91'),
            Name(station, 'raw', 'Fn_A91'),
            Name(station, 'raw', 'BaB_A91'),
            Name(station, 'raw', 'BaG_A91'),
            Name(station, 'raw', 'BaR_A91'),
            Name(station, 'raw', 'IrB_A91'),
            Name(station, 'raw', 'IrG_A91'),
            Name(station, 'raw', 'IrR_A91'),
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
            Name(station, 'clean', 'Q_A91'),
            Name(station, 'clean', 'L_A91'),
            Name(station, 'clean', 'Fn_A91'),
            Name(station, 'clean', 'BaB_A91'),
            Name(station, 'clean', 'BaG_A91'),
            Name(station, 'clean', 'BaR_A91'),
            Name(station, 'clean', 'IrB_A91'),
            Name(station, 'clean', 'IrG_A91'),
            Name(station, 'clean', 'IrR_A91'),
        },
    )
    station_profile_export['aerosol']['avgh']['absorption'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', {
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
            Name(station, 'avgh', 'Q_A91'),
            Name(station, 'avgh', 'L_A91'),
            Name(station, 'avgh', 'Fn_A91'),
            Name(station, 'avgh', 'BaB_A91'),
            Name(station, 'avgh', 'BaG_A91'),
            Name(station, 'avgh', 'BaR_A91'),
            Name(station, 'avgh', 'IrB_A91'),
            Name(station, 'avgh', 'IrG_A91'),
            Name(station, 'avgh', 'IrR_A91'),
        },
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

    for archive in ("raw", ):
         find_key(export_entries["aerosol"][archive], "extensive").columns.extend([
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Ba" + code + "_{instrument_id}", default_header=f"Ba{code}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                        require_tags={"absorption"}, instrument_id="A91",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Ba" + code + "_{instrument_id}", default_header=f"Ba{code}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ])

    for archive in ("raw", "clean",):
        find_key(export_entries["aerosol"][archive], "absorption").columns.extend([
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                        require_tags={"absorption"}, instrument_id="A12",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Ba" + code + "_{instrument_id}", default_header=f"Ba{code}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength,
                                        require_tags={"absorption"}, instrument_id="A91",
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
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_flow",
                                        require_tags={"absorption"}, instrument_id="A91",
                                        exclude_tags={"aethalometer", "thermomaap"})]),
            ExportCSV.Column([Selection(variable_name="path_length_change",
                                        require_tags={"absorption"}, instrument_id="A91",
                                        exclude_tags={"secondary", "aethalometer", "thermomaap"})]),
            ExportCSV.Column([Selection(variable_name="spot_number",
                                        require_tags={"absorption"}, instrument_id="A91",
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
            ExportCSV.Column([Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                        require_tags={"absorption"}, instrument_id="A91",
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
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_flow", cut_size=cut_size,
                                        require_tags={"absorption"}, instrument_id="A91",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Q" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="path_length_change", cut_size=cut_size,
                                        require_tags={"absorption"}, instrument_id="A91",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Ld" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="spot_number", cut_size=cut_size,
                                        require_tags={"absorption"}, instrument_id="A91",
                                        exclude_tags={"aethalometer", "thermomaap"})],
                             header="Fn" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ])


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)