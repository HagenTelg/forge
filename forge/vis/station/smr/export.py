import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("smr"):
    from ..cpd3 import Name, DataExport, DataExportList, detach, profile_export, export_profile_get, export_profile_lookup
    station_profile_export = detach(profile_export)


    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'raw', 'F1_A21'),
                Name(station, 'raw', 'P_A21'),
                Name(station, 'raw', 'IfR_A21'),
                Name(station, 'raw', 'IpR_A21'),
                Name(station, 'raw', 'IrR_A21'),
                Name(station, 'raw', 'Is1_A21'),
                Name(station, 'raw', 'Is2_A21'),
                Name(station, 'raw', 'Pd1_A21'),
                Name(station, 'raw', 'Pd2_A21'),
                Name(station, 'raw', 'Q_A21'),
                Name(station, 'raw', 'Qt_A21'),
                Name(station, 'raw', 'T1_A21'),
                Name(station, 'raw', 'T2_A21'),
                Name(station, 'raw', 'T3_A21'),
                Name(station, 'raw', 'XR_A21'),
            },
        )),
    )
    station_profile_export['aerosol']['clean'].insert(
        DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'clean', 'F1_A21'),
                Name(station, 'clean', 'P_A21'),
                Name(station, 'clean', 'IfR_A21'),
                Name(station, 'clean', 'IpR_A21'),
                Name(station, 'clean', 'IrR_A21'),
                Name(station, 'clean', 'Is1_A21'),
                Name(station, 'clean', 'Is2_A21'),
                Name(station, 'clean', 'Pd1_A21'),
                Name(station, 'clean', 'Pd2_A21'),
                Name(station, 'clean', 'Q_A21'),
                Name(station, 'clean', 'Qt_A21'),
                Name(station, 'clean', 'T1_A21'),
                Name(station, 'clean', 'T2_A21'),
                Name(station, 'clean', 'T3_A21'),
                Name(station, 'clean', 'XR_A21'),
            },
        )),
    )
    station_profile_export['aerosol']['avgh'].insert(
        DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'average', {
                Name(station, 'avgh', 'F1_A21'),
                Name(station, 'avgh', 'P_A21'),
                Name(station, 'avgh', 'IfR_A21'),
                Name(station, 'avgh', 'IpR_A21'),
                Name(station, 'avgh', 'IrR_A21'),
                Name(station, 'avgh', 'Is1_A21'),
                Name(station, 'avgh', 'Is2_A21'),
                Name(station, 'avgh', 'Pd1_A21'),
                Name(station, 'avgh', 'Pd2_A21'),
                Name(station, 'avgh', 'Q_A21'),
                Name(station, 'avgh', 'T1_A21'),
                Name(station, 'avgh', 'T2_A21'),
                Name(station, 'avgh', 'T3_A21'),
                Name(station, 'avgh', 'XR_A21'),
            },
        ), time_limit_days=None),
    )


    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('nephzero', "Nephelometer Zero", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'Tw_S11'),
                Name(station, 'raw', 'Pw_S11'),
                Name(station, 'raw', 'BswB_S11'),
                Name(station, 'raw', 'BswG_S11'),
                Name(station, 'raw', 'BswR_S11'),
                Name(station, 'raw', 'BbswB_S11'),
                Name(station, 'raw', 'BbswG_S11'),
                Name(station, 'raw', 'BbswR_S11'),
            },
        ), time_limit_days=None),
    )


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_profile_get(station, mode_name, export_key,
                                  start_epoch_ms, end_epoch_ms, directory, station_profile_export)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_profile_lookup(station, mode_name, station_profile_export)

else:
    from ..default.export import aerosol_exports, export_get, find_key, export_visible, ExportCSV, Selection, STANDARD_CUT_SIZE_SPLIT, STANDARD_THREE_WAVELENGTHS
    from copy import deepcopy

    export_entries = dict()
    export_entries["aerosol"] = deepcopy(aerosol_exports)

    for archive in ("raw", "clean",):
        export_entries["aerosol"][archive].append(ExportCSV("maap", "MAAP", [
            ExportCSV.Column([Selection(variable_id="X", instrument_code="thermomaap", wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="P", instrument_code="thermomaap")]),
            ExportCSV.Column([Selection(variable_id="Q", instrument_code="thermomaap")]),
            ExportCSV.Column([Selection(variable_id="Ld", instrument_code="thermomaap")]),
            ExportCSV.Column([Selection(variable_id="T1", instrument_code="thermomaap")]),
            ExportCSV.Column([Selection(variable_id="T2", instrument_code="thermomaap")]),
            ExportCSV.Column([Selection(variable_id="T3", instrument_code="thermomaap")]),
            ExportCSV.Column([Selection(variable_id="Pd1", instrument_code="thermomaap")]),
            ExportCSV.Column([Selection(variable_id="Pd2", instrument_code="thermomaap")]),
            ExportCSV.Column([Selection(variable_id="Ir", instrument_code="thermomaap", wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="If", instrument_code="thermomaap", wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="Ip", instrument_code="thermomaap", wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="Is1", instrument_code="thermomaap", wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="Is2", instrument_code="thermomaap", wavelength_number=0)]),
        ]))
    for archive in ("avgh",):
        export_entries["aerosol"][archive].append(ExportCSV("maap", "MAAP", [
            ExportCSV.Column([Selection(variable_id="X", cut_size=cut_size,
                                        instrument_code="thermomaap", wavelength_number=0)],
                             header="X" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="P", cut_size=cut_size,
                                        instrument_code="thermomaap")],
                             header="P" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="Q", cut_size=cut_size,
                                        instrument_code="thermomaap")],
                             header="Q" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="Ld", cut_size=cut_size,
                                        instrument_code="thermomaap")],
                             header="Ld" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="T1", cut_size=cut_size,
                                        instrument_code="thermomaap")],
                             header="T1" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="T2", cut_size=cut_size,
                                        instrument_code="thermomaap")],
                             header="T2" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="T3", cut_size=cut_size,
                                        instrument_code="thermomaap")],
                             header="T3" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="Pd1", cut_size=cut_size,
                                        instrument_code="thermomaap")],
                             header="Pd1" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="Pd2", cut_size=cut_size,
                                        instrument_code="thermomaap")],
                             header="Pd2" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="Ir", cut_size=cut_size,
                                        instrument_code="thermomaap", wavelength_number=0)],
                             header="Ir" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="If", cut_size=cut_size,
                                        instrument_code="thermomaap", wavelength_number=0)],
                             header="If" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="Ip", cut_size=cut_size,
                                        instrument_code="thermomaap", wavelength_number=0)],
                             header="Ip" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="Is1", cut_size=cut_size,
                                        instrument_code="thermomaap", wavelength_number=0)],
                             header="Is1" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="Is2", cut_size=cut_size,
                                        instrument_code="thermomaap", wavelength_number=0)],
                             header="Is2" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ]))

    for archive in ("raw",):
        export_entries["aerosol"][archive].append(ExportCSV("nephzero", "Nephelometer Zero", [
            ExportCSV.Column([Selection(variable_name="wall_scattering_coefficient", wavelength=wavelength,
                                        require_tags={"scattering"}, exclude_tags={"secondary"})],
                             header="Bs" + code + "_{instrument_id}", default_header=f"Bsw{code}", always_present=True)
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                                        require_tags={"scattering"}, exclude_tags={"secondary"})],
                             header="Bbsw" + code + "_{instrument_id}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="zero_temperature",
                                        require_tags={"scattering"}, exclude_tags={"secondary"})]),
            ExportCSV.Column([Selection(variable_name="zero_pressure",
                                        require_tags={"scattering"}, exclude_tags={"secondary"})]),
        ]))

    ebas_export = find_key(export_entries["aerosol"]["raw"], "ebas")
    if ebas_export:
        ebas_export.ebas.add("maap_lev0")
    ebas_export = find_key(export_entries["aerosol"]["clean"], "ebas")
    if ebas_export:
        ebas_export.ebas.add("maap_lev1")
    ebas_export = find_key(export_entries["aerosol"]["avgh"], "ebas")
    if ebas_export:
        ebas_export.ebas.add("maap_lev2")

    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)
