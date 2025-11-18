import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("app"):
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
            Name(station, 'raw', 'T_S13'),
            Name(station, 'raw', 'P_S13'),
            Name(station, 'raw', 'U_S13'),
            Name(station, 'raw', 'T_S14'),
            Name(station, 'raw', 'P_S14'),
            Name(station, 'raw', 'U_S14'),
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
            Name(station, 'raw', 'BsB_S13'),
            Name(station, 'raw', 'BsG_S13'),
            Name(station, 'raw', 'BsR_S13'),
            Name(station, 'raw', 'BbsB_S13'),
            Name(station, 'raw', 'BbsG_S13'),
            Name(station, 'raw', 'BbsR_S13'),
            Name(station, 'raw', 'BsB_S14'),
            Name(station, 'raw', 'BsG_S14'),
            Name(station, 'raw', 'BsR_S14'),
            Name(station, 'raw', 'BbsB_S14'),
            Name(station, 'raw', 'BbsG_S14'),
            Name(station, 'raw', 'BbsR_S14'),
            Name(station, 'raw', 'T_V12'),
            Name(station, 'raw', 'U_V12'),
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
            Name(station, 'clean', 'T_S13'),
            Name(station, 'clean', 'P_S13'),
            Name(station, 'clean', 'U_S13'),
            Name(station, 'clean', 'T_S14'),
            Name(station, 'clean', 'P_S14'),
            Name(station, 'clean', 'U_S14'),
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
            Name(station, 'clean', 'BsB_S13'),
            Name(station, 'clean', 'BsG_S13'),
            Name(station, 'clean', 'BsR_S13'),
            Name(station, 'clean', 'BbsB_S13'),
            Name(station, 'clean', 'BbsG_S13'),
            Name(station, 'clean', 'BbsR_S13'),
            Name(station, 'clean', 'BsB_S14'),
            Name(station, 'clean', 'BsG_S14'),
            Name(station, 'clean', 'BsR_S14'),
            Name(station, 'clean', 'BbsB_S14'),
            Name(station, 'clean', 'BbsG_S14'),
            Name(station, 'clean', 'BbsR_S14'),
            Name(station, 'clean', 'T_V12'),
            Name(station, 'clean', 'U_V12'),
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
            Name(station, 'avgh', 'T_S13'),
            Name(station, 'avgh', 'P_S13'),
            Name(station, 'avgh', 'U_S13'),
            Name(station, 'avgh', 'T_S14'),
            Name(station, 'avgh', 'P_S14'),
            Name(station, 'avgh', 'U_S14'),
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
            Name(station, 'avgh', 'BsB_S13'),
            Name(station, 'avgh', 'BsG_S13'),
            Name(station, 'avgh', 'BsR_S13'),
            Name(station, 'avgh', 'BbsB_S13'),
            Name(station, 'avgh', 'BbsG_S13'),
            Name(station, 'avgh', 'BbsR_S13'),
            Name(station, 'avgh', 'BsB_S14'),
            Name(station, 'avgh', 'BsG_S14'),
            Name(station, 'avgh', 'BsR_S14'),
            Name(station, 'avgh', 'BbsB_S14'),
            Name(station, 'avgh', 'BbsG_S14'),
            Name(station, 'avgh', 'BbsR_S14'),
            Name(station, 'avgh', 'T_V12'),
            Name(station, 'avgh', 'U_V12'),
        },
    )


    station_profile_export['aerosol']['raw']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'raw', f'F1_A81')] +
            [Name(station, 'raw', f'Ba{i + 1}_A81') for i in range(7)] +
            [Name(station, 'raw', f'X{i + 1}_A81') for i in range(7)] +
            [Name(station, 'raw', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
            [Name(station, 'raw', f'Ir{i + 1}_A81') for i in range(7)]
        )
    )
    station_profile_export['aerosol']['clean']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'clean', f'F1_A81')] +
            [Name(station, 'clean', f'Ba{i + 1}_A81') for i in range(7)] +
            [Name(station, 'clean', f'X{i + 1}_A81') for i in range(7)] +
            [Name(station, 'clean', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
            [Name(station, 'clean', f'Ir{i + 1}_A81') for i in range(7)]
        )
    )
    station_profile_export['aerosol']['avgh']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', set(
            [Name(station, 'avgh', f'F1_A81')] +
            [Name(station, 'avgh', f'Ba{i + 1}_A81') for i in range(7)] +
            [Name(station, 'avgh', f'X{i + 1}_A81') for i in range(7)] +
            [Name(station, 'avgh', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
            [Name(station, 'avgh', f'Ir{i + 1}_A81') for i in range(7)]
        )
    )


    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('ccn', "CCN", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'N_N12'),
                Name(station, 'raw', 'Nb_N12'),
                Name(station, 'raw', 'Tu_N12'),
                Name(station, 'raw', 'T1_N12'),
                Name(station, 'raw', 'T2_N12'),
                Name(station, 'raw', 'T3_N12'),
                Name(station, 'raw', 'T4_N12'),
                Name(station, 'raw', 'T5_N12'),
                Name(station, 'raw', 'T6_N12'),
                Name(station, 'raw', 'Q1_N12'),
                Name(station, 'raw', 'Q2_N12'),
                Name(station, 'raw', 'U_N12'),
                Name(station, 'raw', 'P_N12'),
                Name(station, 'raw', 'DT_N12'),
            },
        )),
    )
    station_profile_export['aerosol']['clean'].insert(
        DataExportList.Entry('ccn', "CCN", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'clean', 'N_N12'),
                Name(station, 'clean', 'Nb_N12'),
                Name(station, 'clean', 'Tu_N12'),
                Name(station, 'clean', 'T1_N12'),
                Name(station, 'clean', 'T2_N12'),
                Name(station, 'clean', 'T3_N12'),
                Name(station, 'clean', 'T4_N12'),
                Name(station, 'clean', 'T5_N12'),
                Name(station, 'clean', 'T6_N12'),
                Name(station, 'clean', 'Q1_N12'),
                Name(station, 'clean', 'Q2_N12'),
                Name(station, 'clean', 'U_N12'),
                Name(station, 'clean', 'P_N12'),
                Name(station, 'clean', 'DT_N12'),
            },
        )),
    )
    station_profile_export['aerosol']['avgh'].insert(
        DataExportList.Entry('ccn', "CCN", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'average', {
                Name(station, 'avgh', 'N_N12'),
                Name(station, 'avgh', 'Nb_N12'),
                Name(station, 'avgh', 'Tu_N12'),
                Name(station, 'avgh', 'T1_N12'),
                Name(station, 'avgh', 'T2_N12'),
                Name(station, 'avgh', 'T3_N12'),
                Name(station, 'avgh', 'T4_N12'),
                Name(station, 'avgh', 'T5_N12'),
                Name(station, 'avgh', 'T6_N12'),
                Name(station, 'avgh', 'Q1_N12'),
                Name(station, 'avgh', 'Q2_N12'),
                Name(station, 'avgh', 'U_N12'),
                Name(station, 'avgh', 'P_N12'),
                Name(station, 'avgh', 'DT_N12'),
            },
        )),
    )


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_profile_get(station, mode_name, export_key,
                                  start_epoch_ms, end_epoch_ms, directory, station_profile_export)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_profile_lookup(station, mode_name, station_profile_export)

else:
    from ..default.export import aerosol_exports, export_get, find_key, export_visible, ExportCSV, Selection, \
        STANDARD_CUT_SIZE_SPLIT, STANDARD_THREE_WAVELENGTHS
    from copy import deepcopy

    export_entries = dict()
    export_entries["aerosol"] = deepcopy(aerosol_exports)


    for archive in ("raw", "clean",):
        find_key(export_entries["aerosol"][archive], "scattering").columns.extend([
            ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength,
                                        instrument_id="S12")],
                             header="Bs" + code + "_{instrument_id}", default_header=f"Bs{code}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength,
                                        instrument_id="S12")],
                             header="Bbs" + code + "_{instrument_id}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_temperature", instrument_id="S12")]),
            ExportCSV.Column([Selection(variable_name="sample_humidity", instrument_id="S12")]),
            ExportCSV.Column([Selection(variable_name="sample_pressure", instrument_id="S12")]),
            ExportCSV.Column([Selection(variable_name="inlet_temperature", instrument_id="S12", instrument_code="tsi3563nephelometer")]),
            ExportCSV.Column([Selection(variable_name="inlet_humidity", instrument_id="S12", instrument_code="tsi3563nephelometer")]),
        ] + [
            ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength,
                                        instrument_id="S13")],
                             header="Bs" + code + "_{instrument_id}", default_header=f"Bs{code}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength,
                                        instrument_id="S13")],
                             header="Bbs" + code + "_{instrument_id}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_temperature", instrument_id="S13")]),
            ExportCSV.Column([Selection(variable_name="sample_humidity", instrument_id="S13")]),
            ExportCSV.Column([Selection(variable_name="sample_pressure", instrument_id="S13")]),
        ] + [
            ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength,
                                        instrument_id="S14")],
                             header="Bs" + code + "_{instrument_id}", default_header=f"Bs{code}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength,
                                        instrument_id="S14")],
                             header="Bbs" + code + "_{instrument_id}")
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_temperature", instrument_id="S14")]),
            ExportCSV.Column([Selection(variable_name="sample_humidity", instrument_id="S14")]),
            ExportCSV.Column([Selection(variable_name="sample_pressure", instrument_id="S14")]),
        ] + [
            ExportCSV.Column([Selection(variable_id="T_V12")]),
            ExportCSV.Column([Selection(variable_id="U_V12")]),
            ExportCSV.Column([Selection(variable_id="T_V13")]),
            ExportCSV.Column([Selection(variable_id="U_V13")]),
        ])
    for archive in ("clean",):
        find_key(export_entries["aerosol"][archive], "intensive").columns.extend([
            ExportCSV.Column([Selection(variable_name="sample_temperature",
                                        require_tags={"scattering"}, exclude_tags={"secondary"})],
                             default_header="T", always_present=True),
            ExportCSV.Column([Selection(variable_name="sample_pressure",
                                        require_tags={"scattering"}, exclude_tags={"secondary"})],
                             default_header="P", always_present=True),
        ])
    for archive in ("avgh",):
        find_key(export_entries["aerosol"][archive], "scattering").columns.extend([
            ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                        instrument_id="S12")],
                             header="Bs" + code + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                        instrument_id="S12")],
                             header="Bbs" + code + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_temperature", cut_size=cut_size,
                                        instrument_id="S12")],
                             header="T" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_humidity", cut_size=cut_size,
                                        instrument_id="S12")],
                             header="U" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_pressure", cut_size=cut_size,
                                        instrument_id="S12")],
                             header="P" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="inlet_temperature", cut_size=cut_size,
                                        instrument_id="S12", instrument_code="tsi3563nephelometer")],
                             header="Tu" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="inlet_humidity", cut_size=cut_size,
                                        instrument_id="S12", instrument_code="tsi3563nephelometer")],
                             header="Uu" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                        instrument_id="S13")],
                             header="Bs" + code + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                        instrument_id="S13")],
                             header="Bbs" + code + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_temperature", cut_size=cut_size,
                                        instrument_id="S13")],
                             header="T" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_humidity", cut_size=cut_size,
                                        instrument_id="S13")],
                             header="U" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_pressure", cut_size=cut_size,
                                        instrument_id="S13")],
                             header="P" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                        instrument_id="S14")],
                             header="Bs" + code + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                        instrument_id="S14")],
                             header="Bbs" + code + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_temperature", cut_size=cut_size,
                                        instrument_id="S14")],
                             header="T" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_humidity", cut_size=cut_size,
                                        instrument_id="S14")],
                             header="U" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_pressure", cut_size=cut_size,
                                        instrument_id="S14")],
                             header="P" + record + "_{instrument_id}")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="T_V12", cut_size=cut_size)],
                             header="T" + record + "_V12")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="U_V12", cut_size=cut_size)],
                             header="U" + record + "_V12")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="T_V13", cut_size=cut_size)],
                             header="T" + record + "_V13")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="U_V13", cut_size=cut_size)],
                             header="U" + record + "_V13")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ])
        find_key(export_entries["aerosol"][archive], "intensive").columns.extend([
            ExportCSV.Column([Selection(variable_name="sample_temperature", cut_size=cut_size,
                                        require_tags={"scattering"}, exclude_tags={"secondary"})],
                             header="T" + record + "_{instrument_id}", default_header="T" + record)
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_name="sample_pressure", cut_size=cut_size,
                                        require_tags={"scattering"}, exclude_tags={"secondary"})],
                             header="P" + record + "_{instrument_id}", default_header="P" + record)
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ])

    for archive in ("raw", "clean", "avgh"):
        export_entries["aerosol"][archive].append(ExportCSV("ccn", "CCN", [
            ExportCSV.Column([Selection(variable_name="number_concentration", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="Tu", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="T1", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="T2", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="T3", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="T4", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="T5", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="T6", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="Q1", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="Q2", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="U", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="P", instrument_code="dmtccn")]),
            ExportCSV.Column([Selection(variable_id="DT", instrument_code="dmtccn")]),
        ]))


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)
