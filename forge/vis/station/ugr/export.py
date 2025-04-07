import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("app"):
    from ..cpd3 import Name, DataExport, DataExportList, detach, profile_export, export_profile_get, export_profile_lookup

    station_profile_export = detach(profile_export)


    station_profile_export['aerosol']['raw']['aethalometer'].display = "Aethalometer (A41)"
    station_profile_export['aerosol']['raw']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'raw', f'Ba{i + 1}_A41') for i in range(7)] +
            [Name(station, 'raw', f'X{i + 1}_A41') for i in range(7)] +
            [Name(station, 'raw', f'ZFACTOR{i + 1}_A41') for i in range(7)] +
            [Name(station, 'raw', f'Ir{i + 1}_A41') for i in range(7)]
        )
    )
    station_profile_export['aerosol']['clean']['aethalometer'].display = "Aethalometer (A41)"
    station_profile_export['aerosol']['clean']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'clean', f'Ba{i + 1}_A41') for i in range(7)] +
            [Name(station, 'clean', f'X{i + 1}_A41') for i in range(7)] +
            [Name(station, 'clean', f'ZFACTOR{i + 1}_A41') for i in range(7)] +
            [Name(station, 'clean', f'Ir{i + 1}_A41') for i in range(7)]
        )
    )
    station_profile_export['aerosol']['avgh']['aethalometer'].display = "Aethalometer (A41)"
    station_profile_export['aerosol']['avgh']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', set(
            [Name(station, 'avgh', f'Ba{i + 1}_A41') for i in range(7)] +
            [Name(station, 'avgh', f'X{i + 1}_A41') for i in range(7)] +
            [Name(station, 'avgh', f'ZFACTOR{i + 1}_A41') for i in range(7)] +
            [Name(station, 'avgh', f'Ir{i + 1}_A41') for i in range(7)]
        )
    )


    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('aethalometer2', "Aethalometer (A42)", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
                [Name(station, 'raw', f'Ba{i + 1}_A42') for i in range(7)] +
                [Name(station, 'raw', f'X{i + 1}_A42') for i in range(7)] +
                [Name(station, 'raw', f'ZFACTOR{i + 1}_A42') for i in range(7)] +
                [Name(station, 'raw', f'Ir{i + 1}_A42') for i in range(7)]
            )
        )),
    )
    station_profile_export['aerosol']['clean'].insert(
        DataExportList.Entry('aethalometer2', "Aethalometer (A42)", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
                [Name(station, 'clean', f'Ba{i + 1}_A42') for i in range(7)] +
                [Name(station, 'clean', f'X{i + 1}_A42') for i in range(7)] +
                [Name(station, 'clean', f'ZFACTOR{i + 1}_A42') for i in range(7)] +
                [Name(station, 'clean', f'Ir{i + 1}_A42') for i in range(7)]
            ),
        )),
    )
    station_profile_export['aerosol']['avgh'].insert(
        DataExportList.Entry('aethalometer2', "Aethalometer (A42)", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'average', set(
                [Name(station, 'avgh', f'Ba{i + 1}_A42') for i in range(7)] +
                [Name(station, 'avgh', f'X{i + 1}_A42') for i in range(7)] +
                [Name(station, 'avgh', f'ZFACTOR{i + 1}_A42') for i in range(7)] +
                [Name(station, 'avgh', f'Ir{i + 1}_A42') for i in range(7)]
            ),
        )),
    )


    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'F1_A31'),
                Name(station, 'raw', 'P_A31'),
                Name(station, 'raw', 'IfR_A31'),
                Name(station, 'raw', 'IpR_A31'),
                Name(station, 'raw', 'IrR_A31'),
                Name(station, 'raw', 'Is1_A31'),
                Name(station, 'raw', 'Is2_A31'),
                Name(station, 'raw', 'Pd1_A31'),
                Name(station, 'raw', 'Pd2_A31'),
                Name(station, 'raw', 'Q_A31'),
                Name(station, 'raw', 'Qt_A31'),
                Name(station, 'raw', 'T1_A31'),
                Name(station, 'raw', 'T2_A31'),
                Name(station, 'raw', 'T3_A31'),
                Name(station, 'raw', 'XR_A31'),
            },
        )),
    )
    station_profile_export['aerosol']['clean'].insert(
        DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'clean', 'F1_A31'),
                Name(station, 'clean', 'P_A31'),
                Name(station, 'clean', 'IfR_A31'),
                Name(station, 'clean', 'IpR_A31'),
                Name(station, 'clean', 'IrR_A31'),
                Name(station, 'clean', 'Is1_A31'),
                Name(station, 'clean', 'Is2_A31'),
                Name(station, 'clean', 'Pd1_A31'),
                Name(station, 'clean', 'Pd2_A31'),
                Name(station, 'clean', 'Q_A31'),
                Name(station, 'clean', 'Qt_A31'),
                Name(station, 'clean', 'T1_A31'),
                Name(station, 'clean', 'T2_A31'),
                Name(station, 'clean', 'T3_A31'),
                Name(station, 'clean', 'XR_A31'),
            },
        )),
    )
    station_profile_export['aerosol']['avgh'].insert(
        DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'average', {
                Name(station, 'avgh', 'F1_A31'),
                Name(station, 'avgh', 'P_A31'),
                Name(station, 'avgh', 'IfR_A31'),
                Name(station, 'avgh', 'IpR_A31'),
                Name(station, 'avgh', 'IrR_A31'),
                Name(station, 'avgh', 'Is1_A31'),
                Name(station, 'avgh', 'Is2_A31'),
                Name(station, 'avgh', 'Pd1_A31'),
                Name(station, 'avgh', 'Pd2_A31'),
                Name(station, 'avgh', 'Q_A31'),
                Name(station, 'avgh', 'T1_A31'),
                Name(station, 'avgh', 'T2_A31'),
                Name(station, 'avgh', 'T3_A31'),
                Name(station, 'avgh', 'XR_A31'),
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
    from ..default.export import aerosol_exports, export_get, find_key, export_visible, ExportCSV, Selection
    from copy import deepcopy

    export_entries = dict()
    export_entries["aerosol"] = deepcopy(aerosol_exports)

    for archive in ("raw", "clean", "avgh"):
        find_key(export_entries["aerosol"][archive], "aethalometer").display = "Aethalometer (Primary/A41)"

        aerosol_exports[archive].append(ExportCSV("aethalometer", "Aethalometer (A42)", [
            ExportCSV.Column([Selection(variable_id="Ba", wavelength_number=wl,
                                        require_tags={"aethalometer"}, instrument_id="A42")],
                             header="Ba" + str(wl+1) + "_{instrument_id}", default_header=f"Ba{wl+1}", always_present=True)
            for wl in range(7)
        ] + [
            ExportCSV.Column([Selection(variable_name="equivalent_black_carbon", wavelength_number=wl,
                                        require_tags={"aethalometer"}, instrument_id="A42")],
                             header="X" + str(wl+1) + "_{instrument_id}", default_header=f"X{wl+1}", always_present=True)
            for wl in range(7)
        ] + [
            ExportCSV.Column([Selection(variable_id="Ir", wavelength_number=wl,
                                        require_tags={"aethalometer"}, instrument_id="A42")],
                             header="Ir" + str(wl+1) + "_{instrument_id}", default_header=f"Ir{wl+1}")
            for wl in range(7)
        ] + [
            ExportCSV.Column([Selection(variable_name="correction_factor", wavelength_number=wl,
                                        require_tags={"aethalometer", "mageeae33"}, instrument_id="A42")],
                             header="ZFACTOR" + str(wl+1) + "_{instrument_id}", default_header=f"ZFACTOR{wl+1}")
            for wl in range(7)
        ]))

    for archive in ("raw", "clean", "avgh"):
        export_entries["aerosol"][archive].append(ExportCSV("maap", "MAAP", [
            ExportCSV.Column([Selection(variable_id="X", instrument_code="thermomaap", instrument_id="A31",
                                        wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="P", instrument_code="thermomaap", instrument_id="A31")]),
            ExportCSV.Column([Selection(variable_id="Q", instrument_code="thermomaap", instrument_id="A31")]),
            ExportCSV.Column([Selection(variable_id="Ld", instrument_code="thermomaap", instrument_id="A31")]),
            ExportCSV.Column([Selection(variable_id="T1", instrument_code="thermomaap", instrument_id="A31")]),
            ExportCSV.Column([Selection(variable_id="T2", instrument_code="thermomaap", instrument_id="A31")]),
            ExportCSV.Column([Selection(variable_id="T3", instrument_code="thermomaap", instrument_id="A31")]),
            ExportCSV.Column([Selection(variable_id="Pd1", instrument_code="thermomaap", instrument_id="A31")]),
            ExportCSV.Column([Selection(variable_id="Pd2", instrument_code="thermomaap", instrument_id="A31")]),
            ExportCSV.Column([Selection(variable_id="Ir", instrument_code="thermomaap", instrument_id="A31",
                                        wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="If", instrument_code="thermomaap", instrument_id="A31",
                                        wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="Ip", instrument_code="thermomaap", instrument_id="A31",
                                        wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="Is1", instrument_code="thermomaap", instrument_id="A31",
                                        wavelength_number=0)]),
            ExportCSV.Column([Selection(variable_id="Is2", instrument_code="thermomaap", instrument_id="A31",
                                        wavelength_number=0)]),
        ]))

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

    ebas_export = find_key(export_entries["aerosol"]["raw"], "ebas")
    if ebas_export:
        ebas_export.ebas.add("maap_lev0")
        ebas_export.ebas.add("dmtccn_lev0")
    ebas_export = find_key(export_entries["aerosol"]["clean"], "ebas")
    if ebas_export:
        ebas_export.ebas.add("maap_lev1")
        ebas_export.ebas.add("dmtccn_lev1")
    ebas_export = find_key(export_entries["aerosol"]["avgh"], "ebas")
    if ebas_export:
        ebas_export.ebas.add("maap_lev2")


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)
