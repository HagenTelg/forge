import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("spo"):
    from ..cpd3 import Name, DataExport, detach, profile_export, export_profile_get, export_profile_lookup

    station_profile_export = detach(profile_export)

    station_profile_export['aerosol']['raw']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'raw', 'N_N31'),
            Name(station, 'raw', 'N_N41'),
            Name(station, 'raw', 'N_N42'),
        },
    )
    station_profile_export['aerosol']['clean']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'clean', 'N_N31'),
            Name(station, 'clean', 'N_N41'),
            Name(station, 'clean', 'N_N42'),
        },
    )
    station_profile_export['aerosol']['avgh']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', {
            Name(station, 'avgh', 'N_N31'),
            Name(station, 'avgh', 'N_N41'),
            Name(station, 'avgh', 'N_N42'),
        },
    )

    station_profile_export['aerosol']['raw']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'raw', f'Ba{i + 1}_A82') for i in range(7)] +
            [Name(station, 'raw', f'X{i + 1}_A82') for i in range(7)] +
            [Name(station, 'raw', f'ZFACTOR{i + 1}_A82') for i in range(7)] +
            [Name(station, 'raw', f'Ir{i + 1}_A82') for i in range(7)]
        )
    )
    station_profile_export['aerosol']['clean']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'clean', f'Ba{i + 1}_A82') for i in range(7)] +
            [Name(station, 'clean', f'X{i + 1}_A82') for i in range(7)] +
            [Name(station, 'clean', f'ZFACTOR{i + 1}_A82') for i in range(7)] +
            [Name(station, 'clean', f'Ir{i + 1}_A82') for i in range(7)]
        )
    )
    station_profile_export['aerosol']['avgh']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', set(
            [Name(station, 'avgh', f'Ba{i + 1}_A82') for i in range(7)] +
            [Name(station, 'avgh', f'X{i + 1}_A82') for i in range(7)] +
            [Name(station, 'avgh', f'ZFACTOR{i + 1}_A82') for i in range(7)] +
            [Name(station, 'avgh', f'Ir{i + 1}_A82') for i in range(7)]
        )
    )


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_profile_get(station, mode_name, export_key,
                                  start_epoch_ms, end_epoch_ms, directory, station_profile_export)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_profile_lookup(station, mode_name, station_profile_export)

else:
    from ..default.export import aerosol_exports, export_get, export_visible, ExportCSV, Selection, STANDARD_CUT_SIZE_SPLIT
    from copy import deepcopy

    export_entries = dict()
    export_entries["aerosol"] = deepcopy(aerosol_exports)

    for archive in ("raw", "clean",):
        for modify in export_entries["aerosol"][archive]:
            if modify.key != "counts":
                continue
            modify.columns = [
                ExportCSV.Column([Selection(variable_name="number_concentration", instrument_id="N31")],),
                ExportCSV.Column([Selection(variable_name="number_concentration", instrument_id="N41")],),
                ExportCSV.Column([Selection(variable_name="number_concentration", instrument_id="N42")],),
            ]

    for archive in ("avgh",):
        for modify in export_entries["aerosol"][archive]:
            if modify.key != "counts":
                continue
            modify.columns = [
                ExportCSV.Column([Selection(variable_name="number_concentration", cut_size=cut_size,
                                            instrument_id="N31")],
                                 header="N" + record + "_{instrument_id}")
                for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            ] + [
                ExportCSV.Column([Selection(variable_name="number_concentration", cut_size=cut_size,
                                            instrument_id="N41")],
                                 header="N" + record + "_{instrument_id}")
                for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            ] + [
                ExportCSV.Column([Selection(variable_name="number_concentration", cut_size=cut_size,
                                            instrument_id="N42")],
                                 header="N" + record + "_{instrument_id}")
                for record, cut_size in STANDARD_CUT_SIZE_SPLIT
            ]

    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)