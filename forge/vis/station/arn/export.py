import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("arn"):
    from ..cpd3 import Name, DataExport, detach, profile_export, export_profile_get, export_profile_lookup

    station_profile_export = detach(profile_export)

    station_profile_export['aerosol']['raw']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'raw', 'N_N23'),
        },
    )
    station_profile_export['aerosol']['clean']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'clean', 'N_N23'),
        },
    )
    station_profile_export['aerosol']['avgh']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', {
            Name(station, 'avgh', 'N_N23'),
        },
    )

    station_profile_export['aerosol']['raw']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
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
            Name(station, 'raw', 'Q_Q11'),
            Name(station, 'raw', 'Q_Q12'),
        },
    )
    station_profile_export['aerosol']['clean']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'clean', 'T_S11'),
            Name(station, 'clean', 'P_S11'),
            Name(station, 'clean', 'U_S11'),
            Name(station, 'clean', 'BsB_S11'),
            Name(station, 'clean', 'BsG_S11'),
            Name(station, 'clean', 'BsR_S11'),
            Name(station, 'clean', 'BbsB_S11'),
            Name(station, 'clean', 'BbsG_S11'),
            Name(station, 'clean', 'BbsR_S11'),
            Name(station, 'clean', 'Q_Q11'),
            Name(station, 'clean', 'Q_Q12'),
        },
    )
    station_profile_export['aerosol']['avgh']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'avgh', 'T_S11'),
            Name(station, 'avgh', 'P_S11'),
            Name(station, 'avgh', 'U_S11'),
            Name(station, 'avgh', 'BsB_S11'),
            Name(station, 'avgh', 'BsG_S11'),
            Name(station, 'avgh', 'BsR_S11'),
            Name(station, 'avgh', 'BbsB_S11'),
            Name(station, 'avgh', 'BbsG_S11'),
            Name(station, 'avgh', 'BbsR_S11'),
            Name(station, 'avgh', 'Q_Q11'),
            Name(station, 'avgh', 'Q_Q12'),
        },
    )


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_profile_get(station, mode_name, export_key,
                                  start_epoch_ms, end_epoch_ms, directory, station_profile_export)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_profile_lookup(station, mode_name, station_profile_export)


else:
    from ..default.export import aerosol_exports, export_get, find_key, export_visible, ExportCSV, Selection, STANDARD_CUT_SIZE_SPLIT
    from copy import deepcopy

    export_entries = dict()
    export_entries["aerosol"] = deepcopy(aerosol_exports)


    for archive in ("raw", "clean",):
        find_key(export_entries["aerosol"][archive], "scattering").columns.extend([
            ExportCSV.Column([Selection(variable_id="Q", instrument_id="Q11")]),
            ExportCSV.Column([Selection(variable_id="Q", instrument_id="Q12")]),
        ])
    for archive in ("avgh",):
        find_key(export_entries["aerosol"][archive], "scattering").columns.extend([
            ExportCSV.Column([Selection(variable_id="Q", cut_size=cut_size, instrument_id="Q11")],
                             header="Q" + record + "_{instrument_id}", default_header="Q" + record + "_Q11")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ] + [
            ExportCSV.Column([Selection(variable_id="Q", cut_size=cut_size, instrument_id="Q12")],
                             header="Q" + record + "_{instrument_id}", default_header="Q" + record + "_Q12")
            for record, cut_size in STANDARD_CUT_SIZE_SPLIT
        ])


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)
