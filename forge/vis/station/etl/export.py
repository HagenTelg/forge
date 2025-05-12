import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3("etl"):
    from ..cpd3 import Name, DataExport, DataExportList, detach, profile_export, export_profile_get, export_profile_lookup

    station_profile_export = detach(profile_export)


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
    station_profile_export['aerosol']['clean'].insert(
        DataExportList.Entry('grimm', "Grimm", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'clean', 'Ns_N11'),
                Name(station, 'clean', 'Nn_N11'),
                Name(station, 'clean', 'N_N11'),
            },
        ))
    )
    station_profile_export['aerosol']['avgh'].insert(
        DataExportList.Entry('grimm', "Grimm", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'avgh', 'Ns_N11'),
                Name(station, 'avgh', 'Nn_N11'),
                Name(station, 'avgh', 'Nb_N11'),
                Name(station, 'avgh', 'N_N11'),
            },
        ))
    )


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_profile_get(station, mode_name, export_key,
                                  start_epoch_ms, end_epoch_ms, directory, station_profile_export)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_profile_lookup(station, mode_name, station_profile_export)
