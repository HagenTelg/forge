import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3():
    from ..cpd3 import Name, DataExport, DataExportList, detach, profile_export, export_profile_get, export_profile_lookup

    station_profile_export = detach(profile_export)


    station_profile_export['aerosol']['raw'].insert(
        DataExportList.Entry('ambient', "Ambient Meteorological", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'WS_XM1'), Name(station, 'raw', 'WD_XM1'),
                Name(station, 'raw', 'T1_XM1'), Name(station, 'raw', 'U1_XM1'), Name(station, 'raw', 'TD1_XM1'),
                Name(station, 'raw', 'P_XM1'),
                Name(station, 'raw', 'WI_XM1'),
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

    for archive in ("raw", "clean", "avgh"):
        export_entries["aerosol"][archive].append(ExportCSV("ambient", "Ambient Meteorological", [
            ExportCSV.Column([Selection(variable_name="wind_speed", require_tags={"met"}, exclude_tags={"secondary"})],),
            ExportCSV.Column([Selection(variable_name="wind_direction", require_tags={"met"}, exclude_tags={"secondary"})],),
            ExportCSV.Column([Selection(variable_name="ambient_temperature", require_tags={"met"}, exclude_tags={"secondary"})],),
            ExportCSV.Column([Selection(variable_name="ambient_humidity", require_tags={"met"}, exclude_tags={"secondary"})],),
            ExportCSV.Column([Selection(variable_name="ambient_pressure", require_tags={"met"}, exclude_tags={"secondary"})],),
        ], format=ExportCSV.Format(cut_size=False)))
        export_entries["aerosol"][archive].append(ExportCSV("purpleair", "Purple Air", [
            ExportCSV.Column([Selection(variable_id="T", instrument_code="purpleair", exclude_tags={"secondary"})],),
            ExportCSV.Column([Selection(variable_id="U", instrument_code="purpleair", exclude_tags={"secondary"})],),
            ExportCSV.Column([Selection(variable_id="P", instrument_code="purpleair", exclude_tags={"secondary"})],),
            ExportCSV.Column([Selection(variable_id="Ipa", instrument_code="purpleair", exclude_tags={"secondary"})],),
            ExportCSV.Column([Selection(variable_id="Ipb", instrument_code="purpleair", exclude_tags={"secondary"})],),
        ], format=ExportCSV.Format(cut_size=False)))


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)
