import typing
from forge.vis.export import Export, ExportList
from ..cpd3 import use_cpd3


if use_cpd3():
    from ..cpd3 import Name, DataExport, DataExportList, detach, profile_export, export_profile_get, export_profile_lookup
    station_profile_export = detach(profile_export)

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
    from ..default.export import aerosol_exports, export_get, export_visible, ExportCSV, Selection, \
        STANDARD_THREE_WAVELENGTHS
    from copy import deepcopy

    export_entries = dict()
    export_entries["aerosol"] = deepcopy(aerosol_exports)

    export_entries["aerosol"]["raw"].append(ExportCSV("nephzero", "Nephelometer Zero", [
        ExportCSV.Column([Selection(variable_name="zero_temperature",
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         default_header="Tw", always_present=True),
        ExportCSV.Column([Selection(variable_name="zero_pressure",
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         default_header="Pw", always_present=True),
    ] + [
        ExportCSV.Column([Selection(variable_name="wall_scattering_coefficient", wavelength=wavelength,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="Bsw" + code + "_{instrument_id}", default_header=f"Bsw{code}", always_present=True)
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                                    require_tags={"scattering"}, exclude_tags={"secondary"})],
                         header="Bbsw" + code + "_{instrument_id}", default_header=f"Bbsw{code}", always_present=True)
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))


    def get(station: str, mode_name: str, export_key: str,
            start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


    async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
        return export_visible(station, mode_name, export_entries)