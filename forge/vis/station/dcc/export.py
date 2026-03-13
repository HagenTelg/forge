import typing
from forge.vis.export import Export, ExportList
from ..default.export import aerosol_exports, export_get, find_key, export_visible, ExportCSV, Selection, STANDARD_THREE_WAVELENGTHS
from copy import deepcopy

export_entries = dict()
export_entries["aerosol"] = deepcopy(aerosol_exports)

for archive in ("raw", "clean",):
    find_key(export_entries["aerosol"][archive], "scattering").columns.extend([
        ExportCSV.Column([Selection(variable_name="zero_scattering_coefficient", wavelength=wavelength,
                                    require_tags={"scattering"}, exclude_tags={"secondary"},
                                    variable_type=Selection.VariableType.State)],
                         header="Bsz" + code + "_{instrument_id}")
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        ExportCSV.Column([Selection(variable_name="zero_backscattering_coefficient", wavelength=wavelength,
                                    require_tags={"scattering"}, exclude_tags={"secondary"},
                                    variable_type=Selection.VariableType.State)],
                         header="Bbsz" + code + "_{instrument_id}")
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ])

    find_key(export_entries["aerosol"][archive], "absorption").columns.extend([
        ExportCSV.Column([Selection(variable_name="transmittance", wavelength=wavelength,
                                    require_tags={"absorption"},
                                    exclude_tags={"secondary", "aethalometer", "thermomaap"})],
                         header="Ir" + code + "_{instrument_id}")
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ])


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, export_entries)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_visible(station, mode_name, export_entries)
