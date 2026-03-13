import typing
from ..default.data import aerosol_data, data_get, DataStream, DataRecord, Selection, STANDARD_THREE_WAVELENGTHS

data_records = dict()
data_records.update(aerosol_data)

data_records[f"aerosol-raw-nephzero"] = DataRecord(dict([
    (f"Bsz{code}", [Selection(variable_name="zero_scattering_coefficient", wavelength=wavelength,
                              variable_type=Selection.VariableType.State,
                              require_tags={"scattering"}, exclude_tags={"secondary"})])
    for code, wavelength in STANDARD_THREE_WAVELENGTHS
] + [
    (f"Bbsz{code}", [Selection(variable_name="zero_backscattering_coefficient", wavelength=wavelength,
                              variable_type=Selection.VariableType.State,
                              require_tags={"scattering"}, exclude_tags={"secondary"})])
    for code, wavelength in STANDARD_THREE_WAVELENGTHS
]))

data_records["aerosol-raw-psapstatus"] = DataRecord({
    "IrG": [Selection(variable_name="transmittance", wavelength=(500, 600),
                      instrument_code="psap3w")],
    "IfG": [Selection(variable_name="reference_intensity", wavelength=(500, 600),
                      instrument_code="psap3w")],
    "IpG": [Selection(variable_name="sample_intensity", wavelength=(500, 600),
                      instrument_code="psap3w")],
    "IrcG": [Selection(variable_name="instrument_transmittance", wavelength=(500, 600),
                      instrument_code="psap3w")],
    "Q": [Selection(variable_name="sample_flow",
                    instrument_code="psap3w")],
})


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)