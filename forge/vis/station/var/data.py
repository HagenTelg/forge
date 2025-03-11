import typing
from ..cpd3 import use_cpd3


if use_cpd3():
    from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data
    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['psap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A12'): 'BaB',
            Name(station, 'raw', 'BaG_A12'): 'BaG',
            Name(station, 'raw', 'BaR_A12'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['psap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A12', {'pm10'}): 'BaB',
            Name(station, 'raw', 'BaG_A12', {'pm10'}): 'BaG',
            Name(station, 'raw', 'BaR_A12', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['psap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A12', {'pm25'}): 'BaB',
            Name(station, 'raw', 'BaG_A12', {'pm25'}): 'BaG',
            Name(station, 'raw', 'BaR_A12', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['psap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A12', {'pm1'}): 'BaB',
            Name(station, 'raw', 'BaG_A12', {'pm1'}): 'BaG',
            Name(station, 'raw', 'BaR_A12', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['realtime']['psap-whole'] = {
        RealtimeTranslator.Key('BaB_A12'): 'BaB',
        RealtimeTranslator.Key('BaG_A12'): 'BaG',
        RealtimeTranslator.Key('BaR_A12'): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['psap-pm10'] = {
        RealtimeTranslator.Key('BaB_A12', {'pm10'}): 'BaB',
        RealtimeTranslator.Key('BaG_A12', {'pm10'}): 'BaG',
        RealtimeTranslator.Key('BaR_A12', {'pm10'}): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['psap-pm25'] = {
        RealtimeTranslator.Key('BaB_A12', {'pm25'}): 'BaB',
        RealtimeTranslator.Key('BaG_A12', {'pm25'}): 'BaG',
        RealtimeTranslator.Key('BaR_A12', {'pm25'}): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['psap-pm1'] = {
        RealtimeTranslator.Key('BaB_A12', {'pm1'}): 'BaB',
        RealtimeTranslator.Key('BaG_A12', {'pm1'}): 'BaG',
        RealtimeTranslator.Key('BaR_A12', {'pm1'}): 'BaR',
    }
    station_profile_data['aerosol']['editing']['psap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A12'): 'BaB',
            Name(station, 'clean', 'BaG_A12'): 'BaG',
            Name(station, 'clean', 'BaR_A12'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['psap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A12', {'pm10'}): 'BaB',
            Name(station, 'clean', 'BaG_A12', {'pm10'}): 'BaG',
            Name(station, 'clean', 'BaR_A12', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['psap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A12', {'pm25'}): 'BaB',
            Name(station, 'clean', 'BaG_A12', {'pm25'}): 'BaG',
            Name(station, 'clean', 'BaR_A12', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['psap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A12', {'pm1'}): 'BaB',
            Name(station, 'clean', 'BaG_A12', {'pm1'}): 'BaG',
            Name(station, 'clean', 'BaR_A12', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['psap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A12'): 'BaB',
            Name(station, 'clean', 'BaG_A12'): 'BaG',
            Name(station, 'clean', 'BaR_A12'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['psap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A12', {'pm10'}): 'BaB',
            Name(station, 'clean', 'BaG_A12', {'pm10'}): 'BaG',
            Name(station, 'clean', 'BaR_A12', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['psap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A12', {'pm25'}): 'BaB',
            Name(station, 'clean', 'BaG_A12', {'pm25'}): 'BaG',
            Name(station, 'clean', 'BaR_A12', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['psap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A12', {'pm1'}): 'BaB',
            Name(station, 'clean', 'BaG_A12', {'pm1'}): 'BaG',
            Name(station, 'clean', 'BaR_A12', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['psap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A12'): 'BaB',
            Name(station, 'avgh', 'BaG_A12'): 'BaG',
            Name(station, 'avgh', 'BaR_A12'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['psap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A12', {'pm10'}): 'BaB',
            Name(station, 'avgh', 'BaG_A12', {'pm10'}): 'BaG',
            Name(station, 'avgh', 'BaR_A12', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['psap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A12', {'pm25'}): 'BaB',
            Name(station, 'avgh', 'BaG_A12', {'pm25'}): 'BaG',
            Name(station, 'avgh', 'BaR_A12', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['psap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A12', {'pm1'}): 'BaB',
            Name(station, 'avgh', 'BaG_A12', {'pm1'}): 'BaG',
            Name(station, 'avgh', 'BaR_A12', {'pm1'}): 'BaR',
        }, send
    )

    station_profile_data['aerosol']['raw']['psapstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'IrG_A12'): 'IrG',
            Name(station, 'raw', 'IrG_A12', {'pm10'}): 'IrG',
            Name(station, 'raw', 'IrG_A12', {'pm1'}): 'IrG',
            Name(station, 'raw', 'IrG_A12', {'pm25'}): 'IrG',
            Name(station, 'raw', 'IfG_A12'): 'IfG',
            Name(station, 'raw', 'IfG_A12', {'pm10'}): 'IfG',
            Name(station, 'raw', 'IfG_A12', {'pm1'}): 'IfG',
            Name(station, 'raw', 'IfG_A12', {'pm25'}): 'IfG',
            Name(station, 'raw', 'IpG_A12'): 'IpG',
            Name(station, 'raw', 'IpG_A12', {'pm10'}): 'IpG',
            Name(station, 'raw', 'IpG_A12', {'pm1'}): 'IpG',
            Name(station, 'raw', 'IpG_A12', {'pm25'}): 'IpG',
            Name(station, 'raw', 'Q_A12'): 'Q',
            Name(station, 'raw', 'Q_A12', {'pm10'}): 'Q',
            Name(station, 'raw', 'Q_A12', {'pm1'}): 'Q',
            Name(station, 'raw', 'Q_A12', {'pm25'}): 'Q',
        }, send
    )
    station_profile_data['aerosol']['realtime']['psapstatus'] = {
        RealtimeTranslator.Key('IrG_A12'): 'IrG',
        RealtimeTranslator.Key('IrG_A12', {'pm10'}): 'IrG',
        RealtimeTranslator.Key('IrG_A12', {'pm1'}): 'IrG',
        RealtimeTranslator.Key('IrG_A12', {'pm25'}): 'IrG',
        RealtimeTranslator.Key('IfG_A12'): 'IfG',
        RealtimeTranslator.Key('IfG_A12', {'pm10'}): 'IfG',
        RealtimeTranslator.Key('IfG_A12', {'pm1'}): 'IfG',
        RealtimeTranslator.Key('IfG_A12', {'pm25'}): 'IfG',
        RealtimeTranslator.Key('IpG_A12'): 'IpG',
        RealtimeTranslator.Key('IpG_A12', {'pm10'}): 'IpG',
        RealtimeTranslator.Key('IpG_A12', {'pm1'}): 'IpG',
        RealtimeTranslator.Key('IpG_A12', {'pm25'}): 'IpG',
        RealtimeTranslator.Key('Q_A12'): 'Q',
        RealtimeTranslator.Key('Q_A12', {'pm10'}): 'Q',
        RealtimeTranslator.Key('Q_A12', {'pm1'}): 'Q',
        RealtimeTranslator.Key('Q_A12', {'pm25'}): 'Q',
    }


    station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Tx_S11'): 'Tnephcell', Name(station, 'raw', 'Ux_S11'): 'Unephcell',
            Name(station, 'raw', 'Tx_S11', {'pm10'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm10'}): 'Unephcell',
            Name(station, 'raw', 'Tx_S11', {'pm1'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm1'}): 'Unephcell',
            Name(station, 'raw', 'Tx_S11', {'pm25'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm25'}): 'Unephcell',

            Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',
        }, send
    )
    station_profile_data['aerosol']['realtime']['temperature'] = {
        RealtimeTranslator.Key('Tx_S11'): 'Tnephcell', RealtimeTranslator.Key('Ux_S11'): 'Unephcell',
        RealtimeTranslator.Key('Tx_S11', {'pm10'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm10'}): 'Unephcell',
        RealtimeTranslator.Key('Tx_S11', {'pm1'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm1'}): 'Unephcell',
        RealtimeTranslator.Key('Tx_S11', {'pm25'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm25'}): 'Unephcell',

        RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_CUT_SIZE_SPLIT, STANDARD_THREE_WAVELENGTHS

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)

    for archive in ("raw", "editing", "clean", "avgh"):
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
            data_records[f"aerosol-{archive}-psap-{record}"] = DataRecord(dict([
                (f"Ba{code}", [Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                         instrument_code="psap3w")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ]))
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        data_records[f"aerosol-realtime-psap-{record}"] = RealtimeRecord(dict([
            (f"Ba{code}", [RealtimeSelection(f"Ba{code}", variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                             instrument_code="psap3w")])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))

    data_records["aerosol-raw-psapstatus"] = DataRecord({
        "IrG": [Selection(variable_name="transmittance", wavelength=(500, 600),
                          instrument_code="psap3w")],
        "IfG": [Selection(variable_name="reference_intensity", wavelength=(500, 600),
                          instrument_code="psap3w")],
        "IpG": [Selection(variable_name="sample_intensity", wavelength=(500, 600),
                          instrument_code="psap3w")],
        "Q": [Selection(variable_name="sample_flow",
                        instrument_code="psap3w")],
    })
    data_records["aerosol-realtime-psapstatus"] = RealtimeRecord({
        "IrG": [RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600),
                                  instrument_code="psap3w")],
        "IfG": [RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600),
                                  instrument_code="psap3w")],
        "IpG": [RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600),
                                  instrument_code="psap3w")],
        "Q": [RealtimeSelection("Q", variable_name="sample_flow",
                                instrument_code="psap3w")],
    })

    data_records["aerosol-raw-temperature"] = DataRecord({
        "Tnephcell": [Selection(variable_name="cell_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephcell": [Selection(variable_name="cell_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })
    data_records["aerosol-raw-temperature"] = RealtimeRecord({
        "Tnephcell": [RealtimeSelection(variable_name="cell_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephcell": [RealtimeSelection(variable_name="cell_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)