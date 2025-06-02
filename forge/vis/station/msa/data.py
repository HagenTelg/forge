import typing
from ..cpd3 import use_cpd3


if use_cpd3("msa"):
    from ..cpd3 import DataStream, DataReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)

    station_profile_data['aerosol']['raw']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BacR_A21'): 'Ba',
            Name(station, 'raw', 'XR_A21'): 'X',
        }, send
    )
    station_profile_data['aerosol']['realtime']['maap'] = {
        RealtimeTranslator.Key('BacR_A21'): 'Ba',
        RealtimeTranslator.Key('XR_A21'): 'X',
    }
    
    station_profile_data['aerosol']['raw']['maapstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_A21'): 'Psample',
            Name(station, 'raw', 'T1_A21'): 'Tambient',
            Name(station, 'raw', 'T2_A21'): 'Tmeasurementhead',
            Name(station, 'raw', 'T3_A21'): 'Tsystem',
            Name(station, 'raw', 'Q_A21'): 'Qsample',
            Name(station, 'raw', 'IrR_A21'): 'Ir',
            Name(station, 'raw', 'IfR_A21'): 'If',
            Name(station, 'raw', 'IpR_A21'): 'Ip',
            Name(station, 'raw', 'Is1_A21'): 'Is1',
            Name(station, 'raw', 'Is2_A21'): 'Is2',
            Name(station, 'raw', 'Is1R_A21'): 'Is1',
            Name(station, 'raw', 'Is2R_A21'): 'Is2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['maapstatus'] = {
        RealtimeTranslator.Key('P_A21'): 'Psample',
        RealtimeTranslator.Key('T1_A21'): 'Tambient',
        RealtimeTranslator.Key('T2_A21'): 'Tmeasurementhead',
        RealtimeTranslator.Key('T3_A21'): 'Tsystem',
        RealtimeTranslator.Key('Q_A21'): 'Qsample',
        RealtimeTranslator.Key('IrR_A21'): 'Ir',
        RealtimeTranslator.Key('IfR_A21'): 'If',
        RealtimeTranslator.Key('IpR_A21'): 'Ip',
        RealtimeTranslator.Key('Is1_A21'): 'Is1',
        RealtimeTranslator.Key('Is2_A21'): 'Is2',
        RealtimeTranslator.Key('Is1R_A21'): 'Is1',
        RealtimeTranslator.Key('Is2R_A21'): 'Is2',
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
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-maap"] = DataRecord({
            "Ba": [Selection(variable_name="light_absorption", wavelength_number=0,
                             instrument_code="thermomaap")],
            "X": [Selection(variable_name="equivalent_black_carbon", wavelength_number=0,
                            instrument_code="thermomaap")],
        })
    data_records[f"aerosol-realtime-maap"] = RealtimeRecord({
        "Ba": [RealtimeSelection("Ba", variable_name="light_absorption", wavelength_number=0,
                                 instrument_code="thermomaap")],
        "X": [RealtimeSelection("X", variable_name="equivalent_black_carbon", wavelength_number=0,
                                instrument_code="thermomaap")],
    })
    data_records["aerosol-raw-maapstatus"] = DataRecord({
        "Psample": [Selection(variable_name="sample_pressure", instrument_code="thermomaap")],
        "Tambient": [Selection(variable_name="sample_temperature", instrument_code="thermomaap")],
        "Tmeasurementhead": [Selection(variable_name="measurement_head_temperature", instrument_code="thermomaap")],
        "Tsystem": [Selection(variable_name="system_temperature", instrument_code="thermomaap")],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="thermomaap")],
        "Ir": [Selection(variable_name="transmittance", instrument_code="thermomaap", wavelength_number=0)],
        "If": [Selection(variable_name="reference_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Ip": [Selection(variable_name="sample_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Is1": [Selection(variable_name="backscatter_135_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Is2": [Selection(variable_name="backscatter_165_intensity", instrument_code="thermomaap", wavelength_number=0)],
    })
    data_records["aerosol-realtime-maapstatus"] = RealtimeRecord({
        "Psample": [RealtimeSelection("P", variable_name="sample_pressure", instrument_code="thermomaap")],
        "Tambient": [RealtimeSelection("Tsample", variable_name="sample_temperature", instrument_code="thermomaap")],
        "Tmeasurementhead": [RealtimeSelection("Thead", variable_name="measurement_head_temperature", instrument_code="thermomaap")],
        "Tsystem": [RealtimeSelection("Tsystem", variable_name="system_temperature", instrument_code="thermomaap")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="thermomaap")],
        "Ir": [RealtimeSelection("Ir", variable_name="transmittance", instrument_code="thermomaap", wavelength_number=0)],
        "If": [RealtimeSelection("If", variable_name="reference_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Ip": [RealtimeSelection("Ip", variable_name="sample_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Is1": [RealtimeSelection("Is135", variable_name="backscatter_135_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Is2": [RealtimeSelection("Is165", variable_name="backscatter_165_intensity", instrument_code="thermomaap", wavelength_number=0)],
    })

    data_records["aerosol-raw-temperature"] = DataRecord({
        "Tnephcell": [Selection(variable_name="cell_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephcell": [Selection(variable_name="cell_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })
    data_records["aerosol-raw-temperature"] = RealtimeRecord({
        "Tnephcell": [RealtimeSelection("Tcell", variable_name="cell_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephcell": [RealtimeSelection("Ucell", variable_name="cell_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })
    
    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)