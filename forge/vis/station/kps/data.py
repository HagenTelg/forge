import typing
from ..cpd3 import use_cpd3


if use_cpd3("kps"):
    from ..cpd3 import DataStream, DataReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['flow'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Q_Q11'): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm10'}): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm1'}): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm25'}): 'sample',
            Name(station, 'raw', 'Q_Q12'): 'dilution',
            Name(station, 'raw', 'Pd_P01'): 'pitot',
        }, send
    )
    station_profile_data['aerosol']['realtime']['flow'] = {
        RealtimeTranslator.Key('Q_Q11'): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm10'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm1'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm25'}): 'sample',
        RealtimeTranslator.Key('Q_Q12'): 'dilution',
        RealtimeTranslator.Key('Pd_P01'): 'pitot',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)

    data_records["aerosol-raw-flow"] = DataRecord({
        "sample": [Selection(variable_id="Q_Q11")],
        "dilution": [Selection(variable_id="Q_Q12")],
        "pitot": [Selection(variable_id="Pd_P01")],
    })
    data_records["aerosol-realtime-flow"] = RealtimeRecord({
        "sample": [RealtimeSelection("Q_Q11", variable_id="Q_Q11")],
        "dilution": [RealtimeSelection("Q_Q12", variable_id="Q_Q12")],
        "pitot": [RealtimeSelection("Pd_P01", variable_id="Pd_P01")],
    })


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
