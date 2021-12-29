import typing
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
