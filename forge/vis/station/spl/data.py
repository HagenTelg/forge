import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'N_N71'): 'cnc',
        Name(station, 'raw', 'N_N74'): 'cnc2',
        Name(station, 'raw', 'N_N73'): 'ccn',
    }, send
)
station_profile_data['aerosol']['realtime']['cnc'] = {
    RealtimeTranslator.Key('N_N71'): 'cnc',
    RealtimeTranslator.Key('N_N74'): 'cnc2',
    RealtimeTranslator.Key('N_N73'): 'ccn',
}
station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'N_N71'): 'cnc',
        Name(station, 'clean', 'N_N74'): 'cnc2',
        Name(station, 'clean', 'N_N73'): 'ccn',
    }, send
)
station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'N_N71'): 'cnc',
        Name(station, 'clean', 'N_N74'): 'cnc2',
        Name(station, 'clean', 'N_N73'): 'ccn',
    }, send
)
station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'N_N71'): 'cnc',
        Name(station, 'avgh', 'N_N74'): 'cnc2',
        Name(station, 'avgh', 'N_N73'): 'ccn',
    }, send
)

station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Q_Q71'): 'Qsample',
        Name(station, 'raw', 'Q_Q72'): 'Qdrier',
        Name(station, 'raw', 'T1_N71'): 'Tsaturator',
        Name(station, 'raw', 'T2_N71'): 'Tcondenser',
    }, send
)
station_profile_data['aerosol']['raw']['cpcstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_N74'): 'Tsaturator',
        Name(station, 'raw', 'T2_N74'): 'Tcondenser',
        Name(station, 'raw', 'T3_N74'): 'Toptics',
        Name(station, 'raw', 'T4_N74'): 'Tcabinet',
        Name(station, 'raw', 'Q_N74'): 'Qsample',
        Name(station, 'raw', 'Qu_N74'): 'Qinlet',
        Name(station, 'raw', 'P_N74'): 'Psample',
        Name(station, 'raw', 'Pd1_N74'): 'PDnozzle',
        Name(station, 'raw', 'Pd2_N74'): 'ODorifice',
        Name(station, 'raw', 'A_N74'): 'Alaser',
    }, send
)
station_profile_data['aerosol']['realtime']['cpcstatus'] = {
    RealtimeTranslator.Key('Q_Q71'): 'Qsample',
    RealtimeTranslator.Key('Q_Q72'): 'Qdrier',
    RealtimeTranslator.Key('T1_N71'): 'Tsaturator',
    RealtimeTranslator.Key('T2_N71'): 'Tcondenser',
}
station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
    RealtimeTranslator.Key('T1_N74'): 'Tsaturator',
    RealtimeTranslator.Key('T2_N74'): 'Tcondenser',
    RealtimeTranslator.Key('T3_N74'): 'Toptics',
    RealtimeTranslator.Key('T4_N74'): 'Tcabinet',
    RealtimeTranslator.Key('Q_N74'): 'Qsample',
    RealtimeTranslator.Key('Qu_N74'): 'Qinlet',
    RealtimeTranslator.Key('P_N74'): 'Psample',
    RealtimeTranslator.Key('Pd1_N74'): 'PDnozzle',
    RealtimeTranslator.Key('Pd2_N74'): 'ODorifice',
    RealtimeTranslator.Key('A_N74'): 'Alaser',
}


station_profile_data['aerosol']['raw']['ccnstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Tu_N73'): 'Tinlet',
        Name(station, 'raw', 'T1_N73'): 'Ttec1',
        Name(station, 'raw', 'T2_N73'): 'Ttec2',
        Name(station, 'raw', 'T3_N73'): 'Ttec3',
        Name(station, 'raw', 'T4_N73'): 'Tsample',
        Name(station, 'raw', 'T5_N73'): 'Topc',
        Name(station, 'raw', 'T6_N73'): 'Tnafion',
        Name(station, 'raw', 'Q1_N73'): 'Qsample',
        Name(station, 'raw', 'Q2_N73'): 'Qsheath',
        Name(station, 'raw', 'Uc_N73'): 'SScalc',
    }, send
)
station_profile_data['aerosol']['realtime']['ccnstatus'] = {
    RealtimeTranslator.Key('Tu_N73'): 'Tinlet',
    RealtimeTranslator.Key('T1_N73'): 'Ttec1',
    RealtimeTranslator.Key('T2_N73'): 'Ttec2',
    RealtimeTranslator.Key('T3_N73'): 'Ttec3',
    RealtimeTranslator.Key('T4_N73'): 'Tsample',
    RealtimeTranslator.Key('T5_N73'): 'Topc',
    RealtimeTranslator.Key('T6_N73'): 'Tnafion',
    RealtimeTranslator.Key('Q1_N73'): 'Qsample',
    RealtimeTranslator.Key('Q2_N73'): 'Qsheath',
    RealtimeTranslator.Key('Uc_N73'): 'SScalc',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
