import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_N61'): 'Tsaturator',
        Name(station, 'raw', 'T2_N61'): 'Tcondenser',
        Name(station, 'raw', 'T3_N61'): 'Toptics',
        Name(station, 'raw', 'T4_N61'): 'Tcabinet',
        Name(station, 'raw', 'Q_N61'): 'Qsample',
        Name(station, 'raw', 'Qu_N61'): 'Qinlet',
        Name(station, 'raw', 'P_N61'): 'Psample',
        Name(station, 'raw', 'Pd1_N61'): 'PDnozzle',
        Name(station, 'raw', 'Pd2_N61'): 'ODorifice',
        Name(station, 'raw', 'A_N61'): 'Alaser',
    }, send
)
station_profile_data['aerosol']['realtime']['cpcstatus'] = {
    RealtimeTranslator.Key('T1_N61'): 'Tsaturator',
    RealtimeTranslator.Key('T2_N61'): 'Tcondenser',
    RealtimeTranslator.Key('T3_N61'): 'Toptics',
    RealtimeTranslator.Key('T4_N61'): 'Tcabinet',
    RealtimeTranslator.Key('Q_N61'): 'Qsample',
    RealtimeTranslator.Key('Qu_N61'): 'Qinlet',
    RealtimeTranslator.Key('P_N61'): 'Psample',
    RealtimeTranslator.Key('Pd1_N61'): 'PDnozzle',
    RealtimeTranslator.Key('Pd2_N61'): 'ODorifice',
    RealtimeTranslator.Key('A_N61'): 'Alaser',
}


station_profile_data['aerosol']['raw']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Ns_N12'): 'Dp',
        Name(station, 'raw', 'Nn_N12'): 'dNdlogDp',
        Name(station, 'raw', 'Nb_N12'): 'dN',
    }, send
)
station_profile_data['aerosol']['editing']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'Ns_N12'): 'Dp',
        Name(station, 'clean', 'Nn_N12'): 'dNdlogDp',
        Name(station, 'clean', 'Nb_N12'): 'dN',
        Name(station, 'clean', 'N_N12'): 'N',
        Name(station, 'clean', 'BsB_N12'): 'BsB',
        Name(station, 'clean', 'BsG_N12'): 'BsG',
        Name(station, 'clean', 'BsR_N12'): 'BsR',
    }, send
)
station_profile_data['aerosol']['clean']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'Ns_N12'): 'Dp',
        Name(station, 'clean', 'Nn_N12'): 'dNdlogDp',
        Name(station, 'clean', 'Nb_N12'): 'dN',
        Name(station, 'clean', 'N_N12'): 'N',
        Name(station, 'clean', 'BsB_N12'): 'BsB',
        Name(station, 'clean', 'BsG_N12'): 'BsG',
        Name(station, 'clean', 'BsR_N12'): 'BsR',
    }, send
)
station_profile_data['aerosol']['avgh']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'Ns_N12'): 'Dp',
        Name(station, 'avgh', 'Nn_N12'): 'dNdlogDp',
        Name(station, 'avgh', 'Nb_N12'): 'dN',
        Name(station, 'avgh', 'N_N12'): 'N',
        Name(station, 'avgh', 'BsB_N12'): 'BsB',
        Name(station, 'avgh', 'BsG_N12'): 'BsG',
        Name(station, 'avgh', 'BsR_N12'): 'BsR',
    }, send
)


station_profile_data['aerosol']['raw']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Ns_N11'): 'Dp',
        Name(station, 'raw', 'Nn_N11'): 'dNdlogDp',
        Name(station, 'raw', 'Nb_N11'): 'dN',
    }, send
)
station_profile_data['aerosol']['raw']['grimm'] = {
    RealtimeTranslator.Key('Ns_N11'): 'Dp',
    RealtimeTranslator.Key('Nn_N11'): 'dNdlogDp',
    RealtimeTranslator.Key('Nb_N11'): 'dN',
}
station_profile_data['aerosol']['editing']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'Ns_N11'): 'Dp',
        Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
        Name(station, 'clean', 'Nb_N11'): 'dN',
        Name(station, 'clean', 'N_N11'): 'N',
        Name(station, 'clean', 'BsB_N11'): 'BsB',
        Name(station, 'clean', 'BsG_N11'): 'BsG',
        Name(station, 'clean', 'BsR_N11'): 'BsR',
    }, send
)
station_profile_data['aerosol']['clean']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'Ns_N11'): 'Dp',
        Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
        Name(station, 'clean', 'Nb_N11'): 'dN',
        Name(station, 'clean', 'N_N11'): 'N',
        Name(station, 'clean', 'BsB_N11'): 'BsB',
        Name(station, 'clean', 'BsG_N11'): 'BsG',
        Name(station, 'clean', 'BsR_N11'): 'BsR',
    }, send
)
station_profile_data['aerosol']['avgh']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'Ns_N11'): 'Dp',
        Name(station, 'avgh', 'Nn_N11'): 'dNdlogDp',
        Name(station, 'avgh', 'Nb_N11'): 'dN',
        Name(station, 'avgh', 'N_N11'): 'N',
        Name(station, 'avgh', 'BsB_N11'): 'BsB',
        Name(station, 'avgh', 'BsG_N11'): 'BsG',
        Name(station, 'avgh', 'BsR_N11'): 'BsR',
    }, send
)

station_profile_data['aerosol']['raw']['grimmstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Q_N11'): 'Qsample',
    }, send
)
station_profile_data['aerosol']['raw']['grimmstatus'] = {
    RealtimeTranslator.Key('Q_N11'): 'Qsample',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
