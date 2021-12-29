import typing
from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)

station_profile_data['aerosol']['raw']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Ns_N11'): 'Dp',
        Name(station, 'raw', 'Nn_N11'): 'dNdlogDp',
        Name(station, 'raw', 'Nb_N11'): 'dN',
        Name(station, 'raw', 'N_N12'): 'Nraw',
    }, send
)
station_profile_data['aerosol']['editing']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'Ns_N11'): 'Dp',
        Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
        Name(station, 'clean', 'Nb_N11'): 'dN',
        Name(station, 'clean', 'N_N12'): 'Nraw',
        Name(station, 'clean', 'N_N11'): 'N',
        Name(station, 'clean', 'BsB_N11'): 'BsB',
        Name(station, 'clean', 'BsG_N11'): 'BsG',
        Name(station, 'clean', 'BsR_N11'): 'BsR',
    }, send
)
station_profile_data['aerosol']['clean']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'Ns_N11'): 'Dp',
        Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
        Name(station, 'clean', 'Nb_N11'): 'dN',
        Name(station, 'clean', 'N_N12'): 'Nraw',
        Name(station, 'clean', 'N_N11'): 'N',
        Name(station, 'clean', 'BsB_N11'): 'BsB',
        Name(station, 'clean', 'BsG_N11'): 'BsG',
        Name(station, 'clean', 'BsR_N11'): 'BsR',
    }, send
)
station_profile_data['aerosol']['avgh']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'Ns_N11'): 'Dp',
        Name(station, 'avgh', 'Nn_N11'): 'dNdlogDp',
        Name(station, 'avgh', 'Nb_N11'): 'dN',
        Name(station, 'avgh', 'N_N12'): 'Nraw',
        Name(station, 'avgh', 'N_N11'): 'N',
        Name(station, 'avgh', 'BsB_N11'): 'BsB',
        Name(station, 'avgh', 'BsG_N11'): 'BsG',
        Name(station, 'avgh', 'BsR_N11'): 'BsR',
    }, send
)
station_profile_data['aerosol']['raw']['dmpsstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_N11'): 'Taerosol', Name(station, 'raw', 'T2_N11'): 'Tsheath',
        Name(station, 'raw', 'P1_N11'): 'Paerosol', Name(station, 'raw', 'P2_N11'): 'Psheath',
        Name(station, 'raw', 'Q1_N11'): 'Qaerosol', Name(station, 'raw', 'Q2_N11'): 'Qsheath',
    }, send
)


station_profile_data['aerosol']['raw']['pops'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Ns_N21'): 'Dp',
        Name(station, 'raw', 'Nn_N21'): 'dNdlogDp',
        Name(station, 'raw', 'Nb_N21'): 'dN',
    }, send
)
station_profile_data['aerosol']['editing']['pops'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'Ns_N21'): 'Dp',
        Name(station, 'clean', 'Nn_N21'): 'dNdlogDp',
        Name(station, 'clean', 'Nb_N21'): 'dN',
        Name(station, 'clean', 'N_N21'): 'N',
        Name(station, 'clean', 'BsB_N21'): 'BsB',
        Name(station, 'clean', 'BsG_N21'): 'BsG',
        Name(station, 'clean', 'BsR_N21'): 'BsR',
    }, send
)
station_profile_data['aerosol']['clean']['pops'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'Ns_N21'): 'Dp',
        Name(station, 'clean', 'Nn_N21'): 'dNdlogDp',
        Name(station, 'clean', 'Nb_N21'): 'dN',
        Name(station, 'clean', 'N_N21'): 'N',
        Name(station, 'clean', 'BsB_N21'): 'BsB',
        Name(station, 'clean', 'BsG_N21'): 'BsG',
        Name(station, 'clean', 'BsR_N21'): 'BsR',
    }, send
)
station_profile_data['aerosol']['avgh']['pops'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'Ns_N21'): 'Dp',
        Name(station, 'avgh', 'Nn_N21'): 'dNdlogDp',
        Name(station, 'avgh', 'Nb_N21'): 'dN',
        Name(station, 'avgh', 'N_N21'): 'N',
        Name(station, 'avgh', 'BsB_N21'): 'BsB',
        Name(station, 'avgh', 'BsG_N21'): 'BsG',
        Name(station, 'avgh', 'BsR_N21'): 'BsR',
    }, send
)
station_profile_data['aerosol']['realtime']['pops'] = {
    RealtimeTranslator.Key('Ns_N21'): 'Dp',
    RealtimeTranslator.Key('Nn_N21'): 'dNdlogDp',
    RealtimeTranslator.Key('Nb_N21'): 'dN',
}

station_profile_data['aerosol']['raw']['popsstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_N21'): 'Tpressure',
        Name(station, 'raw', 'T2_N21'): 'Tlaser',
        Name(station, 'raw', 'T3_N21'): 'Tinternal',

        Name(station, 'raw', 'Q_N21'): 'Qsample',

        Name(station, 'raw', 'P_N21'): 'Pboard',
    }, send
)
station_profile_data['aerosol']['realtime']['popsstatus'] = {
    RealtimeTranslator.Key('T1_N21'): 'Tpressure',
    RealtimeTranslator.Key('T2_N21'): 'Tlaser',
    RealtimeTranslator.Key('T3_N21'): 'Tinternal',

    RealtimeTranslator.Key('Q_N21'): 'Qsample',

    RealtimeTranslator.Key('P_N21'): 'Pboard',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
