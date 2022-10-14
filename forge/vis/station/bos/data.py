import typing
from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'N_N71'): 'cnc',
        Name(station, 'raw', 'N_N72'): 'cnc2',
    }, send
)
station_profile_data['aerosol']['realtime']['cnc'] = {
    RealtimeTranslator.Key('N_N71'): 'cnc',
    RealtimeTranslator.Key('N_N72'): 'cnc2',
}
station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'N_N71'): 'cnc',
        Name(station, 'clean', 'N_N72'): 'cnc2',
    }, send
)
station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'N_N71'): 'cnc',
        Name(station, 'clean', 'N_N72'): 'cnc2',
    }, send
)
station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'N_N71'): 'cnc',
        Name(station, 'avgh', 'N_N72'): 'cnc2',
    }, send
)

station_profile_data['aerosol']['raw']['cpcstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Tu_N72'): 'Tinlet',
        Name(station, 'raw', 'TDu_N72'): 'TDinlet',
        Name(station, 'raw', 'Uu_N72'): 'Uinlet',
        Name(station, 'raw', 'T1_N72'): 'Tconditioner',
        Name(station, 'raw', 'T2_N72'): 'Tinitiator',
        Name(station, 'raw', 'T3_N72'): 'Tmoderator',
        Name(station, 'raw', 'T4_N72'): 'Toptics',
        Name(station, 'raw', 'T5_N72'): 'Theatsink',
        Name(station, 'raw', 'T6_N72'): 'Tcase',
        Name(station, 'raw', 'TD1_N72'): 'TDgrowth',
        Name(station, 'raw', 'Q_N72'): 'Qsample',
        Name(station, 'raw', 'P_N72'): 'Psample',
        Name(station, 'raw', 'PCT_N72'): 'PCTwick',
        Name(station, 'raw', 'V_N72'): 'Vpulse',
    }, send
)
station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
    RealtimeTranslator.Key('Tu_N72'): 'Tinlet',
    RealtimeTranslator.Key('TDu_N72'): 'TDinlet',
    RealtimeTranslator.Key('Uu_N72'): 'Uinlet',
    RealtimeTranslator.Key('T1_N72'): 'Tconditioner',
    RealtimeTranslator.Key('T2_N72'): 'Tinitiator',
    RealtimeTranslator.Key('T3_N72'): 'Tmoderator',
    RealtimeTranslator.Key('T4_N72'): 'Toptics',
    RealtimeTranslator.Key('T5_N72'): 'Theatsink',
    RealtimeTranslator.Key('T6_N72'): 'Tcase',
    RealtimeTranslator.Key('TD1_N72'): 'TDgrowth',
    RealtimeTranslator.Key('Q_N72'): 'Qsample',
    RealtimeTranslator.Key('P_N72'): 'Psample',
    RealtimeTranslator.Key('PCT_N72'): 'PCTwick',
    RealtimeTranslator.Key('V_N72'): 'Vpulse',
}


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


station_profile_data['aerosol']['raw']['t640-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'X_M11'): 'X',
    }, send
)
station_profile_data['aerosol']['raw']['t640-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'X_M11', {'pm10'}): 'X',
    }, send
)
station_profile_data['aerosol']['raw']['t640-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'X_M11', {'pm25'}): 'X',
    }, send
)
station_profile_data['aerosol']['raw']['t640-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'X_M11', {'pm1'}): 'X',
    }, send
)
station_profile_data['aerosol']['realtime']['t640-whole'] = {
    RealtimeTranslator.Key('X_M11'): 'X',
}
station_profile_data['aerosol']['realtime']['t640-pm10'] ={
    RealtimeTranslator.Key('X_M11', {'pm10'}): 'X',
}
station_profile_data['aerosol']['realtime']['t640-pm25'] = {
    RealtimeTranslator.Key('X_M11', {'pm25'}): 'X',
}
station_profile_data['aerosol']['realtime']['t640-pm1'] = {
    RealtimeTranslator.Key('X_M11', {'pm1'}): 'X',
}
station_profile_data['aerosol']['editing']['t640-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'X_M11'): 'X',
    }, send
)
station_profile_data['aerosol']['editing']['t640-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'X_M11', {'pm10'}): 'X',
    }, send
)
station_profile_data['aerosol']['editing']['t640-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'X_M11', {'pm25'}): 'X',
    }, send
)
station_profile_data['aerosol']['editing']['t640-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'X_M11', {'pm1'}): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['t640-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'X_M11'): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['t640-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'X_M11', {'pm10'}): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['t640-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'X_M11', {'pm25'}): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['t640-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'X_M11', {'pm1'}): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['t640-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'X_M11'): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['t640-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'X_M11', {'pm10'}): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['t640-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'X_M11', {'pm25'}): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['t640-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'X_M11', {'pm1'}): 'X',
    }, send
)

station_profile_data['aerosol']['raw']['t640status'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_M11'): 'Tsample',
        Name(station, 'raw', 'T2_M11'): 'Tambient',
        Name(station, 'raw', 'T3_M11'): 'Tasc',
        Name(station, 'raw', 'T4_M11'): 'Tled',
        Name(station, 'raw', 'T5_M11'): 'Tbox',
        Name(station, 'raw', 'U1_M11'): 'Usample',
        Name(station, 'raw', 'P_M11'): 'Psample',
        Name(station, 'raw', 'Q1_M11'): 'Qsample',
        Name(station, 'raw', 'Q2_M11'): 'Qbypass',
    }, send
)
station_profile_data['aerosol']['realtime']['t640status'] = {
    RealtimeTranslator.Key('T1_M11'): 'Tsample',
    RealtimeTranslator.Key('T2_M11'): 'Tambient',
    RealtimeTranslator.Key('T3_M11'): 'Tasc',
    RealtimeTranslator.Key('T4_M11'): 'Tled',
    RealtimeTranslator.Key('T5_M11'): 'Tbox',
    RealtimeTranslator.Key('U1_M11'): 'Usample',
    RealtimeTranslator.Key('P_M11'): 'Psample',
    RealtimeTranslator.Key('Q1_M11'): 'Qsample',
    RealtimeTranslator.Key('Q2_M11'): 'Qbypass',

}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
