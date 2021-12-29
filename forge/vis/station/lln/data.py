import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Q_Q71'): 'Qsample',
        Name(station, 'raw', 'Q_Q72'): 'Qdrier',
        Name(station, 'raw', 'T1_N71'): 'Tsaturator',
        Name(station, 'raw', 'T2_N71'): 'Tcondenser',
    }, send
)
station_profile_data['aerosol']['realtime']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        RealtimeTranslator.Key('Q_Q71'): 'Qsample',
        RealtimeTranslator.Key('Q_Q72'): 'Qdrier',
        RealtimeTranslator.Key('T1_N71'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N71'): 'Tcondenser',
    }, send
)

station_profile_data['aerosol']['raw']['clap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A12'): 'BaB',
        Name(station, 'raw', 'BaG_A12'): 'BaG',
        Name(station, 'raw', 'BaR_A12'): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A12', {'pm10'}): 'BaB',
        Name(station, 'raw', 'BaG_A12', {'pm10'}): 'BaG',
        Name(station, 'raw', 'BaR_A12', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A12', {'pm25'}): 'BaB',
        Name(station, 'raw', 'BaG_A12', {'pm25'}): 'BaG',
        Name(station, 'raw', 'BaR_A12', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A12', {'pm1'}): 'BaB',
        Name(station, 'raw', 'BaG_A12', {'pm1'}): 'BaG',
        Name(station, 'raw', 'BaR_A12', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['realtime']['clap-whole'] = {
    RealtimeTranslator.Key('BaB_A12'): 'BaB',
    RealtimeTranslator.Key('BaG_A12'): 'BaG',
    RealtimeTranslator.Key('BaR_A12'): 'BaR',
}
station_profile_data['aerosol']['realtime']['clap-pm10'] = {
    RealtimeTranslator.Key('BaB_A12', {'pm10'}): 'BaB',
    RealtimeTranslator.Key('BaG_A12', {'pm10'}): 'BaG',
    RealtimeTranslator.Key('BaR_A12', {'pm10'}): 'BaR',
}
station_profile_data['aerosol']['realtime']['clap-pm25'] = {
    RealtimeTranslator.Key('BaB_A12', {'pm25'}): 'BaB',
    RealtimeTranslator.Key('BaG_A12', {'pm25'}): 'BaG',
    RealtimeTranslator.Key('BaR_A12', {'pm25'}): 'BaR',
}
station_profile_data['aerosol']['realtime']['clap-pm1'] = {
    RealtimeTranslator.Key('BaB_A12', {'pm1'}): 'BaB',
    RealtimeTranslator.Key('BaG_A12', {'pm1'}): 'BaG',
    RealtimeTranslator.Key('BaR_A12', {'pm1'}): 'BaR',
}
station_profile_data['aerosol']['editing']['clap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A12'): 'BaB',
        Name(station, 'clean', 'BaG_A12'): 'BaG',
        Name(station, 'clean', 'BaR_A12'): 'BaR',
    }, send
)
station_profile_data['aerosol']['editing']['clap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A12', {'pm10'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm10'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['editing']['clap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A12', {'pm25'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm25'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['editing']['clap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A12', {'pm1'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm1'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A12'): 'BaB',
        Name(station, 'clean', 'BaG_A12'): 'BaG',
        Name(station, 'clean', 'BaR_A12'): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A12', {'pm10'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm10'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A12', {'pm25'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm25'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A12', {'pm1'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm1'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A12'): 'BaB',
        Name(station, 'avgh', 'BaG_A12'): 'BaG',
        Name(station, 'avgh', 'BaR_A12'): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A12', {'pm10'}): 'BaB',
        Name(station, 'avgh', 'BaG_A12', {'pm10'}): 'BaG',
        Name(station, 'avgh', 'BaR_A12', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A12', {'pm25'}): 'BaB',
        Name(station, 'avgh', 'BaG_A12', {'pm25'}): 'BaG',
        Name(station, 'avgh', 'BaR_A12', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A12', {'pm1'}): 'BaB',
        Name(station, 'avgh', 'BaG_A12', {'pm1'}): 'BaG',
        Name(station, 'avgh', 'BaR_A12', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clapstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
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
        Name(station, 'raw', 'T1_A12'): 'Tsample',
        Name(station, 'raw', 'T1_A12', {'pm10'}): 'Tsample',
        Name(station, 'raw', 'T1_A12', {'pm1'}): 'Tsample',
        Name(station, 'raw', 'T1_A12', {'pm25'}): 'Tsample',
        Name(station, 'raw', 'T2_A12'): 'Tcase',
        Name(station, 'raw', 'T2_A12', {'pm10'}): 'Tcase',
        Name(station, 'raw', 'T2_A12', {'pm1'}): 'Tcase',
        Name(station, 'raw', 'T2_A12', {'pm25'}): 'Tcase',
        Name(station, 'raw', 'Fn_A12'): 'spot',
    }, send
)

station_profile_data['aerosol']['raw']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A91'): 'BaB',
        Name(station, 'raw', 'BaG_A91'): 'BaG',
        Name(station, 'raw', 'BaR_A91'): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A91', {'pm10'}): 'BaB',
        Name(station, 'raw', 'BaG_A91', {'pm10'}): 'BaG',
        Name(station, 'raw', 'BaR_A91', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A91', {'pm25'}): 'BaB',
        Name(station, 'raw', 'BaG_A91', {'pm25'}): 'BaG',
        Name(station, 'raw', 'BaR_A91', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A91', {'pm1'}): 'BaB',
        Name(station, 'raw', 'BaG_A91', {'pm1'}): 'BaG',
        Name(station, 'raw', 'BaR_A91', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['realtime']['clap2-whole'] = {
    RealtimeTranslator.Key('BaB_A91'): 'BaB',
    RealtimeTranslator.Key('BaG_A91'): 'BaG',
    RealtimeTranslator.Key('BaR_A91'): 'BaR',
}
station_profile_data['aerosol']['realtime']['clap2-pm10'] = {
    RealtimeTranslator.Key('BaB_A91', {'pm10'}): 'BaB',
    RealtimeTranslator.Key('BaG_A91', {'pm10'}): 'BaG',
    RealtimeTranslator.Key('BaR_A91', {'pm10'}): 'BaR',
}
station_profile_data['aerosol']['realtime']['clap2-pm25'] = {
    RealtimeTranslator.Key('BaB_A91', {'pm25'}): 'BaB',
    RealtimeTranslator.Key('BaG_A91', {'pm25'}): 'BaG',
    RealtimeTranslator.Key('BaR_A91', {'pm25'}): 'BaR',
}
station_profile_data['aerosol']['realtime']['clap2-pm1'] = {
    RealtimeTranslator.Key('BaB_A91', {'pm1'}): 'BaB',
    RealtimeTranslator.Key('BaG_A91', {'pm1'}): 'BaG',
    RealtimeTranslator.Key('BaR_A91', {'pm1'}): 'BaR',
}
station_profile_data['aerosol']['editing']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A91'): 'BaB',
        Name(station, 'clean', 'BaG_A91'): 'BaG',
        Name(station, 'clean', 'BaR_A91'): 'BaR',
    }, send
)
station_profile_data['aerosol']['editing']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A91', {'pm10'}): 'BaB',
        Name(station, 'clean', 'BaG_A91', {'pm10'}): 'BaG',
        Name(station, 'clean', 'BaR_A91', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['editing']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A91', {'pm25'}): 'BaB',
        Name(station, 'clean', 'BaG_A91', {'pm25'}): 'BaG',
        Name(station, 'clean', 'BaR_A91', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['editing']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A91', {'pm1'}): 'BaB',
        Name(station, 'clean', 'BaG_A91', {'pm1'}): 'BaG',
        Name(station, 'clean', 'BaR_A91', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A91'): 'BaB',
        Name(station, 'clean', 'BaG_A91'): 'BaG',
        Name(station, 'clean', 'BaR_A91'): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A91', {'pm10'}): 'BaB',
        Name(station, 'clean', 'BaG_A91', {'pm10'}): 'BaG',
        Name(station, 'clean', 'BaR_A91', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A91', {'pm25'}): 'BaB',
        Name(station, 'clean', 'BaG_A91', {'pm25'}): 'BaG',
        Name(station, 'clean', 'BaR_A91', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A91', {'pm1'}): 'BaB',
        Name(station, 'clean', 'BaG_A91', {'pm1'}): 'BaG',
        Name(station, 'clean', 'BaR_A91', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A91'): 'BaB',
        Name(station, 'avgh', 'BaG_A91'): 'BaG',
        Name(station, 'avgh', 'BaR_A91'): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A91', {'pm10'}): 'BaB',
        Name(station, 'avgh', 'BaG_A91', {'pm10'}): 'BaG',
        Name(station, 'avgh', 'BaR_A91', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A91', {'pm25'}): 'BaB',
        Name(station, 'avgh', 'BaG_A91', {'pm25'}): 'BaG',
        Name(station, 'avgh', 'BaR_A91', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A91', {'pm1'}): 'BaB',
        Name(station, 'avgh', 'BaG_A91', {'pm1'}): 'BaG',
        Name(station, 'avgh', 'BaR_A91', {'pm1'}): 'BaR',
    }, send
)

station_profile_data['aerosol']['raw']['clapstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'IrG_A91'): 'IrG',
        Name(station, 'raw', 'IrG_A91', {'pm10'}): 'IrG',
        Name(station, 'raw', 'IrG_A91', {'pm1'}): 'IrG',
        Name(station, 'raw', 'IrG_A91', {'pm25'}): 'IrG',
        Name(station, 'raw', 'IfG_A91'): 'IfG',
        Name(station, 'raw', 'IfG_A91', {'pm10'}): 'IfG',
        Name(station, 'raw', 'IfG_A91', {'pm1'}): 'IfG',
        Name(station, 'raw', 'IfG_A91', {'pm25'}): 'IfG',
        Name(station, 'raw', 'IpG_A91'): 'IpG',
        Name(station, 'raw', 'IpG_A91', {'pm10'}): 'IpG',
        Name(station, 'raw', 'IpG_A91', {'pm1'}): 'IpG',
        Name(station, 'raw', 'IpG_A91', {'pm25'}): 'IpG',
        Name(station, 'raw', 'Q_A91'): 'Q',
        Name(station, 'raw', 'Q_A91', {'pm10'}): 'Q',
        Name(station, 'raw', 'Q_A91', {'pm1'}): 'Q',
        Name(station, 'raw', 'Q_A91', {'pm25'}): 'Q',
        Name(station, 'raw', 'T1_A91'): 'Tsample',
        Name(station, 'raw', 'T1_A91', {'pm10'}): 'Tsample',
        Name(station, 'raw', 'T1_A91', {'pm1'}): 'Tsample',
        Name(station, 'raw', 'T1_A91', {'pm25'}): 'Tsample',
        Name(station, 'raw', 'T2_A91'): 'Tcase',
        Name(station, 'raw', 'T2_A91', {'pm10'}): 'Tcase',
        Name(station, 'raw', 'T2_A91', {'pm1'}): 'Tcase',
        Name(station, 'raw', 'T2_A91', {'pm25'}): 'Tcase',
        Name(station, 'raw', 'Fn_A91'): 'spot',
    }, send
)
station_profile_data['aerosol']['realtime']['clapstatus2'] = {
    RealtimeTranslator.Key('IrG_A91'): 'IrG',
    RealtimeTranslator.Key('IrG_A91', {'pm10'}): 'IrG',
    RealtimeTranslator.Key('IrG_A91', {'pm1'}): 'IrG',
    RealtimeTranslator.Key('IrG_A91', {'pm25'}): 'IrG',
    RealtimeTranslator.Key('IfG_A91'): 'IfG',
    RealtimeTranslator.Key('IfG_A91', {'pm10'}): 'IfG',
    RealtimeTranslator.Key('IfG_A91', {'pm1'}): 'IfG',
    RealtimeTranslator.Key('IfG_A91', {'pm25'}): 'IfG',
    RealtimeTranslator.Key('IpG_A91'): 'IpG',
    RealtimeTranslator.Key('IpG_A91', {'pm10'}): 'IpG',
    RealtimeTranslator.Key('IpG_A91', {'pm1'}): 'IpG',
    RealtimeTranslator.Key('IpG_A91', {'pm25'}): 'IpG',
    RealtimeTranslator.Key('Q_A91'): 'Q',
    RealtimeTranslator.Key('Q_A91', {'pm10'}): 'Q',
    RealtimeTranslator.Key('Q_A91', {'pm1'}): 'Q',
    RealtimeTranslator.Key('Q_A91', {'pm25'}): 'Q',
    RealtimeTranslator.Key('T1_A91'): 'Tsample',
    RealtimeTranslator.Key('T1_A91', {'pm10'}): 'Tsample',
    RealtimeTranslator.Key('T1_A91', {'pm1'}): 'Tsample',
    RealtimeTranslator.Key('T1_A91', {'pm25'}): 'Tsample',
    RealtimeTranslator.Key('T2_A91'): 'Tcase',
    RealtimeTranslator.Key('T2_A91', {'pm10'}): 'Tcase',
    RealtimeTranslator.Key('T2_A91', {'pm1'}): 'Tcase',
    RealtimeTranslator.Key('T2_A91', {'pm25'}): 'Tcase',
    RealtimeTranslator.Key('Fn_A91'): 'spot',
}
station_profile_data['aerosol']['raw']['psapstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'IrG_A11'): 'IrG',
        Name(station, 'raw', 'IrG_A11', {'pm10'}): 'IrG',
        Name(station, 'raw', 'IrG_A11', {'pm1'}): 'IrG',
        Name(station, 'raw', 'IrG_A11', {'pm25'}): 'IrG',
        Name(station, 'raw', 'IfG_A11'): 'IfG',
        Name(station, 'raw', 'IfG_A11', {'pm10'}): 'IfG',
        Name(station, 'raw', 'IfG_A11', {'pm1'}): 'IfG',
        Name(station, 'raw', 'IfG_A11', {'pm25'}): 'IfG',
        Name(station, 'raw', 'IpG_A11'): 'IpG',
        Name(station, 'raw', 'IpG_A11', {'pm10'}): 'IpG',
        Name(station, 'raw', 'IpG_A11', {'pm1'}): 'IpG',
        Name(station, 'raw', 'IpG_A11', {'pm25'}): 'IpG',
        Name(station, 'raw', 'Q_A11'): 'Q',
        Name(station, 'raw', 'Q_A11', {'pm10'}): 'Q',
        Name(station, 'raw', 'Q_A11', {'pm1'}): 'Q',
        Name(station, 'raw', 'Q_A11', {'pm25'}): 'Q',
    }, send
)
station_profile_data['aerosol']['realtime']['psapstatus'] = {
    RealtimeTranslator.Key('IrG_A11'): 'IrG',
    RealtimeTranslator.Key('IrG_A11', {'pm10'}): 'IrG',
    RealtimeTranslator.Key('IrG_A11', {'pm1'}): 'IrG',
    RealtimeTranslator.Key('IrG_A11', {'pm25'}): 'IrG',
    RealtimeTranslator.Key('IfG_A11'): 'IfG',
    RealtimeTranslator.Key('IfG_A11', {'pm10'}): 'IfG',
    RealtimeTranslator.Key('IfG_A11', {'pm1'}): 'IfG',
    RealtimeTranslator.Key('IfG_A11', {'pm25'}): 'IfG',
    RealtimeTranslator.Key('IpG_A11'): 'IpG',
    RealtimeTranslator.Key('IpG_A11', {'pm10'}): 'IpG',
    RealtimeTranslator.Key('IpG_A11', {'pm1'}): 'IpG',
    RealtimeTranslator.Key('IpG_A11', {'pm25'}): 'IpG',
    RealtimeTranslator.Key('Q_A11'): 'Q',
    RealtimeTranslator.Key('Q_A11', {'pm10'}): 'Q',
    RealtimeTranslator.Key('Q_A11', {'pm1'}): 'Q',
    RealtimeTranslator.Key('Q_A11', {'pm25'}): 'Q',
}


station_profile_data['aerosol']['raw']['aethalometerstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Q_A81'): 'Q',
    }, send
)
station_profile_data['aerosol']['realtime']['aethalometerstatus'] = {
    RealtimeTranslator.Key('Q_A81'): 'Q',
}


station_profile_data['aerosol']['raw']['tca'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'X1_M81'): 'X',
    }, send
)
station_profile_data['aerosol']['realtime']['tca'] = {
    RealtimeTranslator.Key('X1_M81'): 'X',
}
station_profile_data['aerosol']['editing']['tca'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'X1_M81'): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['tca'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'X1_M81'): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['tca'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'X1_M81'): 'X',
    }, send
)

station_profile_data['aerosol']['raw']['tcastatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'X2_M81'): 'CO2',
        Name(station, 'raw', 'T1_M81'): 'Tchamber1',
        Name(station, 'raw', 'T2_M81'): 'Tchamber2',
        Name(station, 'raw', 'T3_M81'): 'Tlicor',
        Name(station, 'raw', 'TD3_M81'): 'TDlicor',
        Name(station, 'raw', 'P_M81'): 'Plicor',
        Name(station, 'raw', 'Q1_M81'): 'Qsample',
        Name(station, 'raw', 'Q2_M81'): 'Qanalytic',
    }, send
)
station_profile_data['aerosol']['raw']['tcastatus'] = {
    RealtimeTranslator.Key('X2_M81'): 'CO2',
    RealtimeTranslator.Key('T1_M81'): 'Tchamber1',
    RealtimeTranslator.Key('T2_M81'): 'Tchamber2',
    RealtimeTranslator.Key('T3_M81'): 'Tlicor',
    RealtimeTranslator.Key('TD3_M81'): 'TDlicor',
    RealtimeTranslator.Key('P_M81'): 'Plicor',
    RealtimeTranslator.Key('Q1_M81'): 'Qsample',
    RealtimeTranslator.Key('Q2_M81'): 'Qanalytic',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
