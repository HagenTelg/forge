import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)

station_profile_data['aerosol']['raw']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BsB_S12'): 'BsB',
        Name(station, 'raw', 'BsG_S12'): 'BsG',
        Name(station, 'raw', 'BsR_S12'): 'BsR',
        Name(station, 'raw', 'BbsB_S12'): 'BbsB',
        Name(station, 'raw', 'BbsG_S12'): 'BbsG',
        Name(station, 'raw', 'BbsR_S12'): 'BbsR',
    }, send
)
station_profile_data['aerosol']['raw']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BsB_S12', {'pm10'}): 'BsB',
        Name(station, 'raw', 'BsG_S12', {'pm10'}): 'BsG',
        Name(station, 'raw', 'BsR_S12', {'pm10'}): 'BsR',
        Name(station, 'raw', 'BbsB_S12', {'pm10'}): 'BbsB',
        Name(station, 'raw', 'BbsG_S12', {'pm10'}): 'BbsG',
        Name(station, 'raw', 'BbsR_S12', {'pm10'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['raw']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BsB_S12', {'pm1'}): 'BsB',
        Name(station, 'raw', 'BsG_S12', {'pm1'}): 'BsG',
        Name(station, 'raw', 'BsR_S12', {'pm1'}): 'BsR',
        Name(station, 'raw', 'BbsB_S12', {'pm1'}): 'BbsB',
        Name(station, 'raw', 'BbsG_S12', {'pm1'}): 'BbsG',
        Name(station, 'raw', 'BbsR_S12', {'pm1'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['realtime']['scattering2-whole'] = {
    RealtimeTranslator.Key('BsB_S12'): 'BsB',
    RealtimeTranslator.Key('BsG_S12'): 'BsG',
    RealtimeTranslator.Key('BsR_S12'): 'BsR',
    RealtimeTranslator.Key('BbsB_S12'): 'BbsB',
    RealtimeTranslator.Key('BbsG_S12'): 'BbsG',
    RealtimeTranslator.Key('BbsR_S12'): 'BbsR',
}
station_profile_data['aerosol']['realtime']['scattering2-pm10'] = {
    RealtimeTranslator.Key('BsB_S12', {'pm10'}): 'BsB',
    RealtimeTranslator.Key('BsG_S12', {'pm10'}): 'BsG',
    RealtimeTranslator.Key('BsR_S12', {'pm10'}): 'BsR',
    RealtimeTranslator.Key('BbsB_S12', {'pm10'}): 'BbsB',
    RealtimeTranslator.Key('BbsG_S12', {'pm10'}): 'BbsG',
    RealtimeTranslator.Key('BbsR_S12', {'pm10'}): 'BbsR',
}
station_profile_data['aerosol']['realtime']['scattering2-pm1'] = {
    RealtimeTranslator.Key('BsB_S12', {'pm1'}): 'BsB',
    RealtimeTranslator.Key('BsG_S12', {'pm1'}): 'BsG',
    RealtimeTranslator.Key('BsR_S12', {'pm1'}): 'BsR',
    RealtimeTranslator.Key('BbsB_S12', {'pm1'}): 'BbsB',
    RealtimeTranslator.Key('BbsG_S12', {'pm1'}): 'BbsG',
    RealtimeTranslator.Key('BbsR_S12', {'pm1'}): 'BbsR',
}
station_profile_data['aerosol']['editing']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BsB_S12', {'pm10'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm10'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm10'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm10'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm10'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm10'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['editing']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BsB_S12', {'pm1'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm1'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm1'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm1'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm1'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm1'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['clean']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BsB_S12', {'pm10'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm10'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm10'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm10'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm10'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm10'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['clean']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BsB_S12', {'pm1'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm1'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm1'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm1'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm1'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm1'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['avgh']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BsB_S12', {'pm10'}): 'BsB',
        Name(station, 'avgh', 'BsG_S12', {'pm10'}): 'BsG',
        Name(station, 'avgh', 'BsR_S12', {'pm10'}): 'BsR',
        Name(station, 'avgh', 'BbsB_S12', {'pm10'}): 'BbsB',
        Name(station, 'avgh', 'BbsG_S12', {'pm10'}): 'BbsG',
        Name(station, 'avgh', 'BbsR_S12', {'pm10'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['avgh']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BsB_S12', {'pm1'}): 'BsB',
        Name(station, 'avgh', 'BsG_S12', {'pm1'}): 'BsG',
        Name(station, 'avgh', 'BsR_S12', {'pm1'}): 'BsR',
        Name(station, 'avgh', 'BbsB_S12', {'pm1'}): 'BbsB',
        Name(station, 'avgh', 'BbsG_S12', {'pm1'}): 'BbsG',
        Name(station, 'avgh', 'BbsR_S12', {'pm1'}): 'BbsR',
    }, send
)

station_profile_data['aerosol']['raw']['nephzero2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BswB_S12'): 'BswB',
        Name(station, 'raw', 'BswG_S12'): 'BswG',
        Name(station, 'raw', 'BswR_S12'): 'BswR',
        Name(station, 'raw', 'BbswB_S12'): 'BbswB',
        Name(station, 'raw', 'BbswG_S12'): 'BbswG',
        Name(station, 'raw', 'BbswR_S12'): 'BbswR',
    }, send
)
station_profile_data['aerosol']['raw']['nephstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'CfG_S12'): 'CfG',
        Name(station, 'raw', 'CfG_S12', {'pm10'}): 'CfG',
        Name(station, 'raw', 'CfG_S12', {'pm1'}): 'CfG',
        Name(station, 'raw', 'CfG_S12', {'pm25'}): 'CfG',
    }, send
)
station_profile_data['aerosol']['realtime']['nephzero2'] = {
    RealtimeTranslator.Key('BswB_S12'): 'BswB',
    RealtimeTranslator.Key('BswG_S12'): 'BswG',
    RealtimeTranslator.Key('BswR_S12'): 'BswR',
    RealtimeTranslator.Key('BbswB_S12'): 'BbswB',
    RealtimeTranslator.Key('BbswG_S12'): 'BbswG',
    RealtimeTranslator.Key('BbswR_S12'): 'BbswR',
}
station_profile_data['aerosol']['realtime']['nephstatus2'] = {
    RealtimeTranslator.Key('CfG_S12'): 'CfG',
    RealtimeTranslator.Key('CfG_S12', {'pm10'}): 'CfG',
    RealtimeTranslator.Key('CfG_S12', {'pm1'}): 'CfG',
    RealtimeTranslator.Key('CfG_S12', {'pm25'}): 'CfG',
}


station_profile_data['aerosol']['raw']['pressure'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_XM1'): 'ambient',
        Name(station, 'raw', 'P_S11'): 'neph',
        Name(station, 'raw', 'P_S11', {'pm10'}): 'neph',
        Name(station, 'raw', 'P_S11', {'pm25'}): 'neph',
        Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
    }, send
)
station_profile_data['aerosol']['realtime']['pressure'] = {
    RealtimeTranslator.Key('P_XM1'): 'ambient',
    RealtimeTranslator.Key('P_S11'): 'neph',
    RealtimeTranslator.Key('P_S11', {'pm10'}): 'neph',
    RealtimeTranslator.Key('P_S11', {'pm25'}): 'neph',
    RealtimeTranslator.Key('P_S11', {'pm1'}): 'neph',
}

station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Tu_N71'): 'Tinlet',
        Name(station, 'raw', 'TDu_N71'): 'TDinlet',
        Name(station, 'raw', 'Uu_N71'): 'Uinlet',
        Name(station, 'raw', 'T1_N71'): 'Tconditioner',
        Name(station, 'raw', 'T2_N71'): 'Tinitiator',
        Name(station, 'raw', 'T3_N71'): 'Tmoderator',
        Name(station, 'raw', 'T4_N71'): 'Toptics',
        Name(station, 'raw', 'T5_N71'): 'Theatsink',
        Name(station, 'raw', 'T6_N71'): 'Tpcb',
        Name(station, 'raw', 'T7_N71'): 'Tcabinet',
        Name(station, 'raw', 'Q_N71'): 'Qsample',
        Name(station, 'raw', 'P_N71'): 'Psample',
        Name(station, 'raw', 'Pd_N71'): 'PDorifice',
    }, send
)
station_profile_data['aerosol']['realtime']['cpcstatus'] = {
    RealtimeTranslator.Key('Tu_N71'): 'Tinlet',
    RealtimeTranslator.Key('TDu_N71'): 'TDinlet',
    RealtimeTranslator.Key('Uu_N71'): 'Uinlet',
    RealtimeTranslator.Key('T1_N71'): 'Tconditioner',
    RealtimeTranslator.Key('T2_N71'): 'Tinitiator',
    RealtimeTranslator.Key('T3_N71'): 'Tmoderator',
    RealtimeTranslator.Key('T4_N71'): 'Toptics',
    RealtimeTranslator.Key('T5_N71'): 'Theatsink',
    RealtimeTranslator.Key('T6_N71'): 'Tpcb',
    RealtimeTranslator.Key('T7_N71'): 'Tcabinet',
    RealtimeTranslator.Key('Q_N71'): 'Qsample',
    RealtimeTranslator.Key('P_N71'): 'Psample',
    RealtimeTranslator.Key('Pd_N71'): 'PDorifice',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
