import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A12'): 'BaB',
        Name(station, 'raw', 'BaG_A12'): 'BaG',
        Name(station, 'raw', 'BaR_A12'): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A12', {'pm10'}): 'BaB',
        Name(station, 'raw', 'BaG_A12', {'pm10'}): 'BaG',
        Name(station, 'raw', 'BaR_A12', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A12', {'pm25'}): 'BaB',
        Name(station, 'raw', 'BaG_A12', {'pm25'}): 'BaG',
        Name(station, 'raw', 'BaR_A12', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['raw']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BaB_A12', {'pm1'}): 'BaB',
        Name(station, 'raw', 'BaG_A12', {'pm1'}): 'BaG',
        Name(station, 'raw', 'BaR_A12', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['realtime']['clap2-whole'] = {
    RealtimeTranslator.Key('BaB_A12'): 'BaB',
    RealtimeTranslator.Key('BaG_A12'): 'BaG',
    RealtimeTranslator.Key('BaR_A12'): 'BaR',
}
station_profile_data['aerosol']['realtime']['clap2-pm10'] = {
    RealtimeTranslator.Key('BaB_A12', {'pm10'}): 'BaB',
    RealtimeTranslator.Key('BaG_A12', {'pm10'}): 'BaG',
    RealtimeTranslator.Key('BaR_A12', {'pm10'}): 'BaR',
}
station_profile_data['aerosol']['realtime']['clap2-pm25'] = {
    RealtimeTranslator.Key('BaB_A12', {'pm25'}): 'BaB',
    RealtimeTranslator.Key('BaG_A12', {'pm25'}): 'BaG',
    RealtimeTranslator.Key('BaR_A12', {'pm25'}): 'BaR',
}
station_profile_data['aerosol']['realtime']['clap2-pm1'] = {
    RealtimeTranslator.Key('BaB_A12', {'pm1'}): 'BaB',
    RealtimeTranslator.Key('BaG_A12', {'pm1'}): 'BaG',
    RealtimeTranslator.Key('BaR_A12', {'pm1'}): 'BaR',
}
station_profile_data['aerosol']['editing']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A12'): 'BaB',
        Name(station, 'clean', 'BaG_A12'): 'BaG',
        Name(station, 'clean', 'BaR_A12'): 'BaR',
    }, send
)
station_profile_data['aerosol']['editing']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A12', {'pm10'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm10'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['editing']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A12', {'pm25'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm25'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['editing']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BaB_A12', {'pm1'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm1'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A12'): 'BaB',
        Name(station, 'clean', 'BaG_A12'): 'BaG',
        Name(station, 'clean', 'BaR_A12'): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A12', {'pm10'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm10'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A12', {'pm25'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm25'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['clean']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BaB_A12', {'pm1'}): 'BaB',
        Name(station, 'clean', 'BaG_A12', {'pm1'}): 'BaG',
        Name(station, 'clean', 'BaR_A12', {'pm1'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A12'): 'BaB',
        Name(station, 'avgh', 'BaG_A12'): 'BaG',
        Name(station, 'avgh', 'BaR_A12'): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A12', {'pm10'}): 'BaB',
        Name(station, 'avgh', 'BaG_A12', {'pm10'}): 'BaG',
        Name(station, 'avgh', 'BaR_A12', {'pm10'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A12', {'pm25'}): 'BaB',
        Name(station, 'avgh', 'BaG_A12', {'pm25'}): 'BaG',
        Name(station, 'avgh', 'BaR_A12', {'pm25'}): 'BaR',
    }, send
)
station_profile_data['aerosol']['avgh']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BaB_A12', {'pm1'}): 'BaB',
        Name(station, 'avgh', 'BaG_A12', {'pm1'}): 'BaG',
        Name(station, 'avgh', 'BaR_A12', {'pm1'}): 'BaR',
    }, send
)

station_profile_data['aerosol']['raw']['clapstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
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
station_profile_data['aerosol']['realtime']['clapstatus2'] = {
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
    RealtimeTranslator.Key('T1_A12'): 'Tsample',
    RealtimeTranslator.Key('T1_A12', {'pm10'}): 'Tsample',
    RealtimeTranslator.Key('T1_A12', {'pm1'}): 'Tsample',
    RealtimeTranslator.Key('T1_A12', {'pm25'}): 'Tsample',
    RealtimeTranslator.Key('T2_A12'): 'Tcase',
    RealtimeTranslator.Key('T2_A12', {'pm10'}): 'Tcase',
    RealtimeTranslator.Key('T2_A12', {'pm1'}): 'Tcase',
    RealtimeTranslator.Key('T2_A12', {'pm25'}): 'Tcase',
    RealtimeTranslator.Key('Fn_A12'): 'spot',
}


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


station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T_V51'): 'Tinlet', Name(station, 'raw', 'U_V51'): 'Uinlet',
        Name(station, 'raw', 'T1_XM1'): 'Tambient',
        Name(station, 'raw', 'U1_XM1'): 'Uambient',
        Name(station, 'raw', 'TD1_XM1'): 'TDambient',
        Name(station, 'raw', 'T1_XM2'): 'Tpwd',

        Name(station, 'raw', 'T_V11'): 'Tsample', Name(station, 'raw', 'U_V11'): 'Usample',
        Name(station, 'raw', 'T_V11', {'pm10'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm10'}): 'Usample',
        Name(station, 'raw', 'T_V11', {'pm1'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm1'}): 'Usample',
        Name(station, 'raw', 'T_V11', {'pm25'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm25'}): 'Usample',

        Name(station, 'raw', 'Tx_S11'): 'Tnephcell', Name(station, 'raw', 'Ux_S11'): 'Unephcell',
        Name(station, 'raw', 'Tx_S11', {'pm10'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm10'}): 'Unephcell',
        Name(station, 'raw', 'Tx_S11', {'pm1'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm1'}): 'Unephcell',
        Name(station, 'raw', 'Tx_S11', {'pm25'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm25'}): 'Unephcell',

        Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',

        Name(station, 'raw', 'T_V01'): 'Troom', Name(station, 'raw', 'U_V01'): 'Uroom',
    }, send
)
station_profile_data['aerosol']['realtime']['temperature'] = {
    RealtimeTranslator.Key('T_V51'): 'Tinlet', RealtimeTranslator.Key('U_V51'): 'Uinlet',
    RealtimeTranslator.Key('T1_XM1'): 'Tambient',
    RealtimeTranslator.Key('U1_XM1'): 'Uambient',
    RealtimeTranslator.Key('TD1_XM1'): 'TDambient',
    RealtimeTranslator.Key('T1_XM2'): 'Tpwd',

    RealtimeTranslator.Key('T_V11'): 'Tsample', RealtimeTranslator.Key('U_V11'): 'Usample',
    RealtimeTranslator.Key('T_V11', {'pm10'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm10'}): 'Usample',
    RealtimeTranslator.Key('T_V11', {'pm1'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm1'}): 'Usample',
    RealtimeTranslator.Key('T_V11', {'pm25'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm25'}): 'Usample',

    RealtimeTranslator.Key('Tx_S11'): 'Tnephcell', RealtimeTranslator.Key('Ux_S11'): 'Unephcell',
    RealtimeTranslator.Key('Tx_S11', {'pm10'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm10'}): 'Unephcell',
    RealtimeTranslator.Key('Tx_S11', {'pm1'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm1'}): 'Unephcell',
    RealtimeTranslator.Key('Tx_S11', {'pm25'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm25'}): 'Unephcell',

    RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',

    RealtimeTranslator.Key('T_V01'): 'Troom', RealtimeTranslator.Key('U_V01'): 'Uroom',
}


station_profile_data['aerosol']['raw']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'WI_XM1'): 'precipitation',
        Name(station, 'raw', 'WZ_XM2'): 'visibility',
        Name(station, 'raw', 'VA_XM1'): 'radiation',
        Name(station, 'raw', 'R_XM1'): 'radiation',
    }, send
)
station_profile_data['aerosol']['realtime']['clouds'] = {
    RealtimeTranslator.Key('WI_XM1'): 'precipitation',
    RealtimeTranslator.Key('WZ_XM2'): 'visibility',
    RealtimeTranslator.Key('VA_XM1'): 'radiation',
    RealtimeTranslator.Key('R_XM1'): 'radiation',
}
station_profile_data['aerosol']['editing']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'WI_XM1'): 'precipitation',
        Name(station, 'clean', 'WZ_XM2'): 'visibility',
        Name(station, 'clean', 'VA_XM1'): 'radiation',
        Name(station, 'clean', 'R_XM1'): 'radiation',
    }, send
)
station_profile_data['aerosol']['clean']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'WI_XM1'): 'precipitation',
        Name(station, 'clean', 'WZ_XM2'): 'visibility',
        Name(station, 'clean', 'VA_XM1'): 'radiation',
        Name(station, 'clean', 'R_XM1'): 'radiation',
    }, send
)
station_profile_data['aerosol']['avgh']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'WI_XM1'): 'precipitation',
        Name(station, 'avgh', 'WZ_XM2'): 'visibility',
        Name(station, 'avgh', 'VA_XM1'): 'radiation',
        Name(station, 'avgh', 'R_XM1'): 'radiation',
    }, send
)


station_profile_data['aerosol']['raw']['hurricane'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'WS_XM3'): 'WS',
        Name(station, 'raw', 'WD_XM3'): 'WD',
        Name(station, 'raw', 'P_S81'): 'P',
        Name(station, 'raw', 'T_S81'): 'T',
        Name(station, 'raw', 'U_S81'): 'U',
        Name(station, 'raw', 'Ipa_S81'): 'IBsa',
        Name(station, 'raw', 'Ipb_S81'): 'IBsb',
        Name(station, 'raw', 'Bs_S81'): 'Bs',
    }, send
)
station_profile_data['aerosol']['realtime']['hurricane'] = {
    RealtimeTranslator.Key('WS_XM3'): 'WS',
    RealtimeTranslator.Key('WD_XM3'): 'WD',
    RealtimeTranslator.Key('P_S81'): 'P',
    RealtimeTranslator.Key('U_S81'): 'U',
    RealtimeTranslator.Key('T_S81'): 'T',
    RealtimeTranslator.Key('Ipa_S81'): 'IBsa',
    RealtimeTranslator.Key('Ipb_S81'): 'IBsb',
    RealtimeTranslator.Key('Bs_S81'): 'Bs',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
