import typing
from ..cpd3 import DataStream, DataReader, EditedReader, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)

station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Q_Q71'): 'Qsample',
        Name(station, 'raw', 'Q_Q72'): 'Qdrier',
        Name(station, 'raw', 'T1_N71'): 'Tsaturator',
        Name(station, 'raw', 'T2_N71'): 'Tcondenser',
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

station_profile_data['aerosol']['raw']['aethalometerstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Q_A81'): 'Q',
    }, send
)

station_profile_data['aerosol']['raw']['tca'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'X1_M81'): 'X',
    }, send
)
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


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
