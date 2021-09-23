import typing
from ..cpd3 import DataStream, DataReader, EditedReader, ContaminationReader, EditedContaminationReader, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'F1_N23'),
        Name(station, 'raw', 'F1_S11'),
        Name(station, 'raw', 'F1_A11'),
    }, send
)
station_profile_data['aerosol']['clean']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'F1_N23'),
        Name(station, 'clean', 'F1_S11'),
        Name(station, 'clean', 'F1_A11'),
    }, send
)
station_profile_data['aerosol']['avgh']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'F1_N23'),
        Name(station, 'avgh', 'F1_S11'),
        Name(station, 'avgh', 'F1_A11'),
    }, send
)
station_profile_data['aerosol']['editing']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedContaminationReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'F1_N23'),
        Name(station, 'clean', 'F1_S11'),
        Name(station, 'clean', 'F1_A11'),
    }, send
)

station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'N_N23'): 'cnc',
    }, send
)
station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'N_N23'): 'cnc',
    }, send
)
station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'N_N23'): 'cnc',
    }, send
)
station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'N_N23'): 'cnc',
    }, send
)


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


station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T_Q11'): 'Tsample',
        Name(station, 'raw', 'T_Q11', {'pm10'}): 'Tsample',
        Name(station, 'raw', 'T_Q11', {'pm1'}): 'Tsample',
        Name(station, 'raw', 'T_Q11', {'pm25'}): 'Tsample',
        
        Name(station, 'raw', 'T_Q12'): 'Tdilution',
        Name(station, 'raw', 'T_Q12', {'pm10'}): 'Tdilution',
        Name(station, 'raw', 'T_Q12', {'pm1'}): 'Tdilution',
        Name(station, 'raw', 'T_Q12', {'pm25'}): 'Tdilution',

        Name(station, 'raw', 'Tu_S11'): 'Tnephinlet', Name(station, 'raw', 'Uu_S11'): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm10'}): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm1'}): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm25'}): 'Unephinlet',

        Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',
    }, send
)

station_profile_data['aerosol']['raw']['pressure'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_Q12'): 'dilution',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11'): 'neph',
        Name(station, 'raw', 'P_Q11'): 'sample',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm10'}): 'neph',
        Name(station, 'raw', 'P_Q11', {'pm10'}): 'sample',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm25'}): 'neph',
        Name(station, 'raw', 'P_Q11', {'pm25'}): 'sample',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
        Name(station, 'raw', 'P_Q11', {'pm1'}): 'sample',
    }, send
)


station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_N23'): 'Tsaturator',
        Name(station, 'raw', 'T2_N23'): 'Tcondenser',
        Name(station, 'raw', 'T3_N23'): 'Toptics',
        Name(station, 'raw', 'T4_N23'): 'Tcabinet',
        Name(station, 'raw', 'Q_N23'): 'Qsample',
        Name(station, 'raw', 'Qu_N23'): 'Qinlet',
        Name(station, 'raw', 'P_N23'): 'Psample',
        Name(station, 'raw', 'Pd1_N23'): 'PDnozzle',
        Name(station, 'raw', 'Pd2_N23'): 'ODorifice',
        Name(station, 'raw', 'A_N23'): 'Alaser',
    }, send
)


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
