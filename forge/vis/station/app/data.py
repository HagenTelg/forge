import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


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
        Name(station, 'raw', 'Vl_S12'): 'Vl',
        Name(station, 'raw', 'Vl_S12', {'pm10'}): 'Vl',
        Name(station, 'raw', 'Vl_S12', {'pm1'}): 'Vl',
        Name(station, 'raw', 'Vl_S12', {'pm25'}): 'Vl',
        Name(station, 'raw', 'Al_S12'): 'Al',
        Name(station, 'raw', 'Al_S12', {'pm10'}): 'Al',
        Name(station, 'raw', 'Al_S12', {'pm1'}): 'Al',
        Name(station, 'raw', 'Al_S12', {'pm25'}): 'Al',
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
    RealtimeTranslator.Key('Vl_S12'): 'Vl',
    RealtimeTranslator.Key('Vl_S12', {'pm10'}): 'Vl',
    RealtimeTranslator.Key('Vl_S12', {'pm1'}): 'Vl',
    RealtimeTranslator.Key('Vl_S12', {'pm25'}): 'Vl',
    RealtimeTranslator.Key('Al_S12'): 'Al',
    RealtimeTranslator.Key('Al_S12', {'pm10'}): 'Al',
    RealtimeTranslator.Key('Al_S12', {'pm1'}): 'Al',
    RealtimeTranslator.Key('Al_S12', {'pm25'}): 'Al',
}


station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T_V51'): 'Tinlet', Name(station, 'raw', 'U_V51'): 'Uinlet',
        Name(station, 'raw', 'T_V01'): 'Taux', Name(station, 'raw', 'U_V01'): 'Uaux',
        Name(station, 'raw', 'T1_XM1'): 'Tambient',
        Name(station, 'raw', 'U1_XM1'): 'Uambient',
        Name(station, 'raw', 'TD1_XM1'): 'TDambient',

        Name(station, 'raw', 'T_V11'): 'Tsample', Name(station, 'raw', 'U_V11'): 'Usample',
        Name(station, 'raw', 'T_V11', {'pm10'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm10'}): 'Usample',
        Name(station, 'raw', 'T_V11', {'pm1'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm1'}): 'Usample',
        Name(station, 'raw', 'T_V11', {'pm25'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm25'}): 'Usample',

        Name(station, 'raw', 'T_V12'): 'Toutlet', Name(station, 'raw', 'U_V12'): 'Uoutlet',
        Name(station, 'raw', 'T_V12', {'pm10'}): 'Toutlet', Name(station, 'raw', 'U_V12', {'pm10'}): 'Uoutlet',
        Name(station, 'raw', 'T_V12', {'pm1'}): 'Toutlet', Name(station, 'raw', 'U_V12', {'pm1'}): 'Uoutlet',
        Name(station, 'raw', 'T_V12', {'pm25'}): 'Toutlet', Name(station, 'raw', 'U_V12', {'pm25'}): 'Uoutlet',

        Name(station, 'raw', 'Tu_S11'): 'Tnephinlet', Name(station, 'raw', 'Uu_S11'): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm10'}): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm1'}): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm25'}): 'Unephinlet',

        Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',

        Name(station, 'raw', 'Tu_S12'): 'Tnephinlet2', Name(station, 'raw', 'Uu_S12'): 'Unephinlet2',
        Name(station, 'raw', 'Tu_S12', {'pm10'}): 'Tnephinlet2', Name(station, 'raw', 'Uu_S12', {'pm10'}): 'Unephinlet2',
        Name(station, 'raw', 'Tu_S12', {'pm1'}): 'Tnephinlet2', Name(station, 'raw', 'Uu_S12', {'pm1'}): 'Unephinlet2',
        Name(station, 'raw', 'Tu_S12', {'pm25'}): 'Tnephinlet2', Name(station, 'raw', 'Uu_S12', {'pm25'}): 'Unephinlet2',

        Name(station, 'raw', 'T_S12'): 'Tneph2', Name(station, 'raw', 'U_S12'): 'Uneph2',
        Name(station, 'raw', 'T_S12', {'pm10'}): 'Tneph2', Name(station, 'raw', 'U_S12', {'pm10'}): 'Uneph2',
        Name(station, 'raw', 'T_S12', {'pm1'}): 'Tneph2', Name(station, 'raw', 'U_S12', {'pm1'}): 'Uneph2',
        Name(station, 'raw', 'T_S12', {'pm25'}): 'Tneph2', Name(station, 'raw', 'U_S12', {'pm25'}): 'Uneph2',
    }, send
)
station_profile_data['aerosol']['realtime']['temperature'] = {
    RealtimeTranslator.Key('T_V51'): 'Tinlet', RealtimeTranslator.Key('U_V51'): 'Uinlet',
    RealtimeTranslator.Key('T_V01'): 'Taux', RealtimeTranslator.Key('U_V01'): 'Uaux',
    RealtimeTranslator.Key('T1_XM1'): 'Tambient',
    RealtimeTranslator.Key('U1_XM1'): 'Uambient',
    RealtimeTranslator.Key('TD1_XM1'): 'TDambient',

    RealtimeTranslator.Key('T_V11'): 'Tsample', RealtimeTranslator.Key('U_V11'): 'Usample',
    RealtimeTranslator.Key('T_V11', {'pm10'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm10'}): 'Usample',
    RealtimeTranslator.Key('T_V11', {'pm1'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm1'}): 'Usample',
    RealtimeTranslator.Key('T_V11', {'pm25'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm25'}): 'Usample',

    RealtimeTranslator.Key('T_V12'): 'Toutlet', RealtimeTranslator.Key('U_V12'): 'Uoutlet',
    RealtimeTranslator.Key('T_V12', {'pm10'}): 'Toutlet', RealtimeTranslator.Key('U_V12', {'pm10'}): 'Uoutlet',
    RealtimeTranslator.Key('T_V12', {'pm1'}): 'Toutlet', RealtimeTranslator.Key('U_V12', {'pm1'}): 'Uoutlet',
    RealtimeTranslator.Key('T_V12', {'pm25'}): 'Toutlet', RealtimeTranslator.Key('U_V12', {'pm25'}): 'Uoutlet',

    RealtimeTranslator.Key('Tu_S11'): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11'): 'Unephinlet',
    RealtimeTranslator.Key('Tu_S11', {'pm10'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm10'}): 'Unephinlet',
    RealtimeTranslator.Key('Tu_S11', {'pm1'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm1'}): 'Unephinlet',
    RealtimeTranslator.Key('Tu_S11', {'pm25'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm25'}): 'Unephinlet',

    RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',

    RealtimeTranslator.Key('Tu_S12'): 'Tnephinlet2', RealtimeTranslator.Key('Uu_S12'): 'Unephinlet2',
    RealtimeTranslator.Key('Tu_S12', {'pm10'}): 'Tnephinlet2', RealtimeTranslator.Key('Uu_S12', {'pm10'}): 'Unephinlet2',
    RealtimeTranslator.Key('Tu_S12', {'pm1'}): 'Tnephinlet2', RealtimeTranslator.Key('Uu_S12', {'pm1'}): 'Unephinlet2',
    RealtimeTranslator.Key('Tu_S12', {'pm25'}): 'Tnephinlet2', RealtimeTranslator.Key('Uu_S12', {'pm25'}): 'Unephinlet2',

    RealtimeTranslator.Key('T_S12'): 'Tneph2', RealtimeTranslator.Key('U_S12'): 'Uneph2',
    RealtimeTranslator.Key('T_S12', {'pm10'}): 'Tneph2', RealtimeTranslator.Key('U_S12', {'pm10'}): 'Uneph2',
    RealtimeTranslator.Key('T_S12', {'pm1'}): 'Tneph2', RealtimeTranslator.Key('U_S12', {'pm1'}): 'Uneph2',
    RealtimeTranslator.Key('T_S12', {'pm25'}): 'Tneph2', RealtimeTranslator.Key('U_S12', {'pm25'}): 'Uneph2',
}


station_profile_data['aerosol']['raw']['pressure'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_XM1'): 'ambient',
        Name(station, 'raw', 'Pd_P01'): 'pitot',
        Name(station, 'raw', 'Pd_P12'): 'vacuum',
        Name(station, 'raw', 'Pd_P12', {'pm10'}): 'vacuum',
        Name(station, 'raw', 'Pd_P12', {'pm1'}): 'vacuum',
        Name(station, 'raw', 'Pd_P12', {'pm25'}): 'vacuum',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm10'}): 'neph',
        Name(station, 'raw', 'P_S12', {'pm10'}): 'neph2',
        Name(station, 'raw', 'Pd_P11', {'pm10'}): 'impactor',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
        Name(station, 'raw', 'P_S12', {'pm1'}): 'neph2',
        Name(station, 'raw', 'Pd_P11', {'pm1'}): 'impactor',
    }, send
)
station_profile_data['aerosol']['realtime']['pressure'] = {
    RealtimeTranslator.Key('P_XM1'): 'ambient',
    RealtimeTranslator.Key('Pd_P01'): 'pitot',
    RealtimeTranslator.Key('Pd_P12'): 'vacuum',
    RealtimeTranslator.Key('Pd_P12', {'pm10'}): 'vacuum',
    RealtimeTranslator.Key('Pd_P12', {'pm1'}): 'vacuum',
    RealtimeTranslator.Key('Pd_P12', {'pm25'}): 'vacuum',
}
station_profile_data['aerosol']['realtime']['samplepressure-pm10'] = {
    RealtimeTranslator.Key('P_S11', {'pm10'}): 'neph',
    RealtimeTranslator.Key('P_S12', {'pm10'}): 'neph2',
    RealtimeTranslator.Key('Pd_P11', {'pm10'}): 'impactor',
}
station_profile_data['aerosol']['realtime']['samplepressure-pm1'] = {
    RealtimeTranslator.Key('P_S11', {'pm1'}): 'neph',
    RealtimeTranslator.Key('P_S12', {'pm1'}): 'neph2',
    RealtimeTranslator.Key('Pd_P11', {'pm1'}): 'impactor',
}


station_profile_data['aerosol']['raw']['humidograph-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BsG_S11', {'pm10'}): 'dry',
        Name(station, 'raw', 'BsG_S12', {'pm10'}): 'wet',
    }, send
)
station_profile_data['aerosol']['raw']['humidograph-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BsG_S11', {'pm1'}): 'dry',
        Name(station, 'raw', 'BsG_S12', {'pm1'}): 'wet',
    }, send
)
station_profile_data['aerosol']['realtime']['humidograph-pm10'] = {
    RealtimeTranslator.Key('BsG_S11', {'pm10'}): 'dry',
    RealtimeTranslator.Key('BsG_S12', {'pm10'}): 'wet',
}
station_profile_data['aerosol']['realtime']['humidograph-pm1'] = {
    RealtimeTranslator.Key('BsG_S11', {'pm1'}): 'dry',
    RealtimeTranslator.Key('BsG_S12', {'pm1'}): 'wet',
}
station_profile_data['aerosol']['clean']['humidograph-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BsG_S11', {'pm10'}): 'dry',
        Name(station, 'clean', 'BsG_S12', {'pm10'}): 'wet',
    }, send
)
station_profile_data['aerosol']['clean']['humidograph-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BsG_S11', {'pm1'}): 'dry',
        Name(station, 'clean', 'BsG_S12', {'pm1'}): 'wet',
    }, send
)
station_profile_data['aerosol']['avgh']['humidograph-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BsG_S11', {'pm10'}): 'dry',
        Name(station, 'avgh', 'BsG_S12', {'pm10'}): 'wet',
    }, send
)
station_profile_data['aerosol']['avgh']['humidograph-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BsG_S11', {'pm1'}): 'dry',
        Name(station, 'avgh', 'BsG_S12', {'pm1'}): 'wet',
    }, send
)


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
