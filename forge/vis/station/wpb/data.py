import typing
from math import nan
from ..cpd3 import use_cpd3


if use_cpd3():
    from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'N_N71'): 'cnc',
            Name(station, 'raw', 'N_N61'): 'cnc',
            Name(station, 'raw', 'N_N72'): 'cnc2',
            Name(station, 'raw', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cnc'] = {
        RealtimeTranslator.Key('N_N71'): 'cnc',
        RealtimeTranslator.Key('N_N61'): 'cnc',
        RealtimeTranslator.Key('N_N72'): 'cnc2',
        RealtimeTranslator.Key('N_N62'): 'cnc2',
    }
    station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'N_N71'): 'cnc',
            Name(station, 'clean', 'N_N61'): 'cnc',
            Name(station, 'clean', 'N_N72'): 'cnc2',
            Name(station, 'clean', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'N_N71'): 'cnc',
            Name(station, 'clean', 'N_N61'): 'cnc',
            Name(station, 'clean', 'N_N72'): 'cnc2',
            Name(station, 'clean', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'N_N71'): 'cnc',
            Name(station, 'avgh', 'N_N61'): 'cnc',
            Name(station, 'avgh', 'N_N72'): 'cnc2',
            Name(station, 'avgh', 'N_N62'): 'cnc2',
        }, send
    )

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
            Name(station, 'raw', 'T6_N72'): 'Tpcb',
            Name(station, 'raw', 'T7_N72'): 'Tcabinet',
            Name(station, 'raw', 'Q_N72'): 'Qsample',
            Name(station, 'raw', 'P_N72'): 'Psample',
            Name(station, 'raw', 'Pd_N72'): 'PDorifice',
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
    station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
        RealtimeTranslator.Key('Tu_N72'): 'Tinlet',
        RealtimeTranslator.Key('TDu_N72'): 'TDinlet',
        RealtimeTranslator.Key('Uu_N72'): 'Uinlet',
        RealtimeTranslator.Key('T1_N72'): 'Tconditioner',
        RealtimeTranslator.Key('T2_N72'): 'Tinitiator',
        RealtimeTranslator.Key('T3_N72'): 'Tmoderator',
        RealtimeTranslator.Key('T4_N72'): 'Toptics',
        RealtimeTranslator.Key('T5_N72'): 'Theatsink',
        RealtimeTranslator.Key('T6_N72'): 'Tpcb',
        RealtimeTranslator.Key('T7_N72'): 'Tcabinet',
        RealtimeTranslator.Key('Q_N72'): 'Qsample',
        RealtimeTranslator.Key('P_N72'): 'Psample',
        RealtimeTranslator.Key('Pd_N72'): 'PDorifice',
    }


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
    station_profile_data['aerosol']['raw']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A12', {'pm1'}): 'BaB',
            Name(station, 'raw', 'BaG_A12', {'pm1'}): 'BaG',
            Name(station, 'raw', 'BaR_A12', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A12', {'pm25'}): 'BaB',
            Name(station, 'raw', 'BaG_A12', {'pm25'}): 'BaG',
            Name(station, 'raw', 'BaR_A12', {'pm25'}): 'BaR',
        }, send
    )
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
            Name(station, 'clean', 'BaR_A12', {'pm1'}): 'BaR',
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
    station_profile_data['aerosol']['realtime']['clap2-pm1'] = {
        RealtimeTranslator.Key('BaB_A12', {'pm1'}): 'BaB',
        RealtimeTranslator.Key('BaG_A12', {'pm1'}): 'BaG',
        RealtimeTranslator.Key('BaR_A12', {'pm1'}): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['clap2-pm25'] = {
        RealtimeTranslator.Key('BaB_A12', {'pm25'}): 'BaB',
        RealtimeTranslator.Key('BaG_A12', {'pm25'}): 'BaG',
        RealtimeTranslator.Key('BaR_A12', {'pm25'}): 'BaR',
    }

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
        }, send
    )
    station_profile_data['aerosol']['realtime']['flow'] = {
        RealtimeTranslator.Key('Q_Q11'): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm10'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm1'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm25'}): 'sample',
    }


    station_profile_data['aerosol']['raw']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'WS_XM1'): 'WS',
            Name(station, 'raw', 'WD_XM1'): 'WD',
        }, send
    )
    station_profile_data['aerosol']['realtime']['wind'] = {
        RealtimeTranslator.Key('WS_XM1'): 'WS',
        RealtimeTranslator.Key('WD_XM1'): 'WD',
    }
    station_profile_data['aerosol']['clean']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'WS_XM1'): 'WS',
            Name(station, 'clean', 'WD_XM1'): 'WD',
        }, send
    )
    station_profile_data['aerosol']['avgh']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'WS_XM1'): 'WS',
            Name(station, 'avgh', 'WD_XM1'): 'WD',
        }, send
    )
    station_profile_data['aerosol']['editing']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'WS_XM1'): 'WS',
            Name(station, 'clean', 'WD_XM1'): 'WD',
        }, send
    )


    station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T_V21'): 'Track', Name(station, 'raw', 'U_V21'): 'Urack',

            Name(station, 'raw', 'T1_XM1'): 'Tambient',
            Name(station, 'raw', 'U1_XM1'): 'Uambient',
            Name(station, 'raw', 'TD1_XM1'): 'TDambient',

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
        }, send
    )
    station_profile_data['aerosol']['realtime']['temperature'] = {
        RealtimeTranslator.Key('T_V21'): 'Track', RealtimeTranslator.Key('U_V21'): 'Urack',

        RealtimeTranslator.Key('T1_XM1'): 'Tambient',
        RealtimeTranslator.Key('U1_XM1'): 'Uambient',
        RealtimeTranslator.Key('TD1_XM1'): 'TDambient',

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
    }


    station_profile_data['aerosol']['raw']['nephstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'CfG_S11'): 'CfG',
            Name(station, 'raw', 'CfG_S11', {'pm10'}): 'CfG',
            Name(station, 'raw', 'CfG_S11', {'pm1'}): 'CfG',
            Name(station, 'raw', 'CfG_S11', {'pm25'}): 'CfG',
        }, send
    )
    station_profile_data['aerosol']['realtime']['nephstatus'] = {
        RealtimeTranslator.Key('CfG_S11'): 'CfG',
        RealtimeTranslator.Key('CfG_S11', {'pm10'}): 'CfG',
        RealtimeTranslator.Key('CfG_S11', {'pm1'}): 'CfG',
        RealtimeTranslator.Key('CfG_S11', {'pm25'}): 'CfG',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_THREE_WAVELENGTHS, STANDARD_CUT_SIZE_SPLIT

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)


    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-cnc"] = DataRecord({
            "cnc": [Selection(variable_name="number_concentration",
                              require_tags={"cpc"}, exclude_tags={"secondary"})],
            "cnc2": [Selection(variable_name="number_concentration", instrument_id="N72"),
                     Selection(variable_name="number_concentration", instrument_id="N62")],
        })
    data_records["aerosol-realtime-cnc"] = RealtimeRecord({
        "cnc": [RealtimeSelection("N", variable_name="number_concentration",
                                  require_tags={"cpc"}, exclude_tags={"secondary"})],
        "cnc2": [RealtimeSelection("N", variable_name="number_concentration",
                                   instrument_id="N72"),
                 RealtimeSelection("N", variable_name="number_concentration",
                                   instrument_id="N62")],
    })

    data_records["aerosol-raw-cpcstatus"] = DataRecord({
        "Tinlet": [Selection(variable_name="inlet_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   Selection(variable_name="inlet_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "TDinlet": [Selection(variable_name="inlet_dewpoint", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="inlet_dewpoint", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Uinlet": [Selection(variable_name="inlet_humidity", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   Selection(variable_name="inlet_humidity", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tconditioner": [Selection(variable_name="conditioner_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                         Selection(variable_name="conditioner_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tinitiator": [Selection(variable_name="initiator_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                       Selection(variable_name="initiator_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tmoderator": [Selection(variable_name="moderator_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                       Selection(variable_name="moderator_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Toptics": [Selection(variable_name="optics_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="optics_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Theatsink": [Selection(variable_name="heatsink_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                      Selection(variable_name="heatsink_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tcase": [Selection(variable_name="case_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tboard": [Selection(variable_name="pcb_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   Selection(variable_name="board_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "TDgrowth": [Selection(variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="sample_flow", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Psample": [Selection(variable_name="pressure", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="pressure", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "PCTwick": [Selection(variable_name="wick_saturation", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Vpulse": [Selection(variable_name="pulse_height", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-cpcstatus"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "TDinlet": [RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Uinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tconditioner": [
            RealtimeSelection("Tconditioner", variable_name="conditioner_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
            RealtimeSelection("Tconditioner", variable_name="conditioner_temperature",
                              instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tinitiator": [
            RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
            RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tmoderator": [
            RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
            RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Theatsink": [
            RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
            RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tboard": [RealtimeSelection("Tpcb", variable_name="pcb_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   RealtimeSelection("Tboard", variable_name="board_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "TDgrowth": [
            RealtimeSelection("TDgrowth", variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Psample": [RealtimeSelection("P", variable_name="pressure", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("P", variable_name="pressure", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "PCTwick": [RealtimeSelection("PCTwick", variable_name="wick_saturation", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Vpulse": [RealtimeSelection("Vpulse", variable_name="pulse_height", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
    })

    data_records["aerosol-raw-cpcstatus2"] = DataRecord({
        "Tinlet": [Selection(variable_name="inlet_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                   Selection(variable_name="inlet_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "TDinlet": [Selection(variable_name="inlet_dewpoint", instrument_code="admagic200cpc", instrument_id="N72"),
                    Selection(variable_name="inlet_dewpoint", instrument_code="admagic250cpc", instrument_id="N72")],
        "Uinlet": [Selection(variable_name="inlet_humidity", instrument_code="admagic200cpc", instrument_id="N72"),
                   Selection(variable_name="inlet_humidity", instrument_code="admagic250cpc", instrument_id="N72")],
        "Tconditioner": [Selection(variable_name="conditioner_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                         Selection(variable_name="conditioner_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Tinitiator": [Selection(variable_name="initiator_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                       Selection(variable_name="initiator_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Tmoderator": [Selection(variable_name="moderator_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                       Selection(variable_name="moderator_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Toptics": [Selection(variable_name="optics_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                    Selection(variable_name="optics_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Theatsink": [Selection(variable_name="heatsink_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                      Selection(variable_name="heatsink_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Tcase": [Selection(variable_name="case_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Tboard": [Selection(variable_name="pcb_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                   Selection(variable_name="board_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "TDgrowth": [Selection(variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc", instrument_id="N72")],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="admagic200cpc", instrument_id="N72"),
                    Selection(variable_name="sample_flow", instrument_code="admagic250cpc", instrument_id="N72")],
        "Psample": [Selection(variable_name="pressure", instrument_code="admagic200cpc", instrument_id="N72"),
                    Selection(variable_name="pressure", instrument_code="admagic250cpc", instrument_id="N72")],
        "PCTwick": [Selection(variable_name="wick_saturation", instrument_code="admagic250cpc", instrument_id="N72")],
        "Vpulse": [Selection(variable_name="pulse_height", instrument_code="admagic250cpc", instrument_id="N72")],
    })
    data_records["aerosol-realtime-cpcstatus2"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                   RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "TDinlet": [RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic200cpc", instrument_id="N72"),
                    RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic250cpc", instrument_id="N72")],
        "Uinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic200cpc", instrument_id="N72"),
                   RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic250cpc", instrument_id="N72")],
        "Tconditioner": [
            RealtimeSelection("Tconditioner", variable_name="conditioner_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
            RealtimeSelection("Tconditioner", variable_name="conditioner_temperature",
                              instrument_code="admagic250cpc", instrument_id="N72")],
        "Tinitiator": [
            RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
            RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Tmoderator": [
            RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
            RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                    RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Theatsink": [
            RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
            RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "Tboard": [RealtimeSelection("Tpcb", variable_name="pcb_temperature", instrument_code="admagic200cpc", instrument_id="N72"),
                   RealtimeSelection("Tboard", variable_name="board_temperature", instrument_code="admagic250cpc", instrument_id="N72")],
        "TDgrowth": [
            RealtimeSelection("TDgrowth", variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc", instrument_id="N72")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic200cpc", instrument_id="N72"),
                    RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic250cpc", instrument_id="N72")],
        "Psample": [RealtimeSelection("P", variable_name="pressure", instrument_code="admagic200cpc", instrument_id="N72"),
                    RealtimeSelection("P", variable_name="pressure", instrument_code="admagic250cpc", instrument_id="N72")],
        "PCTwick": [RealtimeSelection("PCTwick", variable_name="wick_saturation", instrument_code="admagic250cpc", instrument_id="N72")],
        "Vpulse": [RealtimeSelection("Vpulse", variable_name="pulse_height", instrument_code="admagic250cpc", instrument_id="N72")],
    })

    for archive in ("raw", "editing", "clean", "avgh"):
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
            data_records[f"aerosol-{archive}-clap2-{record}"] = DataRecord(dict([
                (f"Ba{code}", [Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                         instrument_id="A12", exclude_tags={"aethalometer", "thermomaap"})])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ]))
    data_records["aerosol-raw-clapstatus2"] = DataRecord({
        "IrG": [Selection(variable_name="transmittance", wavelength=(500, 600),
                          instrument_code="clap", instrument_id="A12"),
                Selection(variable_name="transmittance", wavelength=(500, 600),
                          instrument_code="bmitap", instrument_id="A12")],
        "IfG": [Selection(variable_name="reference_intensity", wavelength=(500, 600),
                          instrument_code="clap", instrument_id="A12"),
                Selection(variable_name="reference_intensity", wavelength=(500, 600),
                          instrument_code="bmitap", instrument_id="A12")],
        "IpG": [Selection(variable_name="sample_intensity", wavelength=(500, 600),
                          instrument_code="clap", instrument_id="A12"),
                Selection(variable_name="sample_intensity", wavelength=(500, 600),
                          instrument_code="bmitap", instrument_id="A12")],
        "Q": [Selection(variable_name="sample_flow",
                        instrument_code="clap", instrument_id="A12"),
              Selection(variable_name="sample_flow",
                        instrument_code="bmitap", instrument_id="A12")],
        "Tsample": [Selection(variable_name="sample_temperature",
                              instrument_code="clap", instrument_id="A12"),
                    Selection(variable_name="sample_temperature",
                              instrument_code="bmitap", instrument_id="A12")],
        "Tcase": [Selection(variable_name="case_temperature",
                            instrument_code="clap", instrument_id="A12"),
                  Selection(variable_name="case_temperature",
                            instrument_code="bmitap", instrument_id="A12")],
        "spot": [Selection(variable_name="spot_number",
                           instrument_code="clap", instrument_id="A12",
                           variable_type=Selection.VariableType.State),
                 Selection(variable_name="spot_number",
                           instrument_code="bmitap", instrument_id="A12",
                           variable_type=Selection.VariableType.State)],
    }, hold_fields={"spot"})
    data_records["aerosol-realtime-clapstatus"] = RealtimeRecord({
        "IrG": [RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600),
                                  instrument_code="clap", instrument_id="A12"),
                RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600),
                                  instrument_code="bmitap", instrument_id="A12")],
        "IfG": [RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600),
                                  instrument_code="clap", instrument_id="A12"),
                RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600),
                                  instrument_code="bmitap", instrument_id="A12")],
        "IpG": [RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600),
                                  instrument_code="clap", instrument_id="A12"),
                RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600),
                                  instrument_code="bmitap", instrument_id="A12")],
        "Q": [RealtimeSelection("Q", variable_name="sample_flow",
                                instrument_code="clap", instrument_id="A12"),
              RealtimeSelection("Q", variable_name="sample_flow",
                                instrument_code="bmitap", instrument_id="A12")],
        "Tsample": [RealtimeSelection("Tsample", variable_name="sample_temperature",
                                      instrument_code="clap", instrument_id="A12"),
                    RealtimeSelection("Tsample", variable_name="sample_temperature",
                                      instrument_code="bmitap", instrument_id="A12")],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature",
                                    instrument_code="clap", instrument_id="A12"),
                  RealtimeSelection("Tcase", variable_name="case_temperature",
                                    instrument_code="bmitap", instrument_id="A12")],
        "spot": [RealtimeSelection("Fn", variable_name="spot_number",
                                   instrument_code="clap", instrument_id="A12",
                                   variable_type=Selection.VariableType.State),
                 RealtimeSelection("Fn", variable_name="spot_number",
                                   instrument_code="bmitap", instrument_id="A12",
                                   variable_type=Selection.VariableType.State)],
    }, hold_fields={"spot"})


    data_records["aerosol-raw-temperature"] = DataRecord({
        "Track": [Selection(variable_id="T_V21")], "Urack": [Selection(variable_id="U_V21")],
        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
        "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],

        "Tsample": [Selection(variable_id="T_V11")], "Usample": [Selection(variable_id="U_V11")],

        "Tnephcell": [Selection(variable_name="cell_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephcell": [Selection(variable_name="cell_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-temperature"] = RealtimeRecord({
        "Track": [RealtimeSelection("T_V21", variable_id="T_V21")], "Urack": [RealtimeSelection("U_V21", variable_id="U_V21")],
        "Tambient": [RealtimeSelection("Tambient", variable_id="T1", instrument_id="XM1")],
        "Uambient": [RealtimeSelection("Uambient", variable_id="U1", instrument_id="XM1")],
        "TDambient": [RealtimeSelection("TDambient", variable_id="TD1", instrument_id="XM1")],

        "Tsample": [RealtimeSelection("T_V11", variable_id="T_V11")], "Usample": [RealtimeSelection("U_V11", variable_id="U_V11")],

        "Tnephcell": [RealtimeSelection("Tcell", variable_name="cell_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephcell": [RealtimeSelection("Ucell", variable_name="cell_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
