import typing
from ..cpd3 import use_cpd3


if use_cpd3("alt"):
    from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)

    station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'N_N61'): 'cnc',
            Name(station, 'raw', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'N_N61'): 'cnc',
            Name(station, 'clean', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'N_N61'): 'cnc',
            Name(station, 'clean', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'N_N61'): 'cnc',
            Name(station, 'avgh', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cnc'] = {
        RealtimeTranslator.Key('N_N61'): 'cnc',
        RealtimeTranslator.Key('N_N62'): 'cnc2',
    }

    station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_N61'): 'Tsaturator',
            Name(station, 'raw', 'T2_N61'): 'Tcondenser',
            Name(station, 'raw', 'T3_N61'): 'Toptics',
            Name(station, 'raw', 'T4_N61'): 'Tcabinet',
            Name(station, 'raw', 'Q_N61'): 'Qsample',
            Name(station, 'raw', 'Qu_N61'): 'Qinlet',
        }, send
    )

    station_profile_data['aerosol']['realtime']['cpcstatus'] = {
        RealtimeTranslator.Key('T1_N61'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N61'): 'Tcondenser',
        RealtimeTranslator.Key('T3_N61'): 'Toptics',
        RealtimeTranslator.Key('T4_N61'): 'Tcabinet',
        RealtimeTranslator.Key('Q_N61'): 'Qsample',
        RealtimeTranslator.Key('Qu_N61'): 'Qinlet',
    }

    station_profile_data['aerosol']['raw']['cpcstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_N62'): 'Tsaturator',
            Name(station, 'raw', 'T2_N62'): 'Tcondenser',
            Name(station, 'raw', 'T3_N62'): 'Toptics',
            Name(station, 'raw', 'T4_N62'): 'Tcabinet',
            Name(station, 'raw', 'Q_N62'): 'Qsample',
            Name(station, 'raw', 'Qu_N62'): 'Qinlet',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
        RealtimeTranslator.Key('T1_N62'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N62'): 'Tcondenser',
        RealtimeTranslator.Key('T3_N62'): 'Toptics',
        RealtimeTranslator.Key('T4_N62'): 'Tcabinet',
        RealtimeTranslator.Key('Q_N62'): 'Qsample',
        RealtimeTranslator.Key('Qu_N62'): 'Qinlet',
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


    station_profile_data['aerosol']['raw']['psap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A13'): 'BaB',
            Name(station, 'raw', 'BaG_A13'): 'BaG',
            Name(station, 'raw', 'BaR_A13'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['psap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A13', {'pm10'}): 'BaB',
            Name(station, 'raw', 'BaG_A13', {'pm10'}): 'BaG',
            Name(station, 'raw', 'BaR_A13', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['psap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A13', {'pm25'}): 'BaB',
            Name(station, 'raw', 'BaG_A13', {'pm25'}): 'BaG',
            Name(station, 'raw', 'BaR_A13', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['psap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A13', {'pm1'}): 'BaB',
            Name(station, 'raw', 'BaG_A13', {'pm1'}): 'BaG',
            Name(station, 'raw', 'BaR_A13', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['psap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A13'): 'BaB',
            Name(station, 'clean', 'BaG_A13'): 'BaG',
            Name(station, 'clean', 'BaR_A13'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['psap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A13', {'pm10'}): 'BaB',
            Name(station, 'clean', 'BaG_A13', {'pm10'}): 'BaG',
            Name(station, 'clean', 'BaR_A13', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['psap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A13', {'pm25'}): 'BaB',
            Name(station, 'clean', 'BaG_A13', {'pm25'}): 'BaG',
            Name(station, 'clean', 'BaR_A13', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['psap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A13', {'pm1'}): 'BaB',
            Name(station, 'clean', 'BaG_A13', {'pm1'}): 'BaG',
            Name(station, 'clean', 'BaR_A13', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['psap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A13'): 'BaB',
            Name(station, 'clean', 'BaG_A13'): 'BaG',
            Name(station, 'clean', 'BaR_A13'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['psap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A13', {'pm10'}): 'BaB',
            Name(station, 'clean', 'BaG_A13', {'pm10'}): 'BaG',
            Name(station, 'clean', 'BaR_A13', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['psap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A13', {'pm25'}): 'BaB',
            Name(station, 'clean', 'BaG_A13', {'pm25'}): 'BaG',
            Name(station, 'clean', 'BaR_A13', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['psap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A13', {'pm1'}): 'BaB',
            Name(station, 'clean', 'BaG_A13', {'pm1'}): 'BaG',
            Name(station, 'clean', 'BaR_A13', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['psap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A13'): 'BaB',
            Name(station, 'avgh', 'BaG_A13'): 'BaG',
            Name(station, 'avgh', 'BaR_A13'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['psap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A13', {'pm10'}): 'BaB',
            Name(station, 'avgh', 'BaG_A13', {'pm10'}): 'BaG',
            Name(station, 'avgh', 'BaR_A13', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['psap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A13', {'pm25'}): 'BaB',
            Name(station, 'avgh', 'BaG_A13', {'pm25'}): 'BaG',
            Name(station, 'avgh', 'BaR_A13', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['psap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A13', {'pm1'}): 'BaB',
            Name(station, 'avgh', 'BaG_A13', {'pm1'}): 'BaG',
            Name(station, 'avgh', 'BaR_A13', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['realtime']['psap-whole'] = {
        RealtimeTranslator.Key('BaB_A13'): 'BaB',
        RealtimeTranslator.Key('BaG_A13'): 'BaG',
        RealtimeTranslator.Key('BaR_A13'): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['psap-pm10'] = {
        RealtimeTranslator.Key('BaB_A13', {'pm10'}): 'BaB',
        RealtimeTranslator.Key('BaG_A13', {'pm10'}): 'BaG',
        RealtimeTranslator.Key('BaR_A13', {'pm10'}): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['psap-pm25'] = {
        RealtimeTranslator.Key('BaB_A13', {'pm25'}): 'BaB',
        RealtimeTranslator.Key('BaG_A13', {'pm25'}): 'BaG',
        RealtimeTranslator.Key('BaR_A13', {'pm25'}): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['psap-pm1'] = {
        RealtimeTranslator.Key('BaB_A13', {'pm1'}): 'BaB',
        RealtimeTranslator.Key('BaG_A13', {'pm1'}): 'BaG',
        RealtimeTranslator.Key('BaR_A13', {'pm1'}): 'BaR',
    }

    station_profile_data['aerosol']['raw']['psapstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'IrG_A13'): 'IrG',
            Name(station, 'raw', 'IrG_A13', {'pm10'}): 'IrG',
            Name(station, 'raw', 'IrG_A13', {'pm1'}): 'IrG',
            Name(station, 'raw', 'IrG_A13', {'pm25'}): 'IrG',
            Name(station, 'raw', 'IfG_A13'): 'IfG',
            Name(station, 'raw', 'IfG_A13', {'pm10'}): 'IfG',
            Name(station, 'raw', 'IfG_A13', {'pm1'}): 'IfG',
            Name(station, 'raw', 'IfG_A13', {'pm25'}): 'IfG',
            Name(station, 'raw', 'IpG_A13'): 'IpG',
            Name(station, 'raw', 'IpG_A13', {'pm10'}): 'IpG',
            Name(station, 'raw', 'IpG_A13', {'pm1'}): 'IpG',
            Name(station, 'raw', 'IpG_A13', {'pm25'}): 'IpG',
            Name(station, 'raw', 'Q_A13'): 'Q',
            Name(station, 'raw', 'Q_A13', {'pm10'}): 'Q',
            Name(station, 'raw', 'Q_A13', {'pm1'}): 'Q',
            Name(station, 'raw', 'Q_A13', {'pm25'}): 'Q',
        }, send
    )
    station_profile_data['aerosol']['realtime']['psapstatus'] = {
        RealtimeTranslator.Key('IrG_A13'): 'IrG',
        RealtimeTranslator.Key('IrG_A13', {'pm10'}): 'IrG',
        RealtimeTranslator.Key('IrG_A13', {'pm1'}): 'IrG',
        RealtimeTranslator.Key('IrG_A13', {'pm25'}): 'IrG',
        RealtimeTranslator.Key('IfG_A13'): 'IfG',
        RealtimeTranslator.Key('IfG_A13', {'pm10'}): 'IfG',
        RealtimeTranslator.Key('IfG_A13', {'pm1'}): 'IfG',
        RealtimeTranslator.Key('IfG_A13', {'pm25'}): 'IfG',
        RealtimeTranslator.Key('IpG_A13'): 'IpG',
        RealtimeTranslator.Key('IpG_A13', {'pm10'}): 'IpG',
        RealtimeTranslator.Key('IpG_A13', {'pm1'}): 'IpG',
        RealtimeTranslator.Key('IpG_A13', {'pm25'}): 'IpG',
        RealtimeTranslator.Key('Q_A13'): 'Q',
        RealtimeTranslator.Key('Q_A13', {'pm10'}): 'Q',
        RealtimeTranslator.Key('Q_A13', {'pm1'}): 'Q',
        RealtimeTranslator.Key('Q_A13', {'pm25'}): 'Q',
    }

    station_profile_data['aerosol']['raw']['ae33'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, dict(
            [(Name(station, 'raw', f'Ba{i+1}_A82'), f'Ba{i+1}') for i in range(7)] +
            [(Name(station, 'raw', f'X{i+1}_A82'), f'X{i+1}') for i in range(7)] +
            [(Name(station, 'raw', f'ZFACTOR{i+1}_A82'), f'CF{i+1}') for i in range(7)] +
            [(Name(station, 'raw', f'Ir{i+1}_A82'), f'Ir{i+1}') for i in range(7)]
        ), send
    )
    station_profile_data['aerosol']['editing']['ae33'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
            [(Name(station, 'clean', f'Ba{i + 1}_A82'), f'Ba{i + 1}') for i in range(7)] +
            [(Name(station, 'clean', f'X{i + 1}_A82'), f'X{i + 1}') for i in range(7)] +
            [(Name(station, 'clean', f'ZFACTOR{i + 1}_A82'), f'CF{i + 1}') for i in range(7)] +
            [(Name(station, 'clean', f'Ir{i + 1}_A82'), f'Ir{i + 1}') for i in range(7)]
        ), send
    )
    station_profile_data['aerosol']['clean']['ae33'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, dict(
            [(Name(station, 'clean', f'Ba{i+1}_A82'), f'Ba{i+1}') for i in range(7)] +
            [(Name(station, 'clean', f'X{i+1}_A82'), f'X{i+1}') for i in range(7)] +
            [(Name(station, 'clean', f'ZFACTOR{i+1}_A82'), f'CF{i+1}') for i in range(7)] +
            [(Name(station, 'clean', f'Ir{i+1}_A82'), f'Ir{i+1}') for i in range(7)]
        ), send
    )
    station_profile_data['aerosol']['avgh']['ae33'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, dict(
            [(Name(station, 'avgh', f'Ba{i+1}_A82'), f'Ba{i+1}') for i in range(7)] +
            [(Name(station, 'avgh', f'X{i+1}_A82'), f'X{i+1}') for i in range(7)] +
            [(Name(station, 'avgh', f'ZFACTOR{i+1}_A82'), f'CF{i+1}') for i in range(7)] +
            [(Name(station, 'avgh', f'Ir{i+1}_A82'), f'Ir{i+1}') for i in range(7)]
        ), send
    )
    station_profile_data['aerosol']['realtime']['ae33'] = dict(
        [(RealtimeTranslator.Key(f'Ba{i + 1}_A82'), f'Ba{i + 1}') for i in range(7)] +
        [(RealtimeTranslator.Key(f'X{i + 1}_A82'), f'X{i + 1}') for i in range(7)] +
        [(RealtimeTranslator.Key(f'ZFACTOR{i + 1}_A82'), f'CF{i + 1}') for i in range(7)] +
        [(RealtimeTranslator.Key(f'Ir{i + 1}_A82'), f'Ir{i + 1}') for i in range(7)]
    )

    station_profile_data['aerosol']['raw']['ae33status'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_A82'): 'Tcontroller',
            Name(station, 'raw', 'T2_A82'): 'Tsupply',
            Name(station, 'raw', 'T3_A82'): 'Tled',
        }, send
    )
    station_profile_data['aerosol']['realtime']['ae33status'] = {
        RealtimeTranslator.Key('T1_A82'): 'Tcontroller',
        RealtimeTranslator.Key('T2_A82'): 'Tsupply',
        RealtimeTranslator.Key('T3_A82'): 'Tled',
        RealtimeTranslator.Key('Q1_A82'): 'Q1',
        RealtimeTranslator.Key('Q2_A82'): 'Q2',
    }


    station_profile_data['aerosol']['raw']['ae31'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, dict(
            [(Name(station, 'raw', f'Ba{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
            [(Name(station, 'raw', f'X{i+1}_A81'), f'X{i+1}') for i in range(7)] +
            [(Name(station, 'raw', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)]
        ), send
    )
    station_profile_data['aerosol']['editing']['ae31'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
            [(Name(station, 'clean', f'Ba{i + 1}_A81'), f'Ba{i + 1}') for i in range(7)] +
            [(Name(station, 'clean', f'X{i + 1}_A81'), f'X{i + 1}') for i in range(7)] +
            [(Name(station, 'clean', f'Ir{i + 1}_A81'), f'Ir{i + 1}') for i in range(7)]
        ), send
    )
    station_profile_data['aerosol']['clean']['ae31'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, dict(
            [(Name(station, 'clean', f'Ba{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
            [(Name(station, 'clean', f'X{i+1}_A81'), f'X{i+1}') for i in range(7)] +
            [(Name(station, 'clean', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)]
        ), send
    )
    station_profile_data['aerosol']['avgh']['ae31'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, dict(
            [(Name(station, 'avgh', f'Ba{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
            [(Name(station, 'avgh', f'X{i+1}_A81'), f'X{i+1}') for i in range(7)] +
            [(Name(station, 'avgh', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)]
        ), send
    )
    station_profile_data['aerosol']['realtime']['ae31'] = dict(
        [(RealtimeTranslator.Key(f'Ba{i + 1}_A81'), f'Ba{i + 1}') for i in range(7)] +
        [(RealtimeTranslator.Key(f'X{i + 1}_A81'), f'X{i + 1}') for i in range(7)] +
        [(RealtimeTranslator.Key(f'Ir{i + 1}_A81'), f'Ir{i + 1}') for i in range(7)]
    )
    station_profile_data['aerosol']['raw']['ae31status'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Q_A81'): 'Q',
        }, send
    )


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
            "cnc2": [Selection(variable_name="number_concentration", instrument_id="N62")],
        })
    data_records["aerosol-realtime-cnc"] = RealtimeRecord({
        "cnc": [RealtimeSelection("N", variable_name="number_concentration",
                                  require_tags={"cpc"}, exclude_tags={"secondary"})],
        "cnc2": [RealtimeSelection("N", variable_name="number_concentration",
                                   instrument_id="N62")],
    })

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-aethalometer"] = DataRecord(dict(
            [(f"Ba{wl+1}", [Selection(variable_id="Ba", wavelength_number=wl,
                                      instrument_code="mageeae31")])
             for wl in range(7)] +
            [(f"X{wl+1}", [Selection(variable_name="equivalent_black_carbon", wavelength_number=wl,
                                     instrument_code="mageeae31")])
             for wl in range(7)] +
            [(f"Ir{wl+1}", [Selection(variable_id="Ir", wavelength_number=wl,
                                      instrument_code="mageeae31")])
             for wl in range(7)]
        ))
        data_records[f"aerosol-{archive}-ae33"] = DataRecord(dict(
            [(f"Ba{wl+1}", [Selection(variable_id="Ba", wavelength_number=wl,
                                      instrument_code="mageeae33")])
             for wl in range(7)] +
            [(f"X{wl+1}", [Selection(variable_name="equivalent_black_carbon", wavelength_number=wl,
                                     instrument_code="mageeae33")])
             for wl in range(7)] +
            [(f"Ir{wl+1}", [Selection(variable_id="Ir", wavelength_number=wl,
                                      instrument_code="mageeae33")])
             for wl in range(7)] +
            [(f"CF{wl+1}", [Selection(variable_name="correction_factor", wavelength_number=wl,
                                      instrument_code="mageeae33")])
             for wl in range(7)]
        ))
    data_records[f"aerosol-realtime-aethalometer"] = RealtimeRecord(dict(
        [(f"Ba{wl+1}", [RealtimeSelection(f"Ba{wl+1}", variable_id="Ba", wavelength_number=wl,
                                          instrument_code="mageeae31")])
         for wl in range(7)] +
        [(f"X{wl+1}", [RealtimeSelection(f"X{wl+1}", variable_name="equivalent_black_carbon", wavelength_number=wl,
                                         instrument_code="mageeae31")])
         for wl in range(7)] +
        [(f"Ir{wl+1}", [RealtimeSelection(f"Ir{wl+1}", variable_id="Ir", wavelength_number=wl,
                                          instrument_code="mageeae31")])
         for wl in range(7)] +
        [(f"CF{wl+1}", [RealtimeSelection(f"k{wl+1}",variable_name="correction_factor", wavelength_number=wl,
                                          instrument_code="mageeae31")])
         for wl in range(7)]
    ))
    data_records[f"aerosol-realtime-ae33"] = RealtimeRecord(dict(
        [(f"Ba{wl+1}", [RealtimeSelection(f"Ba{wl+1}", variable_id="Ba", wavelength_number=wl,
                                          instrument_code="mageeae33")])
         for wl in range(7)] +
        [(f"X{wl+1}", [RealtimeSelection(f"X{wl+1}", variable_name="equivalent_black_carbon", wavelength_number=wl,
                                         instrument_code="mageeae33")])
         for wl in range(7)] +
        [(f"Ir{wl+1}", [RealtimeSelection(f"Ir{wl+1}", variable_id="Ir", wavelength_number=wl,
                                          instrument_code="mageeae33")])
         for wl in range(7)] +
        [(f"CF{wl+1}", [RealtimeSelection(f"k{wl+1}",variable_name="correction_factor", wavelength_number=wl,
                                          instrument_code="mageeae33")])
         for wl in range(7)]
    ))
    data_records["aerosol-raw-aethalometerstatus"] = DataRecord({
        "Q": [Selection(variable_name="sample_flow", instrument_code="mageeae31")],
    })
    data_records["aerosol-realtime-aethalometerstatus"] = RealtimeRecord({
        "Q": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="mageeae31")],
    })
    data_records["aerosol-raw-ae33status"] = DataRecord({
        "Tcontroller": [Selection(variable_name="controller_temperature", instrument_code="mageeae33")],
        "Tsupply": [Selection(variable_name="supply_temperature", instrument_code="mageeae33")],
        "Tled": [Selection(variable_name="led_temperature", instrument_code="mageeae33")],
        "Q1": [Selection(variable_name="spot_one_flow", instrument_code="mageeae33")],
        "Q2": [Selection(variable_name="spot_two_flow", instrument_code="mageeae33")],
        "Q": [Selection(variable_name="sample_flow", instrument_code="mageeae33")],
    })
    data_records["aerosol-realtime-ae33status"] = RealtimeRecord({
        "Tcontroller": [RealtimeSelection("Tcontroller", variable_name="controller_temperature", instrument_code="mageeae33")],
        "Tsupply": [RealtimeSelection("Tsupply", variable_name="supply_temperature", instrument_code="mageeae33")],
        "Tled": [RealtimeSelection("Tled", variable_name="led_temperature", instrument_code="mageeae33")],
        "Q1": [RealtimeSelection("Q1", variable_name="spot_one_flow", instrument_code="mageeae33")],
        "Q2": [RealtimeSelection("Q2", variable_name="spot_two_flow", instrument_code="mageeae33")],
        "Q": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="mageeae33")],
    })

    for archive in ("raw", "editing", "clean", "avgh"):
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
            data_records[f"aerosol-{archive}-clap2-{record}"] = DataRecord(dict([
                (f"Ba{code}", [Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                         instrument_id="A12")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ]))
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
            data_records[f"aerosol-{archive}-psap-{record}"] = DataRecord(dict([
                (f"Ba{code}", [Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                         instrument_id="A13")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ]))
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        data_records[f"aerosol-realtime-clap2-{record}"] = RealtimeRecord(dict([
            (f"Ba{code}", [RealtimeSelection(f"Ba{code}", variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                             instrument_id="A12")])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))
        data_records[f"aerosol-realtime-psap-{record}"] = RealtimeRecord(dict([
            (f"Ba{code}", [RealtimeSelection(f"Ba{code}", variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                             instrument_id="A13")])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))
    data_records["aerosol-raw-clapstatus2"] = DataRecord({
        "IrG": [Selection(variable_name="transmittance", wavelength=(500, 600), instrument_id="A12")],
        "IfG": [Selection(variable_name="reference_intensity", wavelength=(500, 600), instrument_id="A12")],
        "IpG": [Selection(variable_name="sample_intensity", wavelength=(500, 600), instrument_id="A12")],
        "Q": [Selection(variable_name="sample_flow", instrument_id="A12")],
        "Tsample": [Selection(variable_name="sample_temperature", instrument_id="A12")],
        "Tcase": [Selection(variable_name="case_temperature", instrument_id="A12")],
        "spot": [Selection(variable_name="spot_number", instrument_id="A12", 
                           variable_type=Selection.VariableType.State)],
    }, hold_fields={"spot"})
    data_records["aerosol-realtime-clapstatus2"] = RealtimeRecord({
        "IrG": [RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600), instrument_id="A12")],
        "IfG": [RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600), instrument_id="A12")],
        "IpG": [RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600), instrument_id="A12")],
        "Q": [RealtimeSelection("Q", variable_name="sample_flow", instrument_id="A12")],
        "Tsample": [RealtimeSelection("Tsample", variable_name="sample_temperature", instrument_id="A12")],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature", instrument_id="A12")],
        "spot": [RealtimeSelection("Fn", variable_name="spot_number", instrument_id="A12", 
                                   variable_type=Selection.VariableType.State)],
    }, hold_fields={"spot"})
    data_records["aerosol-raw-psapstatus"] = DataRecord({
        "IrG": [Selection(variable_name="transmittance", wavelength=(500, 600), instrument_id="A13")],
        "IfG": [Selection(variable_name="reference_intensity", wavelength=(500, 600), instrument_id="A13")],
        "IpG": [Selection(variable_name="sample_intensity", wavelength=(500, 600), instrument_id="A13")],
        "Q": [Selection(variable_name="sample_flow", instrument_id="A13")],
    })
    data_records["aerosol-realtime-psapstatus"] = RealtimeRecord({
        "IrG": [RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600), instrument_id="A13")],
        "IfG": [RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600), instrument_id="A13")],
        "IpG": [RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600), instrument_id="A13")],
        "Q": [RealtimeSelection("Q", variable_name="sample_flow", instrument_id="A13")],
    })

    data_records["aerosol-raw-cpcstatus"] = DataRecord({
        "Tsaturator": [Selection(variable_name="saturator_temperature",
                                 instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Tcondenser": [Selection(variable_name="condenser_temperature",
                                 instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Toptics": [Selection(variable_name="optics_temperature",
                              instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Tcabinet": [Selection(variable_name="cabinet_temperature",
                               instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Qsample": [Selection(variable_name="sample_flow",
                              instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Qinlet": [Selection(variable_name="inlet_flow",
                             instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Psample": [Selection(variable_name="pressure",
                              instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "PDnozzle": [Selection(variable_name="nozzle_pressure_drop",
                               instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "PDorifice": [Selection(variable_name="orifice_pressure_drop",
                                instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Alaser": [Selection(variable_name="laser_current",
                             instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-cpcstatus"] = RealtimeRecord({
        "Tsaturator": [RealtimeSelection("Tsaturator", variable_name="saturator_temperature",
                                         instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Tcondenser": [RealtimeSelection("Tcondenser", variable_name="condenser_temperature",
                                         instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature",
                                      instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Tcabinet": [RealtimeSelection("Tcabinet", variable_name="cabinet_temperature",
                                       instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow",
                                      instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Qinlet": [RealtimeSelection("Qinlet", variable_name="inlet_flow",
                                     instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Psample": [RealtimeSelection("P", variable_name="pressure",
                                      instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "PDnozzle": [RealtimeSelection("PDnozzle", variable_name="nozzle_pressure_drop",
                                       instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "PDorifice": [RealtimeSelection("PDorifice", variable_name="orifice_pressure_drop",
                                        instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Alaser": [RealtimeSelection("Alaser", variable_name="laser_current",
                                     instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
    })
    data_records["aerosol-raw-cpcstatus2"] = DataRecord({
        "Tsaturator": [Selection(variable_name="saturator_temperature",
                                 instrument_code="tsi377xcpc", instrument_id="N62")],
        "Tcondenser": [Selection(variable_name="condenser_temperature",
                                 instrument_code="tsi377xcpc", instrument_id="N62")],
        "Toptics": [Selection(variable_name="optics_temperature",
                              instrument_code="tsi377xcpc", instrument_id="N62")],
        "Tcabinet": [Selection(variable_name="cabinet_temperature",
                               instrument_code="tsi377xcpc", instrument_id="N62")],
        "Qsample": [Selection(variable_name="sample_flow",
                              instrument_code="tsi377xcpc", instrument_id="N62")],
        "Qinlet": [Selection(variable_name="inlet_flow",
                             instrument_code="tsi377xcpc", instrument_id="N62")],
        "Psample": [Selection(variable_name="pressure",
                              instrument_code="tsi377xcpc", instrument_id="N62")],
        "PDnozzle": [Selection(variable_name="nozzle_pressure_drop",
                               instrument_code="tsi377xcpc", instrument_id="N62")],
        "PDorifice": [Selection(variable_name="orifice_pressure_drop",
                                instrument_code="tsi377xcpc", instrument_id="N62")],
        "Alaser": [Selection(variable_name="laser_current",
                             instrument_code="tsi377xcpc", instrument_id="N62")],
    })
    data_records["aerosol-realtime-cpcstatus2"] = RealtimeRecord({
        "Tsaturator": [RealtimeSelection("Tsaturator", variable_name="saturator_temperature",
                                         instrument_code="tsi377xcpc", instrument_id="N62")],
        "Tcondenser": [RealtimeSelection("Tcondenser", variable_name="condenser_temperature",
                                         instrument_code="tsi377xcpc", instrument_id="N62")],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature",
                                      instrument_code="tsi377xcpc", instrument_id="N62")],
        "Tcabinet": [RealtimeSelection("Tcabinet", variable_name="cabinet_temperature",
                                       instrument_code="tsi377xcpc", instrument_id="N62")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow",
                                      instrument_code="tsi377xcpc", instrument_id="N62")],
        "Qinlet": [RealtimeSelection("Qinlet", variable_name="inlet_flow",
                                     instrument_code="tsi377xcpc", instrument_id="N62")],
        "Psample": [RealtimeSelection("P", variable_name="pressure",
                                      instrument_code="tsi377xcpc", instrument_id="N62")],
        "PDnozzle": [RealtimeSelection("PDnozzle", variable_name="nozzle_pressure_drop",
                                       instrument_code="tsi377xcpc", instrument_id="N62")],
        "PDorifice": [RealtimeSelection("PDorifice", variable_name="orifice_pressure_drop",
                                        instrument_code="tsi377xcpc", instrument_id="N62")],
        "Alaser": [RealtimeSelection("Alaser", variable_name="laser_current",
                                     instrument_code="tsi377xcpc", instrument_id="N62")],
    })

    data_records["aerosol-raw-grimm"] = DataRecord({
        "Dp": [Selection(variable_id="Ns", instrument_code="grimm110xopc")],
        "dNdlogDp": [Selection(variable_id="Nn", instrument_code="grimm110xopc")],
        "dN": [Selection(variable_id="Nb", instrument_code="grimm110xopc")],
    })
    for archive in ("editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-grimm"] = DataRecord(dict([
            ("Dp", [Selection(variable_id="Ns", instrument_code="grimm110xopc")]),
            ("dNdlogDp", [Selection(variable_id="Nn", instrument_code="grimm110xopc")]),
            ("dN", [Selection(variable_id="Nb", instrument_code="grimm110xopc")]),
            ("N", [Selection(variable_id="N", instrument_code="grimm110xopc")])] + [
                (f"Bs{code}", [Selection(variable_id="Bs", wavelength=wavelength,
                                         instrument_code="grimm110xopc")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
        )
    data_records["aerosol-raw-grimmstatus"] = DataRecord({
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="grimm110xopc")],
    })
    data_records["aerosol-realtime-grimmstatus"] = RealtimeRecord({
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="grimm110xopc")],
    })

    data_records["aerosol-raw-smps"] = DataRecord({
        "Dp": [Selection(variable_id="Ns", instrument_id="N12")],
        "dNdlogDp": [Selection(variable_id="Nn", instrument_id="N12")],
        "dN": [Selection(variable_id="Nb", instrument_id="N12")],
    })
    for archive in ("editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-smps"] = DataRecord(dict([
            ("Dp", [Selection(variable_id="Ns", instrument_id="N12")]),
            ("dNdlogDp", [Selection(variable_id="Nn", instrument_id="N12")]),
            ("dN", [Selection(variable_id="Nb", instrument_id="N12")]),
            ("N", [Selection(variable_id="N", instrument_id="N12")])] + [
                (f"Bs{code}", [Selection(variable_id="Bs", wavelength=wavelength,
                                         instrument_id="N12")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
        )


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)