import typing
from ..cpd3 import use_cpd3


if use_cpd3():
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
    station_profile_data['aerosol']['realtime']['cpcstatus'] = {
        RealtimeTranslator.Key('Q_Q71'): 'Qsample',
        RealtimeTranslator.Key('Q_Q72'): 'Qdrier',
        RealtimeTranslator.Key('T1_N71'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N71'): 'Tcondenser',
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
    station_profile_data['aerosol']['raw']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BsB_S12', {'pm25'}): 'BsB',
            Name(station, 'raw', 'BsG_S12', {'pm25'}): 'BsG',
            Name(station, 'raw', 'BsR_S12', {'pm25'}): 'BsR',
            Name(station, 'raw', 'BbsB_S12', {'pm25'}): 'BbsB',
            Name(station, 'raw', 'BbsG_S12', {'pm25'}): 'BbsG',
            Name(station, 'raw', 'BbsR_S12', {'pm25'}): 'BbsR',
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
    station_profile_data['aerosol']['realtime']['scattering2-pm25'] = {
        RealtimeTranslator.Key('BsB_S12', {'pm25'}): 'BsB',
        RealtimeTranslator.Key('BsG_S12', {'pm25'}): 'BsG',
        RealtimeTranslator.Key('BsR_S12', {'pm25'}): 'BsR',
        RealtimeTranslator.Key('BbsB_S12', {'pm25'}): 'BbsB',
        RealtimeTranslator.Key('BbsG_S12', {'pm25'}): 'BbsG',
        RealtimeTranslator.Key('BbsR_S12', {'pm25'}): 'BbsR',
    }
    station_profile_data['aerosol']['realtime']['scattering2-pm1'] = {
        RealtimeTranslator.Key('BsB_S12', {'pm1'}): 'BsB',
        RealtimeTranslator.Key('BsG_S12', {'pm1'}): 'BsG',
        RealtimeTranslator.Key('BsR_S12', {'pm1'}): 'BsR',
        RealtimeTranslator.Key('BbsB_S12', {'pm1'}): 'BbsB',
        RealtimeTranslator.Key('BbsG_S12', {'pm1'}): 'BbsG',
        RealtimeTranslator.Key('BbsR_S12', {'pm1'}): 'BbsR',
    }
    station_profile_data['aerosol']['editing']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BsB_S12'): 'BsB',
            Name(station, 'clean', 'BsG_S12'): 'BsG',
            Name(station, 'clean', 'BsR_S12'): 'BsR',
            Name(station, 'clean', 'BbsB_S12'): 'BbsB',
            Name(station, 'clean', 'BbsG_S12'): 'BbsG',
            Name(station, 'clean', 'BbsR_S12'): 'BbsR',
        }, send
    )
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
    station_profile_data['aerosol']['clean']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S12'): 'BsB',
            Name(station, 'clean', 'BsG_S12'): 'BsG',
            Name(station, 'clean', 'BsR_S12'): 'BsR',
            Name(station, 'clean', 'BbsB_S12'): 'BbsB',
            Name(station, 'clean', 'BbsG_S12'): 'BbsG',
            Name(station, 'clean', 'BbsR_S12'): 'BbsR',
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
    station_profile_data['aerosol']['clean']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S12', {'pm25'}): 'BsB',
            Name(station, 'clean', 'BsG_S12', {'pm25'}): 'BsG',
            Name(station, 'clean', 'BsR_S12', {'pm25'}): 'BsR',
            Name(station, 'clean', 'BbsB_S12', {'pm25'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S12', {'pm25'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S12', {'pm25'}): 'BbsR',
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
    station_profile_data['aerosol']['avgh']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S12'): 'BsB',
            Name(station, 'avgh', 'BsG_S12'): 'BsG',
            Name(station, 'avgh', 'BsR_S12'): 'BsR',
            Name(station, 'avgh', 'BbsB_S12'): 'BbsB',
            Name(station, 'avgh', 'BbsG_S12'): 'BbsG',
            Name(station, 'avgh', 'BbsR_S12'): 'BbsR',
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
    station_profile_data['aerosol']['avgh']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S12', {'pm25'}): 'BsB',
            Name(station, 'avgh', 'BsG_S12', {'pm25'}): 'BsG',
            Name(station, 'avgh', 'BsR_S12', {'pm25'}): 'BsR',
            Name(station, 'avgh', 'BbsB_S12', {'pm25'}): 'BbsB',
            Name(station, 'avgh', 'BbsG_S12', {'pm25'}): 'BbsG',
            Name(station, 'avgh', 'BbsR_S12', {'pm25'}): 'BbsR',
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


    station_profile_data['aerosol']['raw']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X_N11', {'pm10'}): 'PM10',
            Name(station, 'raw', 'X_N11', {'pm25'}): 'PM25',
            Name(station, 'raw', 'X_N11', {'pm1'}): 'PM1',
            Name(station, 'raw', 'Ns_N11'): 'Dp',
            Name(station, 'raw', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'raw', 'Nb_N11'): 'dN',
        }, send
    )
    station_profile_data['aerosol']['realtime']['grimm'] = {
        RealtimeTranslator.Key('X_N11', {'pm10'}): 'PM10',
        RealtimeTranslator.Key('X_N11', {'pm25'}): 'PM25',
        RealtimeTranslator.Key('X_N11', {'pm1'}): 'PM1',
        RealtimeTranslator.Key('Ns_N11'): 'Dp',
        RealtimeTranslator.Key('Nn_N11'): 'dNdlogDp',
        RealtimeTranslator.Key('Nb_N11'): 'dN',
    }
    station_profile_data['aerosol']['editing']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'X_N11', {'pm10'}): 'PM10',
            Name(station, 'clean', 'X_N11', {'pm25'}): 'PM25',
            Name(station, 'clean', 'X_N11', {'pm1'}): 'PM1',
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
            Name(station, 'clean', 'X_N11', {'pm10'}): 'PM10',
            Name(station, 'clean', 'X_N11', {'pm25'}): 'PM25',
            Name(station, 'clean', 'X_N11', {'pm1'}): 'PM1',
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
            Name(station, 'avgh', 'X_N11', {'pm10'}): 'PM10',
            Name(station, 'avgh', 'X_N11', {'pm25'}): 'PM25',
            Name(station, 'avgh', 'X_N11', {'pm1'}): 'PM1',
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
    station_profile_data['aerosol']['realtime']['grimmstatus'] = {
        RealtimeTranslator.Key('Q_N11'): 'Qsample',
    }


    station_profile_data['aerosol']['raw']['ozone'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X_G81'): 'thermo',
            Name(station, 'raw', 'X_G82'): 'ecotech',
            Name(station, 'raw', 'X_G83'): 'twob',
        }, send
    )
    station_profile_data['aerosol']['realtime']['ozone'] = {
        RealtimeTranslator.Key('X_G81'): 'thermo',
    }
    station_profile_data['aerosol']['raw']['gasses'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X1_G71'): 'CO',
            Name(station, 'raw', 'X2_G71'): 'CO2',
        }, send
    )
    station_profile_data['aerosol']['raw']['noy'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X1_G72'): 'NO',
            Name(station, 'raw', 'X2_G72'): 'NOy',
        }, send
    )


    station_profile_data['aerosol']['raw']['flow'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Q_Q11'): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm10'}): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm1'}): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm25'}): 'sample',
            Name(station, 'raw', 'Q_Q12'): 'dilution',
            Name(station, 'raw', 'Q_Q12', {'pm10'}): 'dilution',
            Name(station, 'raw', 'Q_Q12', {'pm1'}): 'dilution',
            Name(station, 'raw', 'Q_Q12', {'pm25'}): 'dilution',
            Name(station, 'raw', 'Q_Q81'): 'gas',
        }, send
    )

    station_profile_data['aerosol']['realtime']['flow'] = {
        RealtimeTranslator.Key('Q_Q11'): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm10'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm1'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm25'}): 'sample',
        RealtimeTranslator.Key('Q_Q12'): 'dilution',
        RealtimeTranslator.Key('Q_Q12', {'pm10'}): 'dilution',
        RealtimeTranslator.Key('Q_Q12', {'pm1'}): 'dilution',
        RealtimeTranslator.Key('Q_Q12', {'pm25'}): 'dilution',
        RealtimeTranslator.Key('Q_Q81'): 'gas',
    }


    station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T_V01'): 'Troom', Name(station, 'raw', 'U_V01'): 'Uroom',
            Name(station, 'raw', 'T_V02'): 'Tcr1000', Name(station, 'raw', 'U_V02'): 'Ucr1000',
            Name(station, 'raw', 'T_V03'): 'Troom2', Name(station, 'raw', 'U_V03'): 'Uroom2',

            Name(station, 'raw', 'T1_XM1'): 'Tambient',
            Name(station, 'raw', 'U1_XM1'): 'Uambient',
            Name(station, 'raw', 'TD1_XM1'): 'TDambient',

            Name(station, 'raw', 'T2_XM1'): 'Tsheltered',
            Name(station, 'raw', 'U2_XM1'): 'Usheltered',
            Name(station, 'raw', 'TD2_XM1'): 'TDsheltered',

            Name(station, 'raw', 'T_V11'): 'Tsample', Name(station, 'raw', 'U_V11'): 'Usample',
            Name(station, 'raw', 'T_V11', {'pm10'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm10'}): 'Usample',
            Name(station, 'raw', 'T_V11', {'pm1'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm1'}): 'Usample',
            Name(station, 'raw', 'T_V11', {'pm25'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm25'}): 'Usample',

            Name(station, 'raw', 'Tu_S11'): 'Tnephinlet', Name(station, 'raw', 'Uu_S11'): 'Unephinlet',
            Name(station, 'raw', 'Tu_S11', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm10'}): 'Unephinlet',
            Name(station, 'raw', 'Tu_S11', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm1'}): 'Unephinlet',
            Name(station, 'raw', 'Tu_S11', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm25'}): 'Unephinlet',

            Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',

            Name(station, 'raw', 'Tx1_XM1'): 'Tthermodenuder1',
            Name(station, 'raw', 'Tx2_XM1'): 'Tthermodenuder2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['temperature'] = {
        RealtimeTranslator.Key('T_V01'): 'Troom', RealtimeTranslator.Key('U_V01'): 'Uroom',
        RealtimeTranslator.Key('T_V02'): 'Tcr1000', RealtimeTranslator.Key('U_V02'): 'Ucr1000',

        RealtimeTranslator.Key('T_V11'): 'Tsample', RealtimeTranslator.Key('U_V11'): 'Usample',
        RealtimeTranslator.Key('T_V11', {'pm10'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm10'}): 'Usample',
        RealtimeTranslator.Key('T_V11', {'pm1'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm1'}): 'Usample',
        RealtimeTranslator.Key('T_V11', {'pm25'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm25'}): 'Usample',

        RealtimeTranslator.Key('Tu_S11'): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11'): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S11', {'pm10'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm10'}): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S11', {'pm1'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm1'}): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S11', {'pm25'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm25'}): 'Unephinlet',

        RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',
    }

    station_profile_data['aerosol']['raw']['ambient'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_XM1'): 'Tambient',
            Name(station, 'raw', 'U1_XM1'): 'Uambient',
            Name(station, 'raw', 'TD1_XM1'): 'TDambient',

            Name(station, 'raw', 'T2_XM1'): 'Tsheltered',
            Name(station, 'raw', 'U2_XM1'): 'Usheltered',
            Name(station, 'raw', 'TD2_XM1'): 'TDsheltered',
        }, send
    )

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, ozone_data, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_THREE_WAVELENGTHS, STANDARD_CUT_SIZE_SPLIT

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)
    data_records.update(ozone_data)

    for archive in ("raw", "editing", "clean", "avgh"):
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
            data_records[f"aerosol-{archive}-scattering2-{record}"] = DataRecord(dict([
                (f"Bs{code}", [Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                         require_tags={"scattering", "secondary"})])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ] + [
                (f"Bbs{code}", [Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                         require_tags={"scattering", "secondary"})])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ]))
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
            data_records[f"aerosol-{archive}-clap2-{record}"] = DataRecord(dict([
                (f"Ba{code}", [Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                         require_tags={"absorption", "secondary"},
                                         exclude_tags={"aethalometer", "thermomaap"})])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ]))
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        data_records[f"aerosol-realtime-scattering2-{record}"] = RealtimeRecord(dict([
            (f"Bs{code}", [RealtimeSelection(f"Bs{code}", variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                             require_tags={"scattering", "secondary"})])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            (f"Bbs{code}", [RealtimeSelection(f"Bbs{code}", variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                              require_tags={"scattering", "secondary"})])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        data_records[f"aerosol-realtime-clap2-{record}"] = RealtimeRecord(dict([
            (f"Ba{code}", [RealtimeSelection(f"Ba{code}",
                                             variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                             require_tags={"absorption", "secondary"},
                                             exclude_tags={"aethalometer", "thermomaap"})])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))

    data_records["aerosol-raw-cpcstatus"] = DataRecord({
        "Qsample": [Selection(variable_name="sample_flow",
                              require_tags={"cpc"}, exclude_tags={"secondary"}),
                    Selection(variable_id="Q_Q71"), Selection(variable_id="Q_Q61")],
        "Qdrier": [Selection(variable_id="Q_Q72"), Selection(variable_id="Q_Q62")],
        "Tsaturator": [Selection(variable_name="saturator_temperature", instrument_code="tsi3010cpc")],
        "Tcondenser": [Selection(variable_name="condenser_temperature", instrument_code="tsi3010cpc")],
    })
    data_records["aerosol-realtime-cpcstatus"] = RealtimeRecord({
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow",
                                      require_tags={"cpc"}, exclude_tags={"secondary"}),
                    RealtimeSelection("Q_Q71", variable_id="Q_Q71"), RealtimeSelection("Q_Q61", variable_id="Q_Q61")],
        "Qdrier": [RealtimeSelection("Q_Q72", variable_id="Q_Q72"), RealtimeSelection("Q_Q62", variable_id="Q_Q62")],
        "Tsaturator": [RealtimeSelection("Tsaturator", variable_name="saturator_temperature", instrument_code="tsi3010cpc")],
        "Tcondenser": [RealtimeSelection("Tcondenser", variable_name="condenser_temperature", instrument_code="tsi3010cpc")],
    })

    data_records["aerosol-raw-nephzero2"] = DataRecord(dict([
        (f"Bsw{code}", [Selection(variable_name="wall_scattering_coefficient", wavelength=wavelength,
                                  variable_type=Selection.VariableType.State,
                                  require_tags={"scattering", "secondary"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        (f"Bbsw{code}", [Selection(variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                                  variable_type=Selection.VariableType.State,
                                  require_tags={"scattering", "secondary"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))
    data_records["aerosol-realtime-nephzero2"] = RealtimeRecord(dict([
        (f"Bsw{code}", [RealtimeSelection(f"Bsw{code}", variable_name="wall_scattering_coefficient", wavelength=wavelength,
                                          variable_type=Selection.VariableType.State,
                                          require_tags={"scattering", "secondary"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        (f"Bbsw{code}", [RealtimeSelection(f"Bbsw{code}", variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                                           variable_type=Selection.VariableType.State,
                                           require_tags={"scattering", "secondary"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))

    data_records["aerosol-raw-nephstatus"] = DataRecord({
        "CfG": [Selection(variable_name="reference_counts", wavelength=(500, 600),
                          require_tags={"scattering", "secondary"})],
        "Vl": [Selection(variable_name="lamp_voltage",
                         require_tags={"scattering", "secondary"})],
        "Al": [Selection(variable_name="lamp_current",
                         require_tags={"scattering", "secondary"})],
    })
    data_records["aerosol-realtime-nephstatus2"] = RealtimeRecord({
        "CfG": [RealtimeSelection("CfG", variable_name="reference_counts", wavelength=(500, 600),
                                  require_tags={"scattering", "secondary"})],
        "Vl": [RealtimeSelection("Vl", variable_name="lamp_voltage",
                                 require_tags={"scattering", "secondary"})],
        "Al": [RealtimeSelection("Al", variable_name="lamp_current",
                                 require_tags={"scattering", "secondary"})],
    })

    data_records["aerosol-raw-clapstatus2"] = DataRecord({
        "IrG": [Selection(variable_name="transmittance", wavelength=(500, 600),
                          instrument_code="clap", require_tags={"secondary"}),
                Selection(variable_name="transmittance", wavelength=(500, 600),
                          instrument_code="bmitap", require_tags={"secondary"})],
        "IfG": [Selection(variable_name="reference_intensity", wavelength=(500, 600),
                          instrument_code="clap", require_tags={"secondary"}),
                Selection(variable_name="reference_intensity", wavelength=(500, 600),
                          instrument_code="bmitap", require_tags={"secondary"})],
        "IpG": [Selection(variable_name="sample_intensity", wavelength=(500, 600),
                          instrument_code="clap", require_tags={"secondary"}),
                Selection(variable_name="sample_intensity", wavelength=(500, 600),
                          instrument_code="bmitap", require_tags={"secondary"})],
        "Q": [Selection(variable_name="sample_flow",
                        instrument_code="clap", require_tags={"secondary"}),
              Selection(variable_name="sample_flow",
                        instrument_code="bmitap", require_tags={"secondary"})],
        "Tsample": [Selection(variable_name="sample_temperature",
                              instrument_code="clap", require_tags={"secondary"}),
                    Selection(variable_name="sample_temperature",
                              instrument_code="bmitap", require_tags={"secondary"})],
        "Tcase": [Selection(variable_name="case_temperature",
                            instrument_code="clap", require_tags={"secondary"}),
                  Selection(variable_name="case_temperature",
                            instrument_code="bmitap", require_tags={"secondary"})],
        "spot": [Selection(variable_name="spot_number",
                           instrument_code="clap", require_tags={"secondary"},
                           variable_type=Selection.VariableType.State),
                 Selection(variable_name="spot_number",
                           instrument_code="bmitap", require_tags={"secondary"},
                           variable_type=Selection.VariableType.State)],
    }, hold_fields={"spot"})
    data_records["aerosol-realtime-clapstatus2"] = RealtimeRecord({
        "IrG": [RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600),
                                  instrument_code="clap", require_tags={"secondary"}),
                RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600),
                                  instrument_code="bmitap", require_tags={"secondary"})],
        "IfG": [RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600),
                                  instrument_code="clap", require_tags={"secondary"}),
                RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600),
                                  instrument_code="bmitap", require_tags={"secondary"})],
        "IpG": [RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600),
                                  instrument_code="clap", require_tags={"secondary"}),
                RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600),
                                  instrument_code="bmitap", require_tags={"secondary"})],
        "Q": [RealtimeSelection("Q", variable_name="sample_flow",
                                instrument_code="clap", require_tags={"secondary"}),
              RealtimeSelection("Q", variable_name="sample_flow",
                                instrument_code="bmitap", require_tags={"secondary"})],
        "Tsample": [RealtimeSelection("Tsample", variable_name="sample_temperature",
                                      instrument_code="clap", require_tags={"secondary"}),
                    RealtimeSelection("Tsample", variable_name="sample_temperature",
                                      instrument_code="bmitap", require_tags={"secondary"})],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature",
                                    instrument_code="clap", require_tags={"secondary"}),
                  RealtimeSelection("Tcase", variable_name="case_temperature",
                                    instrument_code="bmitap", require_tags={"secondary"})],
        "spot": [RealtimeSelection("Fn", variable_name="spot_number",
                                   instrument_code="clap", require_tags={"secondary"},
                                   variable_type=Selection.VariableType.State),
                 RealtimeSelection("Fn", variable_name="spot_number",
                                   instrument_code="bmitap", require_tags={"secondary"},
                                   variable_type=Selection.VariableType.State)],
    }, hold_fields={"spot"})

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-grimm"] = DataRecord({
            "Dp": [Selection(variable_name="diameter", instrument_code="grimm110xopc")],
            "dN": [Selection(variable_name="number_distribution", instrument_code="grimm110xopc")],
            "PM1": [Selection(variable_name="mass_concentration", instrument_code="grimm110xopc",
                              dimension_at=[("mass_diameter", 1.0)])],
            "PM25": [Selection(variable_name="mass_concentration", instrument_code="grimm110xopc",
                               dimension_at=[("mass_diameter", 2.5)])],
            "PM10": [Selection(variable_name="mass_concentration", instrument_code="grimm110xopc",
                               dimension_at=[("mass_diameter", 10)])],
        })
    data_records["aerosol-realtime-grimm"] = RealtimeRecord({
        "Dp": [RealtimeSelection("Dp", variable_name="diameter", instrument_code="grimm110xopc")],
        "dN": [RealtimeSelection("dN", variable_name="number_distribution", instrument_code="grimm110xopc")],
        "PM1": [RealtimeSelection("X1", variable_name="mass_concentration", instrument_code="grimm110xopc",
                                  dimension_at=[("mass_diameter", 1.0)])],
        "PM25": [RealtimeSelection("X25", variable_name="mass_concentration", instrument_code="grimm110xopc",
                                   dimension_at=[("mass_diameter", 2.5)])],
        "PM10": [RealtimeSelection("X10", variable_name="mass_concentration", instrument_code="grimm110xopc",
                                   dimension_at=[("mass_diameter", 10)])],
    })
    data_records["aerosol-raw-grimmstatus"] = DataRecord({
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="grimm110xopc")],
    })
    data_records["aerosol-realtime-grimmstatus"] = RealtimeRecord({
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="grimm110xopc")],
    })

    data_records["aerosol-raw-ozone"] = DataRecord({
        "thermo": [
            Selection(standard_name="mole_fraction_of_ozone_in_air", instrument_code="thermo49"),
            Selection(standard_name="mole_fraction_of_ozone_in_air", instrument_code="thermo49iq"),
        ],
        "ecotech": [Selection(variable_id="X_G82")],
        "twob": [Selection(variable_id="X_G83")],
    })
    data_records["aerosol-realtime-ozone"] = RealtimeRecord({
        "thermo": [
            RealtimeSelection("X", standard_name="mole_fraction_of_ozone_in_air", instrument_code="thermo49"),
            RealtimeSelection("X", standard_name="mole_fraction_of_ozone_in_air", instrument_code="thermo49iq"),
        ],
    })

    data_records["aerosol-raw-gasses"] = DataRecord({
        "CO": [Selection(variable_name="carbon_monoxide_mixing_ratio", instrument_id="G71")],
        "CO2": [Selection(variable_name="carbon_dioxide_mixing_ratio", instrument_id="G71")],
    })
    data_records["aerosol-raw-noy"] = DataRecord({
        "NO": [Selection(variable_name="nitrogen_monoxide_mixing_ratio", instrument_id="G72")],
        "NOy": [Selection(variable_name="nitrogen_oxide_mixing_ratio", instrument_id="G72")],
    })

    data_records["aerosol-raw-flow"] = DataRecord({
        "sample": [Selection(variable_id="Q_Q11")],
        "dilution": [Selection(variable_id="Q_Q12")],
        "gas": [Selection(variable_id="Q_Q81")],
    })
    data_records["aerosol-realtime-flow"] = DataRecord({
        "sample": [RealtimeSelection("Q_Q11", variable_id="Q_Q11")],
        "dilution": [RealtimeSelection("Q_Q12", variable_id="Q_Q12")],
    })

    data_records["aerosol-raw-temperature"] = DataRecord({
        "Troom": [Selection(variable_id="T_V01")], "Uroom": [Selection(variable_id="U_V01")],
        "Tcr1000": [Selection(variable_id="T_V02")], "Ucr1000": [Selection(variable_id="U_V02")],
        "Troom2": [Selection(variable_id="T_V03")], "Uroom2": [Selection(variable_id="U_V03")],

        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
        "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],

        "Tsheltered": [Selection(variable_id="T2", instrument_id="XM1")],
        "Usheltered": [Selection(variable_id="U2", instrument_id="XM1")],
        "TDsheltered": [Selection(variable_id="TD2", instrument_id="XM1")],

        "Tsample": [Selection(variable_id="T_V11")], "Usample": [Selection(variable_id="U_V11")],

        "Tnephinlet": [Selection(variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [Selection(variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],

        "Tthermodenuder1": [Selection(variable_id="Tx1", instrument_id="XM1")],
        "Tthermodenuder2": [Selection(variable_id="Tx2", instrument_id="XM1")],
    })
    data_records["aerosol-realtime-temperature"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("T_V51", variable_id="T_V51")], "Uinlet": [RealtimeSelection("U_V51", variable_id="U_V51")],

        "Tsample": [RealtimeSelection("T_V11", variable_id="T_V11")], "Usample": [RealtimeSelection("U_V11", variable_id="U_V11")],

        "Tnephinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })

    data_records["aerosol-raw-ambient"] = DataRecord({
        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
        "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],

        "Tsheltered": [Selection(variable_id="T2", instrument_id="XM1")],
        "Usheltered": [Selection(variable_id="U2", instrument_id="XM1")],
        "TDsheltered": [Selection(variable_id="TD2", instrument_id="XM1")],
    })


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
