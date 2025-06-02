import typing
from ..cpd3 import use_cpd3


if use_cpd3("zep"):
    from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['scattering-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BsB_S41'): 'BsB',
            Name(station, 'raw', 'BsG_S41'): 'BsG',
            Name(station, 'raw', 'BsR_S41'): 'BsR',
            Name(station, 'raw', 'BbsB_S41'): 'BbsB',
            Name(station, 'raw', 'BbsG_S41'): 'BbsG',
            Name(station, 'raw', 'BbsR_S41'): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['raw']['scattering-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BsB_S41', {'pm10'}): 'BsB',
            Name(station, 'raw', 'BsG_S41', {'pm10'}): 'BsG',
            Name(station, 'raw', 'BsR_S41', {'pm10'}): 'BsR',
            Name(station, 'raw', 'BbsB_S41', {'pm10'}): 'BbsB',
            Name(station, 'raw', 'BbsG_S41', {'pm10'}): 'BbsG',
            Name(station, 'raw', 'BbsR_S41', {'pm10'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['raw']['scattering-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BsB_S41', {'pm25'}): 'BsB',
            Name(station, 'raw', 'BsG_S41', {'pm25'}): 'BsG',
            Name(station, 'raw', 'BsR_S41', {'pm25'}): 'BsR',
            Name(station, 'raw', 'BbsB_S41', {'pm25'}): 'BbsB',
            Name(station, 'raw', 'BbsG_S41', {'pm25'}): 'BbsG',
            Name(station, 'raw', 'BbsR_S41', {'pm25'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['raw']['scattering-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BsB_S41', {'pm1'}): 'BsB',
            Name(station, 'raw', 'BsG_S41', {'pm1'}): 'BsG',
            Name(station, 'raw', 'BsR_S41', {'pm1'}): 'BsR',
            Name(station, 'raw', 'BbsB_S41', {'pm1'}): 'BbsB',
            Name(station, 'raw', 'BbsG_S41', {'pm1'}): 'BbsG',
            Name(station, 'raw', 'BbsR_S41', {'pm1'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['realtime']['scattering-whole'] = {
        RealtimeTranslator.Key('BsB_S41'): 'BsB',
        RealtimeTranslator.Key('BsG_S41'): 'BsG',
        RealtimeTranslator.Key('BsR_S41'): 'BsR',
        RealtimeTranslator.Key('BbsB_S41'): 'BbsB',
        RealtimeTranslator.Key('BbsG_S41'): 'BbsG',
        RealtimeTranslator.Key('BbsR_S41'): 'BbsR',
    }
    station_profile_data['aerosol']['realtime']['scattering-pm10'] = {
        RealtimeTranslator.Key('BsB_S41', {'pm10'}): 'BsB',
        RealtimeTranslator.Key('BsG_S41', {'pm10'}): 'BsG',
        RealtimeTranslator.Key('BsR_S41', {'pm10'}): 'BsR',
        RealtimeTranslator.Key('BbsB_S41', {'pm10'}): 'BbsB',
        RealtimeTranslator.Key('BbsG_S41', {'pm10'}): 'BbsG',
        RealtimeTranslator.Key('BbsR_S41', {'pm10'}): 'BbsR',
    }
    station_profile_data['aerosol']['realtime']['scattering-pm25'] = {
        RealtimeTranslator.Key('BsB_S41', {'pm25'}): 'BsB',
        RealtimeTranslator.Key('BsG_S41', {'pm25'}): 'BsG',
        RealtimeTranslator.Key('BsR_S41', {'pm25'}): 'BsR',
        RealtimeTranslator.Key('BbsB_S41', {'pm25'}): 'BbsB',
        RealtimeTranslator.Key('BbsG_S41', {'pm25'}): 'BbsG',
        RealtimeTranslator.Key('BbsR_S41', {'pm25'}): 'BbsR',
    }
    station_profile_data['aerosol']['realtime']['scattering-pm1'] = {
        RealtimeTranslator.Key('BsB_S41', {'pm1'}): 'BsB',
        RealtimeTranslator.Key('BsG_S41', {'pm1'}): 'BsG',
        RealtimeTranslator.Key('BsR_S41', {'pm1'}): 'BsR',
        RealtimeTranslator.Key('BbsB_S41', {'pm1'}): 'BbsB',
        RealtimeTranslator.Key('BbsG_S41', {'pm1'}): 'BbsG',
        RealtimeTranslator.Key('BbsR_S41', {'pm1'}): 'BbsR',
    }
    station_profile_data['aerosol']['editing']['scattering-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BsB_S41'): 'BsB',
            Name(station, 'clean', 'BsG_S41'): 'BsG',
            Name(station, 'clean', 'BsR_S41'): 'BsR',
            Name(station, 'clean', 'BbsB_S41'): 'BbsB',
            Name(station, 'clean', 'BbsG_S41'): 'BbsG',
            Name(station, 'clean', 'BbsR_S41'): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['editing']['scattering-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BsB_S41', {'pm10'}): 'BsB',
            Name(station, 'clean', 'BsG_S41', {'pm10'}): 'BsG',
            Name(station, 'clean', 'BsR_S41', {'pm10'}): 'BsR',
            Name(station, 'clean', 'BbsB_S41', {'pm10'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S41', {'pm10'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S41', {'pm10'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['editing']['scattering-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BsB_S41', {'pm25'}): 'BsB',
            Name(station, 'clean', 'BsG_S41', {'pm25'}): 'BsG',
            Name(station, 'clean', 'BsR_S41', {'pm25'}): 'BsR',
            Name(station, 'clean', 'BbsB_S41', {'pm25'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S41', {'pm25'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S41', {'pm25'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['editing']['scattering-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BsB_S41', {'pm1'}): 'BsB',
            Name(station, 'clean', 'BsG_S41', {'pm1'}): 'BsG',
            Name(station, 'clean', 'BsR_S41', {'pm1'}): 'BsR',
            Name(station, 'clean', 'BbsB_S41', {'pm1'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S41', {'pm1'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S41', {'pm1'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['scattering-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S41'): 'BsB',
            Name(station, 'clean', 'BsG_S41'): 'BsG',
            Name(station, 'clean', 'BsR_S41'): 'BsR',
            Name(station, 'clean', 'BbsB_S41'): 'BbsB',
            Name(station, 'clean', 'BbsG_S41'): 'BbsG',
            Name(station, 'clean', 'BbsR_S41'): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['scattering-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S41', {'pm10'}): 'BsB',
            Name(station, 'clean', 'BsG_S41', {'pm10'}): 'BsG',
            Name(station, 'clean', 'BsR_S41', {'pm10'}): 'BsR',
            Name(station, 'clean', 'BbsB_S41', {'pm10'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S41', {'pm10'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S41', {'pm10'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['scattering-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S41', {'pm25'}): 'BsB',
            Name(station, 'clean', 'BsG_S41', {'pm25'}): 'BsG',
            Name(station, 'clean', 'BsR_S41', {'pm25'}): 'BsR',
            Name(station, 'clean', 'BbsB_S41', {'pm25'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S41', {'pm25'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S41', {'pm25'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['scattering-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S41', {'pm1'}): 'BsB',
            Name(station, 'clean', 'BsG_S41', {'pm1'}): 'BsG',
            Name(station, 'clean', 'BsR_S41', {'pm1'}): 'BsR',
            Name(station, 'clean', 'BbsB_S41', {'pm1'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S41', {'pm1'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S41', {'pm1'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['scattering-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S41'): 'BsB',
            Name(station, 'avgh', 'BsG_S41'): 'BsG',
            Name(station, 'avgh', 'BsR_S41'): 'BsR',
            Name(station, 'avgh', 'BbsB_S41'): 'BbsB',
            Name(station, 'avgh', 'BbsG_S41'): 'BbsG',
            Name(station, 'avgh', 'BbsR_S41'): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['scattering-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S41', {'pm10'}): 'BsB',
            Name(station, 'avgh', 'BsG_S41', {'pm10'}): 'BsG',
            Name(station, 'avgh', 'BsR_S41', {'pm10'}): 'BsR',
            Name(station, 'avgh', 'BbsB_S41', {'pm10'}): 'BbsB',
            Name(station, 'avgh', 'BbsG_S41', {'pm10'}): 'BbsG',
            Name(station, 'avgh', 'BbsR_S41', {'pm10'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['scattering-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S41', {'pm25'}): 'BsB',
            Name(station, 'avgh', 'BsG_S41', {'pm25'}): 'BsG',
            Name(station, 'avgh', 'BsR_S41', {'pm25'}): 'BsR',
            Name(station, 'avgh', 'BbsB_S41', {'pm25'}): 'BbsB',
            Name(station, 'avgh', 'BbsG_S41', {'pm25'}): 'BbsG',
            Name(station, 'avgh', 'BbsR_S41', {'pm25'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['scattering-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S41', {'pm1'}): 'BsB',
            Name(station, 'avgh', 'BsG_S41', {'pm1'}): 'BsG',
            Name(station, 'avgh', 'BsR_S41', {'pm1'}): 'BsR',
            Name(station, 'avgh', 'BbsB_S41', {'pm1'}): 'BbsB',
            Name(station, 'avgh', 'BbsG_S41', {'pm1'}): 'BbsG',
            Name(station, 'avgh', 'BbsR_S41', {'pm1'}): 'BbsR',
        }, send
    )

    station_profile_data['aerosol']['raw']['nephzero'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BswB_S41'): 'BswB',
            Name(station, 'raw', 'BswG_S41'): 'BswG',
            Name(station, 'raw', 'BswR_S41'): 'BswR',
            Name(station, 'raw', 'BbswB_S41'): 'BbswB',
            Name(station, 'raw', 'BbswG_S41'): 'BbswG',
            Name(station, 'raw', 'BbswR_S41'): 'BbswR',
        }, send
    )
    station_profile_data['aerosol']['raw']['nephstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'CfG_S41'): 'CfG',
            Name(station, 'raw', 'CfG_S41', {'pm10'}): 'CfG',
            Name(station, 'raw', 'CfG_S41', {'pm1'}): 'CfG',
            Name(station, 'raw', 'CfG_S41', {'pm25'}): 'CfG',
            Name(station, 'raw', 'Vl_S41'): 'Vl',
            Name(station, 'raw', 'Vl_S41', {'pm10'}): 'Vl',
            Name(station, 'raw', 'Vl_S41', {'pm1'}): 'Vl',
            Name(station, 'raw', 'Vl_S41', {'pm25'}): 'Vl',
            Name(station, 'raw', 'Al_S41'): 'Al',
            Name(station, 'raw', 'Al_S41', {'pm10'}): 'Al',
            Name(station, 'raw', 'Al_S41', {'pm1'}): 'Al',
            Name(station, 'raw', 'Al_S41', {'pm25'}): 'Al',
        }, send
    )
    station_profile_data['aerosol']['realtime']['nephzero'] = {
        RealtimeTranslator.Key('BswB_S41'): 'BswB',
        RealtimeTranslator.Key('BswG_S41'): 'BswG',
        RealtimeTranslator.Key('BswR_S41'): 'BswR',
        RealtimeTranslator.Key('BbswB_S41'): 'BbswB',
        RealtimeTranslator.Key('BbswG_S41'): 'BbswG',
        RealtimeTranslator.Key('BbswR_S41'): 'BbswR',
    }
    station_profile_data['aerosol']['realtime']['nephstatus'] = {
        RealtimeTranslator.Key('CfG_S41'): 'CfG',
        RealtimeTranslator.Key('CfG_S41', {'pm10'}): 'CfG',
        RealtimeTranslator.Key('CfG_S41', {'pm1'}): 'CfG',
        RealtimeTranslator.Key('CfG_S41', {'pm25'}): 'CfG',
        RealtimeTranslator.Key('Vl_S41'): 'Vl',
        RealtimeTranslator.Key('Vl_S41', {'pm10'}): 'Vl',
        RealtimeTranslator.Key('Vl_S41', {'pm1'}): 'Vl',
        RealtimeTranslator.Key('Vl_S41', {'pm25'}): 'Vl',
        RealtimeTranslator.Key('Al_S41'): 'Al',
        RealtimeTranslator.Key('Al_S41', {'pm10'}): 'Al',
        RealtimeTranslator.Key('Al_S41', {'pm1'}): 'Al',
        RealtimeTranslator.Key('Al_S41', {'pm25'}): 'Al',
    }


    station_profile_data['aerosol']['raw']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BsB_S13'): 'BsB',
            Name(station, 'raw', 'BsG_S13'): 'BsG',
            Name(station, 'raw', 'BsR_S13'): 'BsR',
            Name(station, 'raw', 'BbsB_S13'): 'BbsB',
            Name(station, 'raw', 'BbsG_S13'): 'BbsG',
            Name(station, 'raw', 'BbsR_S13'): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['raw']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BsB_S13', {'pm10'}): 'BsB',
            Name(station, 'raw', 'BsG_S13', {'pm10'}): 'BsG',
            Name(station, 'raw', 'BsR_S13', {'pm10'}): 'BsR',
            Name(station, 'raw', 'BbsB_S13', {'pm10'}): 'BbsB',
            Name(station, 'raw', 'BbsG_S13', {'pm10'}): 'BbsG',
            Name(station, 'raw', 'BbsR_S13', {'pm10'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['raw']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BsB_S13', {'pm25'}): 'BsB',
            Name(station, 'raw', 'BsG_S13', {'pm25'}): 'BsG',
            Name(station, 'raw', 'BsR_S13', {'pm25'}): 'BsR',
            Name(station, 'raw', 'BbsB_S13', {'pm25'}): 'BbsB',
            Name(station, 'raw', 'BbsG_S13', {'pm25'}): 'BbsG',
            Name(station, 'raw', 'BbsR_S13', {'pm25'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['raw']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BsB_S13', {'pm1'}): 'BsB',
            Name(station, 'raw', 'BsG_S13', {'pm1'}): 'BsG',
            Name(station, 'raw', 'BsR_S13', {'pm1'}): 'BsR',
            Name(station, 'raw', 'BbsB_S13', {'pm1'}): 'BbsB',
            Name(station, 'raw', 'BbsG_S13', {'pm1'}): 'BbsG',
            Name(station, 'raw', 'BbsR_S13', {'pm1'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['realtime']['scattering2-whole'] = {
        RealtimeTranslator.Key('BsB_S13'): 'BsB',
        RealtimeTranslator.Key('BsG_S13'): 'BsG',
        RealtimeTranslator.Key('BsR_S13'): 'BsR',
        RealtimeTranslator.Key('BbsB_S13'): 'BbsB',
        RealtimeTranslator.Key('BbsG_S13'): 'BbsG',
        RealtimeTranslator.Key('BbsR_S13'): 'BbsR',
    }
    station_profile_data['aerosol']['realtime']['scattering2-pm10'] = {
        RealtimeTranslator.Key('BsB_S13', {'pm10'}): 'BsB',
        RealtimeTranslator.Key('BsG_S13', {'pm10'}): 'BsG',
        RealtimeTranslator.Key('BsR_S13', {'pm10'}): 'BsR',
        RealtimeTranslator.Key('BbsB_S13', {'pm10'}): 'BbsB',
        RealtimeTranslator.Key('BbsG_S13', {'pm10'}): 'BbsG',
        RealtimeTranslator.Key('BbsR_S13', {'pm10'}): 'BbsR',
    }
    station_profile_data['aerosol']['realtime']['scattering2-pm25'] = {
        RealtimeTranslator.Key('BsB_S13', {'pm25'}): 'BsB',
        RealtimeTranslator.Key('BsG_S13', {'pm25'}): 'BsG',
        RealtimeTranslator.Key('BsR_S13', {'pm25'}): 'BsR',
        RealtimeTranslator.Key('BbsB_S13', {'pm25'}): 'BbsB',
        RealtimeTranslator.Key('BbsG_S13', {'pm25'}): 'BbsG',
        RealtimeTranslator.Key('BbsR_S13', {'pm25'}): 'BbsR',
    }
    station_profile_data['aerosol']['realtime']['scattering2-pm1'] = {
        RealtimeTranslator.Key('BsB_S13', {'pm1'}): 'BsB',
        RealtimeTranslator.Key('BsG_S13', {'pm1'}): 'BsG',
        RealtimeTranslator.Key('BsR_S13', {'pm1'}): 'BsR',
        RealtimeTranslator.Key('BbsB_S13', {'pm1'}): 'BbsB',
        RealtimeTranslator.Key('BbsG_S13', {'pm1'}): 'BbsG',
        RealtimeTranslator.Key('BbsR_S13', {'pm1'}): 'BbsR',
    }
    station_profile_data['aerosol']['editing']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BsB_S13'): 'BsB',
            Name(station, 'clean', 'BsG_S13'): 'BsG',
            Name(station, 'clean', 'BsR_S13'): 'BsR',
            Name(station, 'clean', 'BbsB_S13'): 'BbsB',
            Name(station, 'clean', 'BbsG_S13'): 'BbsG',
            Name(station, 'clean', 'BbsR_S13'): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['editing']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BsB_S13', {'pm10'}): 'BsB',
            Name(station, 'clean', 'BsG_S13', {'pm10'}): 'BsG',
            Name(station, 'clean', 'BsR_S13', {'pm10'}): 'BsR',
            Name(station, 'clean', 'BbsB_S13', {'pm10'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S13', {'pm10'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S13', {'pm10'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['editing']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BsB_S13', {'pm25'}): 'BsB',
            Name(station, 'clean', 'BsG_S13', {'pm25'}): 'BsG',
            Name(station, 'clean', 'BsR_S13', {'pm25'}): 'BsR',
            Name(station, 'clean', 'BbsB_S13', {'pm25'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S13', {'pm25'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S13', {'pm25'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['editing']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BsB_S13', {'pm1'}): 'BsB',
            Name(station, 'clean', 'BsG_S13', {'pm1'}): 'BsG',
            Name(station, 'clean', 'BsR_S13', {'pm1'}): 'BsR',
            Name(station, 'clean', 'BbsB_S13', {'pm1'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S13', {'pm1'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S13', {'pm1'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S13'): 'BsB',
            Name(station, 'clean', 'BsG_S13'): 'BsG',
            Name(station, 'clean', 'BsR_S13'): 'BsR',
            Name(station, 'clean', 'BbsB_S13'): 'BbsB',
            Name(station, 'clean', 'BbsG_S13'): 'BbsG',
            Name(station, 'clean', 'BbsR_S13'): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S13', {'pm10'}): 'BsB',
            Name(station, 'clean', 'BsG_S13', {'pm10'}): 'BsG',
            Name(station, 'clean', 'BsR_S13', {'pm10'}): 'BsR',
            Name(station, 'clean', 'BbsB_S13', {'pm10'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S13', {'pm10'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S13', {'pm10'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S13', {'pm25'}): 'BsB',
            Name(station, 'clean', 'BsG_S13', {'pm25'}): 'BsG',
            Name(station, 'clean', 'BsR_S13', {'pm25'}): 'BsR',
            Name(station, 'clean', 'BbsB_S13', {'pm25'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S13', {'pm25'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S13', {'pm25'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BsB_S13', {'pm1'}): 'BsB',
            Name(station, 'clean', 'BsG_S13', {'pm1'}): 'BsG',
            Name(station, 'clean', 'BsR_S13', {'pm1'}): 'BsR',
            Name(station, 'clean', 'BbsB_S13', {'pm1'}): 'BbsB',
            Name(station, 'clean', 'BbsG_S13', {'pm1'}): 'BbsG',
            Name(station, 'clean', 'BbsR_S13', {'pm1'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S13'): 'BsB',
            Name(station, 'avgh', 'BsG_S13'): 'BsG',
            Name(station, 'avgh', 'BsR_S13'): 'BsR',
            Name(station, 'avgh', 'BbsB_S13'): 'BbsB',
            Name(station, 'avgh', 'BbsG_S13'): 'BbsG',
            Name(station, 'avgh', 'BbsR_S13'): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S13', {'pm10'}): 'BsB',
            Name(station, 'avgh', 'BsG_S13', {'pm10'}): 'BsG',
            Name(station, 'avgh', 'BsR_S13', {'pm10'}): 'BsR',
            Name(station, 'avgh', 'BbsB_S13', {'pm10'}): 'BbsB',
            Name(station, 'avgh', 'BbsG_S13', {'pm10'}): 'BbsG',
            Name(station, 'avgh', 'BbsR_S13', {'pm10'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S13', {'pm25'}): 'BsB',
            Name(station, 'avgh', 'BsG_S13', {'pm25'}): 'BsG',
            Name(station, 'avgh', 'BsR_S13', {'pm25'}): 'BsR',
            Name(station, 'avgh', 'BbsB_S13', {'pm25'}): 'BbsB',
            Name(station, 'avgh', 'BbsG_S13', {'pm25'}): 'BbsG',
            Name(station, 'avgh', 'BbsR_S13', {'pm25'}): 'BbsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BsB_S13', {'pm1'}): 'BsB',
            Name(station, 'avgh', 'BsG_S13', {'pm1'}): 'BsG',
            Name(station, 'avgh', 'BsR_S13', {'pm1'}): 'BsR',
            Name(station, 'avgh', 'BbsB_S13', {'pm1'}): 'BbsB',
            Name(station, 'avgh', 'BbsG_S13', {'pm1'}): 'BbsG',
            Name(station, 'avgh', 'BbsR_S13', {'pm1'}): 'BbsR',
        }, send
    )

    station_profile_data['aerosol']['raw']['nephzero2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BswB_S13'): 'BswB',
            Name(station, 'raw', 'BswG_S13'): 'BswG',
            Name(station, 'raw', 'BswR_S13'): 'BswR',
            Name(station, 'raw', 'BbswB_S13'): 'BbswB',
            Name(station, 'raw', 'BbswG_S13'): 'BbswG',
            Name(station, 'raw', 'BbswR_S13'): 'BbswR',
        }, send
    )
    station_profile_data['aerosol']['raw']['nephstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'CfG_S13'): 'CfG',
            Name(station, 'raw', 'CfG_S13', {'pm10'}): 'CfG',
            Name(station, 'raw', 'CfG_S13', {'pm1'}): 'CfG',
            Name(station, 'raw', 'CfG_S13', {'pm25'}): 'CfG',
        }, send
    )
    station_profile_data['aerosol']['realtime']['nephzero2'] = {
        RealtimeTranslator.Key('BswB_S13'): 'BswB',
        RealtimeTranslator.Key('BswG_S13'): 'BswG',
        RealtimeTranslator.Key('BswR_S13'): 'BswR',
        RealtimeTranslator.Key('BbswB_S13'): 'BbswB',
        RealtimeTranslator.Key('BbswG_S13'): 'BbswG',
        RealtimeTranslator.Key('BbswR_S13'): 'BbswR',
    }
    station_profile_data['aerosol']['realtime']['nephstatus2'] = {
        RealtimeTranslator.Key('CfG_S13'): 'CfG',
        RealtimeTranslator.Key('CfG_S13', {'pm10'}): 'CfG',
        RealtimeTranslator.Key('CfG_S13', {'pm1'}): 'CfG',
        RealtimeTranslator.Key('CfG_S13', {'pm25'}): 'CfG',
    }


    station_profile_data['aerosol']['raw']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BacR_A31'): 'Ba',
            Name(station, 'raw', 'XR_A31'): 'X',
        }, send
    )
    station_profile_data['aerosol']['realtime']['maap'] = {
        RealtimeTranslator.Key('BacR_A31'): 'Ba',
        RealtimeTranslator.Key('XR_A31'): 'X',
    }
    station_profile_data['aerosol']['editing']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BacR_A31'): 'Ba',
            Name(station, 'clean', 'XR_A31'): 'X',
        }, send
    )
    station_profile_data['aerosol']['clean']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BacR_A31'): 'Ba',
            Name(station, 'clean', 'XR_A31'): 'X',
        }, send
    )
    station_profile_data['aerosol']['avgh']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BacR_A31'): 'Ba',
            Name(station, 'avgh', 'XR_A31'): 'X',
        }, send
    )

    station_profile_data['aerosol']['raw']['maapstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_A31'): 'Psample',
            Name(station, 'raw', 'T1_A31'): 'Tambient',
            Name(station, 'raw', 'T2_A31'): 'Tmeasurementhead',
            Name(station, 'raw', 'T3_A31'): 'Tsystem',
            Name(station, 'raw', 'Q_A31'): 'Qsample',
            Name(station, 'raw', 'IrR_A31'): 'Ir',
            Name(station, 'raw', 'IfR_A31'): 'If',
            Name(station, 'raw', 'IpR_A31'): 'Ip',
            Name(station, 'raw', 'Is1_A31'): 'Is1',
            Name(station, 'raw', 'Is2_A31'): 'Is2',
            Name(station, 'raw', 'Is1R_A31'): 'Is1',
            Name(station, 'raw', 'Is2R_A31'): 'Is2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['maapstatus'] = {
        RealtimeTranslator.Key('P_A31'): 'Psample',
        RealtimeTranslator.Key('T1_A31'): 'Tambient',
        RealtimeTranslator.Key('T2_A31'): 'Tmeasurementhead',
        RealtimeTranslator.Key('T3_A31'): 'Tsystem',
        RealtimeTranslator.Key('Q_A31'): 'Qsample',
        RealtimeTranslator.Key('IrR_A31'): 'Ir',
        RealtimeTranslator.Key('IfR_A31'): 'If',
        RealtimeTranslator.Key('IpR_A31'): 'Ip',
        RealtimeTranslator.Key('Is1_A31'): 'Is1',
        RealtimeTranslator.Key('Is2_A31'): 'Is2',
        RealtimeTranslator.Key('Is1R_A31'): 'Is1',
        RealtimeTranslator.Key('Is2R_A31'): 'Is2',
    }


    station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Tu_S41'): 'Tnephinlet', Name(station, 'raw', 'Uu_S41'): 'Unephinlet',
            Name(station, 'raw', 'Tu_S41', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S41', {'pm10'}): 'Unephinlet',
            Name(station, 'raw', 'Tu_S41', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S41', {'pm1'}): 'Unephinlet',
            Name(station, 'raw', 'Tu_S41', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S41', {'pm25'}): 'Unephinlet',

            Name(station, 'raw', 'T_S41'): 'Tneph', Name(station, 'raw', 'U_S41'): 'Uneph',
            Name(station, 'raw', 'T_S41', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S41', {'pm10'}): 'Uneph',
            Name(station, 'raw', 'T_S41', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S41', {'pm1'}): 'Uneph',
            Name(station, 'raw', 'T_S41', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S41', {'pm25'}): 'Uneph',


            Name(station, 'raw', 'T_S13'): 'Tneph2', Name(station, 'raw', 'U_S13'): 'Uneph2',
            Name(station, 'raw', 'T_S13', {'pm10'}): 'Tneph2', Name(station, 'raw', 'U_S13', {'pm10'}): 'Uneph2',
            Name(station, 'raw', 'T_S13', {'pm1'}): 'Tneph2', Name(station, 'raw', 'U_S13', {'pm1'}): 'Uneph2',
            Name(station, 'raw', 'T_S13', {'pm25'}): 'Tneph2', Name(station, 'raw', 'U_S13', {'pm25'}): 'Uneph2',

            Name(station, 'raw', 'Tx_S13'): 'Tnephcell2', Name(station, 'raw', 'Ux_S13'): 'Unephcell2',
            Name(station, 'raw', 'Tx_S13', {'pm10'}): 'Tnephcell2', Name(station, 'raw', 'Ux_S13', {'pm10'}): 'Unephcell2',
            Name(station, 'raw', 'Tx_S13', {'pm1'}): 'Tnephcell2', Name(station, 'raw', 'Ux_S13', {'pm1'}): 'Unephcell2',
            Name(station, 'raw', 'Tx_S13', {'pm25'}): 'Tnephcell2', Name(station, 'raw', 'Ux_S13', {'pm25'}): 'Unephcell2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['temperature'] = {
        RealtimeTranslator.Key('Tu_S41'): 'Tnephinlet', RealtimeTranslator.Key('Uu_S41'): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S41', {'pm10'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S41', {'pm10'}): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S41', {'pm1'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S41', {'pm1'}): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S41', {'pm25'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S41', {'pm25'}): 'Unephinlet',

        RealtimeTranslator.Key('T_S41'): 'Tneph', RealtimeTranslator.Key('U_S41'): 'Uneph',
        RealtimeTranslator.Key('T_S41', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S41', {'pm10'}): 'Uneph',
        RealtimeTranslator.Key('T_S41', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S41', {'pm1'}): 'Uneph',
        RealtimeTranslator.Key('T_S41', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S41', {'pm25'}): 'Uneph',


        RealtimeTranslator.Key('T_S13'): 'Tneph2', RealtimeTranslator.Key('U_S13'): 'Uneph2',
        RealtimeTranslator.Key('T_S13', {'pm10'}): 'Tneph2', RealtimeTranslator.Key('U_S13', {'pm10'}): 'Uneph2',
        RealtimeTranslator.Key('T_S13', {'pm1'}): 'Tneph2', RealtimeTranslator.Key('U_S13', {'pm1'}): 'Uneph2',
        RealtimeTranslator.Key('T_S13', {'pm25'}): 'Tneph2', RealtimeTranslator.Key('U_S13', {'pm25'}): 'Uneph2',

        RealtimeTranslator.Key('Tx_S13'): 'Tnephcell2', RealtimeTranslator.Key('Ux_S13'): 'Unephcell2',
        RealtimeTranslator.Key('Tx_S13', {'pm10'}): 'Tnephcell2', RealtimeTranslator.Key('Ux_S13', {'pm10'}): 'Unephcell2',
        RealtimeTranslator.Key('Tx_S13', {'pm1'}): 'Tnephcell2', RealtimeTranslator.Key('Ux_S13', {'pm1'}): 'Unephcell2',
        RealtimeTranslator.Key('Tx_S13', {'pm25'}): 'Tnephcell2', RealtimeTranslator.Key('Ux_S13', {'pm25'}): 'Unephcell2',
    }

    station_profile_data['aerosol']['raw']['samplepressure-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_S41'): 'neph',
            Name(station, 'raw', 'P_S13'): 'neph2',
        }, send
    )
    station_profile_data['aerosol']['raw']['samplepressure-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_S41', {'pm10'}): 'neph',
            Name(station, 'raw', 'P_S13', {'pm10'}): 'neph2',
        }, send
    )
    station_profile_data['aerosol']['raw']['samplepressure-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_S41', {'pm25'}): 'neph',
            Name(station, 'raw', 'P_S13', {'pm25'}): 'neph2',
        }, send
    )
    station_profile_data['aerosol']['raw']['samplepressure-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_S41', {'pm1'}): 'neph',
            Name(station, 'raw', 'P_S13', {'pm1'}): 'neph2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['samplepressure-whole'] = {
        RealtimeTranslator.Key('P_S41'): 'neph',
        RealtimeTranslator.Key('P_S13'): 'neph2',
    }
    station_profile_data['aerosol']['realtime']['samplepressure-pm10'] = {
        RealtimeTranslator.Key('P_S41', {'pm10'}): 'neph',
        RealtimeTranslator.Key('P_S13', {'pm10'}): 'neph2',
    }
    station_profile_data['aerosol']['realtime']['samplepressure-pm25'] = {
        RealtimeTranslator.Key('P_S41', {'pm25'}): 'neph',
        RealtimeTranslator.Key('P_S13', {'pm25'}): 'neph2',
    }
    station_profile_data['aerosol']['realtime']['samplepressure-pm1'] = {
        RealtimeTranslator.Key('P_S41', {'pm1'}): 'neph',
        RealtimeTranslator.Key('P_S13', {'pm1'}): 'neph2',
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
        data_records[f"aerosol-{archive}-maap"] = DataRecord({
            "Ba": [Selection(variable_name="light_absorption", wavelength_number=0,
                             instrument_code="thermomaap")],
            "X": [Selection(variable_name="equivalent_black_carbon", wavelength_number=0,
                            instrument_code="thermomaap")],
        })
    data_records[f"aerosol-realtime-maap"] = RealtimeRecord({
        "Ba": [RealtimeSelection("Ba", variable_name="light_absorption", wavelength_number=0,
                                 instrument_code="thermomaap")],
        "X": [RealtimeSelection("X", variable_name="equivalent_black_carbon", wavelength_number=0,
                                instrument_code="thermomaap")],
    })
    data_records["aerosol-raw-maapstatus"] = DataRecord({
        "Psample": [Selection(variable_name="sample_pressure", instrument_code="thermomaap")],
        "Tambient": [Selection(variable_name="sample_temperature", instrument_code="thermomaap")],
        "Tmeasurementhead": [Selection(variable_name="measurement_head_temperature", instrument_code="thermomaap")],
        "Tsystem": [Selection(variable_name="system_temperature", instrument_code="thermomaap")],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="thermomaap")],
        "Ir": [Selection(variable_name="transmittance", instrument_code="thermomaap", wavelength_number=0)],
        "If": [Selection(variable_name="reference_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Ip": [Selection(variable_name="sample_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Is1": [Selection(variable_name="backscatter_135_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Is2": [Selection(variable_name="backscatter_165_intensity", instrument_code="thermomaap", wavelength_number=0)],
    })
    data_records["aerosol-realtime-maapstatus"] = RealtimeRecord({
        "Psample": [RealtimeSelection("P", variable_name="sample_pressure", instrument_code="thermomaap")],
        "Tambient": [RealtimeSelection("Tsample", variable_name="sample_temperature", instrument_code="thermomaap")],
        "Tmeasurementhead": [RealtimeSelection("Thead", variable_name="measurement_head_temperature", instrument_code="thermomaap")],
        "Tsystem": [RealtimeSelection("Tsystem", variable_name="system_temperature", instrument_code="thermomaap")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="thermomaap")],
        "Ir": [RealtimeSelection("Ir", variable_name="transmittance", instrument_code="thermomaap", wavelength_number=0)],
        "If": [RealtimeSelection("If", variable_name="reference_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Ip": [RealtimeSelection("Ip", variable_name="sample_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Is1": [RealtimeSelection("Is135", variable_name="backscatter_135_intensity", instrument_code="thermomaap", wavelength_number=0)],
        "Is2": [RealtimeSelection("Is165", variable_name="backscatter_165_intensity", instrument_code="thermomaap", wavelength_number=0)],
    })

    data_records["aerosol-raw-temperature"] = DataRecord({
        "Tnephinlet": [Selection(variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [Selection(variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],

        "Tnephcell2": [Selection(variable_name="cell_temperature", require_tags={"scattering"}, instrument_id="S13")],
        "Unephcell2": [Selection(variable_name="cell_humidity", require_tags={"scattering"}, instrument_id="S13")],
        "Tneph2": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, instrument_id="S13")],
        "Uneph2": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, instrument_id="S13")],
    })
    data_records["aerosol-raw-temperature"] = RealtimeRecord({
        "Tnephinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],

        "Tnephcell2": [RealtimeSelection("Tcell", variable_name="cell_temperature", require_tags={"scattering"}, instrument_id="S13")],
        "Unephcell2": [RealtimeSelection("Ucell", variable_name="cell_humidity", require_tags={"scattering"}, instrument_id="S13")],
        "Tneph2": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, instrument_id="S13")],
        "Uneph2": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, instrument_id="S13")],
    })
    
    data_records["aerosol-raw-nephzero2"] = DataRecord(dict([
        (f"Bsw{code}", [Selection(variable_name="wall_scattering_coefficient", wavelength=wavelength,
                                  variable_type=Selection.VariableType.State,
                                  instrument_id="S13")])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        (f"Bbsw{code}", [Selection(variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                                  variable_type=Selection.VariableType.State,
                                  instrument_id="S13")])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))
    data_records["aerosol-raw-nephstatus2"] = DataRecord({
        "CfG": [Selection(variable_name="reference_counts", wavelength=(500, 600),
                          instrument_id="S13")],
    })
    data_records["aerosol-realtime-nephzero2"] = RealtimeRecord(dict([
        (f"Bsw{code}", [RealtimeSelection(f"Bsw{code}", variable_name="wall_scattering_coefficient", wavelength=wavelength,
                                          variable_type=Selection.VariableType.State,
                                          instrument_id="S13")])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        (f"Bbsw{code}", [RealtimeSelection(f"Bbsw{code}", variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                                           variable_type=Selection.VariableType.State,
                                           instrument_id="S13")])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))
    data_records["aerosol-realtime-nephstatus2"] = RealtimeRecord({
        "CfG": [RealtimeSelection("CfG", variable_name="reference_counts", wavelength=(500, 600),
                                  instrument_id="S13")],
    })
    for archive in ("raw", "editing", "clean", "avgh"):
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
            data_records[f"aerosol-{archive}-scattering2-{record}"] = DataRecord(dict([
                (f"Bs{code}", [Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                         instrument_id="S13")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ] + [
                (f"Bbs{code}", [Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                         instrument_id="S13")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ]))
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        data_records[f"aerosol-realtime-scattering2-{record}"] = RealtimeRecord(dict([
            (f"Bs{code}", [RealtimeSelection(f"Bs{code}", variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                             instrument_id="S13")])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            (f"Bbs{code}", [RealtimeSelection(f"Bbs{code}", variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                              instrument_id="S13")])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)