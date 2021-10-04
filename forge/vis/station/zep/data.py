import typing
from ..cpd3 import DataStream, DataReader, EditedReader, Name, data_profile_get, detach, profile_data


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


station_profile_data['aerosol']['raw']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BacR_A31'): 'Ba',
        Name(station, 'raw', 'XR_A31'): 'X',
    }, send
)
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
    }, send
)


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



def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
