import typing
from math import nan
from ..cpd3 import use_cpd3


if use_cpd3():
    from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'N_N71'): 'cnc',
            Name(station, 'raw', 'N_N72'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cnc'] = {
        RealtimeTranslator.Key('N_N71'): 'cnc',
        RealtimeTranslator.Key('N_N72'): 'cnc2',
    }
    station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'N_N71'): 'cnc',
            Name(station, 'clean', 'N_N72'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'N_N71'): 'cnc',
            Name(station, 'clean', 'N_N72'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'N_N71'): 'cnc',
            Name(station, 'avgh', 'N_N72'): 'cnc2',
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
            Name(station, 'raw', 'T6_N72'): 'Tcase',
            Name(station, 'raw', 'T7_N72'): 'Tboard',
            Name(station, 'raw', 'TD1_N72'): 'TDgrowth',
            Name(station, 'raw', 'Q_N72'): 'Qsample',
            Name(station, 'raw', 'P_N72'): 'Psample',
            Name(station, 'raw', 'PCT_N72'): 'PCTwick',
            Name(station, 'raw', 'V_N72'): 'Vpulse',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
        RealtimeTranslator.Key('Tu_N72'): 'Tinlet',
        RealtimeTranslator.Key('TDu_N72'): 'TDinlet',
        RealtimeTranslator.Key('Uu_N72'): 'Uinlet',
        RealtimeTranslator.Key('T1_N72'): 'Tconditioner',
        RealtimeTranslator.Key('T2_N72'): 'Tinitiator',
        RealtimeTranslator.Key('T3_N72'): 'Tmoderator',
        RealtimeTranslator.Key('T4_N72'): 'Toptics',
        RealtimeTranslator.Key('T5_N72'): 'Theatsink',
        RealtimeTranslator.Key('T6_N72'): 'Tcase',
        RealtimeTranslator.Key('T7_N72'): 'Tboard',
        RealtimeTranslator.Key('TD1_N72'): 'TDgrowth',
        RealtimeTranslator.Key('Q_N72'): 'Qsample',
        RealtimeTranslator.Key('P_N72'): 'Psample',
        RealtimeTranslator.Key('PCT_N72'): 'PCTwick',
        RealtimeTranslator.Key('V_N72'): 'Vpulse',
    }


    station_profile_data['aerosol']['raw']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Ns_N11'): 'Dp',
            Name(station, 'raw', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'raw', 'Nb_N11'): 'dN',
            Name(station, 'raw', 'N_N12'): 'Nraw',
        }, send
    )
    station_profile_data['aerosol']['editing']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'Ns_N11'): 'Dp',
            Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N11'): 'dN',
            Name(station, 'clean', 'N_N12'): 'Nraw',
            Name(station, 'clean', 'N_N11'): 'N',
            Name(station, 'clean', 'BsB_N11'): 'BsB',
            Name(station, 'clean', 'BsG_N11'): 'BsG',
            Name(station, 'clean', 'BsR_N11'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'Ns_N11'): 'Dp',
            Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N11'): 'dN',
            Name(station, 'clean', 'N_N12'): 'Nraw',
            Name(station, 'clean', 'N_N11'): 'N',
            Name(station, 'clean', 'BsB_N11'): 'BsB',
            Name(station, 'clean', 'BsG_N11'): 'BsG',
            Name(station, 'clean', 'BsR_N11'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'Ns_N11'): 'Dp',
            Name(station, 'avgh', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'avgh', 'Nb_N11'): 'dN',
            Name(station, 'avgh', 'N_N12'): 'Nraw',
            Name(station, 'avgh', 'N_N11'): 'N',
            Name(station, 'avgh', 'BsB_N11'): 'BsB',
            Name(station, 'avgh', 'BsG_N11'): 'BsG',
            Name(station, 'avgh', 'BsR_N11'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['raw']['dmpsstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_N11'): 'Taerosol', Name(station, 'raw', 'T2_N11'): 'Tsheath',
            Name(station, 'raw', 'P1_N11'): 'Paerosol', Name(station, 'raw', 'P2_N11'): 'Psheath',
            Name(station, 'raw', 'Q1_N11'): 'Qaerosol', Name(station, 'raw', 'Q2_N11'): 'Qsheath',
        }, send
    )


    station_profile_data['aerosol']['raw']['pops'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Ns_N21'): 'Dp',
            Name(station, 'raw', 'Nn_N21'): 'dNdlogDp',
            Name(station, 'raw', 'Nb_N21'): 'dN',
        }, send
    )
    station_profile_data['aerosol']['editing']['pops'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'Ns_N21'): 'Dp',
            Name(station, 'clean', 'Nn_N21'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N21'): 'dN',
            Name(station, 'clean', 'N_N21'): 'N',
            Name(station, 'clean', 'BsB_N21'): 'BsB',
            Name(station, 'clean', 'BsG_N21'): 'BsG',
            Name(station, 'clean', 'BsR_N21'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['pops'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'Ns_N21'): 'Dp',
            Name(station, 'clean', 'Nn_N21'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N21'): 'dN',
            Name(station, 'clean', 'N_N21'): 'N',
            Name(station, 'clean', 'BsB_N21'): 'BsB',
            Name(station, 'clean', 'BsG_N21'): 'BsG',
            Name(station, 'clean', 'BsR_N21'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['pops'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'Ns_N21'): 'Dp',
            Name(station, 'avgh', 'Nn_N21'): 'dNdlogDp',
            Name(station, 'avgh', 'Nb_N21'): 'dN',
            Name(station, 'avgh', 'N_N21'): 'N',
            Name(station, 'avgh', 'BsB_N21'): 'BsB',
            Name(station, 'avgh', 'BsG_N21'): 'BsG',
            Name(station, 'avgh', 'BsR_N21'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['realtime']['pops'] = {
        RealtimeTranslator.Key('Ns_N21'): 'Dp',
        RealtimeTranslator.Key('Nn_N21'): 'dNdlogDp',
        RealtimeTranslator.Key('Nb_N21'): 'dN',
    }

    station_profile_data['aerosol']['raw']['popsstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_N21'): 'Tpressure',
            Name(station, 'raw', 'T2_N21'): 'Tlaser',
            Name(station, 'raw', 'T3_N21'): 'Tinternal',

            Name(station, 'raw', 'Q_N21'): 'Qsample',

            Name(station, 'raw', 'P_N21'): 'Pboard',
        }, send
    )
    station_profile_data['aerosol']['realtime']['popsstatus'] = {
        RealtimeTranslator.Key('T1_N21'): 'Tpressure',
        RealtimeTranslator.Key('T2_N21'): 'Tlaser',
        RealtimeTranslator.Key('T3_N21'): 'Tinternal',

        RealtimeTranslator.Key('Q_N21'): 'Qsample',

        RealtimeTranslator.Key('P_N21'): 'Pboard',
    }


    station_profile_data['aerosol']['raw']['t640-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X_M11'): 'X',
        }, send
    )
    station_profile_data['aerosol']['raw']['t640-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X_M11', {'pm10'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['raw']['t640-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X_M11', {'pm25'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['raw']['t640-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X_M11', {'pm1'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['realtime']['t640-whole'] = {
        RealtimeTranslator.Key('X_M11'): 'X',
    }
    station_profile_data['aerosol']['realtime']['t640-pm10'] = {
        RealtimeTranslator.Key('X_M11', {'pm10'}): 'X',
    }
    station_profile_data['aerosol']['realtime']['t640-pm25'] = {
        RealtimeTranslator.Key('X_M11', {'pm25'}): 'X',
    }
    station_profile_data['aerosol']['realtime']['t640-pm1'] = {
        RealtimeTranslator.Key('X_M11', {'pm1'}): 'X',
    }
    station_profile_data['aerosol']['editing']['t640-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'X_M11'): 'X',
        }, send
    )
    station_profile_data['aerosol']['editing']['t640-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'X_M11', {'pm10'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['editing']['t640-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'X_M11', {'pm25'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['editing']['t640-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'X_M11', {'pm1'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['clean']['t640-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'X_M11'): 'X',
        }, send
    )
    station_profile_data['aerosol']['clean']['t640-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'X_M11', {'pm10'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['clean']['t640-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'X_M11', {'pm25'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['clean']['t640-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'X_M11', {'pm1'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['avgh']['t640-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'X_M11'): 'X',
        }, send
    )
    station_profile_data['aerosol']['avgh']['t640-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'X_M11', {'pm10'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['avgh']['t640-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'X_M11', {'pm25'}): 'X',
        }, send
    )
    station_profile_data['aerosol']['avgh']['t640-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'X_M11', {'pm1'}): 'X',
        }, send
    )

    station_profile_data['aerosol']['raw']['t640status'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_M11'): 'Tsample',
            Name(station, 'raw', 'T2_M11'): 'Tambient',
            Name(station, 'raw', 'T3_M11'): 'Tasc',
            Name(station, 'raw', 'T4_M11'): 'Tled',
            Name(station, 'raw', 'T5_M11'): 'Tbox',
            Name(station, 'raw', 'U1_M11'): 'Usample',
            Name(station, 'raw', 'P_M11'): 'Psample',
            Name(station, 'raw', 'Q1_M11'): 'Qsample',
            Name(station, 'raw', 'Q2_M11'): 'Qbypass',
        }, send
    )
    station_profile_data['aerosol']['realtime']['t640status'] = {
        RealtimeTranslator.Key('T1_M11'): 'Tsample',
        RealtimeTranslator.Key('T2_M11'): 'Tambient',
        RealtimeTranslator.Key('T3_M11'): 'Tasc',
        RealtimeTranslator.Key('T4_M11'): 'Tled',
        RealtimeTranslator.Key('T5_M11'): 'Tbox',
        RealtimeTranslator.Key('U1_M11'): 'Usample',
        RealtimeTranslator.Key('P_M11'): 'Psample',
        RealtimeTranslator.Key('Q1_M11'): 'Qsample',
        RealtimeTranslator.Key('Q2_M11'): 'Qbypass',
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
            Name(station, 'raw', 'Pd_P01'): 'pitot',
            Name(station, 'raw', 'Pd_P12'): 'vacuum',
            Name(station, 'raw', 'Pd_P12', {'pm10'}): 'vacuum',
            Name(station, 'raw', 'Pd_P12', {'pm1'}): 'vacuum',
            Name(station, 'raw', 'Pd_P12', {'pm25'}): 'vacuum',
            Name(station, 'raw', 'P_S11'): 'dPneph-whole',
            Name(station, 'raw', 'P_S11', {'pm10'}): 'dPneph-pm10',
            Name(station, 'raw', 'P_S11', {'pm25'}): 'dPneph-pm25',
            Name(station, 'raw', 'P_S11', {'pm1'}): 'dPneph-pm1',
        }, send
    )
    station_profile_data['aerosol']['realtime']['pressure'] = {
        RealtimeTranslator.Key('P_XM1'): 'ambient',
        RealtimeTranslator.Key('P_S11'): 'neph',
        RealtimeTranslator.Key('Pd_P01'): 'pitot',
        RealtimeTranslator.Key('Pd_P12'): 'vacuum',
        RealtimeTranslator.Key('Pd_P12', {'pm10'}): 'vacuum',
        RealtimeTranslator.Key('Pd_P12', {'pm1'}): 'vacuum',
        RealtimeTranslator.Key('Pd_P12', {'pm25'}): 'vacuum',
        RealtimeTranslator.Key('P_S11', {'pm10'}): 'neph',
        RealtimeTranslator.Key('P_S11', {'pm25'}): 'neph',
        RealtimeTranslator.Key('P_S11', {'pm1'}): 'neph',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, ozone_data, met_data, radiation_data, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_THREE_WAVELENGTHS, STANDARD_CUT_SIZE_SPLIT

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(ozone_data)
    data_records.update(met_data)
    data_records.update(radiation_data)

    for archive in ("raw", "edited", "clean", "avgh"):
        data_records[f"aerosol-{archive}-cnc"] = DataRecord({
            "cnc": [Selection(variable_name="number_concentration",
                              require_tags={"cpc"}, exclude_tags={"secondary"})],
            "cnc2": [Selection(variable_name="number_concentration", instrument_id="N72")],
        })
    data_records["aerosol-realtime-cnc"] = RealtimeRecord({
        "cnc": [RealtimeSelection("N", variable_name="number_concentration",
                                  require_tags={"cpc"}, exclude_tags={"secondary"})],
        "cnc2": [RealtimeSelection("N", variable_name="number_concentration",
                                   instrument_id="N72")],
    })

    data_records["aerosol-raw-cpcstatus2"] = DataRecord({
        "Tinlet": [Selection(variable_name="inlet_temperature", instrument_code="admagic200cpc"),
                   Selection(variable_name="inlet_temperature", instrument_code="admagic250cpc")],
        "TDinlet": [Selection(variable_name="inlet_dewpoint", instrument_code="admagic200cpc"),
                    Selection(variable_name="inlet_dewpoint", instrument_code="admagic250cpc")],
        "Uinlet": [Selection(variable_name="inlet_humidity", instrument_code="admagic200cpc"),
                   Selection(variable_name="inlet_humidity", instrument_code="admagic250cpc")],
        "Tconditioner": [Selection(variable_name="conditioner_temperature", instrument_code="admagic200cpc"),
                         Selection(variable_name="conditioner_temperature", instrument_code="admagic250cpc")],
        "Tinitiator": [Selection(variable_name="initiator_temperature", instrument_code="admagic200cpc"),
                       Selection(variable_name="initiator_temperature", instrument_code="admagic250cpc")],
        "Tmoderator": [Selection(variable_name="moderator_temperature", instrument_code="admagic200cpc"),
                       Selection(variable_name="moderator_temperature", instrument_code="admagic250cpc")],
        "Toptics": [Selection(variable_name="optics_temperature", instrument_code="admagic200cpc"),
                    Selection(variable_name="optics_temperature", instrument_code="admagic250cpc")],
        "Theatsink": [Selection(variable_name="heatsink_temperature", instrument_code="admagic200cpc"),
                      Selection(variable_name="heatsink_temperature", instrument_code="admagic250cpc")],
        "Tcase": [Selection(variable_name="case_temperature", instrument_code="admagic250cpc")],
        "Tboard": [Selection(variable_name="pcb_temperature", instrument_code="admagic200cpc"),
                   Selection(variable_name="board_temperature", instrument_code="admagic250cpc")],
        "TDgrowth": [Selection(variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc")],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="admagic200cpc"),
                    Selection(variable_name="sample_flow", instrument_code="admagic250cpc")],
        "Psample": [Selection(variable_name="pressure", instrument_code="admagic200cpc"),
                    Selection(variable_name="pressure", instrument_code="admagic250cpc")],
        "PCTwick": [Selection(variable_name="wick_saturation", instrument_code="admagic250cpc")],
        "Vpulse": [Selection(variable_name="pulse_height", instrument_code="admagic250cpc")],
    })
    data_records["aerosol-realtime-cpcstatus2"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic200cpc"),
                   RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic250cpc")],
        "TDinlet": [RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic200cpc"),
                    RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic250cpc")],
        "Uinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic200cpc"),
                   RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic250cpc")],
        "Tconditioner": [RealtimeSelection("Tconditioner", variable_name="conditioner_temperature", instrument_code="admagic200cpc"),
                         RealtimeSelection("Tconditioner", variable_name="conditioner_temperature", instrument_code="admagic250cpc")],
        "Tinitiator": [RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic200cpc"),
                       RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic250cpc")],
        "Tmoderator": [RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic200cpc"),
                       RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic250cpc")],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic200cpc"),
                    RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic250cpc")],
        "Theatsink": [RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic200cpc"),
                      RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic250cpc")],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature", instrument_code="admagic250cpc")],
        "Tboard": [RealtimeSelection("Tpcb", variable_name="pcb_temperature", instrument_code="admagic200cpc"),
                   RealtimeSelection("Tboard", variable_name="board_temperature", instrument_code="admagic250cpc")],
        "TDgrowth": [RealtimeSelection("TDgrowth", variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic200cpc"),
                    RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic250cpc")],
        "Psample": [RealtimeSelection("P", variable_name="pressure", instrument_code="admagic200cpc"),
                    RealtimeSelection("P", variable_name="pressure", instrument_code="admagic250cpc")],
        "PCTwick": [RealtimeSelection("PCTwick", variable_name="wick_saturation", instrument_code="admagic250cpc")],
        "Vpulse": [RealtimeSelection("Vpulse", variable_name="pulse_height", instrument_code="admagic250cpc")],
    })

    data_records["aerosol-raw-dmps"] = DataRecord({
        "Dp": [Selection(variable_id="Ns", instrument_id="N11")],
        "dNdlogDp": [Selection(variable_id="Nn", instrument_id="N11")],
        "dN": [Selection(variable_id="Nb", instrument_id="N11")],
        "Nraw": [Selection(variable_id="N", instrument_id="N12")],
    })
    data_records["aerosol-raw-dmpsstatus"] = DataRecord({
        "Taerosol": [Selection(variable_id="T1", instrument_id="N11")],
        "Tsheath": [Selection(variable_id="T2", instrument_id="N11")],
        "Paerosol": [Selection(variable_id="P1", instrument_id="N11")],
        "Psheath": [Selection(variable_id="P2", instrument_id="N11")],
        "Qaerosol": [Selection(variable_id="Q1", instrument_id="N11")],
        "Qsheath": [Selection(variable_id="Q2", instrument_id="N11")],
    })
    for archive in ("editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-dmps"] = DataRecord(dict([
            ("Dp", [Selection(variable_id="Ns", instrument_id="N11")]),
            ("dNdlogDp", [Selection(variable_id="Nn", instrument_id="N11")]),
            ("dN", [Selection(variable_id="Nb", instrument_id="N11")]),
            ("Nraw", [Selection(variable_id="N", instrument_id="N12")]),
            ("N", [Selection(variable_id="N", instrument_id="N11")])] + [
                (f"Bs{code}", [Selection(variable_id="Bs", wavelength=wavelength,
                                         instrument_id="N11")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
        )

    data_records["aerosol-raw-pops"] = DataRecord({
        "Dp": [Selection(variable_name="diameter", instrument_code="csdpops")],
        "dN": [Selection(variable_name="number_distribution", instrument_code="csdpops")],
    })
    data_records["aerosol-raw-popsstatus"] = DataRecord({
        "Tpressure": [Selection(variable_name="temperature_of_pressure", instrument_code="csdpops")],
        "Tlaser": [Selection(variable_name="laser_temperature", instrument_code="csdpops")],
        "Tinternal": [Selection(variable_name="internal_temperature", instrument_code="csdpops")],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="csdpops")],
        "Pboard": [Selection(variable_name="pressure", instrument_code="csdpops")],
    })
    data_records["aerosol-realtime-pops"] = RealtimeRecord({
        "Dp": [RealtimeSelection("Dp", variable_name="diameter", instrument_code="csdpops",
                                 variable_type=Selection.VariableType.State)],
        "dN": [RealtimeSelection("dN", variable_name="number_distribution", instrument_code="csdpops")],
    })
    data_records["aerosol-realtime-popsstatus"] = RealtimeRecord({
        "Tpressure": [RealtimeSelection("Tpressure", variable_name="temperature_of_pressure", instrument_code="csdpops")],
        "Tlaser": [RealtimeSelection("Tlaser", variable_name="laser_temperature", instrument_code="csdpops")],
        "Tinternal": [RealtimeSelection("Tinternal", variable_name="internal_temperature", instrument_code="csdpops")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="csdpops")],
        "Pboard": [RealtimeSelection("P", variable_name="pressure", instrument_code="csdpops")],
    })
    for archive in ("editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-pops"] = DataRecord(dict([
            ("Dp", [Selection(variable_name="diameter", instrument_code="csdpops")]),
            ("dNdlogDp", [Selection(variable_id="Nn", instrument_code="csdpops")]),
            ("dN", [Selection(variable_name="number_distribution", instrument_code="csdpops")])] + [
                (f"Bs{code}", [Selection(variable_id="Bs", wavelength=wavelength,
                                         instrument_code="csdpops")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
        )

    data_records["aerosol-raw-t640status"] = DataRecord({
        "Tsample": [Selection(variable_name="sample_temperature", instrument_code="teledynet640")],
        "Tambient": [Selection(variable_name="ambient_temperature", instrument_code="teledynet640")],
        "Tasc": [Selection(variable_name="asc_temperature", instrument_code="teledynet640")],
        "Tled": [Selection(variable_name="led_temperature", instrument_code="teledynet640")],
        "Tbox": [Selection(variable_name="box_temperature", instrument_code="teledynet640")],
        "Usample": [Selection(variable_name="sample_humidity", instrument_code="teledynet640")],
        "Psample": [Selection(variable_name="pressure", instrument_code="teledynet640")],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="teledynet640")],
        "Qbypass": [Selection(variable_name="bypass_flow", instrument_code="teledynet640")],
    })
    data_records["aerosol-realtime-t640status"] = RealtimeRecord({
        "Tsample": [RealtimeSelection("Tsample", variable_name="sample_temperature", instrument_code="teledynet640")],
        "Tambient": [RealtimeSelection("Tambient", variable_name="ambient_temperature", instrument_code="teledynet640")],
        "Tasc": [RealtimeSelection("Tasc", variable_name="asc_temperature", instrument_code="teledynet640")],
        "Tled": [RealtimeSelection("Tled", variable_name="led_temperature", instrument_code="teledynet640")],
        "Tbox": [RealtimeSelection("Tbox", variable_name="box_temperature", instrument_code="teledynet640")],
        "Usample": [RealtimeSelection("Usample", variable_name="sample_humidity", instrument_code="teledynet640")],
        "Psample": [RealtimeSelection("Psample", variable_name="pressure", instrument_code="teledynet640")],
        "Qsample": [RealtimeSelection("Qsample", variable_name="sample_flow", instrument_code="teledynet640")],
        "Qbypass": [RealtimeSelection("Qbypass", variable_name="bypass_flow", instrument_code="teledynet640")],
    })
    for archive in ("raw", "editing", "clean", "avgh"):
        for record, selected_size in (("whole", nan), ("pm10", 10.0), ("pm25", 2.5), ("pm1", 1.0)):
            data_records[f"aerosol-{archive}-t640-{record}"] = DataRecord({
                "X": [
                    Selection(variable_name="mass_concentration", instrument_code="teledynet640",
                              dimension_at=(("diameter", selected_size), )),
                ],
            })
    for record, selected_size, suffix in (("pm10", 10.0, "10"), ("pm25", 2.5, "25"), ("pm1", 1.0, "1")):
        data_records[f"aerosol-realtime-t640-{record}"] = RealtimeRecord({
            "X": [RealtimeSelection(f"X{suffix}", variable_name="mass_concentration",
                                    instrument_code="teledynet640",
                                    dimension_at=(("diameter", selected_size),))],
        })

    data_records["aerosol-raw-nephzero2"] = DataRecord(dict([
        (f"Bsw{code}", [Selection(variable_name="wall_scattering_coefficient", wavelength=wavelength,
                                  variable_type=Selection.VariableType.State,
                                  instrument_id="S12")])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        (f"Bbsw{code}", [Selection(variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                                  variable_type=Selection.VariableType.State,
                                  instrument_id="S12")])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))
    data_records["aerosol-raw-nephstatus2"] = DataRecord({
        "CfG": [Selection(variable_name="reference_counts", wavelength=(500, 600),
                          instrument_id="S12")],
    })
    data_records["aerosol-realtime-nephzero2"] = RealtimeRecord(dict([
        (f"Bsw{code}", [RealtimeSelection(f"Bsw{code}", variable_name="wall_scattering_coefficient", wavelength=wavelength,
                                          variable_type=Selection.VariableType.State,
                                          instrument_id="S12")])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        (f"Bbsw{code}", [RealtimeSelection(f"Bbsw{code}", variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                                           variable_type=Selection.VariableType.State,
                                           instrument_id="S12")])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))
    data_records["aerosol-realtime-nephstatus2"] = RealtimeRecord({
        "CfG": [RealtimeSelection("CfG", variable_name="reference_counts", wavelength=(500, 600),
                                  instrument_id="S12")],
    })
    for archive in ("raw", "editing", "clean", "avgh"):
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
            data_records[f"aerosol-{archive}-scattering2-{record}"] = DataRecord(dict([
                (f"Bs{code}", [Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                         instrument_id="S12")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ] + [
                (f"Bbs{code}", [Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                         instrument_id="S12")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ]))
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        data_records[f"aerosol-realtime-scattering2-{record}"] = RealtimeRecord(dict([
            (f"Bs{code}", [RealtimeSelection(f"Bs{code}", variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                             instrument_id="S12")])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            (f"Bbs{code}", [RealtimeSelection(f"Bbs{code}", variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                              instrument_id="S12")])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))

    data_records["aerosol-raw-pressure"] = DataRecord(dict([
        ("ambient", [Selection(variable_id="P", instrument_id="XM1")]),
        ("pitot", [Selection(variable_id="Pd_P01")]),
        ("vacuum", [Selection(variable_id="Pd_P12")])] + [
        (f"dPneph-{cut_size}", [Selection(variable_name="sample_pressure", cut_size=cut_size,
                                          require_tags={"scattering"}, exclude_tags={"secondary"})])
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ]))
    data_records["aerosol-realtime-pressure"] = RealtimeRecord(dict([
        ("ambient", [RealtimeSelection("P", variable_id="P", instrument_id="XM1")]),
        ("pitot", [RealtimeSelection("Pd_P01", variable_id="Pd_P01")]),
        ("vacuum", [RealtimeSelection("Pd_P12", variable_id="Pd_P12")])] + [
        (f"dPneph-{cut_size}", [RealtimeSelection("Psample", variable_name="sample_pressure", cut_size=cut_size,
                                                  require_tags={"scattering"}, exclude_tags={"secondary"})])
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ]))


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
