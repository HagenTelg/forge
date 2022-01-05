import typing
from ..cpd3 import DataStream, DataReader, EditedReader, ContaminationReader, EditedContaminationReader, Name, data_profile_get


sail_splash: typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]] = {
    'raw': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'F1_RMSURFRAD'),
                Name(station, 'raw', 'F1_RADSYS2'),
            }, send
        ),

        'solar': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Rdg_RMSURFRAD'): 'Rdg_RMSURFRAD',
                Name(station, 'raw', 'Rug_RMSURFRAD'): 'Rug_RMSURFRAD',
                Name(station, 'raw', 'Rdn_RMSURFRAD'): 'Rdn_RMSURFRAD',
                Name(station, 'raw', 'Rdf_RMSURFRAD'): 'Rdf_RMSURFRAD',
                
                Name(station, 'raw', 'Rdg_RADSYS2'): 'Rdg_RADSYS2',
                Name(station, 'raw', 'Rug_RADSYS2'): 'Rug_RADSYS2',
                Name(station, 'raw', 'Rdn_RADSYS2'): 'Rdn_RADSYS2',
                Name(station, 'raw', 'Rdf_RADSYS2'): 'Rdf_RADSYS2',
                Name(station, 'raw', 'Rst_RADSYS2'): 'Rst_RADSYS2',
                Name(station, 'raw', 'Rsd_RADSYS2'): 'Rsd_RADSYS2',
            }, send
        ),

        'ir': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Rdi_RMSURFRAD'): 'Rdi_RMSURFRAD',
                Name(station, 'raw', 'Rui_RMSURFRAD'): 'Rui_RMSURFRAD',

                Name(station, 'raw', 'Rdi_RADSYS2'): 'Rdi_RADSYS2',
                Name(station, 'raw', 'Rui_RADSYS2'): 'Rui_RADSYS2',
            }, send
        ),

        'pyranometertemperature': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Tdic_RMSURFRAD'): 'Tdic_RMSURFRAD',
                Name(station, 'raw', 'Tdid_RMSURFRAD'): 'Tdid_RMSURFRAD',
                Name(station, 'raw', 'Tuic_RMSURFRAD'): 'Tuic_RMSURFRAD',
                Name(station, 'raw', 'Tuid_RMSURFRAD'): 'Tuid_RMSURFRAD',
                Name(station, 'raw', 'T_RMSURFRAD'): 'T_RMSURFRAD',
                
                Name(station, 'raw', 'Tdic_RADSYS2'): 'Tdic_RADSYS2',
                Name(station, 'raw', 'Tdid_RADSYS2'): 'Tdid_RADSYS2',
                Name(station, 'raw', 'Tuic_RADSYS2'): 'Tuic_RADSYS2',
                Name(station, 'raw', 'Tuid_RADSYS2'): 'Tuid_RADSYS2',
                Name(station, 'raw', 'T_RADSYS2'): 'T_RADSYS2',
            }, send
        ),

        'albedo-rmsurfrad': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Rdg_RMSURFRAD'): 'down',
                Name(station, 'raw', 'Rug_RMSURFRAD'): 'up',
                Name(station, 'raw', 'ZSA_RMSURFRAD'): 'zsa',
            }, send
        ),
        'albedo-radsys2': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Rdg_RADSYS2'): 'down',
                Name(station, 'raw', 'Rug_RADSYS2'): 'up',
                Name(station, 'raw', 'ZSA_RADSYS2'): 'zsa',
            }, send
        ),

        'totalratio-rmsurfrad': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Rdn_RMSURFRAD'): 'direct',
                Name(station, 'raw', 'Rdf_RMSURFRAD'): 'diffuse',
                Name(station, 'raw', 'Rdg_RMSURFRAD'): 'global',
            }, send
        ),
        'totalratio-radsys2': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Rst_RADSYS2'): 'total',
                Name(station, 'raw', 'Rdg_RADSYS2'): 'global',
            }, send
        ),

        'ambient': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T_RMSURFRAD'): 'T_RMSURFRAD',
                Name(station, 'raw', 'U_RMSURFRAD'): 'U_RMSURFRAD',
                Name(station, 'raw', 'P_RMSURFRAD'): 'P_RMSURFRAD',
                Name(station, 'raw', 'WS_RMSURFRAD'): 'WS_RMSURFRAD',
                Name(station, 'raw', 'WD_RMSURFRAD'): 'WD_RMSURFRAD',
                
                Name(station, 'raw', 'T_RADSYS2'): 'T_RADSYS2',
                Name(station, 'raw', 'U_RADSYS2'): 'U_RADSYS2',
                Name(station, 'raw', 'P_RADSYS2'): 'P_RADSYS2',
                Name(station, 'raw', 'WS_RADSYS2'): 'WS_RADSYS2',
                Name(station, 'raw', 'WD_RADSYS2'): 'WD_RADSYS2',
            }, send
        ),
    },

    'editing': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: EditedContaminationReader(
            start_epoch_ms, end_epoch_ms, station, 'radiation', {
                Name(station, 'clean', 'F1_RMSURFRAD'),
                Name(station, 'clean', 'F1_RADSYS2'),
            }, send
        ),

        'solar': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'radiation', {
                Name(station, 'clean', 'Rdg_RMSURFRAD'): 'Rdg_RMSURFRAD',
                Name(station, 'clean', 'Rug_RMSURFRAD'): 'Rug_RMSURFRAD',
                Name(station, 'clean', 'Rdn_RMSURFRAD'): 'Rdn_RMSURFRAD',
                Name(station, 'clean', 'Rdf_RMSURFRAD'): 'Rdf_RMSURFRAD',

                Name(station, 'clean', 'Rdg_RADSYS2'): 'Rdg_RADSYS2',
                Name(station, 'clean', 'Rug_RADSYS2'): 'Rug_RADSYS2',
                Name(station, 'clean', 'Rdn_RADSYS2'): 'Rdn_RADSYS2',
                Name(station, 'clean', 'Rdf_RADSYS2'): 'Rdf_RADSYS2',
                Name(station, 'clean', 'Rst_RADSYS2'): 'Rst_RADSYS2',
                Name(station, 'clean', 'Rsd_RADSYS2'): 'Rsd_RADSYS2',
            }, send
        ),

        'ir': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'radiation', {
                Name(station, 'clean', 'Rdi_RMSURFRAD'): 'Rdi_RMSURFRAD',
                Name(station, 'clean', 'Rui_RMSURFRAD'): 'Rui_RMSURFRAD',

                Name(station, 'clean', 'Rdi_RADSYS2'): 'Rdi_RADSYS2',
                Name(station, 'clean', 'Rui_RADSYS2'): 'Rui_RADSYS2',
            }, send
        ),

        'pyranometertemperature': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'radiation', {
                Name(station, 'clean', 'Tdic_RMSURFRAD'): 'Tdic_RMSURFRAD',
                Name(station, 'clean', 'Tdid_RMSURFRAD'): 'Tdid_RMSURFRAD',
                Name(station, 'clean', 'Tuic_RMSURFRAD'): 'Tuic_RMSURFRAD',
                Name(station, 'clean', 'Tuid_RMSURFRAD'): 'Tuid_RMSURFRAD',
                Name(station, 'clean', 'T_RMSURFRAD'): 'T_RMSURFRAD',

                Name(station, 'clean', 'Tdic_RADSYS2'): 'Tdic_RADSYS2',
                Name(station, 'clean', 'Tdid_RADSYS2'): 'Tdid_RADSYS2',
                Name(station, 'clean', 'Tuic_RADSYS2'): 'Tuic_RADSYS2',
                Name(station, 'clean', 'Tuid_RADSYS2'): 'Tuid_RADSYS2',
                Name(station, 'clean', 'T_RADSYS2'): 'T_RADSYS2',
            }, send
        ),
        
        'albedo-rmsurfrad': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'radiation', {
                Name(station, 'clean', 'Rdg_RMSURFRAD'): 'down',
                Name(station, 'clean', 'Rug_RMSURFRAD'): 'up',
                Name(station, 'clean', 'ZSA_RMSURFRAD'): 'zsa',
            }, send
        ),
        'albedo-radsys2': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'radiation', {
                Name(station, 'clean', 'Rdg_RADSYS2'): 'down',
                Name(station, 'clean', 'Rug_RADSYS2'): 'up',
                Name(station, 'clean', 'ZSA_RADSYS2'): 'zsa',
            }, send
        ),
        
        'totalratio-rmsurfrad': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'radiation', {
                Name(station, 'clean', 'Rdn_RMSURFRAD'): 'direct',
                Name(station, 'clean', 'Rdf_RMSURFRAD'): 'diffuse',
                Name(station, 'clean', 'Rdg_RMSURFRAD'): 'global',
            }, send
        ),
        'totalratio-radsys2': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'radiation', {
                Name(station, 'clean', 'Rst_RADSYS2'): 'total',
                Name(station, 'clean', 'Rdg_RADSYS2'): 'global',
            }, send
        ),

        'ambient': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'radiation', {
                Name(station, 'clean', 'T_RMSURFRAD'): 'T_RMSURFRAD',
                Name(station, 'clean', 'U_RMSURFRAD'): 'U_RMSURFRAD',
                Name(station, 'clean', 'P_RMSURFRAD'): 'P_RMSURFRAD',
                Name(station, 'clean', 'WS_RMSURFRAD'): 'WS_RMSURFRAD',
                Name(station, 'clean', 'WD_RMSURFRAD'): 'WD_RMSURFRAD',

                Name(station, 'clean', 'T_RADSYS2'): 'T_RADSYS2',
                Name(station, 'clean', 'U_RADSYS2'): 'U_RADSYS2',
                Name(station, 'clean', 'P_RADSYS2'): 'P_RADSYS2',
                Name(station, 'clean', 'WS_RADSYS2'): 'WS_RADSYS2',
                Name(station, 'clean', 'WD_RADSYS2'): 'WD_RADSYS2',
            }, send
        ),
    },
}

profile_data = {
    'radiation': sail_splash
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, profile_data)
