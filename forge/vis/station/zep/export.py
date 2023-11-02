import typing
from ..cpd3 import Export, ExportList, DataExportList, DataExport, Name, export_profile_get, export_profile_lookup, detach, profile_export


station_profile_export = detach(profile_export)

station_profile_export['aerosol']['raw'].remove('aethalometer')
station_profile_export['aerosol']['clean'].remove('aethalometer')
station_profile_export['aerosol']['avgh'].remove('aethalometer')
station_profile_export['aerosol']['raw'].remove('extensive')
station_profile_export['aerosol']['clean'].remove('extensive')
station_profile_export['aerosol']['avgh'].remove('extensive')
station_profile_export['aerosol']['raw'].remove('absorption')
station_profile_export['aerosol']['clean'].remove('absorption')
station_profile_export['aerosol']['avgh'].remove('absorption')


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'raw', 'F1_A31'),
            Name(station, 'raw', 'P_A31'),
            Name(station, 'raw', 'IfR_A31'),
            Name(station, 'raw', 'IpR_A31'),
            Name(station, 'raw', 'IrR_A31'),
            Name(station, 'raw', 'Is1_A31'),
            Name(station, 'raw', 'Is2_A31'),
            Name(station, 'raw', 'Pd1_A31'),
            Name(station, 'raw', 'Pd2_A31'),
            Name(station, 'raw', 'Q_A31'),
            Name(station, 'raw', 'Qt_A31'),
            Name(station, 'raw', 'T1_A31'),
            Name(station, 'raw', 'T2_A31'),
            Name(station, 'raw', 'T3_A31'),
            Name(station, 'raw', 'XR_A31'),
        },
    )),
)
station_profile_export['aerosol']['clean'].insert(
    DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'clean', 'F1_A31'),
            Name(station, 'clean', 'P_A31'),
            Name(station, 'clean', 'IfR_A31'),
            Name(station, 'clean', 'IpR_A31'),
            Name(station, 'clean', 'IrR_A31'),
            Name(station, 'clean', 'Is1_A31'),
            Name(station, 'clean', 'Is2_A31'),
            Name(station, 'clean', 'Pd1_A31'),
            Name(station, 'clean', 'Pd2_A31'),
            Name(station, 'clean', 'Q_A31'),
            Name(station, 'clean', 'Qt_A31'),
            Name(station, 'clean', 'T1_A31'),
            Name(station, 'clean', 'T2_A31'),
            Name(station, 'clean', 'T3_A31'),
            Name(station, 'clean', 'XR_A31'),
        },
    )),
)
station_profile_export['aerosol']['avgh'].insert(
    DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', {
            Name(station, 'avgh', 'F1_A31'),
            Name(station, 'avgh', 'P_A31'),
            Name(station, 'avgh', 'IfR_A31'),
            Name(station, 'avgh', 'IpR_A31'),
            Name(station, 'avgh', 'IrR_A31'),
            Name(station, 'avgh', 'Is1_A31'),
            Name(station, 'avgh', 'Is2_A31'),
            Name(station, 'avgh', 'Pd1_A31'),
            Name(station, 'avgh', 'Pd2_A31'),
            Name(station, 'avgh', 'Q_A31'),
            Name(station, 'avgh', 'T1_A31'),
            Name(station, 'avgh', 'T2_A31'),
            Name(station, 'avgh', 'T3_A31'),
            Name(station, 'avgh', 'XR_A31'),
        },
    ), time_limit_days=None),
)


station_profile_export['aerosol']['raw']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'basic', {
        Name(station, 'raw', 'T_S13'),
        Name(station, 'raw', 'P_S13'),
        Name(station, 'raw', 'U_S13'),
        Name(station, 'raw', 'T_S41'),
        Name(station, 'raw', 'P_S41'),
        Name(station, 'raw', 'U_S41'),
        Name(station, 'raw', 'BsB_S13'),
        Name(station, 'raw', 'BsG_S13'),
        Name(station, 'raw', 'BsR_S13'),
        Name(station, 'raw', 'BbsB_S13'),
        Name(station, 'raw', 'BbsG_S13'),
        Name(station, 'raw', 'BbsR_S13'),
        Name(station, 'raw', 'BsB_S41'),
        Name(station, 'raw', 'BsG_S41'),
        Name(station, 'raw', 'BsR_S41'),
        Name(station, 'raw', 'BbsB_S41'),
        Name(station, 'raw', 'BbsG_S41'),
        Name(station, 'raw', 'BbsR_S41'),
    },
)
station_profile_export['aerosol']['clean']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'basic', {
        Name(station, 'clean', 'T_S13'),
        Name(station, 'clean', 'P_S13'),
        Name(station, 'clean', 'U_S13'),
        Name(station, 'clean', 'T_S41'),
        Name(station, 'clean', 'P_S41'),
        Name(station, 'clean', 'U_S41'),
        Name(station, 'clean', 'BsB_S13'),
        Name(station, 'clean', 'BsG_S13'),
        Name(station, 'clean', 'BsR_S13'),
        Name(station, 'clean', 'BbsB_S13'),
        Name(station, 'clean', 'BbsG_S13'),
        Name(station, 'clean', 'BbsR_S13'),
        Name(station, 'clean', 'BsB_S41'),
        Name(station, 'clean', 'BsG_S41'),
        Name(station, 'clean', 'BsR_S41'),
        Name(station, 'clean', 'BbsB_S41'),
        Name(station, 'clean', 'BbsG_S41'),
        Name(station, 'clean', 'BbsR_S41'),
    },
)
station_profile_export['aerosol']['avgh']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'basic', {
        Name(station, 'avgh', 'T_S13'),
        Name(station, 'avgh', 'P_S13'),
        Name(station, 'avgh', 'U_S13'),
        Name(station, 'avgh', 'T_S41'),
        Name(station, 'avgh', 'P_S41'),
        Name(station, 'avgh', 'U_S41'),
        Name(station, 'avgh', 'BsB_S13'),
        Name(station, 'avgh', 'BsG_S13'),
        Name(station, 'avgh', 'BsR_S13'),
        Name(station, 'avgh', 'BbsB_S13'),
        Name(station, 'avgh', 'BbsG_S13'),
        Name(station, 'avgh', 'BbsR_S13'),
        Name(station, 'avgh', 'BsB_S41'),
        Name(station, 'avgh', 'BsG_S41'),
        Name(station, 'avgh', 'BsR_S41'),
        Name(station, 'avgh', 'BbsB_S41'),
        Name(station, 'avgh', 'BbsG_S41'),
        Name(station, 'avgh', 'BbsR_S41'),
    },
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
