import typing
from ..cpd3 import Export, ExportList, DataExportList, DataExport, Name, export_profile_get, export_profile_lookup, detach, profile_export


station_profile_export = detach(profile_export)


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'raw', 'F1_A21'),
            Name(station, 'raw', 'P_A21'),
            Name(station, 'raw', 'IfR_A21'),
            Name(station, 'raw', 'IpR_A21'),
            Name(station, 'raw', 'IrR_A21'),
            Name(station, 'raw', 'Is1_A21'),
            Name(station, 'raw', 'Is2_A21'),
            Name(station, 'raw', 'Pd1_A21'),
            Name(station, 'raw', 'Pd2_A21'),
            Name(station, 'raw', 'Q_A21'),
            Name(station, 'raw', 'Qt_A21'),
            Name(station, 'raw', 'T1_A21'),
            Name(station, 'raw', 'T2_A21'),
            Name(station, 'raw', 'T3_A21'),
            Name(station, 'raw', 'XR_A21'),
        },
    )),
)
station_profile_export['aerosol']['clean'].insert(
    DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'clean', 'F1_A21'),
            Name(station, 'clean', 'P_A21'),
            Name(station, 'clean', 'IfR_A21'),
            Name(station, 'clean', 'IpR_A21'),
            Name(station, 'clean', 'IrR_A21'),
            Name(station, 'clean', 'Is1_A21'),
            Name(station, 'clean', 'Is2_A21'),
            Name(station, 'clean', 'Pd1_A21'),
            Name(station, 'clean', 'Pd2_A21'),
            Name(station, 'clean', 'Q_A21'),
            Name(station, 'clean', 'Qt_A21'),
            Name(station, 'clean', 'T1_A21'),
            Name(station, 'clean', 'T2_A21'),
            Name(station, 'clean', 'T3_A21'),
            Name(station, 'clean', 'XR_A21'),
        },
    )),
)
station_profile_export['aerosol']['avgh'].insert(
    DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', {
            Name(station, 'avgh', 'F1_A21'),
            Name(station, 'avgh', 'P_A21'),
            Name(station, 'avgh', 'IfR_A21'),
            Name(station, 'avgh', 'IpR_A21'),
            Name(station, 'avgh', 'IrR_A21'),
            Name(station, 'avgh', 'Is1_A21'),
            Name(station, 'avgh', 'Is2_A21'),
            Name(station, 'avgh', 'Pd1_A21'),
            Name(station, 'avgh', 'Pd2_A21'),
            Name(station, 'avgh', 'Q_A21'),
            Name(station, 'avgh', 'T1_A21'),
            Name(station, 'avgh', 'T2_A21'),
            Name(station, 'avgh', 'T3_A21'),
            Name(station, 'avgh', 'XR_A21'),
        },
    )),
)


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('nephzero', "Nephelometer Zero", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'raw', 'Tw_S11'),
            Name(station, 'raw', 'Pw_S11'),
            Name(station, 'raw', 'BswB_S11'),
            Name(station, 'raw', 'BswG_S11'),
            Name(station, 'raw', 'BswR_S11'),
            Name(station, 'raw', 'BbswB_S11'),
            Name(station, 'raw', 'BbswG_S11'),
            Name(station, 'raw', 'BbswR_S11'),
        },
    )),
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
