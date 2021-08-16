import typing
from ..cpd3 import Export, ExportList, DataExportList, DataExport, Name, export_profile_get, export_profile_lookup, detach, profile_export


station_profile_export = detach(profile_export)

station_profile_export['aerosol']['raw']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms: DataExport(
    start_epoch_ms, end_epoch_ms, 'unsplit', {
        Name(station, 'raw', 'N_N31'),
        Name(station, 'raw', 'N_N41'),
        Name(station, 'raw', 'N_N42'),
    },
)
station_profile_export['aerosol']['clean']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms: DataExport(
    start_epoch_ms, end_epoch_ms, 'unsplit', {
        Name(station, 'clean', 'N_N31'),
        Name(station, 'clean', 'N_N41'),
        Name(station, 'clean', 'N_N42'),
    },
)
station_profile_export['aerosol']['avgh']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms: DataExport(
    start_epoch_ms, end_epoch_ms, 'average', {
        Name(station, 'avgh', 'N_N31'),
        Name(station, 'avgh', 'N_N41'),
        Name(station, 'avgh', 'N_N42'),
    },
)

station_profile_export['aerosol']['raw']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms: DataExport(
    start_epoch_ms, end_epoch_ms, 'unsplit', set(
        [Name(station, 'raw', f'Ba{i + 1}_A82') for i in range(7)] +
        [Name(station, 'raw', f'X{i + 1}_A82') for i in range(7)] +
        [Name(station, 'raw', f'ZFACTOR{i + 1}_A82') for i in range(7)] +
        [Name(station, 'raw', f'Ir{i + 1}_A82') for i in range(7)]
    )
)
station_profile_export['aerosol']['clean']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms: DataExport(
    start_epoch_ms, end_epoch_ms, 'unsplit', set(
        [Name(station, 'clean', f'Ba{i + 1}_A82') for i in range(7)] +
        [Name(station, 'clean', f'X{i + 1}_A82') for i in range(7)] +
        [Name(station, 'clean', f'ZFACTOR{i + 1}_A82') for i in range(7)] +
        [Name(station, 'clean', f'Ir{i + 1}_A82') for i in range(7)]
    )
)
station_profile_export['aerosol']['avgh']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms: DataExport(
    start_epoch_ms, end_epoch_ms, 'average', set(
        [Name(station, 'avgh', f'Ba{i + 1}_A82') for i in range(7)] +
        [Name(station, 'avgh', f'X{i + 1}_A82') for i in range(7)] +
        [Name(station, 'avgh', f'ZFACTOR{i + 1}_A82') for i in range(7)] +
        [Name(station, 'avgh', f'Ir{i + 1}_A82') for i in range(7)]
    )
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
