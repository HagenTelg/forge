import typing
from ..cpd3 import Export, ExportList, DataExportList, DataExport, Name, export_profile_get, export_profile_lookup, detach, profile_export


station_profile_export = detach(profile_export)


station_profile_export['aerosol']['raw']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
        [Name(station, 'raw', f'Ba{i + 1}_A41') for i in range(7)] +
        [Name(station, 'raw', f'X{i + 1}_A41') for i in range(7)] +
        [Name(station, 'raw', f'ZFACTOR{i + 1}_A41') for i in range(7)] +
        [Name(station, 'raw', f'Ir{i + 1}_A41') for i in range(7)]
    )
)
station_profile_export['aerosol']['clean']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
        [Name(station, 'clean', f'Ba{i + 1}_A41') for i in range(7)] +
        [Name(station, 'clean', f'X{i + 1}_A41') for i in range(7)] +
        [Name(station, 'clean', f'ZFACTOR{i + 1}_A41') for i in range(7)] +
        [Name(station, 'clean', f'Ir{i + 1}_A41') for i in range(7)]
    )
)
station_profile_export['aerosol']['avgh']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'average', set(
        [Name(station, 'avgh', f'Ba{i + 1}_A41') for i in range(7)] +
        [Name(station, 'avgh', f'X{i + 1}_A41') for i in range(7)] +
        [Name(station, 'avgh', f'ZFACTOR{i + 1}_A41') for i in range(7)] +
        [Name(station, 'avgh', f'Ir{i + 1}_A41') for i in range(7)]
    )
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
