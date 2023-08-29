import typing
from ..cpd3 import Export, ExportList, DataExportList, DataExport, NativeExport, Name, export_profile_get, export_profile_lookup, detach, profile_export


station_profile_export = detach(profile_export)


station_profile_export['aerosol']['raw']['aethalometer'].display = "Aethalometer (A41)"
station_profile_export['aerosol']['raw']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
        [Name(station, 'raw', f'Ba{i + 1}_A41') for i in range(7)] +
        [Name(station, 'raw', f'X{i + 1}_A41') for i in range(7)] +
        [Name(station, 'raw', f'ZFACTOR{i + 1}_A41') for i in range(7)] +
        [Name(station, 'raw', f'Ir{i + 1}_A41') for i in range(7)]
    )
)
station_profile_export['aerosol']['clean']['aethalometer'].display = "Aethalometer (A41)"
station_profile_export['aerosol']['clean']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
        [Name(station, 'clean', f'Ba{i + 1}_A41') for i in range(7)] +
        [Name(station, 'clean', f'X{i + 1}_A41') for i in range(7)] +
        [Name(station, 'clean', f'ZFACTOR{i + 1}_A41') for i in range(7)] +
        [Name(station, 'clean', f'Ir{i + 1}_A41') for i in range(7)]
    )
)
station_profile_export['aerosol']['avgh']['aethalometer'].display = "Aethalometer (A41)"
station_profile_export['aerosol']['avgh']['aethalometer'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'average', set(
        [Name(station, 'avgh', f'Ba{i + 1}_A41') for i in range(7)] +
        [Name(station, 'avgh', f'X{i + 1}_A41') for i in range(7)] +
        [Name(station, 'avgh', f'ZFACTOR{i + 1}_A41') for i in range(7)] +
        [Name(station, 'avgh', f'Ir{i + 1}_A41') for i in range(7)]
    )
)


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('aethalometer2', "Aethalometer (A42)", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'raw', f'Ba{i + 1}_A42') for i in range(7)] +
            [Name(station, 'raw', f'X{i + 1}_A42') for i in range(7)] +
            [Name(station, 'raw', f'ZFACTOR{i + 1}_A42') for i in range(7)] +
            [Name(station, 'raw', f'Ir{i + 1}_A42') for i in range(7)]
        )
    )),
)
station_profile_export['aerosol']['clean'].insert(
    DataExportList.Entry('aethalometer2', "Aethalometer (A42)", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
            [Name(station, 'clean', f'Ba{i + 1}_A42') for i in range(7)] +
            [Name(station, 'clean', f'X{i + 1}_A42') for i in range(7)] +
            [Name(station, 'clean', f'ZFACTOR{i + 1}_A42') for i in range(7)] +
            [Name(station, 'clean', f'Ir{i + 1}_A42') for i in range(7)]
        ),
    )),
)
station_profile_export['aerosol']['avgh'].insert(
    DataExportList.Entry('aethalometer2', "Aethalometer (A42)", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'average', set(
            [Name(station, 'avgh', f'Ba{i + 1}_A42') for i in range(7)] +
            [Name(station, 'avgh', f'X{i + 1}_A42') for i in range(7)] +
            [Name(station, 'avgh', f'ZFACTOR{i + 1}_A42') for i in range(7)] +
            [Name(station, 'avgh', f'Ir{i + 1}_A42') for i in range(7)]
        ),
    )),
)


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('maap', "MAAP", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
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
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
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
    )),
)


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('cpd3native', "CPD3 Native Format", lambda station, start_epoch_ms, end_epoch_ms, directory: NativeExport(
        start_epoch_ms, end_epoch_ms, directory, station, 'raw',
    ))
)
station_profile_export['aerosol']['clean'].insert(
    DataExportList.Entry('cpd3native', "CPD3 Native Format", lambda station, start_epoch_ms, end_epoch_ms, directory: NativeExport(
        start_epoch_ms, end_epoch_ms, directory, station, 'clean',
    ))
)
station_profile_export['aerosol']['avgh'].insert(
    DataExportList.Entry('cpd3native', "CPD3 Native Format", lambda station, start_epoch_ms, end_epoch_ms, directory: NativeExport(
        start_epoch_ms, end_epoch_ms, directory, station, 'avgh',
    ))
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
