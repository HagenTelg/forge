import typing
import asyncio
import logging
import time
from pathlib import Path
from math import floor, ceil
from ..cpd3 import Export, ExportList, DataExportList, export_profile_get, export_profile_lookup, detach, profile_export, CONFIGURATION

_LOGGER = logging.getLogger(__name__)
_interface = CONFIGURATION.get('CPD3.INTERFACE', 'cpd3_forge_interface')

station_profile_export = detach(profile_export)


class NativeExport(Export):
    def __init__(self, start_epoch_ms: int, end_epoch_ms: int, directory: str, archive: str):
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))
        self.directory = directory
        self.archive = archive

    def export_file_name(self) -> typing.Optional[str]:
        ts = time.gmtime(self.start_epoch)
        return f"CPR-{self.archive.upper()}_{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.c3r"

    async def __call__(self) -> Export.Result:
        _LOGGER.debug(f"Starting native export for {self.start_epoch},{self.end_epoch}")

        target_file = (Path(self.directory) / self.export_file_name()).open('wb')
        exporter = await asyncio.create_subprocess_exec(_interface, 'archive_read',
                                                        str(self.start_epoch), str(self.end_epoch),
                                                        f"cpr:{self.archive}:",
                                                        f"cpr:{self.archive}_meta:",
                                                        stdout=target_file,
                                                        stdin=asyncio.subprocess.DEVNULL)
        await exporter.communicate()
        return Export.Result()


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('cpd3native', "CPD3 Native Format", lambda station, start_epoch_ms, end_epoch_ms, directory: NativeExport(
        start_epoch_ms, end_epoch_ms, directory, 'raw',
    )),
)
station_profile_export['aerosol']['clean'].insert(
    DataExportList.Entry('cpd3native', "CPD3 Native Format", lambda station, start_epoch_ms, end_epoch_ms, directory: NativeExport(
        start_epoch_ms, end_epoch_ms, directory, 'clean',
    )),
)
station_profile_export['aerosol']['avgh'].insert(
    DataExportList.Entry('cpd3native', "CPD3 Native Format", lambda station, start_epoch_ms, end_epoch_ms, directory: NativeExport(
        start_epoch_ms, end_epoch_ms, directory, 'avgh',
    )),
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
