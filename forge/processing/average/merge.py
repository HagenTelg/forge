import typing
import os
import logging
from netCDF4 import Dataset
from forge.data.merge.instrument import MergeInstrument

_LOGGER = logging.getLogger(__name__)


def merge_files(station: str, input_files: typing.List[str], output_file: str) -> None:
    _LOGGER.debug("Merging %d inputs to file %s:%s", len(input_files), station.upper(), output_file)
    open_files: typing.List[Dataset] = list()
    try:
        merger = MergeInstrument()
        for file in input_files:
            file = Dataset(str(file), 'r')
            open_files.append(file)
            merger.append(file)

        output_file = merger.execute(output_file)
        output_file.close()
    except:
        _LOGGER.error(f"Error merging to file %s:%s",
                      station.upper(), output_file, exc_info=True)
        raise
    finally:
        for file in open_files:
            file.close()
        for file in input_files:
            try:
                os.unlink(file)
            except OSError:
                pass
