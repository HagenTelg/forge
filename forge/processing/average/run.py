import typing
import os
import logging
from tempfile import mkstemp
from netCDF4 import Dataset
from forge.formattime import format_iso8601_duration
from forge.processing.average.file import average_file
from forge.processing.average.contamination import invalidate_contamination, copy_contaminated
from forge.processing.average.calculate import FixedIntervalFileAverager, MonthFileAverager

_LOGGER = logging.getLogger(__name__)


def process_avgh(station: str, input_file: str, output_file: str,
                 contaminated_copy_path: typing.Optional[str] = None) -> None:
    def make_averager(times_epoch_ms, averaged_time_ms, nominal_spacing_ms):
        return FixedIntervalFileAverager(60 * 60 * 1000, times_epoch_ms, averaged_time_ms, nominal_spacing_ms)

    input_file = Dataset(str(input_file), 'r+')
    try:
        if contaminated_copy_path:
            contaminated_instrument = copy_contaminated(input_file)
            if contaminated_instrument:
                fd, contaminated_output_file = mkstemp(
                    prefix=f"{station.upper()}-{contaminated_instrument}_",
                    suffix='.nc',
                    dir=contaminated_copy_path,
                )
                os.close(fd)
                contaminated_output_file = Dataset(str(contaminated_output_file), 'w', format='NETCDF4')
                try:
                    average_file(input_file, contaminated_output_file, make_averager)
                    contaminated_output_file.setncattr("time_coverage_resolution", format_iso8601_duration(60 * 60))
                    contaminated_output_file.setncattr("instrument_id", contaminated_instrument)
                    tags = set(str(getattr(contaminated_output_file, "forge_tags", "")).split())
                    tags.add("secondary")
                    tags.add("contaminated")
                    contaminated_output_file.setncattr('forge_tags', " ".join(sorted(tags)))
                finally:
                    contaminated_output_file.close()

        invalidate_contamination(input_file, station)
        output_file = Dataset(str(output_file), 'w', format='NETCDF4')
        try:
            average_file(input_file, output_file, make_averager)
            output_file.setncattr("time_coverage_resolution", format_iso8601_duration(60 * 60))
        finally:
            output_file.close()
    except:
        _LOGGER.error(f"Error generating hourly averages for file %s/%s",
                      station.upper(), input_file, exc_info=True)
        raise
    finally:
        input_file.close()


def process_avgd(station: str, input_file: str, output_file: str) -> None:
    def make_averager(times_epoch_ms, averaged_time_ms, nominal_spacing_ms):
        return FixedIntervalFileAverager(24 * 60 * 60 * 1000, times_epoch_ms, averaged_time_ms, nominal_spacing_ms)

    input_file = Dataset(str(input_file), 'r')
    try:
        output_file = Dataset(str(output_file), 'w', format='NETCDF4')
        try:
            average_file(input_file, output_file, make_averager)
            output_file.setncattr("time_coverage_resolution", format_iso8601_duration(24 * 60 * 60))
        finally:
            output_file.close()
    except:
        _LOGGER.error(f"Error generating daily averages for file %s/%s",
                      station.upper(), input_file, exc_info=True)
        raise
    finally:
        input_file.close()


def process_avgm(station: str, input_file: str, output_file: str) -> None:
    input_file = Dataset(str(input_file), 'r')
    try:
        output_file = Dataset(str(output_file), 'w', format='NETCDF4')
        try:
            average_file(input_file, output_file, MonthFileAverager)
            output_file.setncattr("time_coverage_resolution", "P1M")
        finally:
            output_file.close()
    except:
        _LOGGER.error(f"Error generating monthly averages for file %s/%s",
                      station.upper(), input_file, exc_info=True)
        raise
    finally:
        input_file.close()
