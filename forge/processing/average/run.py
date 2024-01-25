import typing
from netCDF4 import Dataset
from forge.formattime import format_iso8601_duration
from forge.processing.average.file import average_file
from forge.processing.average.contamination import invalidate_contamination
from forge.processing.average.calculate import FixedIntervalFileAverager, MonthFileAverager


def process_avgh(station: str, input_file: str, output_file: str) -> None:
    def make_averager(times_epoch_ms, averaged_time_ms, nominal_spacing_ms):
        return FixedIntervalFileAverager(60 * 60 * 1000, times_epoch_ms, averaged_time_ms, nominal_spacing_ms)

    input_file = Dataset(str(input_file), 'r+')
    try:
        invalidate_contamination(input_file, station)
        output_file = Dataset(str(output_file), 'w', format='NETCDF4')
        try:
            average_file(input_file, output_file, make_averager)
            output_file.setncattr("time_coverage_resolution", format_iso8601_duration(60 * 60))
        finally:
            output_file.close()
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
    finally:
        input_file.close()
