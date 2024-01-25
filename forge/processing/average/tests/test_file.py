import pytest
import numpy as np
from math import nan
from netCDF4 import Dataset, Group
from forge.data.structure import instrument_timeseries
from forge.data.structure.timeseries import time_coordinate, averaged_time_variable, averaged_count_variable, cutsize_variable, cutsize_coordinate
from forge.processing.average import STANDARD_QUANTILES
from forge.processing.average.file import average_file
from forge.processing.average.calculate import FixedIntervalFileAverager


def test_basic_average(tmp_path):
    input_file = Dataset(str(tmp_path / "input.nc"), 'w', format='NETCDF4')
    output_file = Dataset(str(tmp_path / "output.nc"), 'w', format='NETCDF4')

    instrument_timeseries(
        input_file, "NIL", "X1",
        1 * 60 * 60, 3 * 60 * 60,
        60, {"aerosol"}
    )
    data: Group = input_file.createGroup("data")

    var = time_coordinate(data)
    var[:] = range(1 * 60 * 60 * 1000, 3 * 60 * 60 * 1000, 60 * 1000)
    assert var.shape == (120, )
    var = averaged_time_variable(data)
    var[:] = [60 * 1000] * 120
    var = averaged_count_variable(data)
    var[:] = [10] * 120

    var = data.createVariable('system_flags', 'u8', ('time',), fill_value=False)
    var.variable_id = "F1"
    var[0:30] = 0x01
    var[30:60] = 0x02
    var[60:120] = 0

    var = data.createVariable('avg1', 'f8', ('time',), fill_value=nan)
    var.variable_id = "BsG"
    var[:] = np.arange(120)

    data.createDimension('wavelength', 3)
    var = data.createVariable('wavelength', 'f8', ('wavelength', ), fill_value=nan)
    var[:] = [450, 550, 700]

    var = data.createVariable('avgwl1', 'f8', ('time', 'wavelength'), fill_value=nan)
    var[:, 0] = np.arange(120) * 2
    var[:, 1] = np.arange(119).tolist() + [nan]
    var[:, 2] = np.arange(120) * 3

    var = data.createVariable('last1', 'f8', ('time',), fill_value=nan)
    var.cell_methods = "time: last"
    var[:] = np.arange(120)

    var = data.createVariable('first1', 'f8', ('time',), fill_value=nan)
    var.cell_methods = "time: point"
    var[:] = np.arange(120)

    var = data.createVariable('sum1', 'f8', ('time',), fill_value=nan)
    var.cell_methods = "time: sum"
    var[:] = np.arange(120)

    var = data.createVariable('vm1', 'f8', ('time',), fill_value=nan)
    var.cell_methods = "time: mean vd1: vector_direction"
    var[0:60] = 10
    var[60:120] = 5

    var = data.createVariable('vd1', 'f8', ('time',), fill_value=nan)
    var.cell_methods = "time: mean vm1: vector_magnitude"
    var[0:30] = 90
    var[30:60] = 270
    var[60:120] = 45

    def make_averager(times_epoch_ms, averaged_time_ms, nominal_spacing_ms):
        return FixedIntervalFileAverager(60 * 60 * 1000, times_epoch_ms, averaged_time_ms, nominal_spacing_ms)
    average_file(input_file, output_file, make_averager)

    data = output_file.groups['data']
    statistics = data.groups['statistics']

    assert data.dimensions['wavelength'].size == 3
    var = data.variables['wavelength']
    assert var.shape == (3,)
    assert var[:].tolist() == [450, 550, 700]

    assert statistics.groups['quantiles'].dimensions['quantile'].size == len(STANDARD_QUANTILES)
    var = statistics.groups['quantiles'].variables['quantile']
    assert var.dimensions == ('quantile',)
    assert var.shape == (len(STANDARD_QUANTILES),)
    assert var[:].tolist() == pytest.approx(STANDARD_QUANTILES)

    var = data.variables['time']
    assert var.shape == (2,)
    assert var[:].tolist() == [1 * 60 * 60 * 1000, 2 * 60 * 60 * 1000]

    var = data.variables['averaged_time']
    assert var.shape == (2,)
    assert var[:].tolist() == [60 * 60 * 1000, 60 * 60 * 1000]

    var = data.variables['averaged_count']
    assert var.shape == (2,)
    assert var[:].tolist() == [60, 60]

    var = data.variables['system_flags']
    assert var.shape == (2,)
    assert var[:].tolist() == [0x03, 0x00]
    var = statistics.groups['valid_count'].variables['system_flags']
    assert var.shape == (2,)
    assert var[:].tolist() == [60, 60]

    var = data.variables['avg1']
    assert var.variable_id == "BsG"
    assert var.shape == (2,)
    assert var[:].tolist() == pytest.approx([29.5, 89.5])
    var = statistics.groups['valid_count'].variables['avg1']
    assert var.shape == (2,)
    assert var[:].tolist() == [60, 60]
    var = statistics.groups['unweighted_mean'].variables['avg1']
    assert var.shape == (2,)
    assert var[:].tolist() == pytest.approx([29.5, 89.5])
    var = statistics.groups['stddev'].variables['avg1']
    assert var.shape == (2,)
    assert var[:].tolist() == pytest.approx([np.std(np.arange(60), ddof=1), np.std(np.arange(60, 120), ddof=1)])
    var = statistics.groups['quantiles'].variables['avg1']
    assert var.shape == (2, len(STANDARD_QUANTILES))
    assert float(var[0, 0]) == 0
    assert float(var[0, -1]) == 59
    assert float(var[1, 0]) == 60
    assert float(var[1, -1]) == 119
    assert var[0, :].tolist() == pytest.approx(np.nanquantile(np.arange(60), STANDARD_QUANTILES).tolist())
    assert var[1, :].tolist() == pytest.approx(np.nanquantile(np.arange(60, 120), STANDARD_QUANTILES).tolist())

    var = data.variables['avgwl1']
    assert var.shape == (2, 3)
    assert var[:, 0].tolist() == pytest.approx([59.0, 179.0])
    assert var[:, 1].tolist() == pytest.approx([29.5, 89.0])
    assert var[:, 2].tolist() == pytest.approx([88.5, 268.5])
    var = statistics.groups['valid_count'].variables['avgwl1']
    assert var.shape == (2, 3)
    assert var[:, 0].tolist() == [60, 60]
    assert var[:, 1].tolist() == [60, 59]
    assert var[:, 2].tolist() == [60, 60]
    var = statistics.groups['unweighted_mean'].variables['avgwl1']
    assert var.shape == (2, 3)
    assert var[:, 0].tolist() == pytest.approx([59.0, 179.0])
    assert var[:, 1].tolist() == pytest.approx([29.5, 89.0])
    assert var[:, 2].tolist() == pytest.approx([88.5, 268.5])
    var = statistics.groups['stddev'].variables['avgwl1']
    assert var.shape == (2, 3)
    assert var[:, 0].tolist() == pytest.approx([np.std(np.arange(60) * 2, ddof=1), np.std(np.arange(60, 120) * 2, ddof=1)])
    assert var[:, 1].tolist() == pytest.approx([np.std(np.arange(60), ddof=1), np.std(np.arange(60, 119), ddof=1)])
    assert var[:, 2].tolist() == pytest.approx([np.std(np.arange(60) * 3, ddof=1), np.std(np.arange(60, 120) * 3, ddof=1)])
    var = statistics.groups['quantiles'].variables['avgwl1']
    assert var.shape == (2, 3, len(STANDARD_QUANTILES))
    assert float(var[0, 0, 0]) == 0*2
    assert float(var[0, 0, -1]) == 59*2
    assert float(var[0, 1, 0]) == 0
    assert float(var[0, 1, -1]) == 59
    assert float(var[0, 2, 0]) == 0
    assert float(var[0, 2, -1]) == 59*3
    assert float(var[1, 0, 0]) == 60*2
    assert float(var[1, 0, -1]) == 119*2
    assert float(var[1, 1, 0]) == 60
    assert float(var[1, 1, -1]) == 118
    assert float(var[1, 2, 0]) == 60*3
    assert float(var[1, 2, -1]) == 119*3
    assert var[0, 0, :].tolist() == pytest.approx(np.nanquantile(np.arange(60)*2, STANDARD_QUANTILES).tolist())
    assert var[1, 0, :].tolist() == pytest.approx(np.nanquantile(np.arange(60, 120)*2, STANDARD_QUANTILES).tolist())
    assert var[0, 1, :].tolist() == pytest.approx(np.nanquantile(np.arange(60), STANDARD_QUANTILES).tolist())
    assert var[1, 1, :].tolist() == pytest.approx(np.nanquantile(np.arange(60, 119), STANDARD_QUANTILES).tolist())
    assert var[0, 2, :].tolist() == pytest.approx(np.nanquantile(np.arange(60)*3, STANDARD_QUANTILES).tolist())
    assert var[1, 2, :].tolist() == pytest.approx(np.nanquantile(np.arange(60, 120)*3, STANDARD_QUANTILES).tolist())

    var = data.variables['last1']
    assert var.shape == (2,)
    assert var[:].tolist() == [59, 119]
    var = statistics.groups['valid_count'].variables['last1']
    assert var.shape == (2,)
    assert var[:].tolist() == [60, 60]

    var = data.variables['first1']
    assert var.shape == (2,)
    assert var[:].tolist() == [0, 60]
    var = statistics.groups['valid_count'].variables['first1']
    assert var.shape == (2,)
    assert var[:].tolist() == [60, 60]

    var = data.variables['sum1']
    assert var.shape == (2,)
    assert var[:].tolist() == [1770, 5370]
    var = statistics.groups['valid_count'].variables['sum1']
    assert var.shape == (2,)
    assert var[:].tolist() == [60, 60]

    var = data.variables['vm1']
    assert var.shape == (2,)
    assert var[:].tolist() == pytest.approx([0.0, 5.0])
    var = statistics.groups['valid_count'].variables['vm1']
    assert var.shape == (2,)
    assert var[:].tolist() == [60, 60]
    var = statistics.groups['unweighted_mean'].variables['vm1']
    assert var.shape == (2,)
    assert var[:].tolist() == pytest.approx([10.0, 5.0])
    var = statistics.groups['stddev'].variables['vm1']
    assert var.shape == (2,)
    assert var[:].tolist() == pytest.approx([0.0, 0.0])
    var = statistics.groups['quantiles'].variables['vm1']
    assert var.shape == (2, len(STANDARD_QUANTILES))
    assert var[0, :].tolist() == pytest.approx([10.0] * len(STANDARD_QUANTILES))
    assert var[1, :].tolist() == pytest.approx([5.0] * len(STANDARD_QUANTILES))
    var = statistics.groups['stability_factor'].variables['vm1']
    assert var.shape == (2,)
    assert var[:].tolist() == pytest.approx([0.0, 1.0])

    var = data.variables['vd1']
    assert var.shape == (2,)
    assert var[:].tolist() == pytest.approx([180.0, 45.0])


def test_cut_average(tmp_path):
    input_file = Dataset(str(tmp_path / "input.nc"), 'w', format='NETCDF4')
    output_file = Dataset(str(tmp_path / "output.nc"), 'w', format='NETCDF4')

    instrument_timeseries(
        input_file, "NIL", "X1",
        1 * 60 * 60, 3 * 60 * 60,
        60, {"aerosol"}
    )
    data: Group = input_file.createGroup("data")

    var = time_coordinate(data)
    var[:] = range(1 * 60 * 60 * 1000, 3 * 60 * 60 * 1000, 60 * 1000)
    assert var.shape == (120, )

    var = cutsize_variable(data)
    var[0:30] = 1.0
    var[30:90] = 10.0
    var[90:120] = 1.0

    var = data.createVariable('system_flags', 'u8', ('time',), fill_value=False)
    var.variable_id = "F1"
    var.ancillary_variables = 'cut_size'
    var[0:30] = 0x01
    var[30:89] = 0x02
    var[89:120] = 0x04

    var = data.createVariable('unsplit1', 'f8', ('time',), fill_value=nan)
    var[:] = np.arange(120)

    var = data.createVariable('avg1', 'f8', ('time',), fill_value=nan)
    var.variable_id = "BsG"
    var.ancillary_variables = 'cut_size'
    var[:] = np.arange(120)

    def make_averager(times_epoch_ms, averaged_time_ms, nominal_spacing_ms):
        return FixedIntervalFileAverager(60 * 60 * 1000, times_epoch_ms, averaged_time_ms, nominal_spacing_ms)
    average_file(input_file, output_file, make_averager)

    data = output_file.groups['data']
    statistics = data.groups['statistics']

    assert data.dimensions['cut_size'].size == 2
    var = data.variables['cut_size']
    assert var.shape == (2,)
    assert var.dimensions == ('cut_size', )
    assert var[:].tolist() == [1.0, 10.0]

    assert statistics.groups['quantiles'].dimensions['quantile'].size == len(STANDARD_QUANTILES)
    var = statistics.groups['quantiles'].variables['quantile']
    assert var.dimensions == ('quantile',)
    assert var.shape == (len(STANDARD_QUANTILES),)
    assert var[:].tolist() == pytest.approx(STANDARD_QUANTILES)

    var = data.variables['system_flags']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == [0x01, 0x04]
    assert var[:, 1].tolist() == [0x02, 0x06]
    var = statistics.groups['valid_count'].variables['system_flags']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == [30, 30]
    assert var[:, 1].tolist() == [30, 30]

    var = data.variables['avg1']
    assert var.variable_id == "BsG"
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == pytest.approx([14.5, 104.5])
    assert var[:, 1].tolist() == pytest.approx([44.5, 74.5])
    var = statistics.groups['valid_count'].variables['avg1']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == [30, 30]
    assert var[:, 1].tolist() == [30, 30]
    var = statistics.groups['unweighted_mean'].variables['avg1']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == pytest.approx([14.5, 104.5])
    assert var[:, 1].tolist() == pytest.approx([44.5, 74.5])
    var = statistics.groups['stddev'].variables['avg1']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == pytest.approx([np.std(np.arange(30), ddof=1), np.std(np.arange(90, 120), ddof=1)])
    assert var[:, 1].tolist() == pytest.approx([np.std(np.arange(30, 60), ddof=1), np.std(np.arange(60, 90), ddof=1)])
    var = statistics.groups['quantiles'].variables['avg1']
    assert var.shape == (2, 2, len(STANDARD_QUANTILES))
    assert var.dimensions == ('time', 'cut_size', 'quantile')
    assert float(var[0, 0, 0]) == 0
    assert float(var[0, 0, -1]) == 29
    assert float(var[0, 1, 0]) == 30
    assert float(var[0, 1, -1]) == 59
    assert float(var[1, 0, 0]) == 90
    assert float(var[1, 0, -1]) == 119
    assert float(var[1, 1, 0]) == 60
    assert float(var[1, 1, -1]) == 89
    assert var[0, 0, :].tolist() == pytest.approx(np.nanquantile(np.arange(30), STANDARD_QUANTILES).tolist())
    assert var[0, 1, :].tolist() == pytest.approx(np.nanquantile(np.arange(30, 60), STANDARD_QUANTILES).tolist())
    assert var[1, 0, :].tolist() == pytest.approx(np.nanquantile(np.arange(90, 120), STANDARD_QUANTILES).tolist())
    assert var[1, 1, :].tolist() == pytest.approx(np.nanquantile(np.arange(60, 90), STANDARD_QUANTILES).tolist())

    var = data.variables['unsplit1']
    assert var.shape == (2,)
    assert var.dimensions == ('time', )
    assert var[:].tolist() == pytest.approx([29.5, 89.5])
    var = statistics.groups['valid_count'].variables['unsplit1']
    assert var.shape == (2,)
    assert var.dimensions == ('time',)
    assert var[:].tolist() == [60, 60]
    var = statistics.groups['unweighted_mean'].variables['unsplit1']
    assert var.shape == (2,)
    assert var.dimensions == ('time',)
    assert var[:].tolist() == pytest.approx([29.5, 89.5])
    var = statistics.groups['stddev'].variables['unsplit1']
    assert var.shape == (2,)
    assert var.dimensions == ('time',)
    assert var[:].tolist() == pytest.approx([np.std(np.arange(60), ddof=1), np.std(np.arange(60, 120), ddof=1)])
    var = statistics.groups['quantiles'].variables['unsplit1']
    assert var.shape == (2, len(STANDARD_QUANTILES))
    assert var.dimensions == ('time', 'quantile')
    assert float(var[0, 0]) == 0
    assert float(var[0, -1]) == 59
    assert float(var[1, 0]) == 60
    assert float(var[1, -1]) == 119
    assert var[0, :].tolist() == pytest.approx(np.nanquantile(np.arange(60), STANDARD_QUANTILES).tolist())
    assert var[1, :].tolist() == pytest.approx(np.nanquantile(np.arange(60, 120), STANDARD_QUANTILES).tolist())


def test_ufunc_shape(tmp_path):
    input_file = Dataset(str(tmp_path / "input.nc"), 'w', format='NETCDF4')
    output_file = Dataset(str(tmp_path / "output.nc"), 'w', format='NETCDF4')

    instrument_timeseries(
        input_file, "NIL", "X1",
        1 * 60 * 60, 3 * 60 * 60,
        60, {"aerosol"}
    )
    data: Group = input_file.createGroup("data")

    var = time_coordinate(data)
    var[:] = range(1 * 60 * 60 * 1000, 3 * 60 * 60 * 1000, 60 * 1000)
    assert var.shape == (120, )

    data.createDimension('wavelength', 3)
    var = data.createVariable('wavelength', 'f8', ('wavelength',), fill_value=nan)
    var[:] = [450, 550, 700]

    var = data.createVariable('lastwl1', 'f8', ('time', 'wavelength'), fill_value=nan)
    var.cell_methods = 'time: last'
    var[:, 0] = np.arange(119).tolist() + [nan]
    var[:, 1] = np.arange(120) * 2
    var[:, 2] = np.arange(120) * 3

    def make_averager(times_epoch_ms, averaged_time_ms, nominal_spacing_ms):
        return FixedIntervalFileAverager(60 * 60 * 1000, times_epoch_ms, averaged_time_ms, nominal_spacing_ms)
    average_file(input_file, output_file, make_averager)

    data = output_file.groups['data']
    statistics = data.groups['statistics']

    assert data.dimensions['wavelength'].size == 3
    var = data.variables['wavelength']
    assert var.shape == (3,)
    assert var[:].tolist() == [450, 550, 700]

    var = data.variables['lastwl1']
    assert var.shape == (2, 3)
    assert var[:, 0].tolist() == [59, 118]
    assert var[:, 1].tolist() == [59 * 2, 119 * 2]
    assert var[:, 2].tolist() == [59 * 3, 119 * 3]
    var = statistics.groups['valid_count'].variables['lastwl1']
    assert var.shape == (2, 3)
    assert var[:, 0].tolist() == [60, 59]
    assert var[:, 1].tolist() == [60, 60]
    assert var[:, 2].tolist() == [60, 60]


def test_cut_dimension(tmp_path):
    input_file = Dataset(str(tmp_path / "input.nc"), 'w', format='NETCDF4')
    output_file = Dataset(str(tmp_path / "output.nc"), 'w', format='NETCDF4')

    instrument_timeseries(
        input_file, "NIL", "X1",
        1 * 60 * 60, 3 * 60 * 60,
        60, {"aerosol"}
    )
    data: Group = input_file.createGroup("data")

    var = time_coordinate(data)
    var[:] = range(1 * 60 * 60 * 1000, 3 * 60 * 60 * 1000, 60 * 1000)
    assert var.shape == (120, )

    var = cutsize_coordinate(data, 2)
    var[0] = 1.0
    var[1] = 10.0

    var = data.createVariable('system_flags', 'u8', ('time', 'cut_size'), fill_value=False)
    var.variable_id = "F1"
    var[0:30, 0] = 0x01
    var[0:30, 1] = 0x02
    var[30:120, 0] = 0x04
    var[30:120, 1] = 0x08

    var = data.createVariable('unsplit1', 'f8', ('time',), fill_value=nan)
    var[:] = np.arange(120)

    var = data.createVariable('avg1', 'f8', ('time', 'cut_size'), fill_value=nan)
    var.variable_id = "BsG"
    var[:, 0] = np.arange(120)
    var[:, 1] = np.arange(120) * 2

    def make_averager(times_epoch_ms, averaged_time_ms, nominal_spacing_ms):
        return FixedIntervalFileAverager(60 * 60 * 1000, times_epoch_ms, averaged_time_ms, nominal_spacing_ms)
    average_file(input_file, output_file, make_averager)

    data = output_file.groups['data']
    statistics = data.groups['statistics']

    assert data.dimensions['cut_size'].size == 2
    var = data.variables['cut_size']
    assert var.shape == (2,)
    assert var.dimensions == ('cut_size',)
    assert var[:].tolist() == [1.0, 10.0]

    assert statistics.groups['quantiles'].dimensions['quantile'].size == len(STANDARD_QUANTILES)
    var = statistics.groups['quantiles'].variables['quantile']
    assert var.dimensions == ('quantile',)
    assert var.shape == (len(STANDARD_QUANTILES),)
    assert var[:].tolist() == pytest.approx(STANDARD_QUANTILES)

    var = data.variables['system_flags']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == [0x05, 0x04]
    assert var[:, 1].tolist() == [0x0A, 0x08]
    var = statistics.groups['valid_count'].variables['system_flags']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == [60, 60]
    assert var[:, 1].tolist() == [60, 60]

    var = data.variables['avg1']
    assert var.variable_id == "BsG"
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == pytest.approx([29.5, 89.5])
    assert var[:, 1].tolist() == pytest.approx([59.0, 179.0])
    var = statistics.groups['valid_count'].variables['avg1']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == [60, 60]
    assert var[:, 1].tolist() == [60, 60]
    var = statistics.groups['unweighted_mean'].variables['avg1']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == pytest.approx([29.5, 89.5])
    assert var[:, 1].tolist() == pytest.approx([59.0, 179.0])
    var = statistics.groups['stddev'].variables['avg1']
    assert var.shape == (2, 2)
    assert var.dimensions == ('time', 'cut_size')
    assert var[:, 0].tolist() == pytest.approx([np.std(np.arange(60), ddof=1), np.std(np.arange(60, 120), ddof=1)])
    assert var[:, 1].tolist() == pytest.approx([np.std(np.arange(60) * 2, ddof=1), np.std(np.arange(60, 120) * 2, ddof=1)])
    var = statistics.groups['quantiles'].variables['avg1']
    assert var.shape == (2, 2, len(STANDARD_QUANTILES))
    assert var.dimensions == ('time', 'cut_size', 'quantile')
    assert float(var[0, 0, 0]) == 0
    assert float(var[0, 0, -1]) == 59
    assert float(var[0, 1, 0]) == 0
    assert float(var[0, 1, -1]) == 59*2
    assert float(var[1, 0, 0]) == 60
    assert float(var[1, 0, -1]) == 119
    assert float(var[1, 1, 0]) == 60*2
    assert float(var[1, 1, -1]) == 119*2
    assert var[0, 0, :].tolist() == pytest.approx(np.nanquantile(np.arange(60), STANDARD_QUANTILES).tolist())
    assert var[0, 1, :].tolist() == pytest.approx(np.nanquantile(np.arange(60) * 2, STANDARD_QUANTILES).tolist())
    assert var[1, 0, :].tolist() == pytest.approx(np.nanquantile(np.arange(60, 120), STANDARD_QUANTILES).tolist())
    assert var[1, 1, :].tolist() == pytest.approx(np.nanquantile(np.arange(60, 120) * 2, STANDARD_QUANTILES).tolist())

    var = data.variables['unsplit1']
    assert var.shape == (2,)
    assert var.dimensions == ('time',)
    assert var[:].tolist() == pytest.approx([29.5, 89.5])
    var = statistics.groups['valid_count'].variables['unsplit1']
    assert var.shape == (2,)
    assert var.dimensions == ('time',)
    assert var[:].tolist() == [60, 60]
    var = statistics.groups['unweighted_mean'].variables['unsplit1']
    assert var.shape == (2,)
    assert var.dimensions == ('time',)
    assert var[:].tolist() == pytest.approx([29.5, 89.5])
    var = statistics.groups['stddev'].variables['unsplit1']
    assert var.shape == (2,)
    assert var.dimensions == ('time',)
    assert var[:].tolist() == pytest.approx([np.std(np.arange(60), ddof=1), np.std(np.arange(60, 120), ddof=1)])
    var = statistics.groups['quantiles'].variables['unsplit1']
    assert var.shape == (2, len(STANDARD_QUANTILES))
    assert var.dimensions == ('time', 'quantile')
    assert float(var[0, 0]) == 0
    assert float(var[0, -1]) == 59
    assert float(var[1, 0]) == 60
    assert float(var[1, -1]) == 119
    assert var[0, :].tolist() == pytest.approx(np.nanquantile(np.arange(60), STANDARD_QUANTILES).tolist())
    assert var[1, :].tolist() == pytest.approx(np.nanquantile(np.arange(60, 120), STANDARD_QUANTILES).tolist())