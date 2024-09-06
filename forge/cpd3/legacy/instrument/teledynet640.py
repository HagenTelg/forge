import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import InstrumentConverter, read_archive, Selection


class Converter(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "mass", "teledynet640"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "teledynet640"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_X: typing.Dict[float, Converter.Data] = dict()
        for diameter, flavor in (
                (1.0, "pm1"),
                (2.5, "pm25"),
                (10.0, "pm10"),
        ):
            data = self.Data(*self.convert_loaded(read_archive([Selection(
                start=self.file_start,
                end=self.file_end,
                stations=[self.station],
                archives=[self.archive],
                variables=[f"X_{self.instrument_id}"],
                include_meta_archive=False,
                include_default_station=False,
                lacks_flavors=["cover", "stats"],
                has_flavors=[flavor],
            )])))
            if data.time.shape[0] == 0:
                continue
            data_X[diameter] = data
        if not data_X:
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_X.values()]))
        if not super().run():
            return False

        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_Q1 = self.load_variable(f"Q1_{self.instrument_id}")
        data_Q2 = self.load_variable(f"Q2_{self.instrument_id}")
        data_U1 = self.load_variable(f"U1_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_T4 = self.load_variable(f"T4_{self.instrument_id}")
        data_T5 = self.load_variable(f"T5_{self.instrument_id}")
        data_ZSPAN = self.load_variable(f"ZSPAN_{self.instrument_id}")

        g, times = self.data_group(list(data_X.values()))
        self.declare_system_flags(g, times)

        diameters = sorted(data_X.keys())
        g.createDimension("diameter", len(diameters))
        var_diameter = g.createVariable("diameter", "f8", ("diameter",), fill_value=nan)
        var_diameter.coverage_content_type = "coordinate"
        var_diameter.long_name = "upper particle diameter threshold"
        var_diameter.units = "um"
        var_diameter.C_format = "%4.1f"
        var_diameter[:] = diameters

        var_X = g.createVariable("mass_concentration", "f8", ("time", "diameter"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_X)
        var_X.variable_id = "X"
        var_X.coverage_content_type = "physicalMeasurement"
        var_X.cell_methods = "time: mean"
        var_X.long_name = "mass concentration of particles derived from Lorenz-Mie calculation of OPC scattering"
        var_X.units = "ug m-3"
        var_X.C_format = "%7.2f"
        for idx in range(len(diameters)):
            data = data_X[diameters[idx]]
            self.apply_data(times, var_X, data.time, data.value, (idx,))

        var_P = g.createVariable("pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        self.apply_data(times, var_P, data_P)

        var_Q1 = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q1)
        netcdf_timeseries.variable_coordinates(g, var_Q1)
        var_Q1.variable_id = "Q1"
        var_Q1.coverage_content_type = "physicalMeasurement"
        var_Q1.cell_methods = "time: mean"
        var_Q1.C_format = "%4.2f"
        self.apply_data(times, var_Q1, data_Q1)

        var_Q2 = g.createVariable("bypass_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Q2)
        netcdf_timeseries.variable_coordinates(g, var_Q2)
        var_Q2.variable_id = "Q2"
        var_Q2.coverage_content_type = "physicalMeasurement"
        var_Q2.cell_methods = "time: mean"
        var_Q2.long_name = "inlet stack bypass flow"
        self.apply_data(times, var_Q2, data_Q2)

        var_U1 = g.createVariable("sample_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_rh(var_U1)
        netcdf_timeseries.variable_coordinates(g, var_U1)
        var_U1.variable_id = "U1"
        var_U1.coverage_content_type = "physicalMeasurement"
        var_U1.cell_methods = "time: mean"
        self.apply_data(times, var_U1, data_U1)

        var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("ambient_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "ambient temperature measured by the external sensor"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("asc_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_type = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "aerosol sample conditioner tube jacket temperature"
        self.apply_data(times, var_T3, data_T3)

        var_T4 = g.createVariable("led_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T4)
        netcdf_timeseries.variable_coordinates(g, var_T4)
        var_T4.variable_id = "T4"
        var_T4.coverage_content_type = "physicalMeasurement"
        var_T4.cell_methods = "time: mean"
        var_T4.long_name = "OPC LED temperature"
        self.apply_data(times, var_T4, data_T4)

        var_T5 = g.createVariable("box_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T5)
        netcdf_timeseries.variable_coordinates(g, var_T5)
        var_T5.variable_id = "T5"
        var_T5.coverage_content_type = "physicalMeasurement"
        var_T5.cell_methods = "time: mean"
        var_T5.long_name = "internal box temperature"
        self.apply_data(times, var_T5, data_T5)

        var_ZSPAN = g.createVariable("span_deviation", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZSPAN)
        var_ZSPAN.variable_id = "ZSPAN"
        var_ZSPAN.coverage_content_type = "physicalMeasurement"
        var_ZSPAN.cell_methods = "time: mean"
        var_ZSPAN.long_name = "span deviation"
        var_ZSPAN.C_format = "%6.1f"
        self.apply_data(times, var_ZSPAN, data_ZSPAN)

        self.apply_coverage(g, times, f"F1_{self.instrument_id}")

        self.apply_instrument_metadata(f"X_{self.instrument_id}", manufacturer="Teledyne", model="T640")

        return True