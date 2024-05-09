import typing
import numpy as np
import netCDF4
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.data.structure.stp import standard_temperature, standard_pressure
from .converter import InstrumentConverter, read_archive, Selection, variant


class Converter(InstrumentConverter):
    CUT_SPLIT_VARIABLES: typing.Dict[str, typing.Dict[str, str]] = dict()
    UNSPLIT_VARIABLES: typing.Dict[str, typing.Dict[str, str]] = dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @classmethod
    def with_variables(
            cls,
            cut_split: typing.Dict[str, typing.Dict[str, str]],
            unsplit: typing.Dict[str, typing.Dict[str, str]]
    ) -> typing.Type["Converter"]:
        class Result(cls):
            CUT_SPLIT_VARIABLES = cut_split
            UNSPLIT_VARIABLES = unsplit
        return Result

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "azonixumac1050"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "azonixumac1050"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def _apply_variable_data(
            self,
            variables: typing.Dict[str, typing.Dict[str, str]],
            group_vars: typing.List["Converter.Data"] = None,
            **kwargs
    ) -> typing.Optional[typing.Tuple[netCDF4.Group, np.ndarray, typing.List[typing.Tuple[netCDF4.Variable, "Converter.Data"]]]]:
        loaded_data: typing.Dict[str, Converter.Data] = dict()
        for cpd3_name, attributes in variables.items():
            data = self.load_variable(cpd3_name)
            if data.time.shape[0] == 0:
                continue
            loaded_data[cpd3_name] = data

        all_data = list(loaded_data.values()) + (group_vars or [])
        if not all_data:
            return None

        g, times = self.data_group(all_data, **kwargs)

        result: typing.List[typing.Tuple[netCDF4.Variable, Converter.Data]] = list()
        for cpd3_name, data in loaded_data.items():
            var = g.createVariable(cpd3_name, "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var)
            var.variable_id = cpd3_name
            var.coverage_content_type = "physicalMeasurement"
            var.cell_methods = "time: mean"

            if cpd3_name.startswith("Q"):
                standard_temperature(g)
                standard_pressure(g)
                var.ancillary_variables = "standard_temperature standard_pressure"

            for _, value, _ in read_archive([Selection(
                    start=self.file_start,
                    end=self.file_end,
                    stations=[self.station],
                    archives=[self.archive + "_meta"],
                    variables=[cpd3_name],
                    include_meta_archive=False,
                    include_default_station=False,
            )]):
                if not isinstance(value, variant.Metadata):
                    continue

                v = value.get("Channel")
                if v:
                    var.channel = str(v)
                v = value.get("Comment")
                if v:
                    var.comment = str(v)
                else:
                    v = value.get("NoteCalibration")
                    if v:
                        var.comment = str(v)
                v = value.get("Calibration")
                if isinstance(v, list):
                    var.calibration_polynomial = [float(x) for x in v]

            for a, v in variables[cpd3_name].items():
                setattr(var, a, v)
            self.apply_data(times, var, data)
            result.append((var, data))
        return g, times, result

    def run(self) -> bool:
        data_T = self.load_variable(f"T_{self.instrument_id}")
        if data_T.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_T.time)
        if not super().run():
            return False

        data_V = self.load_variable(f"V_{self.instrument_id}")
        data_raw_inputs = self.load_array_variable(f"ZINPUTS_{self.instrument_id}")

        result = self._apply_variable_data(self.CUT_SPLIT_VARIABLES)
        if result:
            g, times, cut_variables = result
            self.apply_cut_size(g, times, cut_variables)
            self.apply_coverage(g, times, cut_variables[0][0].variable_id)

        g, times, _ = self._apply_variable_data(
            self.UNSPLIT_VARIABLES,
            group_vars=[data_T, data_V, data_raw_inputs],
            name="upstream"
        )

        var_T = g.createVariable("board_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T)
        netcdf_timeseries.variable_coordinates(g, var_T)
        var_T.variable_id = "T"
        var_T.coverage_content_type = "physicalMeasurement"
        var_T.long_name = "control board temperature"
        self.apply_data(times, var_T, data_T)

        var_V = g.createVariable("board_voltage", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V)
        var_V.variable_id = "V"
        var_V.coverage_content_Vype = "physicalMeasurement"
        var_V.long_name = "control board supply voltage"
        var_V.C_format = "%5.3f"
        var_V.units = "V"
        self.apply_data(times, var_V, data_V)

        g.createDimension("analog_input", data_raw_inputs.value.shape[1])
        var_raw_inputs = g.createVariable("analog_input", "f8", ("time", "analog_input"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_raw_inputs)
        var_raw_inputs.variable_id = "ZINPUTS"
        var_raw_inputs.coverage_content_Vype = "physicalMeasurement"
        var_raw_inputs.long_name = "raw analog input voltages"
        var_raw_inputs.C_format = "%5.3f"
        var_raw_inputs.units = "V"
        self.apply_data(times, var_raw_inputs, data_raw_inputs)

        self.apply_coverage(g, times, f"T_{self.instrument_id}")

        self.apply_instrument_metadata(f"T_{self.instrument_id}", manufacturer="Azonix", model="uMAC")
        return True