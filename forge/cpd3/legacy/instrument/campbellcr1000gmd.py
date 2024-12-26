import typing
import numpy as np
import netCDF4
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.data.structure.stp import standard_temperature, standard_pressure
from forge.cpd3.convert.instrument.default.units import from_cpd3 as from_cpd3_units
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
        class Result(Converter):
            CUT_SPLIT_VARIABLES = cut_split
            UNSPLIT_VARIABLES = unsplit
        return Result

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "campbellcr1000gmd"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "campbellcr1000gmd"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    @property
    def split_monitor(self) -> typing.Optional[bool]:
        return None

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

        all_data = list(loaded_data.values()) + ([v for v in group_vars if v.time.shape[0] != 0] if group_vars else [])
        if not all_data:
            return None
        if self._average_interval is None and any([v.time.shape != 0 for v in loaded_data.values()]):
            self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in loaded_data.values()]))

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

                if not variables[cpd3_name]:
                    v = value.get("Description")
                    if v:
                        var.long_name = str(v)

                    v_units = value.get("Units")
                    v_format = value.get("Format")
                    apply_unit = from_cpd3_units(
                        str(v_units) if v_units else None,
                        str(v_format) if v_format else None
                    )
                    if apply_unit.units:
                        var.units = apply_unit.units
                    if apply_unit.format:
                        var.C_format = apply_unit.format

            for a, v in variables[cpd3_name].items():
                setattr(var, a, v)
            self.apply_data(times, var, data)
            result.append((var, data))
        return g, times, result

    def run(self) -> bool:
        data_T = self.load_variable(f"T_{self.instrument_id}")
        data_V = self.load_variable(f"V_{self.instrument_id}")
        data_raw_inputs = self.load_array_variable(f"ZINPUTS_{self.instrument_id}")
        if data_T.time.shape[0] != 0 or data_V.time.shape[0] != 0 or data_raw_inputs.time.shape[0] != 0:
            self._average_interval = self.calculate_average_interval(np.concatenate([data_T.time, data_V.time, data_raw_inputs.time]))

        self.apply_instrument_metadata(f"T_{self.instrument_id}", manufacturer="Campbell", model="CR1000-GML")

        cut_split = self._apply_variable_data(self.CUT_SPLIT_VARIABLES)
        if cut_split:
            g, times, cut_variables = cut_split
            self.apply_cut_size(g, times, cut_variables)
            self.apply_coverage(g, times, cut_variables[0][0].variable_id)

        unsplit = self._apply_variable_data(
            self.UNSPLIT_VARIABLES,
            group_vars=[data_T, data_V, data_raw_inputs],
            name="upstream" if cut_split else "data",
        )
        if not super().run():
            return False
        if not unsplit:
            if not cut_split:
                return False
            return True
        g, times, _ = unsplit

        if data_raw_inputs.time.shape[0] > 0:
            g.createDimension("analog_input", data_raw_inputs.value.shape[1])
            var_raw_inputs = g.createVariable("analog_input", "f8", ("time", "analog_input"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_raw_inputs)
            var_raw_inputs.variable_id = "ZINPUTS"
            var_raw_inputs.coverage_content_type = "physicalMeasurement"
            var_raw_inputs.long_name = "raw analog input voltages"
            var_raw_inputs.C_format = "%5.3f"
            var_raw_inputs.units = "V"
            self.apply_data(times, var_raw_inputs, data_raw_inputs)

        self.apply_coverage(g, times, f"T_{self.instrument_id}")

        split_monitor = self.split_monitor
        if split_monitor is None:
            split_monitor = self.calculate_split_monitor(data_V.time)
        if split_monitor and (data_V.time.shape[0] > 0 or data_T.time.shape[0] > 0):
            g, times = self.data_group([data_V], name='status', fill_gaps=False)

        if data_T.time.shape[0] > 0:
            var_T = g.createVariable("board_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T)
            netcdf_timeseries.variable_coordinates(g, var_T)
            var_T.variable_id = "T"
            var_T.coverage_content_type = "physicalMeasurement"
            var_T.long_name = "control board temperature"
            self.apply_data(times, var_T, data_T)

        if data_V.time.shape[0] > 0:
            var_V = g.createVariable("supply_voltage", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_V)
            var_V.variable_id = "V"
            var_V.coverage_content_type = "physicalMeasurement"
            var_V.long_name = "supply voltage"
            var_V.C_format = "%6.3f"
            var_V.units = "V"
            self.apply_data(times, var_V, data_V)

        return True