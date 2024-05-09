import typing
import time
import numpy as np
import netCDF4
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
        self._all_firmware_versions: typing.Dict[str, str] = dict()

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
        return {"aerosol", "lovepid"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "lovepid"

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
            var_name = cpd3_name
            if var_name == "Q_Q11":
                var_name = "sample_flow"

            var = g.createVariable(var_name, "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var)
            var.variable_id = cpd3_name
            var.coverage_content_type = "physicalMeasurement"
            var.cell_methods = "time: mean"

            if cpd3_name.startswith("Q"):
                standard_temperature(g)
                standard_pressure(g)
                var.ancillary_variables = "standard_temperature standard_pressure"

            for identity, value, _ in read_archive([Selection(
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

                v = value.get("Address")
                if v:
                    var.address = "%02X" % int(v)
                else:
                    v = value.get("Channel")
                    if v:
                        var.address = str(v)

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

                source = value.get("Source")
                if source:
                    v = source.get("ControllerID")
                    if v:
                        v = str(v)
                        try:
                            if v.startswith("LOVE"):
                                v = v[4:]
                            week = int(v[0:2])
                            year = int(v[2:4])
                            td = time.gmtime(identity.start or time.time())
                            current_century = td.tm_year - (td.tm_year % 100)
                            year += current_century
                            if year > td.tm_year + 50:
                                year -= 100
                            model = v[4:]
                            var.reported_id = f"{year:04d}w{week:02d}-{model}"
                        except (TypeError, ValueError):
                            pass

                    v = source.get("FirmwareVersion")
                    if v:
                        key = getattr(var, "address", None)
                        if key:
                            self._all_firmware_versions[key] = str(v)

            for a, v in variables[cpd3_name].items():
                setattr(var, a, v)
            self.apply_data(times, var, data)
            result.append((var, data))
        return g, times, result

    def run(self) -> bool:
        data_raw_inputs = self.load_array_variable(f"ZINPUTS_{self.instrument_id}")
        if data_raw_inputs.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_raw_inputs.time)
        if not super().run():
            return False

        result = self._apply_variable_data(self.CUT_SPLIT_VARIABLES)
        if result:
            g, times, cut_variables = result
            self.apply_cut_size(g, times, cut_variables)
            self.apply_coverage(g, times, cut_variables[0][0].variable_id)

        g, times, _ = self._apply_variable_data(
            self.UNSPLIT_VARIABLES,
            group_vars=[data_raw_inputs],
            name="upstream"
        )

        g.createDimension("controller_value", data_raw_inputs.value.shape[1])
        var_raw_inputs = g.createVariable("controller_value", "f8", ("time", "controller_value"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_raw_inputs)
        var_raw_inputs.variable_id = "ZINPUTS"
        var_raw_inputs.coverage_content_Vype = "physicalMeasurement"
        var_raw_inputs.long_name = "raw controller values"
        var_raw_inputs.C_format = "%5.2f"
        self.apply_data(times, var_raw_inputs, data_raw_inputs)

        self.apply_coverage(g, times, f"ZINPUTS_{self.instrument_id}")

        g = self.apply_instrument_metadata(f"ZINPUTS_{self.instrument_id}", manufacturer="Love", model="PID")
        var = g.variables.get("firmware_version")
        if var is None and self._all_firmware_versions:
            var = g.createVariable("firmware_version", str, fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = "instrument firmware version information"
            var[0] = " ".join([self._all_firmware_versions[k] for k in sorted(self._all_firmware_versions.keys())])
        return True