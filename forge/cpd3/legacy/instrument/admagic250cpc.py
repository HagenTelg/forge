import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import InstrumentConverter


class Converter(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "admagic250cpc"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "admagic250cpc"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def _declare_parameters(self, parameters: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(parameters, dict):
            return
        if not parameters:
            return

        g = self.root.createGroup("parameters")

        for name, long_name, units, C_format in (
                ("lset", "laser diode power setpoint", "uW", "%4llu"),
                ("doslope", "slope of the detector offset to laser power", "mV mW-1", "%3llu"),
                ("doint", "intercept of the detector offset to laser power", "mV", "%4llu"),
                ("doff", "detector offset (no effect if doslope is non-zero)", "mV", "%3llu"),
                ("dvlt", "detector voltage", "V", "%3llu"),
                ("dthr", "detector particle counting threshold", "mV", "%3llu"),
                ("pht", "target percentage of particles above upper detector threshold (dthr2) for auto-adjustment", "%", "%3llu"),
                ("dthr2", "upper detector threshold (no effect if pht is non-zero)", "mV", "%4llu"),
                ("qcf", "flow calibration factor", None, "%3llu"),
                ("qtrg", "target volumetric flow rate", "cm3 min-1", "%3llu"),
                ("wtrg", "target wick saturation percentage", "%", "%3llu"),
                ("wdry", "wick saturation percentage that will trigger wick recovery mode", "%", "%3llu"),
                ("wwet", "wick saturation percentage that will exit wick recovery mode", "%", "%3llu"),
                ("wgn", "feedback gain used in the moderator setpoint calculation", "cdegC %-1", "%3llu"),
                ("wmax", "raw wick sensor reading corresponding to 0% saturation", None, "%4llu"),
                ("wmin", "raw wick sensor reading corresponding to 100% saturation", None, "%4llu"),
        ):
            value = parameters.get(name)
            if value is None:
                continue

            var = g.createVariable(name, "u8", (), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = long_name
            if units:
                var.units = units
            var.C_format = C_format
            var[:] = int(value)

        for name, long_name, units, C_format in (
                ("lcur", "laser diode current", "mA", "%5.1f"),
                ("qset", "pump power", "%", "%5.1f"),
                ("heff", "humidifier effectiveness parameter in dewpoint estimator", None, "%4.2f"),
                ("hmax", "maximum expected RH from humidifier parameter in dewpoint estimator", "%", "%4.2f"),
                ("mrefint", "moderator reference intercept parameter in setpoint calculation", "degC", "%4.1f"),
                ("mrefslope", "moderator reference slope parameter in setpoint calculation", "1", "%5.2f"),
        ):
            value = parameters.get(name)
            if value is None:
                continue

            var = g.createVariable(name, "f8", (), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = long_name
            if units:
                var.units = units
            var.C_format = C_format
            var[:] = float(value)

        for name, description in (
                ("tcon", "conditioner"),
                ("tini", "initiator"),
                ("tmod", "moderator"),
                ("topt", "optics"),
        ):
            raw = parameters.get(name)
            if raw is None or not isinstance(raw, dict):
                continue
            value = float(raw.get("Setpoint") or 0)
            mode = str(raw.get("Mode") or "")

            var = g.createVariable(name, "f8", (), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = f"{description} temperature setpoint"
            var.C_format = "%5.1f"
            var.units = "degC"
            var[:] = value

            var = g.createVariable(f"{name}_mode", str, (), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = f"{description} temperature control mode"
            var[0] = mode

        raw = parameters.get("Raw")
        if raw is not None:
            var = g.createVariable("instrument_parameters", str, (), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = "raw responses to parameters read"
            var[0] = str(raw)

    def run(self) -> bool:
        data_N = self.load_variable(f"N_{self.instrument_id}")
        if data_N.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_N.time)
        if not super().run():
            return False

        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_Tu = self.load_variable(f"Tu_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_T4 = self.load_variable(f"T4_{self.instrument_id}")
        data_T5 = self.load_variable(f"T5_{self.instrument_id}")
        data_T6 = self.load_variable(f"T6_{self.instrument_id}")
        data_T7 = self.load_variable(f"T7_{self.instrument_id}")
        data_Uu = self.load_variable(f"Uu_{self.instrument_id}")
        data_TDu = self.load_variable(f"TDu_{self.instrument_id}")
        data_TD1 = self.load_variable(f"TD1_{self.instrument_id}")
        data_PCT = self.load_variable(f"PCT_{self.instrument_id}")
        data_V = self.load_variable(f"V_{self.instrument_id}")

        parameters = self.load_state(f"ZPARAMETERS_{self.instrument_id}", dtype=dict)

        g, times = self.data_group([data_N])
        self.declare_system_flags(g, times)

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.cell_methods = "time: mean"
        var_Q.C_format = "%5.3f"
        self.apply_data(times, var_Q, data_Q)

        var_P = g.createVariable("pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        var_P.long_name = "absolute pressure"
        self.apply_data(times, var_P, data_P)

        var_Tu = g.createVariable("inlet_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_Tu)
        netcdf_timeseries.variable_coordinates(g, var_Tu)
        var_Tu.variable_id = "Tu"
        var_Tu.coverage_content_type = "physicalMeasurement"
        var_Tu.cell_methods = "time: mean"
        var_Tu.long_name = "air temperature at the instrument inlet"
        self.apply_data(times, var_Tu, data_Tu)

        var_T1 = g.createVariable("conditioner_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "temperature of the conditioner (1st stage)"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("initiator_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "temperature of the initiator (2nd stage)"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("moderator_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_type = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "temperature of the moderator (3rd stage)"
        self.apply_data(times, var_T3, data_T3)

        var_T4 = g.createVariable("optics_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T4)
        netcdf_timeseries.variable_coordinates(g, var_T4)
        var_T4.variable_id = "T4"
        var_T4.coverage_content_type = "physicalMeasurement"
        var_T4.cell_methods = "time: mean"
        var_T4.long_name = "temperature of the optics head"
        self.apply_data(times, var_T4, data_T4)

        var_T5 = g.createVariable("heatsink_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T5)
        netcdf_timeseries.variable_coordinates(g, var_T5)
        var_T5.variable_id = "T5"
        var_T5.coverage_content_type = "physicalMeasurement"
        var_T5.cell_methods = "time: mean"
        var_T5.long_name = "temperature of the heatsink"
        self.apply_data(times, var_T5, data_T5)

        var_T6 = g.createVariable("case_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T6)
        netcdf_timeseries.variable_coordinates(g, var_T6)
        var_T6.variable_id = "T6"
        var_T6.coverage_content_type = "physicalMeasurement"
        var_T6.cell_methods = "time: mean"
        var_T6.long_name = "temperature inside the metal case"
        self.apply_data(times, var_T6, data_T6)

        var_T7 = g.createVariable("board_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T7)
        netcdf_timeseries.variable_coordinates(g, var_T7)
        var_T7.variable_id = "T7"
        var_T7.coverage_content_type = "physicalMeasurement"
        var_T7.cell_methods = "time: mean"
        var_T7.long_name = "temperature of the PCB as measured by the differential pressure sensor"
        self.apply_data(times, var_T7, data_T7)

        var_Uu = g.createVariable("inlet_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_rh(var_Uu)
        netcdf_timeseries.variable_coordinates(g, var_Uu)
        var_Uu.variable_id = "Uu"
        var_Uu.coverage_content_type = "physicalMeasurement"
        var_Uu.cell_methods = "time: mean"
        var_Uu.long_name = "relative humidity at the instrument inlet"
        self.apply_data(times, var_Uu, data_Uu)

        var_TDu = g.createVariable("inlet_dewpoint", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_dewpoint(var_TDu)
        netcdf_timeseries.variable_coordinates(g, var_TDu)
        var_TDu.variable_id = "TDu"
        var_TDu.coverage_content_type = "physicalMeasurement"
        var_TDu.cell_methods = "time: mean"
        var_TDu.long_name = "dewpoint calculated from inlet temperature and humidity"
        self.apply_data(times, var_TDu, data_TDu)

        var_TD1 = g.createVariable("growth_tube_dewpoint", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_dewpoint(var_TD1)
        netcdf_timeseries.variable_coordinates(g, var_TD1)
        var_TD1.variable_id = "TD1"
        var_TD1.coverage_content_type = "physicalMeasurement"
        var_TD1.cell_methods = "time: mean"
        var_TD1.long_name = "estimated dewpoint at the start of the growth tube"
        self.apply_data(times, var_TD1, data_TD1)

        var_PCT = g.createVariable("wick_saturation", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_PCT)
        var_PCT.variable_id = "PCT"
        var_PCT.coverage_content_type = "physicalMeasurement"
        var_PCT.cell_methods = "time: mean"
        var_PCT.long_name = "wick saturation measured between initiator and moderator"
        var_PCT.units = "%"
        var_PCT.C_format = "%3.0f"
        self.apply_data(times, var_PCT, data_PCT)

        var_V = g.createVariable("pulse_height", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V)
        var_V.variable_id = "V"
        var_V.coverage_content_type = "physicalMeasurement"
        var_V.cell_methods = "time: mean"
        var_V.long_name = "upper pulse height threshold"
        var_V.units = "mV"
        var_V.C_format = "%4.0f"
        self.apply_data(times, var_V, data_V)

        self.apply_cut_size(g, times, [
            (var_N, data_N),
            (var_Q, data_Q),
            (var_P, data_P),
            (var_Tu, data_Tu),
            (var_T1, data_T1),
            (var_T2, data_T2),
            (var_T3, data_T3),
            (var_T4, data_T4),
            (var_T5, data_T5),
            (var_T6, data_T6),
            (var_T7, data_T7),
            (var_Uu, data_Uu),
            (var_TDu, data_TDu),
            (var_TD1, data_TD1),
            (var_PCT, data_PCT),
            (var_V, data_V),
        ])
        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        if parameters.value.shape[0] > 0:
            self._declare_parameters(dict(parameters.value[-1]))

        self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="Aerosol Dynamics", model="MAGIC 250")

        return True