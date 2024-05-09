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
        return {"met", "aerosol", "vaisalapwdx2"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "vaisalapwdx2"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def _declare_parameters(self, parameters: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(parameters, dict):
            return
        if not parameters:
            return

        g = self.root.createGroup("parameters")

        system_parameters = parameters.get("System")
        if system_parameters is not None:
            var = g.createVariable("system_parameters", str, (), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = "instrument response to the PAR command, representing general operating parameters"
            var[0] = str(system_parameters)

        weather_parameters = parameters.get("Weather")
        if weather_parameters is not None:
            var = g.createVariable("weather_parameters", str, (), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = "instrument response to the WPAR command, representing weather identification parameters"
            var[0] = str(weather_parameters)

    def run(self) -> bool:
        data_WZ = self.load_variable(f"WZ_{self.instrument_id}")
        if data_WZ.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_WZ.time)
        if not super().run():
            return False

        data_WI = self.load_variable(f"WI_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_I = self.load_variable(f"I_{self.instrument_id}")
        data_V1 = self.load_variable(f"V1_{self.instrument_id}")
        data_V2 = self.load_variable(f"V2_{self.instrument_id}")
        data_C1 = self.load_variable(f"C1_{self.instrument_id}")
        data_C2 = self.load_variable(f"C2_{self.instrument_id}")
        data_ZBsp = self.load_variable(f"ZBsp_{self.instrument_id}")
        data_ZBsx = self.load_variable(f"ZBsx_{self.instrument_id}")

        data_WX = self.load_state(f"WX2_{self.instrument_id}", dtype=np.uint64)
        data_ZWXNWS = self.load_state(f"ZWXNWS_{self.instrument_id}", dtype=str)

        parameters = self.load_state(f"ZPARAMETERS_{self.instrument_id}", dtype=dict)

        g, times = self.data_group([data_WZ])
        self.declare_system_flags(g, times)

        var_WZ = g.createVariable("visibility", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_WZ)
        var_WZ.variable_id = "WZ"
        var_WZ.coverage_content_type = "physicalMeasurement"
        var_WZ.cell_methods = "time: mean"
        var_WZ.long_name = "visibility distance"
        var_WZ.units = "km"
        var_WZ.C_format = "%7.3f"
        self.apply_data(times, var_WZ, data_WZ)

        var_WI = g.createVariable("precipitation_rate", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_WI)
        var_WI.variable_id = "WI"
        var_WI.coverage_content_type = "physicalMeasurement"
        var_WI.cell_methods = "time: mean"
        var_WI.long_name = "precipitation rate"
        var_WI.units = "mm h-1"
        var_WI.C_format = "%7.3f"
        self.apply_data(times, var_WI, data_WI)

        var_T1 = g.createVariable("ambient_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "ambient temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("internal_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "internal circuit board temperature"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("drd_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_type = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "DRD precipitation sensor temperature"
        self.apply_data(times, var_T3, data_T3)

        var_I = g.createVariable("background_luminance", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_I)
        var_I.variable_id = "I"
        var_I.coverage_content_type = "physicalMeasurement"
        var_I.cell_methods = "time: mean"
        var_I.long_name = "background luminance sensor reading"
        var_I.units = "cd m-2"
        var_I.C_format = "%8.2f"
        self.apply_data(times, var_I, data_I)

        var_V1 = g.createVariable("led_control_voltage", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V1)
        var_V1.variable_id = "V1"
        var_V1.coverage_content_type = "physicalMeasurement"
        var_V1.cell_methods = "time: mean"
        var_V1.long_name = "LED transmitter control voltage"
        var_V1.units = "V"
        var_V1.C_format = "%5.2f"
        self.apply_data(times, var_V1, data_V1)

        var_V2 = g.createVariable("ambient_light_voltage", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V2)
        var_V2.variable_id = "V2"
        var_V2.coverage_content_type = "physicalMeasurement"
        var_V2.cell_methods = "time: mean"
        var_V2.long_name = "ambient light receiver output voltage"
        var_V2.units = "V"
        var_V2.C_format = "%5.2f"
        self.apply_data(times, var_V2, data_V2)

        var_C1 = g.createVariable("signal_frequency", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_C1)
        var_C1.variable_id = "C1"
        var_C1.coverage_content_type = "physicalMeasurement"
        var_C1.cell_methods = "time: mean"
        var_C1.long_name = "frequency of the transmission signal between the transducer and processor, inversely proportional to visibility"
        var_C1.units = "Hz"
        var_C1.C_format = "%8.2f"
        self.apply_data(times, var_C1, data_C1)

        var_C2 = g.createVariable("offset_frequency", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_C2)
        var_C2.variable_id = "C2"
        var_C2.coverage_content_type = "physicalMeasurement"
        var_C2.cell_methods = "time: mean"
        var_C2.long_name = "measurement signal offset and the lowest possible frequency measurement"
        var_C2.units = "Hz"
        var_C2.C_format = "%6.2f"
        self.apply_data(times, var_C2, data_C2)

        var_ZBsp = g.createVariable("receiver_contamination", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZBsp)
        var_ZBsp.variable_id = "ZBsp"
        var_ZBsp.coverage_content_type = "physicalMeasurement"
        var_ZBsp.cell_methods = "time: mean"
        var_ZBsp.long_name = "receiver contamination backscatter measurement"
        var_ZBsp.C_format = "%5.1f"
        self.apply_data(times, var_ZBsp, data_ZBsp)

        var_ZBsx = g.createVariable("transmitter_contamination", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZBsx)
        var_ZBsx.variable_id = "ZBsx"
        var_ZBsx.coverage_content_type = "physicalMeasurement"
        var_ZBsx.cell_methods = "time: mean"
        var_ZBsx.long_name = "transmitter contamination backscatter control voltage"
        var_ZBsx.units = "V"
        var_ZBsx.C_format = "%5.1f"
        self.apply_data(times, var_ZBsx, data_ZBsx)

        self.apply_coverage(g, times, f"WZ_{self.instrument_id}")

        g, times = self.state_group([data_WX, data_ZWXNWS])

        var_WX = g.createVariable("synop_weather_code", "u8", ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_WX)
        var_WX.variable_id = "WX"
        var_WX.coverage_content_type = "auxiliaryInformation"
        var_WX.cell_methods = "time: point"
        var_WX.long_name = "WMO SYNOP weather code"
        var_WX.C_format = "%2llu"
        self.apply_state(times, var_WX, data_WX)

        var_ZWXNWS = g.createVariable("nws_weather_code", str, ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_ZWXNWS)
        var_ZWXNWS.variable_id = "ZWXNWS"
        var_ZWXNWS.coverage_content_type = "auxiliaryInformation"
        var_ZWXNWS.cell_methods = "time: point"
        var_ZWXNWS.long_name = "NWS weather code"
        self.apply_state(times, var_ZWXNWS, data_ZWXNWS)

        if parameters.value.shape[0] > 0:
            self._declare_parameters(dict(parameters.value[-1]))

        self.apply_instrument_metadata(f"WZ_{self.instrument_id}", manufacturer="Vaisala", generic_model="PWDx2")

        return True