import typing
import re
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import InstrumentConverter


class Converter(InstrumentConverter):
    TEMPERATURES: typing.Dict[str, str] = {
        "1": "at 2m",
    }
    WINDS: typing.Dict[str, str] = {
        "1": "at 10m",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @classmethod
    def with_variables(
            cls,
            temperatures: typing.Dict[str, str],
            winds: typing.Dict[str, str]
    ) -> typing.Type["Converter"]:
        class Result(cls):
            TEMPERATURES = temperatures
            WINDS = winds

        return Result

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"met"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_temperatures = {
            k: self.load_variable(f"T{k if k != '1' else '1?'}_{self.instrument_id}") for k in self.TEMPERATURES.keys()
        }
        data_rh = {
            k: self.load_variable(f"U{k if k != '1' else '1?'}_{self.instrument_id}") for k in self.TEMPERATURES.keys()
        }
        data_dewpoint = {
            k: self.load_variable(f"TD{k if k != '1' else '1?'}_{self.instrument_id}") for k in self.TEMPERATURES.keys()
        }
        data_wind_speed = {
            k: self.load_variable(f"WS{k if k != '1' else '1?'}_{self.instrument_id}") for k in self.WINDS.keys()
        }
        data_wind_direction = {
            k: self.load_variable(f"WD{k if k != '1' else '1?'}_{self.instrument_id}") for k in self.WINDS.keys()
        }
        data_wind_speed_gust = {
            k: self.load_variable(f"WSx{k if k != '1' else '1?'}_{self.instrument_id}") for k in self.WINDS.keys()
        }
        data_WI = self.load_variable(f"WI1?_{self.instrument_id}")
        data_P = self.load_variable(f"P1?_{self.instrument_id}")
        data_Tx = self.load_variable(f"Tx_{self.instrument_id}")
        data_Vx = self.load_variable(f"Vx_{self.instrument_id}")

        if not any(
                [v.time.shape != 0 for v in data_temperatures.values()] +
                [v.time.shape != 0 for v in data_rh.values()] +
                [v.time.shape != 0 for v in data_dewpoint.values()] +
                [v.time.shape != 0 for v in data_wind_speed.values()] +
                [v.time.shape != 0 for v in data_wind_direction.values()] +
                [v.time.shape != 0 for v in data_wind_speed_gust.values()] +
                [data_WI, data_P]
        ):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate(
            [v.time for v in data_temperatures.values()] +
            [v.time for v in data_rh.values()] +
            [v.time for v in data_dewpoint.values()] +
            [v.time for v in data_wind_speed.values()] +
            [v.time for v in data_wind_direction.values()] +
            [v.time for v in data_wind_speed_gust.values()] +
            [data_WI.time, data_P.time]
        ))
        if not super().run():
            return False

        g, times = self.data_group(
            list(data_temperatures.values()) +
            list(data_rh.values()) +
            list(data_dewpoint.values()) +
            list(data_wind_speed.values()) +
            list(data_wind_direction.values()) +
            list(data_wind_speed_gust.values()) +
            [data_WI, data_P]
        )

        if data_P.time.shape[0] > 0:
            var_P = g.createVariable("ambient_pressure", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_pressure(var_P)
            netcdf_timeseries.variable_coordinates(g, var_P)
            var_P.variable_id = "P"
            var_P.coverage_content_Pype = "physicalMeasurement"
            var_P.cell_methods = "time: mean"
            var_P.long_name = "absolute air pressure"
            self.apply_data(times, var_P, data_P)

        if data_WI.time.shape[0] > 0:
            var_WI = g.createVariable("precipitation_rate", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_WI)
            var_WI.variable_id = "WI"
            var_WI.coverage_content_type = "physicalMeasurement"
            var_WI.cell_methods = "time: mean"
            var_WI.long_name = "precipitation rate"
            var_WI.units = "mm h-1"
            var_WI.C_format = "%7.3f"
            self.apply_data(times, var_WI, data_WI)

        if data_Tx.time.shape[0] > 0:
            var_Tx = g.createVariable("board_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_Tx)
            netcdf_timeseries.variable_coordinates(g, var_Tx)
            var_Tx.variable_id = "Tx"
            var_Tx.coverage_content_type = "physicalMeasurement"
            var_Tx.long_name = "control board temperature"
            self.apply_data(times, var_Tx, data_Tx)

        if data_Vx.time.shape[0] > 0:
            var_Vx = g.createVariable("supply_voltage", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Vx)
            var_Vx.variable_id = "Vx"
            var_Vx.coverage_content_type = "physicalMeasurement"
            var_Vx.long_name = "supply voltage"
            var_Vx.C_format = "%6.3f"
            var_Vx.units = "V"
            self.apply_data(times, var_Vx, data_Vx)

        desc_height_match = re.compile(r"\s*at\s+((?:-?\d+(?:.\d*)?)|(-?.\d+))\s*m\s*")

        for k in data_wind_speed.keys():
            wind_speed_name = "wind_speed"
            wind_direction_name = "wind_direction"
            wind_speed_gust_name = "wind_speed_gust"

            measurement_height = None
            if match := desc_height_match.fullmatch(self.WINDS[k]):
                measurement_height = float(match.group(1))
                if k != "1":
                    wind_speed_name += f"_{int(round(measurement_height))}m"
                    wind_direction_name += f"_{int(round(measurement_height))}m"
                    wind_speed_gust_name += f"_{int(round(measurement_height))}m"
            elif k != "1":
                wind_speed_name += f"_{k}"
                wind_direction_name += f"_{k}"
                wind_speed_gust_name += f"_{k}"

            if data_wind_speed[k].time.shape[0] != 0 or data_wind_direction[k].time.shape[0] != 0:
                var_WS = g.createVariable(wind_speed_name, "f8", ("time",), fill_value=nan)
                netcdf_var.variable_wind_speed(var_WS)
                netcdf_timeseries.variable_coordinates(g, var_WS)
                var_WS.variable_id = f"WS{k}"
                var_WS.coverage_content_type = "physicalMeasurement"
                var_WS.cell_methods = f"time: mean {wind_direction_name}: vector_direction"
                if measurement_height:
                    var_WS.height_above_ground_meters = measurement_height
                if self.WINDS[k]:
                    var_WS.long_name = var_WS.long_name + " " + self.WINDS[k]
                self.apply_data(times, var_WS, data_wind_speed[k])

                var_WD = g.createVariable(wind_direction_name, "f8", ("time",), fill_value=nan)
                netcdf_var.variable_wind_direction(var_WD)
                netcdf_timeseries.variable_coordinates(g, var_WD)
                var_WD.variable_id = f"WD{k}"
                var_WD.coverage_content_type = "physicalMeasurement"
                var_WD.cell_methods = f"time: mean {wind_speed_name}: vector_magnitude"
                if measurement_height:
                    var_WD.height_above_ground_meters = measurement_height
                if self.WINDS[k]:
                    var_WD.long_name = var_WD.long_name + " " + self.WINDS[k]
                self.apply_data(times, var_WD, data_wind_direction[k])

            if data_wind_speed_gust[k].time.shape[0] != 0:
                var_ZWSGust = g.createVariable(wind_speed_gust_name, "f8", ("time",), fill_value=nan)
                netcdf_timeseries.variable_coordinates(g, var_ZWSGust)
                var_ZWSGust.variable_id = f"ZWSGust{k}"
                var_ZWSGust.coverage_content_type = "physicalMeasurement"
                var_ZWSGust.cell_methods = "time: mean"
                var_ZWSGust.long_name = "averaged wind gust speed"
                var_ZWSGust.standard_name = "wind_speed_of_gust"
                var_ZWSGust.units = "m s-1"
                var_ZWSGust.C_format = "%4.1f"
                if measurement_height:
                    var_ZWSGust.height_above_ground_meters = measurement_height
                if self.WINDS[k]:
                    var_ZWSGust.long_name = var_ZWSGust.long_name + " " + self.WINDS[k]
                self.apply_data(times, var_ZWSGust, data_wind_speed_gust[k])

        for k in data_temperatures.keys():
            if k == "1":
                temperature_name = "ambient_temperature"
                humidity_name = "ambient_humidity"
                dewpoint_name = "ambient_dewpoint"
            else:
                temperature_name = "temperature"
                humidity_name = "humidity"
                dewpoint_name = "dewpoint"

            measurement_height = None
            if match := desc_height_match.fullmatch(self.TEMPERATURES[k]):
                measurement_height = float(match.group(1))
                if k != "1":
                    temperature_name += f"_{int(round(measurement_height))}m"
                    humidity_name += f"_{int(round(measurement_height))}m"
                    dewpoint_name += f"_{int(round(measurement_height))}m"
            elif k != "1":
                temperature_name += f"_{k}"
                humidity_name += f"_{k}"
                dewpoint_name += f"_{k}"

            if data_temperatures[k].time.shape[0] > 0:
                var_T = g.createVariable(temperature_name, "f8", ("time",), fill_value=nan)
                if k == "1":
                    netcdf_var.variable_air_temperature(var_T)
                else:
                    netcdf_var.variable_temperature(var_T)
                netcdf_timeseries.variable_coordinates(g, var_T)
                var_T.variable_id = f"T{k}"
                var_T.coverage_content_type = "physicalMeasurement"
                var_T.cell_methods = "time: mean"
                var_T.long_name = "ambient air temperature"
                if measurement_height:
                    var_T.height_above_ground_meters = measurement_height
                if self.TEMPERATURES[k]:
                    var_T.long_name = var_T.long_name + " " + self.TEMPERATURES[k]
                self.apply_data(times, var_T, data_temperatures[k])

            if data_rh[k].time.shape[0] > 0:
                var_U = g.createVariable(humidity_name, "f8", ("time",), fill_value=nan)
                if k == "1":
                    netcdf_var.variable_air_rh(var_U)
                else:
                    netcdf_var.variable_rh(var_U)
                netcdf_timeseries.variable_coordinates(g, var_U)
                var_U.variable_id = f"U{k}"
                var_U.coverage_content_type = "physicalMeasurement"
                var_U.cell_methods = "time: mean"
                var_U.long_name = "ambient air humidity"
                if measurement_height:
                    var_U.height_above_ground_meters = measurement_height
                if self.TEMPERATURES[k]:
                    var_U.long_name = var_U.long_name + " " + self.TEMPERATURES[k]
                self.apply_data(times, var_U, data_rh[k])

            if data_dewpoint[k].time.shape[0] > 0:
                var_TD = g.createVariable(dewpoint_name, "f8", ("time",), fill_value=nan)
                if k == "1":
                    netcdf_var.variable_air_dewpoint(var_TD)
                else:
                    netcdf_var.variable_dewpoint(var_TD)
                netcdf_timeseries.variable_coordinates(g, var_TD)
                var_TD.variable_id = f"TD{k}"
                var_TD.coverage_content_type = "physicalMeasurement"
                var_TD.cell_methods = "time: mean"
                var_TD.long_name = "ambient air dewpoint"
                if measurement_height:
                    var_TD.height_above_ground_meters = measurement_height
                if self.TEMPERATURES[k]:
                    var_TD.long_name = var_TD.long_name + " " + self.TEMPERATURES[k]
                self.apply_data(times, var_TD, data_dewpoint[k])

        self.apply_coverage(g, times, f"WS1?_{self.instrument_id}")

        self.apply_instrument_metadata(f"WS1?_{self.instrument_id}")

        return True