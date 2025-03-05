#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.instrument.converter import InstrumentConverter
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.vaisalawmt700 import Converter as VasalaWMT700
from forge.cpd3.legacy.instrument.rrm903nephelometer import Converter as RRM903Nephelometer
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class MetOneBAM1020(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "mass"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_X = self.load_variable(f"X_{self.instrument_id}")
        if data_X.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_X.time)
        if not super().run():
            return False

        data_T = self.load_variable(f"T_{self.instrument_id}")
        data_U = self.load_variable(f"U_{self.instrument_id}")
        data_Q = self.load_variable(f"Q_{self.instrument_id}")

        g, times = self.data_group([data_X])

        var_X = g.createVariable("total_carbon_concentration", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_X)
        var_X.variable_id = "X"
        var_X.coverage_content_type = "physicalMeasurement"
        var_X.cell_methods = "time: mean"
        var_X.long_name = "mass concentration"
        var_X.units = "ug m-3"
        var_X.C_format = "%5.0f"
        self.apply_data(times, var_X, data_X)

        var_T = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T)
        netcdf_timeseries.variable_coordinates(g, var_T)
        var_T.variable_id = "T"
        var_T.coverage_content_type = "physicalMeasurement"
        var_T.cell_methods = "time: mean"
        self.apply_data(times, var_T, data_T)

        var_U = g.createVariable("sample_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_rh(var_U)
        netcdf_timeseries.variable_coordinates(g, var_U)
        var_U.variable_id = "U"
        var_U.coverage_content_type = "physicalMeasurement"
        var_U.cell_methods = "time: mean"
        self.apply_data(times, var_U, data_U)

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.cell_methods = "time: mean"
        var_Q.C_format = "%4.2f"
        self.apply_data(times, var_Q, data_Q)

        self.apply_coverage(g, times, f"X_{self.instrument_id}")

        self.apply_instrument_metadata(f"X_{self.instrument_id}", manufacturer="MetOne", model="BAM-1020")

        return True


class RRNephAlternateWavelength(RRM903Nephelometer):
    WAVELENGTHS = [
        (545.0, "G"),
    ]


class RPICPU(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "rpicpu"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "rpicpu"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_T = self.load_variable(f"T_{self.instrument_id}")
        if data_T.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_T.time)
        if not super().run():
            return False

        g, times = self.data_group([data_T])

        var_T = g.createVariable("cpu_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T)
        netcdf_timeseries.variable_coordinates(g, var_T)
        var_T.variable_id = "T"
        var_T.coverage_content_type = "physicalMeasurement"
        var_T.cell_methods = "time: mean"
        var_T.long_name = "CPU internal temperature"
        self.apply_data(times, var_T, data_T)

        self.apply_coverage(g, times, f"T_{self.instrument_id}")

        self.apply_instrument_metadata(f"T_{self.instrument_id}", manufacturer="RaspberryPi", model="4B")
        return True


class PWRGate(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "pwrgate"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "pwrgate"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_V1 = self.load_variable(f"V1_{self.instrument_id}")
        if data_V1.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_V1.time)
        if not super().run():
            return False

        data_V2 = self.load_variable(f"V2_{self.instrument_id}")
        data_V3 = self.load_variable(f"V3_{self.instrument_id}")
        data_A = self.load_variable(f"A_{self.instrument_id}")

        g, times = self.data_group([data_V1])

        var_V1 = g.createVariable("supply_voltage", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V1)
        var_V1.variable_id = "V1"
        var_V1.coverage_content_type = "physicalMeasurement"
        var_V1.long_name = "input supply voltage"
        var_V1.C_format = "%5.2f"
        var_V1.units = "V"
        self.apply_data(times, var_V1, data_V1)

        var_V2 = g.createVariable("battery_voltage", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V2)
        var_V2.variable_id = "V2"
        var_V2.coverage_content_type = "physicalMeasurement"
        var_V2.long_name = "current battery voltage"
        var_V2.C_format = "%5.2f"
        var_V2.units = "V"
        self.apply_data(times, var_V2, data_V2)

        var_V3 = g.createVariable("solar_voltage", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V3)
        var_V3.variable_id = "V3"
        var_V3.coverage_content_type = "physicalMeasurement"
        var_V3.long_name = "solar panel input voltage"
        var_V3.C_format = "%5.2f"
        var_V3.units = "V"
        self.apply_data(times, var_V3, data_V3)

        var_A = g.createVariable("charger_current", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_A)
        var_A.variable_id = "A"
        var_A.coverage_content_type = "physicalMeasurement"
        var_A.long_name = "current being supplied to the battery for charging"
        var_A.C_format = "%4.2f"
        var_A.units = "A"
        self.apply_data(times, var_A, data_A)

        self.apply_coverage(g, times, f"V1_{self.instrument_id}")

        self.apply_instrument_metadata(f"V1_{self.instrument_id}", manufacturer="WMR", model="EpicPWRgate")
        return True


C.run(STATION, {
    "A11": [
        C('psap3w', start='2006-05-27', end='2014-11-06'),
        C('clap', start='2014-11-06'),
    ],
    "A12": [ C('clap+secondary', start='2012-03-30', end='2014-11-07'), ],
    "A81": [ C('mageeae31', start='2004-12-31', end='2022-03-12'), ],
    "E11": [ C(MetOneBAM1020, start='2020-07-17', end='2022-03-12'), ],
    "N11": [ C('dmtbcp+secondary', start='2018-05-31', end='2018-06-02'), ],
    "N71": [
        C('tsi302xcpc', end='2017-06-02'),
        C('tsi377xcpc', start='2017-07-07', end='2017-09-03'),
        C('tsi3760cpc', start='2018-03-21'),
    ],
    "N72": [ C('tsi377xcpc+secondary', start='2018-06-01'), ],
    "Q12": [ C('tsimfm', start='2012-04-07'), ],
    "Q13": [ C('tsimfm', start='2018-05-31'), ],
    "Q71": [ C('tsimfm', start='2018-03-22'), ],
    "Q72": [ C('tsimfm', start='2018-03-22'), ],
    "S11": [ C('tsi3563nephelometer'), ],
    "S12": [
        C(RRNephAlternateWavelength, start='2011-04-20', end='2012-03-31'),
        C('tsi3563nephelometer+secondary', start='2020-10-07'),
    ],
    "S81": [ C('purpleair', start='2021-10-05'), ],
    "XM1": [ C('vaisalawxt5xx', start='2019-06-10'), ],
    "XM2": [ C('vaisalapwdx2', start='2019-06-07'), ],
    "XM3": [ C(VasalaWMT700.with_instrument_override(serial_number="K4730203").with_added_tag("secondary"), start='2022-03-15'), ],
    "XPI": [ C(RPICPU, start='2021-10-05'), ],
    "XPW": [ C(PWRGate, start='2021-10-05'), ],

    "X1": [
        C(UMAC.with_variables({}, {
            "T_V11": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
            },
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
            "Pd_P11": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "impactor pressure drop",
            },
            "Pd_P12": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "pump vacuum",
            },
            "Q_Q71": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "CPC drier flow",
            },
            "Q_Q12": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "dilution flow",
            },
        }), end='2010-11-03'),
        C(UMAC.with_variables({
            "T_V11": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
            },
            "T_V12": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "RR neph downstream outlet temperature",
            },
            "Pd_P11": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "impactor pressure drop",
            },
            "Pd_P12": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "pump vacuum",
            },
        }, {
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
            "Q_Q71": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "CPC drier flow",
            },
            "Q_Q12": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "dilution flow",
            },
        }), start='2010-11-03', end='2012-04-07'),
        C(UMAC.with_variables({
            "T_V11": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
            },
            "Pd_P11": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "impactor pressure drop",
            },
            "Pd_P12": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "pump vacuum",
            },
        }, {
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
            },
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "room temperature",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
        }), start='2012-04-07'),
    ],
    "X2": [
        C(LovePID.with_variables({}, {
            "U_V51": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "splitter RH",
            },
            "U_V11": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet RH",
            },
            "Q_Q11": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "analyzer flow",
            },
        }), end='2010-11-03'),
        C(LovePID.with_variables({
            "U_V11": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet RH",
            },
            "U_V12": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "RR neph downstream outlet RH",
            },
            "Q_Q11": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "analyzer flow",
            },
        }, {
            "U_V51": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "splitter RH",
            },
        }), start='2010-11-03', end='2012-04-07'),
        C(LovePID.with_variables({
            "U_V11": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet RH",
            },
            "Q_Q11": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "analyzer flow",
            },
        }, {
            "U_V51": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "splitter RH",
            },
            "U_V01": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "room RH",
            },
        }), start='2012-04-07'),
    ],
})
