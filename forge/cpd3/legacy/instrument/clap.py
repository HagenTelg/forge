import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import WavelengthConverter, read_archive, Selection, variant
from forge.data.structure.stp import standard_temperature, standard_pressure


class Converter(WavelengthConverter):
    WAVELENGTHS = [
        (467.0, "B"),
        (528.0, "G"),
        (652.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "clap"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "clap"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    @property
    def split_monitor(self) -> typing.Optional[bool]:
        return None

    @property
    def split_transmittance(self) -> typing.Optional[bool]:
        return None

    def run(self) -> bool:
        data_Ba = self.load_wavelength_variable("Ba")
        if not any([v.time.shape[0] != 0 for v in data_Ba]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Ba]))
        if not super().run():
            return False

        data_Ir = self.load_wavelength_variable("Ir")
        data_Ip = self.load_wavelength_variable("Ip")
        data_If = self.load_wavelength_variable("If")
        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_Ld = self.load_variable(f"Ld_{self.instrument_id}")

        data_Ff = self.load_state(f"Ff_{self.instrument_id}", dtype=np.uint64)
        data_Fn = self.load_state(f"Fn_{self.instrument_id}", dtype=np.uint64)
        spot = self.load_state(f"ZSPOT_{self.instrument_id}", dtype=dict)

        system_flags_time = self.load_variable(f"F1?_{self.instrument_id}", convert=bool, dtype=np.bool_).time

        g, times = self.data_group(data_Ba + [system_flags_time], fill_gaps=False)
        standard_temperature(g)
        standard_pressure(g)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Ba = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Ba, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Ba)
        var_Ba.variable_id = "Ba"
        var_Ba.coverage_content_type = "physicalMeasurement"
        var_Ba.cell_methods = "time: mean"
        var_Ba.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_Ba, data_Ba)

        split_transmittance = self.split_transmittance
        if split_transmittance is None:
            split_transmittance = self.calculate_split_monitor(np.concatenate([v.time for v in data_Ir]))
        if not split_transmittance:
            mon_g = g
            mon_times = times
        elif any([v.time.shape[0] != 0 for v in data_Ir]):
            mon_g, mon_times = self.data_group(data_Ir, name='status', fill_gaps=False)
        else:
            mon_g, mon_times = None, None
            split_transmittance = True

        if mon_g is not None:
            var_Ir = mon_g.createVariable("transmittance", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_transmittance(var_Ir)
            netcdf_timeseries.variable_coordinates(mon_g, var_Ir)
            var_Ir.variable_id = "Ir"
            var_Ir.coverage_content_type = "physicalMeasurement"
            var_Ir.cell_methods = "time: last"
            var_Ir.long_name = "transmittance fraction of light through the filter relative to the amount before sampling on spot one"
            self.apply_wavelength_data(mon_times, var_Ir, data_Ir)

        split_monitor = self.split_monitor
        if split_monitor is None and split_transmittance:
            split_monitor = split_transmittance
        if split_monitor is None:
            split_monitor = self.calculate_split_monitor(data_T1.time)
        if mon_g is None:
            if not split_monitor:
                mon_g = g
                mon_times = times
            elif data_T1.time.shape[0] > 0:
                mon_g, mon_times = self.data_group([data_T1], name='status', fill_gaps=False)
            else:
                mon_g, mon_times = None, None
                split_monitor = True

        if mon_g is not None:
            var_Ip = mon_g.createVariable("sample_intensity", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(mon_g, var_Ip)
            var_Ip.variable_id = "Ip"
            var_Ip.coverage_content_type = "physicalMeasurement"
            var_Ip.cell_methods = "time: mean"
            var_Ip.long_name = "active spot sample intensity"
            var_Ip.C_format = "%10.2f"
            self.apply_wavelength_data(mon_times, var_Ip, data_Ip)

            var_If = mon_g.createVariable("reference_intensity", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(mon_g, var_If)
            var_If.variable_id = "If"
            var_If.coverage_content_type = "physicalMeasurement"
            var_If.cell_methods = "time: mean"
            var_If.long_name = "active spot reference intensity"
            var_If.C_format = "%10.2f"
            self.apply_wavelength_data(mon_times, var_If, data_If)

            var_Q = mon_g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_sample_flow(var_Q)
            netcdf_timeseries.variable_coordinates(mon_g, var_Q)
            var_Q.variable_id = "Q"
            var_Q.coverage_content_Qype = "physicalMeasurement"
            var_Q.cell_methods = "time: mean"
            var_Q.C_format = "%6.3f"
            self.apply_data(mon_times, var_Q, data_Q)
            for identity, value, _ in read_archive([Selection(
                    start=self.file_start,
                    end=self.file_end,
                    stations=[self.station],
                    archives=[self.archive + "_meta"],
                    variables=[f"Q_{self.instrument_id}"],
                    include_meta_archive=False,
                    include_default_station=False,
            )]):
                if not isinstance(value, variant.Metadata):
                    continue

                comment = []
                v = value.get("NoteCalibration")
                if v:
                    comment.append(str(v))
                v = value.get("NoteScale")
                if v:
                    comment.append(str(v))
                v = value.get("NoteFlow")
                if v:
                    comment.append(str(v))
                if comment:
                    var_Q.comment = "\n".join(comment)

            var_T1 = mon_g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_temperature(var_T1)
            netcdf_timeseries.variable_coordinates(mon_g, var_T1)
            var_T1.variable_id = "T1"
            var_T1.coverage_content_T1ype = "physicalMeasurement"
            var_T1.cell_methods = "time: mean"
            self.apply_data(mon_times, var_T1, data_T1)

            var_T2 = mon_g.createVariable("case_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T2)
            netcdf_timeseries.variable_coordinates(mon_g, var_T2)
            var_T2.variable_id = "T2"
            var_T2.coverage_content_type = "physicalMeasurement"
            var_T2.cell_methods = "time: mean"
            var_T2.long_name = "case temperature"
            self.apply_data(mon_times, var_T2, data_T2)

        var_Ld = g.createVariable("path_length_change", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ld)
        var_Ld.variable_id = "Ld"
        var_Ld.coverage_content_type = "physicalMeasurement"
        var_Ld.cell_methods = "time: sum"
        var_Ld.long_name = "change in path sample path length (flow/area)"
        var_Ld.units = "m"
        var_Ld.C_format = "%7.4f"
        var_Ld.ancillary_variables = "standard_temperature standard_pressure"
        data_L_start = self.Data(*self.convert_loaded(read_archive([Selection(
            start=self.file_start,
            end=self.file_end,
            stations=[self.station],
            archives=[self.archive],
            variables=[f"L_{self.instrument_id}"],
            include_meta_archive=False,
            include_default_station=False,
            lacks_flavors=["cover", "stats", "end"],
        )]), is_state=False, dtype=np.float64, return_cut_size=True))
        data_L_end = self.Data(*self.convert_loaded(read_archive([Selection(
            start=self.file_start,
            end=self.file_end,
            stations=[self.station],
            archives=[self.archive],
            variables=[f"L_{self.instrument_id}"],
            include_meta_archive=False,
            include_default_station=False,
            has_flavors=["end"],
            lacks_flavors=["cover", "stats"],
        )]), is_state=False, dtype=np.float64))
        if data_L_start.time.shape[0] > 0:
            calc_Ld = np.full(data_L_start.time.shape, nan, dtype=np.float64)
            if data_L_end.time.shape[0] > 0:
                out_begin = np.searchsorted(data_L_start.time, data_L_end.time[0], side="left")
                out_end = np.searchsorted(data_L_start.time, data_L_end.time[-1], side="right")
                if (out_end - out_begin) == data_L_end.time.shape[0]:
                    calc_Ld[out_begin:out_end] = data_L_end.value - data_L_start.value[out_begin:out_end]

            if data_L_start.time.shape[0] > 1:
                diff_Ld = data_L_start.value[1:] - data_L_start.value[:-1]
                diff_Ld = np.concatenate((
                    [diff_Ld[0]],
                    diff_Ld
                ))
                calc_dest = np.invert(np.isfinite(calc_Ld))
                calc_Ld[calc_dest] = diff_Ld[calc_dest]

            self.apply_data(times, var_Ld, data_L_start.time, calc_Ld)
        self.apply_data(times, var_Ld, data_Ld)

        self.apply_cut_size(g, times, [
            (var_Ld, data_Ld),
            (None, data_L_start),
        ] + ([
            (var_Q, data_Q),
            (var_T1, data_T1),
            (var_T2, data_T2),
        ] if not split_monitor else []), [
            (var_Ba, data_Ba),
        ] + ([
            (var_Ip, data_Ip),
            (var_If, data_If),
        ]if not split_monitor else []) + ([
            (var_Ir, data_Ir),
        ] if not split_transmittance else []), extra_sources=[data_system_flags])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Ba[wlidx].time.shape[0] > data_Ba[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times,f"Ba{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        g, times = self.state_group([data_Ff, data_Fn, spot])

        var_Ff = g.createVariable("filter_id", "u8", ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_Ff)
        var_Ff.variable_id = "Ff"
        var_Ff.coverage_content_type = "auxiliaryInformation"
        var_Ff.cell_methods = "time: point"
        var_Ff.long_name = "filter identifier"
        self.apply_state(times, var_Ff, data_Ff)

        var_Fn = g.createVariable("spot_number", "u8", ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_Fn)
        var_Fn.variable_id = "Fn"
        var_Fn.coverage_content_type = "auxiliaryInformation"
        var_Fn.cell_methods = "time: point"
        var_Fn.long_name = "active spot number"
        self.apply_state(times, var_Fn, data_Fn)

        var_In = g.createVariable("spot_normalization", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_In)
        var_In.variable_id = "In"
        var_In.coverage_content_type = "physicalMeasurement"
        var_In.cell_methods = "time: point"
        var_In.long_name = "sample/reference intensity at spot sampling start"
        var_In.units = "1"
        var_In.C_format = "%9.7f"

        if spot.time.shape[0] != 0:
            data_In = list()
            for spot_data in spot.value:
                add_In = list()
                for wlidx in range(len(self.WAVELENGTHS)):
                    add_In.append(float(spot_data.get(f"In{self.WAVELENGTHS[wlidx][1]}", nan)))
                data_In.append(add_In)
            data_In = np.array(data_In, dtype=np.float64)
            if len(data_In.shape) == 2 and data_In.shape[0] > 0 and data_In.shape[1] > 0:
                for wlidx in range(len(self.WAVELENGTHS)):
                    self.apply_state(times, var_In, spot.time, data_In[:, wlidx], (wlidx,))

        self.apply_instrument_metadata(
            [f"Ba{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="GML", model="CLAP"
        )
        return True
