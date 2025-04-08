import typing
from pathlib import Path
from forge.units import ZERO_C_IN_K
from forge.temp import WorkingDirectory
from forge.product.selection import InstrumentSelection
from . import EBASFile
from .aerosol_instrument import AerosolInstrument


class File(EBASFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            instrument_type=["dmtccn"],
            exclude_tags=["secondary"],
        )]

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "dmtcc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "DMT"

    @property
    def instrument_model(self) -> str:
        return "CCN-100"

    @property
    def instrument_name(self) -> str:
        return f'DMT_CCN-100_{self.station.upper()}'

    @property
    def instrument_type(self) -> str:
        return 'CCNC'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level0_metadata)
        r.update({
            'method': f'{self.lab_code}_CCNC',
            'unit': '1/cm3',
            'comp_name': 'cloud_condensation_nuclei_number_concentration',
            'vol_std_temp': 'instrument internal',
            'vol_std_pressure': 'instrument internal',
            'zero_negative': 'Zero possible',
            'zero_negative_desc': 'Zero values may appear due to statistical variations at very low concentrations',
        })
        return r

    async def __call__(self, output_directory: Path) -> None:
        async with WorkingDirectory() as data_directory:
            data_directory = Path(data_directory)
            await self.fetch_instrument_files(self.instrument_selection, 'raw', data_directory)

            matrix = self.MatrixData(self)
            flags = matrix.flags()
            instrument = matrix.metadata_tracker()
            pressure = matrix.variable(
                comp_name="pressure",
                unit="hPa",
                location="instrument internal",
                matrix="instrument",
                title="p_int",
            )
            temperature = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="instrument internal",
                matrix="instrument",
                title="T_int",
            )
            supersaturation = matrix.variable(
                comp_name="supersaturation",
                unit="%",
                matrix="instrument",
                title="SStgt",
            )
            temperature_gradient = matrix.variable(
                comp_name="temperature_gradient",
                unit="K",
                matrix="instrument",
                title="delTtgt",
            )
            temperature_T1 = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="CCNC column begin",
                matrix="instrument",
                title="T1_meas",
            )
            temperature_T2 = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="CCNC column centre",
                matrix="instrument",
                title="T2_meas",
            )
            temperature_T3 = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="CCNC column end",
                matrix="instrument",
                title="T3_meas",
            )
            temperature_nafion = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="CCNC humidifier",
                matrix="instrument",
                title="Tnaf_ms",
            )
            temperature_inlet = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="CCNC inlet",
                matrix="instrument",
                title="Tinl_ms",
            )
            temperature_opc = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="CCNC OPC",
                matrix="instrument",
                title="Topc_ms",
            )
            flow_rate_sample = matrix.variable(
                comp_name="flow_rate",
                unit="l/min",
                location="CCNC sample line",
                matrix="instrument",
                title="Qsm_ms",
            )
            flow_rate_sheath = matrix.variable(
                comp_name="flow_rate",
                unit="l/min",
                location="CCNC sheath",
                matrix="instrument",
                title="Qtot_ms",
            )
            opc_current = matrix.variable(
                comp_name="electric_current",
                unit="A",
                location="CCNC OPC laser",
                matrix="instrument",
                title="I_las",
            )
            first_stage_monitor = matrix.variable(
                comp_name="electric_tension",
                unit="V",
                location="CCNC OPC first stage",
                matrix="instrument",
                title="U_fst_status",
            )
            proportional_valve = matrix.variable(
                comp_name="electric_tension",
                unit="V",
                location="CCNC flow valve",
                matrix="instrument",
                title="Uvalv",
            )
            ccnc = matrix.variable(
                comp_name="cloud_condensation_nuclei_number_concentration",
                unit="1/cm3",
                detection_limit=[0.007, "1/cm3"],
                detection_limit_desc="Determined only by instrument counting statistics and flow rate",
                title="CCN",
            )

            async for nas, selector, root in matrix.iter_data_files(data_directory):
                flags[nas].integrate_file(root, selector)
                instrument[nas].integrate_file(root)
                for var in self.select_variable(
                        root,
                        {"standard_name": "air_temperature"},
                ):
                    temperature[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x + ZERO_C_IN_K
                    )
                for var in self.select_variable(
                        root,
                        {"standard_name": "air_pressure"},
                ):
                    pressure[nas].integrate_variable(var, selector(var, require_cut_size_match=False))

                for var in self.select_variable(
                        root,
                        {"variable_name": "gradiant_setpoint"},
                ):
                    temperature_gradient[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "tec1_temperature"},
                ):
                    temperature_T1[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "tec2_temperature"},
                ):
                    temperature_T2[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "tec3_temperature"},
                ):
                    temperature_T3[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "nafion_temperature"},
                ):
                    temperature_nafion[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "inlet_temperature"},
                ):
                    temperature_inlet[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "opc_temperature"},
                ):
                    temperature_opc[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "sample_flow"},
                ):
                    flow_rate_sample[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "sheath_flow"},
                ):
                    flow_rate_sheath[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "laser_current"},
                ):
                    opc_current[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x * 1000.0
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "first_stage_monitor"},
                ):
                    first_stage_monitor[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "proportional_valve"},
                ):
                    proportional_valve[nas].integrate_variable(var, selector(var, require_cut_size_match=False))

                for var in self.select_variable(
                        root,
                        {"variable_name": "supersaturation_setting"},
                ):
                    supersaturation[nas].integrate_variable(var, selector(var))
                for var in self.select_variable(
                        root,
                        {"variable_name": "number_concentration"},
                ):
                    ccnc[nas].integrate_variable(var, selector(var))

        for var in supersaturation:
            var.add_characteristic('Actual/target', 'target', self.instrument_type, var.metadata.comp_name, '0')
        for var in temperature_gradient:
            var.add_characteristic('Actual/target', 'target', self.instrument_type, var.metadata.comp_name, '0')
        for var in temperature_T1:
            var.add_characteristic('Actual/target', 'actual', self.instrument_type, var.metadata.comp_name, '0')
        for var in temperature_T2:
            var.add_characteristic('Actual/target', 'actual', self.instrument_type, var.metadata.comp_name, '0')
        for var in temperature_T3:
            var.add_characteristic('Actual/target', 'actual', self.instrument_type, var.metadata.comp_name, '0')
        for var in temperature_nafion:
            var.add_characteristic('Actual/target', 'actual', self.instrument_type, var.metadata.comp_name, '0')
        for var in temperature_inlet:
            var.add_characteristic('Actual/target', 'actual', self.instrument_type, var.metadata.comp_name, '0')
        for var in temperature_opc:
            var.add_characteristic('Actual/target', 'actual', self.instrument_type, var.metadata.comp_name, '0')
        for var in flow_rate_sample:
            var.add_characteristic('Actual/target', 'actual', self.instrument_type, var.metadata.comp_name, '0')
        for var in flow_rate_sheath:
            var.add_characteristic('Actual/target', 'actual', self.instrument_type, var.metadata.comp_name, '0')
        for var in proportional_valve:
            var.add_characteristic('Actual/target', 'actual', self.instrument_type, var.metadata.comp_name, '0')

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                [ccnc[nas], supersaturation[nas]],
                optional=[pressure[nas], temperature[nas], temperature_gradient[nas], temperature_T1[nas],
                          temperature_T2[nas], temperature_T3[nas], temperature_nafion[nas], temperature_inlet[nas],
                          temperature_opc[nas], flow_rate_sample[nas], flow_rate_sheath[nas], opc_current[nas],
                          first_stage_monitor[nas], proportional_valve[nas]],
                flags=flags[nas],
            )
