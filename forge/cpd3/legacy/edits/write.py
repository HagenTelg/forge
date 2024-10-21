import typing
import asyncio
import logging
import random
import re
import numpy as np
from math import floor, ceil, isfinite, nan
from netCDF4 import Dataset, Variable
from tempfile import NamedTemporaryFile
from forge.cpd3.identity import Identity
from forge.cpd3.variant import to_json
from forge.cpd3.timeinterval import TimeUnit, TimeInterval
from forge.logicaltime import start_of_year, containing_year_range, year_bounds
from forge.formattime import format_iso8601_time
from forge.const import MAX_I64
from forge.archive.client import index_lock_key, index_file_name, edit_directives_lock_key, edit_directives_file_name, edit_directives_notification_key, data_lock_key, data_file_name
from forge.archive.client.connection import Connection, LockBackoff, LockDenied
from forge.archive.client.archiveindex import ArchiveIndex as BaseArchiveIndex
from forge.data.structure import edit_directives
from forge.data.structure.editdirectives import edit_file_structure
from forge.data.enum import remap_enum

_LOGGER = logging.getLogger(__name__)


class EditConversionFailed(Exception):
    def __init__(self, message: str):
        super().__init__()
        self.message = message


class EditIndex(BaseArchiveIndex):
    def __init__(self, station: str, archive: str = "raw"):
        super().__init__()
        self.station = station
        self.archive = archive
        self._days: typing.Dict[int, typing.Dict[str, typing.Dict[str, typing.Set[float]]]] = dict()

    async def initialize(self, connection: Connection, year: int) -> None:
        year_start, year_end = year_bounds(year)
        done_instrument: typing.Set[str] = set()
        for variable_id, instrument_vars in self.variable_ids.items():
            for instrument_id, wavelength_count in instrument_vars.items():
                if wavelength_count == 0:
                    continue
                if instrument_id in done_instrument:
                    continue
                done_instrument.add(instrument_id)

                for day_start in range(year_start, year_end, 24 * 60 * 60):
                    def recurse_group(group: Dataset) -> None:
                        for var in group.variables.values():
                            var_id = getattr(var, 'variable_id', None)
                            if var_id is None:
                                continue
                            var_id = str(var_id)
                            if not var_id:
                                continue

                            wavelengths: typing.Set[float] = set()
                            if 'wavelength' in var.dimensions:
                                check_group = group
                                while True:
                                    if 'wavelength' in check_group.dimensions:
                                        wavelengths = set([float(wl) for wl in check_group.variables['wavelength'][:].data])
                                        break
                                    check_group = check_group.parent
                                    assert check_group is not None
                            elif 'wavelength' in getattr(var, 'ancillary_variables', "").split():
                                wavelengths = set([float(wl) for wl in group.variables['wavelength'][:].data])
                            if not wavelengths:
                                continue

                            day = self._days.get(day_start)
                            if not day:
                                day = dict()
                                self._days[day_start] = day
                            day_instrument = day.get(instrument_id)
                            if not day_instrument:
                                day_instrument = dict()
                                day[instrument_id] = day_instrument
                            day_instrument[var_id] = wavelengths

                        for g in group.groups.values():
                            if g.name == 'statistics':
                                continue
                            recurse_group(g)

                    with NamedTemporaryFile(suffix=".nc") as data_file:
                        try:
                            await connection.read_file(
                                data_file_name(self.station, self.archive, instrument_id, day_start),
                                data_file,
                            )
                            data_file.flush()
                        except FileNotFoundError:
                            continue

                        root = Dataset(data_file.name, 'r')
                        try:
                            recurse_group(root)
                        finally:
                            root.close()

    def variable_wavelengths(self, start_epoch: typing.Optional[float], end_epoch: typing.Optional[float],
                             instrument: str, variable: str) -> typing.Set[float]:
        if not self._days:
            return set()
        if start_epoch is None:
            day_start = min(self._days.keys())
        else:
            day_start = int(floor(start_epoch / (24 * 60 * 60))) * 24 * 60 * 60
        if end_epoch is None:
            day_end = max(self._days.keys()) + 24 * 60 * 60
        else:
            day_end = int(ceil(end_epoch / (24 * 60 * 60))) * 24 * 60 * 60
            if day_end < end_epoch:
                day_end += 24 * 60 * 60

        result: typing.Set[float] = set()
        for epoch in range(day_start, day_end, 24 * 60 * 60):
            day = self._days.get(epoch)
            if not day:
                continue
            day_instrument = day.get(instrument)
            if not day_instrument:
                continue
            wavelengths = day_instrument.get(variable)
            if not wavelengths:
                continue
            result.update(wavelengths)
        return result


class EditDirective:
    def __init__(self, identity: Identity, info: typing.Dict[str, typing.Any],
                 modified: typing.Optional[float] = None,
                 allocated_uids: typing.Set[int] = None):
        self.station = identity.station

        self.start_epoch: typing.Optional[float] = identity.start
        self.end_epoch: typing.Optional[float] = identity.end
        if self.start_epoch and self.end_epoch:
            assert self.start_epoch < self.end_epoch

        self.profile: str = identity.variable
        self.author = str(info.get("Author"))
        self.comment = str(info.get("Comment"))
        self._history = list(info.get("History", []))
        self.modified_time: int = int(round((modified if modified else float((self.history[-1] if self.history else dict()).get("At", self.end_epoch))) * 1000))
        parameters = dict(info.get("Parameters", dict()))
        self._action = parameters.get("Action", dict())
        self._trigger = parameters.get("Trigger", dict())
        self.disabled = bool(info.get("Disabled"))

        self.skip_conversion = bool(info.get("SkipForgeConversion"))

        # Since this import is usually only done once, don't worry about collisions with existing
        while True:
            self.unique_id = random.randint(1, 1 << 64)
            if allocated_uids is not None:
                if self.unique_id in allocated_uids:
                    continue
                allocated_uids.add(self.unique_id)
                break

    @property
    def affected_years(self) -> typing.Tuple[int, int]:
        if not self.start_epoch or not self.end_epoch:
            return 0, 1
        return containing_year_range(self.start_epoch, self.end_epoch)

    @property
    def start_time(self) -> int:
        if not self.start_epoch:
            return -MAX_I64
        return int(floor(self.start_epoch * 1000))

    @property
    def end_time(self) -> int:
        if not self.end_epoch:
            return MAX_I64
        return int(ceil(self.end_epoch * 1000))

    @property
    def is_clap_correction(self) -> bool:
        def is_low_tr(instrument: str):
            t = self._action.get("Type")
            if t:
                t = t.lower()
                if t != "invalidate":
                    return False
            sel = self._action.get("Selection")
            if not sel:
                return False
            if isinstance(sel, dict):
                sel = [sel]
            for check in sel:
                var = check.get("Variable")
                if var not in (f"BaB_{instrument}", f"BaG_{instrument}", f"BaR_{instrument}"):
                    return False

            return self._trigger == {'Type': 'Less', 'Right': 0.5, 'Left': {'Value': [{'Variable': f'IrG_{instrument}'}]}} or \
                self._trigger == [{'Type': 'Less', 'Right': 0.5, 'Left': {'Value': [{'Variable': f'IrG_{instrument}'}]}}] or \
                self._trigger == [
                    {'Type': 'Less', 'Right': 0.5, 'Left': {'Value': [{'Variable': f'IrB_{instrument}'}]}},
                    {'Type': 'Less', 'Right': 0.5, 'Left': {'Value': [{'Variable': f'IrG_{instrument}'}]}},
                    {'Type': 'Less', 'Right': 0.5, 'Left': {'Value': [{'Variable': f'IrR_{instrument}'}]}},
                ]

        def is_azumi_filter(instrument: str) -> bool:
            return self._action == {
                "Calibration": [0.0, 0.8],
                "Selection": [
                    {"Variable": f"BaB_{instrument}"},
                    {"Variable": f"BaG_{instrument}"},
                    {"Variable": f"BaR_{instrument}"},
                ],
                "Type": "Polynomial",
            }

        for instrument in ("A11", "A12"):
            if is_low_tr(instrument):
                return True
            if is_azumi_filter(instrument):
                return True
        return False

    def history(
            self,
            index: EditIndex,
            current_action_type: typing.Optional[str] = None, current_action_parameters: typing.Optional[str] = None,
            current_condition_type: typing.Optional[str] = None, current_condition_parameters: typing.Optional[str] = None,
    ) -> str:
        result: typing.List[typing.Dict[str, typing.Any]] = list()

        for entry in reversed(self._history):
            item = {
                'time_epoch_ms': round(entry.get('At', 0) * 1000),
                'user_name': entry.get('User', ''),
            }

            operation = entry.get('Type')
            if operation == 'ProfileChanged':
                item['changed_profile'] = entry['OriginalProfile']
            elif operation == 'BoundsChanged':
                t = entry['OriginalBounds'].get('Start')
                item['changed_start_time'] = round(t * 1000) if t and isfinite(t) else -MAX_I64
                t = entry['OriginalBounds'].get('End')
                item['changed_end_time'] = round(t * 1000) if t and isfinite(t) else MAX_I64
            elif operation == 'ParametersChanged':
                op = entry.get("OriginalParameters", dict())

                try:
                    updated_action_type, updated_action_parameters = self._convert_action(op.get("Action", dict()), index)
                    if updated_action_type != current_action_type:
                        item['changed_action_type'] = updated_action_type
                        current_action_type = updated_action_type
                    if updated_action_parameters != current_action_parameters:
                        item['changed_action_parameters'] = updated_action_type
                        current_action_parameters = updated_action_parameters
                except EditConversionFailed:
                    pass

                try:
                    updated_condition_type, updated_condition_parameters = self._convert_condition(op.get("Trigger", dict()), index, silent=True)
                    if updated_condition_type != current_condition_type:
                        item['changed_condition_type'] = updated_condition_type
                        current_condition_type = updated_condition_type
                    if updated_condition_parameters != current_condition_parameters:
                        item['changed_condition_parameters'] = updated_condition_type
                        current_condition_parameters = updated_condition_parameters
                except EditConversionFailed:
                    pass
            elif operation == 'ExtendChanged':
                continue
            elif operation == 'AuthorChanged':
                item['changed_author'] = entry['OriginalAuthor']
            elif operation == 'CommentChanged':
                item['changed_comment'] = entry['OriginalComment']
            elif operation == 'PriorityChanged':
                continue
            elif operation == 'SystemInternalChanged':
                continue
            elif operation == 'Disabled':
                item['deleted'] = True
            elif operation == 'Enabled':
                item['deleted'] = False
            elif operation == 'Created':
                item['created'] = True

            result.append(item)
        result = list(reversed(result))
        return to_json(result, sort_keys=True)

    def _from_cpd3_selection(
            self,
            index: EditIndex, selection: typing.Any,
    ) -> typing.List[typing.Dict[str, typing.Any]]:
        def match_variable(vars: typing.List[str]) -> typing.List[typing.Dict[str, typing.Any]]:
            matchers: typing.List[re.Pattern] = list()
            for var in vars:
                matchers.append(re.compile(var))

            def any_match(check_variable: str) -> bool:
                for m in matchers:
                    if m.fullmatch(check_variable):
                        return True
                return False

            def assign_wavelength_suffixes(wavelengths: typing.List[float]) -> typing.List[str]:
                def named_suffix(wl: float) -> typing.Optional[str]:
                    if wl < 400:
                        return None
                    elif wl < 500:
                        return "B"
                    elif wl < 600:
                        return "G"
                    elif wl < 750:
                        return "R"
                    return None

                unique_suffixes: typing.Set[str] = set()
                wavelength_suffixes: typing.List[str] = list()
                for wl in wavelengths:
                    s = named_suffix(float(wl))
                    if not s or s in unique_suffixes:
                        return [str(i + 1) for i in range(len(wavelengths))]
                    wavelength_suffixes.append(s)
                    unique_suffixes.add(s)
                return wavelength_suffixes

            result: typing.List[typing.Dict[str, typing.Any]] = list()
            for variable_id, instrument_vars in index.variable_ids.items():
                for instrument_id, wavelength_count in instrument_vars.items():
                    if '_' in variable_id:
                        if not any_match(variable_id):
                            continue
                        result.append({
                            "instrument_id": instrument_id,
                            "variable_id": variable_id,
                        })
                        continue

                    check_variable = f"{variable_id}_{instrument_id}"
                    if any_match(check_variable):
                        result.append({
                            "instrument_id": instrument_id,
                            "variable_id": variable_id,
                        })
                        continue

                    if wavelength_count == 0:
                        continue

                    for code in ["B", "G", "R", "Q"] + [str(i+1) for i in range(wavelength_count)]:
                        if any_match(f"{variable_id}{code}_{instrument_id}"):
                            break
                    else:
                        continue

                    variable_wavelengths = index.variable_wavelengths(self.start_epoch, self.end_epoch,
                                                                      instrument_id, variable_id)
                    if not variable_wavelengths:
                        continue
                    variable_wavelengths = sorted(variable_wavelengths)
                    wavelength_codes = assign_wavelength_suffixes(variable_wavelengths)

                    for idx in range(len(wavelength_codes)):
                        check_variable = f"{variable_id}{wavelength_codes[idx]}_{instrument_id}"
                        if not any_match(check_variable):
                            continue

                        result.append({
                            "instrument_id": instrument_id,
                            "variable_id": variable_id,
                            "wavelength": variable_wavelengths[idx],
                        })

            return result

        if selection is None:
            return []
        elif isinstance(selection, str):
            parts = selection.split(':')
            if len(parts) == 0:
                return []
            elif len(parts) == 1:
                if len(parts[0]) == 0:
                    return []
                return match_variable([parts[0]])
            elif len(parts) == 2:
                if parts[0] != 'raw' and parts[0] != 'clean':
                    raise EditConversionFailed("invalid selection archive")
                return match_variable([parts[1]])
            elif len(parts) == 3:
                if parts[0] != self.station:
                    raise EditConversionFailed("invalid selection station")
                if parts[1] != 'raw' and parts[1] != 'clean':
                    raise EditConversionFailed("invalid selection archive")

                return match_variable([parts[2]])
            else:
                if parts[0] != self.station:
                    raise EditConversionFailed("invalid selection station")
                if parts[1] != 'raw' and parts[1] != 'clean':
                    raise EditConversionFailed("invalid selection archive")

                has_flavors: typing.Set[str] = set()
                lacks_flavors: typing.Set[str] = set()
                exact_flavors: typing.Set[str] = set()
                for i in range(3, len(parts)):
                    flavor = parts[i]
                    if flavor.startswith('!') or flavor.startswith('-'):
                        exact_flavors.clear()
                        lacks_flavors.add(flavor[1:])
                    elif flavor.startswith('='):
                        has_flavors.clear()
                        lacks_flavors.clear()
                        if len(flavor) == 1:
                            exact_flavors.add('')
                        else:
                            exact_flavors.add(flavor[1:])
                    elif flavor.startswith('+'):
                        exact_flavors.clear()
                        has_flavors.add(flavor[1:])
                    else:
                        exact_flavors.clear()
                        has_flavors.add(flavor)

                if has_flavors or lacks_flavors or exact_flavors:
                    raise EditConversionFailed("invalid flavor selection")

                return match_variable([parts[2]])
        elif isinstance(selection, dict):
            return self._from_cpd3_selection(index, [selection])

        has_flavors: typing.Set[str] = set()
        lacks_flavors: typing.Set[str] = set()
        exact_flavors: typing.Set[str] = set()
        find_variables: typing.List[str] = list()
        for entry in selection:
            if 'Station' in entry:
                if entry['Station'] != self.station:
                    raise EditConversionFailed("invalid selection station")
            if 'Archive' in entry:
                if entry['Archive'] != 'raw' and entry['Archive'] != 'clean':
                    raise EditConversionFailed("invalid selection archive")
            if 'Variable' not in entry:
                raise EditConversionFailed("invalid selection variable")

            add_has_flavors: typing.Set[str] = set()
            add_lacks_flavors: typing.Set[str] = set()
            add_exact_flavors: typing.Set[str] = set()
            if 'Flavors' in entry:
                add_exact_flavors = set(entry['Flavors'])
            else:
                if 'HasFlavors' in entry:
                    add_has_flavors = set(entry['HasFlavors'])
                if 'LacksFlavors' in entry:
                    add_exact_flavors = set(entry['LacksFlavors'])
            if has_flavors and add_has_flavors != has_flavors:
                raise EditConversionFailed("invalid mixed flavor selection")
            if lacks_flavors and add_lacks_flavors != lacks_flavors:
                raise EditConversionFailed("invalid mixed flavor selection")
            if exact_flavors and add_exact_flavors != exact_flavors:
                raise EditConversionFailed("invalid mixed flavor selection")

            has_flavors = has_flavors
            lacks_flavors = lacks_flavors
            exact_flavors = exact_flavors

            find_variables.append(entry['Variable'])

        if has_flavors or lacks_flavors or exact_flavors:
            raise EditConversionFailed("invalid flavor selection")

        return match_variable(find_variables)

    def _convert_size_selection(self, selection: typing.Any) -> typing.Optional[float]:
        if selection is None:
            return None

        def convert_selection(var: str,
                              has_flavors: typing.Set[str],
                              lacks_flavors: typing.Set[str],
                              exact_flavors: typing.Set[str]) -> typing.Optional[float]:
            if not exact_flavors and not lacks_flavors:
                if var:
                    return None
                if has_flavors == {"pm10"}:
                    return 10.0
                elif has_flavors == {"pm1"}:
                    return 1.0
                elif has_flavors == {"pm25"}:
                    return 2.5
            elif not exact_flavors and not has_flavors:
                if var != '(((Ba[cfs]*)|(Bb?s)|Be|Ir|L|(N[nbs]?)|(X[cfs]*))[BGRQ0-9]*_.*)|((T|P|U)[0-9]*u?_[SAEN].*)':
                    return None
                if lacks_flavors == {"pm1", "pm10", "pm25"}:
                    return nan
            return None

        if isinstance(selection, str):
            parts = selection.split(':')
            if len(parts) == 0:
                return None
            elif len(parts) == 1:
                return None
            elif len(parts) == 2:
                return None
            elif len(parts) == 3:
                return None
            else:
                if parts[0] != self.station:
                    return None
                if parts[1] != 'raw' and parts[1] != 'clean':
                    return None

                has_flavors: typing.Set[str] = set()
                lacks_flavors: typing.Set[str] = set()
                exact_flavors: typing.Set[str] = set()
                for i in range(3, len(parts)):
                    flavor = parts[i]
                    if flavor.startswith('!') or flavor.startswith('-'):
                        exact_flavors.clear()
                        lacks_flavors.add(flavor[1:])
                    elif flavor.startswith('='):
                        has_flavors.clear()
                        lacks_flavors.clear()
                        if len(flavor) == 1:
                            exact_flavors.add('')
                        else:
                            exact_flavors.add(flavor[1:])
                    elif flavor.startswith('+'):
                        exact_flavors.clear()
                        has_flavors.add(flavor[1:])
                    else:
                        exact_flavors.clear()
                        has_flavors.add(flavor)

                return convert_selection(parts[2], has_flavors, lacks_flavors, exact_flavors)
        elif isinstance(selection, dict):
            return self._convert_size_selection([selection])

        has_flavors: typing.Set[str] = set()
        lacks_flavors: typing.Set[str] = set()
        exact_flavors: typing.Set[str] = set()
        var: typing.Optional[str] = None
        for entry in selection:
            if 'Station' in entry:
                if entry['Station'] != self.station:
                    return None
            if 'Archive' in entry:
                if entry['Archive'] != 'raw' and entry['Archive'] != 'clean':
                    return None
            if 'Variable' not in entry:
                if var:
                    return None
            else:
                if var and var != entry['Variable']:
                    return None
                var = entry['Variable']

            add_has_flavors: typing.Set[str] = set()
            add_lacks_flavors: typing.Set[str] = set()
            add_exact_flavors: typing.Set[str] = set()
            if 'Flavors' in entry:
                add_exact_flavors = set(entry['Flavors'])
            else:
                if 'HasFlavors' in entry:
                    add_has_flavors = set(entry['HasFlavors'])
                if 'LacksFlavors' in entry:
                    add_exact_flavors = set(entry['LacksFlavors'])
            if has_flavors and add_has_flavors != has_flavors:
                return None
            if lacks_flavors and add_lacks_flavors != lacks_flavors:
                return None
            if exact_flavors and add_exact_flavors != exact_flavors:
                return None
            has_flavors = add_has_flavors
            lacks_flavors = add_lacks_flavors
            exact_flavors = add_exact_flavors

        return convert_selection(var, has_flavors, lacks_flavors, exact_flavors)

    def _convert_action(self, action: typing.Dict[str, typing.Any], index: EditIndex) -> typing.Tuple[str, str]:
        def convert_calibration(calibration: typing.Any) -> typing.List[float]:
            if calibration is None:
                return []

            if isinstance(calibration, float) or isinstance(calibration, int):
                return [float(calibration)]
            elif isinstance(calibration, dict):
                return convert_calibration(calibration.get('Coefficients'))
            elif not isinstance(calibration, list):
                return []

            result = []
            for coefficient in calibration:
                try:
                    coefficient = float(coefficient)
                except (ValueError, TypeError):
                    return []
                result.append(coefficient)
            return result

        op = action.get('Type')
        if isinstance(op, str):
            op = op.lower()
        if op in ('contaminate', 'contam'):
            return "Contaminate", ""
        elif op in ('polynomial', 'poly', 'cal', 'calibration'):
            sel = self._from_cpd3_selection(index, action.get("Selection"))
            if not sel:
                raise EditConversionFailed("no variables for calibration")
            return "Calibration", to_json({
                "selection": sel,
                "calibration": convert_calibration(action.get("Calibration")),
            }, sort_keys=True)
        elif op == 'recalibrate':
            sel = self._from_cpd3_selection(index, action.get("Selection"))
            if not sel:
                raise EditConversionFailed("no variables for recalibration")
            return "Recalibrate", to_json({
                "selection": sel,
                "calibration": convert_calibration(action.get("Calibration")),
                "reverse_calibration": convert_calibration(action.get("Original")),
            }, sort_keys=True)
        elif op in ('flowcorrection', 'flowcalibration'):
            inst = action.get('Instrument')
            if not inst:
                raise EditConversionFailed("no instrument for flow correction")
            return "FlowCorrection", to_json({
                "instrument": str(inst),
                "calibration": convert_calibration(action.get("Calibration")),
                "reverse_calibration": convert_calibration(action.get("Original")),
            }, sort_keys=True)
        elif op in ('setcut', 'cut'):
            in_cs = self._convert_size_selection(action.get("Selection"))
            if in_cs is None:
                raise EditConversionFailed("invalid size selection for cut size change")
            if not isfinite(in_cs):
                in_cs = None

            out_cs = str(action.get('Cut', ''))
            if isinstance(out_cs, str):
                out_cs = out_cs.lower()
            if out_cs == "pm1":
                out_cs = 1.0
            elif out_cs == "pm25" or out_cs == "pm2.5":
                out_cs = 2.5
            elif out_cs == "pm10":
                out_cs = 10.0
            else:
                out_cs = None

            result = dict()
            if in_cs is not None:
                result['cutsize'] = in_cs
            if out_cs is not None:
                result['modified_cutsize'] = out_cs

            return "SizeCutFix", to_json(result, sort_keys=True)
        elif op == 'abnormaldataepisode':
            episode_type = str(action.get('EpisodeType', 'WildFire')).lower()
            if episode_type == 'dust':
                episode_type = 'dust'
            else:
                episode_type = 'wild_fire'
            return "AbnormalData", to_json({
                "episode_type": episode_type,
            }, sort_keys=True)
        elif op == 'invalidate' or not op:
            pass
        else:
            raise EditConversionFailed(f"invalid edit type {op}")

        check_size_sel = self._convert_size_selection(action.get("Selection"))
        if check_size_sel is not None:
            result = {
                'modified_cutsize': 'invalidate',
            }
            if isfinite(check_size_sel):
                result['cutsize'] = check_size_sel
            return "SizeCutFix", to_json(result, sort_keys=True)

        sel = self._from_cpd3_selection(index, action.get("Selection"))
        if not sel:
            raise EditConversionFailed("no variables for invalidate")
        return "Invalidate", to_json({
            "selection": sel,
        }, sort_keys=True)
    
    def action(self, index: EditIndex) -> typing.Tuple[str, str]:
        return self._convert_action(self._action, index)

    def _convert_condition(self, trigger: typing.Dict[str, typing.Any], index: EditIndex, silent: bool = False) -> typing.Tuple[str, str]:
        if not trigger:
            return "None", ""

        def to_constant(value: typing.Any) -> typing.Optional[float]:
            if isinstance(value, dict):
                if value.get('Type').lower() == 'constant':
                    value = value.get('Value')
            if value is None:
                return None
            if not isinstance(value, float):
                raise EditConversionFailed("non-constant value in trigger")
            if not isfinite(value):
                return None
            return value

        def to_variable_selection(value: typing.Any) -> typing.List[typing.Dict[str, typing.Any]]:
            if not isinstance(value, dict):
                return []
            op = value.get('Type')
            if not isinstance(op, str):
                return self._from_cpd3_selection(index, value.get('Value'))
            op = op.lower()
            if op == 'constant':
                return []
            elif op == 'sin':
                return []
            elif op == 'cos':
                return []
            elif op == 'log' or op == 'ln':
                return []
            elif op == 'log10':
                return []
            elif op == 'exp':
                return []
            elif op == 'abs' or op == 'absolute' or op == 'absolutevalue':
                return []
            elif op == 'poly' or op == 'polynomial' or op == 'cal' or op == 'calibration':
                return []
            elif op == 'polyinvert' or op == 'polynomialinvert' or op == 'invertcal' or op == 'invertcalibration':
                return []
            elif op == 'mean':
                return []
            elif op == 'sd' or op == 'standarddeviation':
                return []
            elif op == 'quantile':
                return []
            elif op == 'median':
                return []
            elif op == 'maximum' or op == 'max':
                return []
            elif op == 'slope':
                return []
            elif op == 'length' or op == 'duration' or op == 'elapsed':
                return []
            elif op == 'average' or op == 'smoothed':
                return []
            elif op == 'sum' or op == 'add':
                return []
            elif op == 'difference' or op == 'subtract':
                return []
            elif op == 'power':
                return []
            elif op == 'largest':
                return []
            elif op == 'smallest':
                return []
            elif op == 'first' or op == 'firstvalid' or op == 'valid':
                return []

            return self._from_cpd3_selection(index, value.get('Value'))

        def convert_element(element: typing.Dict[str, typing.Any]) -> typing.Tuple[str, typing.Optional[typing.Dict[str, typing.Any]]]:
            if element is None:
                return "None", None

            if isinstance(element, bool):
                if bool(element):
                    return "None", None
                raise EditConversionFailed("trigger element is always false")

            op = element.get('Type')
            if isinstance(op, str):
                op = op.lower()
            if op in ('range', 'insiderange'):
                selection = to_variable_selection(element.get('Value'))
                if len(selection) != 1:
                    raise EditConversionFailed("trigger range selection is invalid")
                return "Threshold", {
                    'lower': to_constant(element.get('Start')),
                    'upper': to_constant(element.get('End')),
                    'selection': selection,
                }
            elif op in ('less', 'lessthan', 'lessequal', 'lessthanorequal', 'lessthanorequalto'):
                selection = to_variable_selection(element.get('Left'))
                if len(selection) == 1:
                    if op in ('lessequal', 'lessthanorequal', 'lessthanorequalto'):
                        if not silent:
                            _LOGGER.debug(f"Threshold equality ignored for {self}")
                    return "Threshold", {
                        'upper': to_constant(element.get('Right')),
                        'selection': selection,
                    }
                selection = to_variable_selection(element.get('Right'))
                if len(selection) == 1:
                    if op in ('lessequal', 'lessthanorequal', 'lessthanorequalto'):
                        if not silent:
                            _LOGGER.debug(f"Threshold equality ignored for {self}")
                    return "Threshold", {
                        'lower': to_constant(element.get('Left')),
                        'selection': selection,
                    }
            elif op in ('greater', 'greaterthan', 'greaterequal', 'greaterthanorequal', 'greaterthanorequalto'):
                selection = to_variable_selection(element.get('Left'))
                if len(selection) == 1:
                    if op in ('greaterequal', 'greaterthanorequal', 'greaterthanorequalto'):
                        if not silent:
                            _LOGGER.debug(f"Threshold equality ignored for {self}")
                    return "Threshold", {
                        'lower': to_constant(element.get('Right')),
                        'selection': selection,
                    }
                selection = to_variable_selection(element.get('Right'))
                if len(selection) == 1:
                    if op in ('greaterequal', 'greaterthanorequal', 'greaterthanorequalto'):
                        if not silent:
                            _LOGGER.debug(f"Threshold equality ignored for {self}")
                    return "Threshold", {
                        'type': 'threshold',
                        'upper': to_constant(element.get('Left')),
                        'selection': selection,
                    }
            elif op in ('periodic', 'moment', 'instant'):
                moments = element.get('Moments', [])
                if isinstance(moments, int):
                    moments = [moments]
                elif not isinstance(moments, list):
                    raise EditConversionFailed("periodic moment list is invalid")
                for i in range(len(moments)):
                    moments[i] = int(moments[i])

                interval = TimeInterval.from_variant(element.get('Interval'), TimeInterval(TimeUnit.Second, 1, True))
                if interval.count != 1:
                    raise EditConversionFailed("periodic moment interval is invalid")
                momentUnit = TimeInterval.from_variant(element.get('MomentUnit'),
                                                       TimeInterval(TimeUnit.Second, 1, True))
                if momentUnit.count != 1:
                    raise EditConversionFailed("periodic moment interval is not one")

                result = {
                    'moments': moments,
                }
                if interval.unit == TimeUnit.Hour:
                    result['interval'] = 'hour'
                    if momentUnit.unit == TimeUnit.Minute:
                        result['division'] = 'minute'
                        return "Periodic", result
                elif interval.unit == TimeUnit.Day:
                    result['interval'] = 'day'
                    if momentUnit.unit == TimeUnit.Minute:
                        result['division'] = 'minute'
                        return "Periodic", result
                    elif momentUnit.unit == TimeUnit.Hour:
                        result['division'] = 'hour'
                        return "Periodic", result
                raise EditConversionFailed("periodic layout is not supported")
            elif op in ('always', 'none'):
                return "None", None

            raise EditConversionFailed("trigger is not supported")

        if isinstance(trigger, dict):
            op = trigger.get('Type', '').lower()
            if op == 'or' or op == 'any':
                trigger = trigger.get('Components', [])

        if isinstance(trigger, list):
            condition_type, condition_parameters = convert_element(trigger[0])

            def or_element(add_parameters: typing.Dict[str, typing.Any]):
                if condition_type == 'None':
                    return
                elif condition_type == 'Threshold':
                    if condition_parameters.get('lower') != add_parameters.get('lower'):
                        raise EditConversionFailed("trigger or threshold lower does not match")
                    if condition_parameters.get('upper') != add_parameters.get('upper'):
                        raise EditConversionFailed("trigger or threshold upper does not match")
                    condition_parameters['selection'].extend(add_parameters['selection'])
                    return
                raise EditConversionFailed("trigger or type incompatible")

            for i in range(1, len(trigger)):
                add_type, add_parameters = convert_element(trigger[i])
                if add_type != condition_type:
                    raise EditConversionFailed("trigger or combination is invalid")
                or_element(add_parameters)
            return condition_type, to_json(condition_parameters, sort_keys=True)

        condition_type, condition_parameters = convert_element(trigger)
        return condition_type, to_json(condition_parameters, sort_keys=True)
    
    def condition(self, index: EditIndex) -> typing.Tuple[str, str]:
        return self._convert_condition(self._trigger, index)

    def __str__(self) -> str:
        comment = self.comment.strip().replace('\n', ' ').replace('\r', ' ')
        return f"{format_iso8601_time(self.start_epoch) if self.start_epoch else 'UNDEF'},{format_iso8601_time(self.end_epoch) if self.end_epoch else 'UNDEF'} by {self.author} '{comment}'"

    def __repr__(self):
        return str(self)


def write_all(
        station: str,
        year_data: typing.Dict[int, typing.List[EditDirective]],
        data_start: float, data_end: float,
) -> typing.Tuple[int, int]:
    total = 0
    modified = 0

    def modify_edits(
            input_file: typing.Optional[str], output_file: str,
            file_start: typing.Optional[float], file_end: typing.Optional[float],
            merge_info: typing.List[EditDirective],
            raw_index: EditIndex,
    ) -> typing.Set[typing.Tuple[int, int]]:
        all_profiles: typing.Set[str] = set()
        for info in merge_info:
            all_profiles.add(info.profile)

        input_root = None
        if input_file:
            input_file = Dataset(input_file, 'r')
            input_root = input_file.groups.get("edits")
            if input_root is not None:
                all_profiles.update(input_root.variables["profile"].datatype.enum_dict.keys())
        else:
            input_file = None

        output_file = Dataset(output_file, 'w', format='NETCDF4')
        try:
            edit_directives(output_file, station, file_start, file_end)
            edit_file_structure(output_file, sorted(all_profiles))
            output_root = output_file.groups["edits"]

            if input_root is not None:
                for var in ("start_time", "end_time", "modified_time", "unique_id",):
                    output_root.variables[var][:] = input_root.variables[var][:]
                for var in ("action_parameters", "condition_parameters", "author", "comment", "history",):
                    input_var = input_root.variables[var]
                    output_var = output_root.variables[var]
                    for idx in range(input_var.shape[0]):
                        output_var[idx] = input_var[idx]
                for var in ("profile", "action_type", "condition_type", "deleted",):
                    remap_enum(input_root.variables[var], output_root.variables[var])

            modified_ranges: typing.Set[typing.Tuple[int, int]] = set()
            for info in merge_info:
                if info.skip_conversion:
                    continue

                # No good way of matching edits, since there's no unique ID to line up with on the CPD3 side, and
                # even if there was, modification on the Forge side would change the Forge unique ID
                if np.any(np.all((
                    output_root.variables['start_time'][:].data == info.start_time,
                    output_root.variables['end_time'][:].data == info.end_time,
                    output_root.variables['modified_time'][:].data == info.modified_time,
                    output_root.variables['profile'][:].data == output_root.variables["profile"].datatype.enum_dict.get(info.profile, -1),
                ), axis=0)):
                    continue
                output_idx = output_root.dimensions["index"].size
                try:
                    action_type, action_parameters = info.action(raw_index)
                    condition_type, condition_parameters = info.condition(raw_index)
                    for var, source in (
                            ("start_time", info.start_time),
                            ("end_time", info.end_time),
                            ("modified_time", info.modified_time),
                            ("unique_id", info.unique_id),
                            ("deleted", 1 if info.disabled else 0),
                            ("profile", output_root.variables["profile"].datatype.enum_dict[info.profile]),
                            ("action_type", output_root.variables["action_type"].datatype.enum_dict[action_type]),
                            ("action_parameters", action_parameters),
                            ("condition_type", output_root.variables["condition_type"].datatype.enum_dict[condition_type]),
                            ("condition_parameters", condition_parameters),
                            ("author", info.author),
                            ("comment", info.comment),
                            ("history", info.history(raw_index, action_type,
                                                     action_parameters, condition_type,
                                                     condition_parameters)),
                    ):
                        output_root.variables[var][output_idx] = source
                except EditConversionFailed as e:
                    if info.disabled:
                        _LOGGER.debug(f"Conversion of edit {info} failed: {e.message}")
                    else:
                        _LOGGER.warning(f"Conversion of edit {info} failed: {e.message}")
                    continue
                modified_ranges.add((info.start_time, info.end_time))

            return modified_ranges
        finally:
            output_file.close()
            if input_file:
                input_file.close()

    async def run() -> None:
        nonlocal total
        nonlocal modified

        async with (await Connection.default_connection("write legacy passed")) as connection:
            backoff = LockBackoff()
            while True:
                total = 0
                modified = 0
                async with connection.transaction(True):
                    try:
                        await connection.lock_write(
                            edit_directives_lock_key(station),
                            -MAX_I64, MAX_I64,
                        )
                        await connection.lock_read(
                            index_lock_key(station, "raw"),
                            -MAX_I64, MAX_I64,
                        )
                        await connection.lock_read(
                            data_lock_key(station, "raw"),
                            -MAX_I64, MAX_I64,
                        )

                        for year, merge_info in year_data.items():
                            raw_index = EditIndex(station)
                            if not year:
                                year_start = None
                                year_end = None
                                archive_name = edit_directives_file_name(station, None)

                                actual_start = min([i.start_time for i in merge_info])
                                actual_end = max([i.end_time for i in merge_info])

                                if actual_start == -MAX_I64:
                                    actual_start = data_start
                                else:
                                    actual_start /= 1000
                                if actual_end == MAX_I64:
                                    actual_end = data_end
                                else:
                                    actual_end /= 1000

                                for index_year in range(*containing_year_range(actual_start, actual_end)):
                                    try:
                                        index_contents = await connection.read_bytes(index_file_name(
                                            station, "raw", start_of_year(index_year)))
                                    except FileNotFoundError:
                                        continue
                                    raw_index.integrate_existing(index_contents)
                                    await raw_index.initialize(connection, index_year)
                            else:
                                year_start = start_of_year(year)
                                year_end = start_of_year(year + 1)
                                archive_name = edit_directives_file_name(station, year_start)
                                try:
                                    index_contents = await connection.read_bytes(index_file_name(
                                        station, "raw", year_start))
                                    raw_index.integrate_existing(index_contents)
                                    await raw_index.initialize(connection, year)
                                except FileNotFoundError:
                                    pass

                            with NamedTemporaryFile(suffix=".nc") as original_file, NamedTemporaryFile(suffix=".nc") as edits_file:
                                try:
                                    await connection.read_file(archive_name, original_file)
                                    original_file.flush()
                                except FileNotFoundError:
                                    original_file.close()
                                    original_file = None

                                modified_ranges = modify_edits(
                                    original_file.name if original_file else None, edits_file.name,
                                    year_start, year_end,
                                    merge_info, raw_index,
                                )
                                if original_file:
                                    original_file.close()

                                if modified_ranges:
                                    edits_file.seek(0)
                                    _LOGGER.debug(f"Writing edit directives data for {station.upper()}/{year or 'UNBOUNDED'}")
                                    await connection.write_file(archive_name, edits_file)
                                    for start, end in modified_ranges:
                                        await connection.send_notification(edit_directives_notification_key(station), start, end)

                            total += len(merge_info)
                            modified += len(modified_ranges)

                        break
                    except LockDenied as ld:
                        _LOGGER.info("Archive busy: %s", ld.status)
                        await backoff()
                        continue

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()
    return total, modified
