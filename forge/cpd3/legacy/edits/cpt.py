#!/usr/bin/env python3

import typing
import os
import re
import logging
from forge.const import STATIONS as VALID_STATIONS
from write import EditDirective as BaseEditDirective, Identity, EditIndex, EditConversionFailed
from default import main

_LOGGER = logging.getLogger(__name__)


class EditDirective(BaseEditDirective):
    def __init__(self, identity: Identity, info: typing.Dict[str, typing.Any],
                 modified: typing.Optional[float] = None,
                 allocated_uids: typing.Set[int] = None):
        super().__init__(identity, info, modified, allocated_uids)

        # Looks like the tailing CPD2 files where not imported in 2010-12-01T14:46:00Z to 2011-06-09T06:02:00Z, so
        # preserve edits by looking outside until fixup
        if self.start_epoch and self.start_epoch > 1291161600 and self.end_epoch and self.end_epoch < 1293840000:
            self.in_file_gap = (1290211200, None)
        elif self.start_epoch and self.start_epoch > 1291161600 and self.end_epoch and self.end_epoch < 1307664000:
            self.in_file_gap = (None, 1309478400)
        else:
            self.in_file_gap = None

        # No actual CPC data available, but lots of edits noting that it's offline
        if self.start_epoch and self.start_epoch > 1346457600 and self._action == {
            "Type": "Invalidate",
            "Selection": [{"Variable": "N_N71"}]
        }:
            self.skip_conversion = True

    def match_index_variables(self, index: EditIndex, vars: typing.List[str]) -> typing.List[typing.Dict[str, typing.Any]]:
        if self.in_file_gap is None:
            return super().match_index_variables(index, vars)
        
        effective_start = self.start_epoch
        if self.in_file_gap[0]:
            effective_start = min(effective_start, self.in_file_gap[0])
        effective_end = self.end_epoch
        if self.in_file_gap[1]:
            effective_end = max(effective_end, self.in_file_gap[1])

        matchers: typing.List[re.Pattern] = list()
        for var in vars:
            try:
                matchers.append(re.compile(var))
            except re.error as e:
                raise EditConversionFailed(f"invalid variable match: {var}") from e

        def any_match(check_variable: str) -> bool:
            for m in matchers:
                if m.fullmatch(check_variable):
                    return True
            return False

        def assign_wavelength_suffixes(wavelengths: typing.List[float]) -> typing.Tuple[
            typing.List[str], typing.List[float]]:
            def named_suffix(wl: float) -> typing.Optional[str]:
                if wl < 400:
                    return None
                elif wl < 500:
                    return "B"
                elif wl < 600:
                    return "G"
                elif wl < 750:
                    return "R"
                elif wl < 900:
                    return "Q"
                return None

            unique_suffixes: typing.Set[str] = set()
            wavelength_suffixes: typing.List[str] = list()
            wavelength_assignments: typing.List[float] = list()
            for wl in wavelengths:
                s = named_suffix(float(wl))
                if not s or s in unique_suffixes:
                    return [str(i + 1) for i in range(len(wavelengths))], wavelengths
                wavelength_suffixes.append(s)
                wavelength_assignments.append(wl)
                unique_suffixes.add(s)
            wavelength_suffixes.extend([str(i + 1) for i in range(len(wavelengths))])
            wavelength_assignments.extend(wavelengths)
            return wavelength_suffixes, wavelength_assignments

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

                if wavelength_count == 0 and variable_id in ("Nb", "Nn"):
                    # Old (CPD2) size distribution edits, do the whole array if the first bin matches
                    check_variable = f"{variable_id}1_{instrument_id}"
                    if any_match(check_variable):
                        result.append({
                            "instrument_id": instrument_id,
                            "variable_id": variable_id,
                        })
                        continue

                if wavelength_count == 0:
                    continue

                for code in ["B", "G", "R", "Q"] + [str(i + 1) for i in range(wavelength_count)]:
                    if any_match(f"{variable_id}{code}_{instrument_id}"):
                        break
                else:
                    continue

                variable_wavelengths = index.variable_wavelengths(effective_start, effective_end,
                                                                  instrument_id, variable_id)
                if not variable_wavelengths:
                    continue
                variable_wavelengths = sorted(variable_wavelengths)
                wavelength_codes, wavelength_assignments = assign_wavelength_suffixes(variable_wavelengths)

                for idx in range(len(wavelength_codes)):
                    check_variable = f"{variable_id}{wavelength_codes[idx]}_{instrument_id}"
                    if not any_match(check_variable):
                        continue

                    result.append({
                        "instrument_id": instrument_id,
                        "variable_id": variable_id,
                        "wavelength": wavelength_assignments[idx],
                    })

        return result


station = os.path.basename(__file__).split('.', 1)[0].lower()
assert station in VALID_STATIONS
main(station, EditDirective)
