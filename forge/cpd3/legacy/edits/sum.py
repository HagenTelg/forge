#!/usr/bin/env python3

import typing
import os
import logging
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.variant import to_json
from write import EditDirective as BaseEditDirective, EditIndex, EditConversionFailed
from default import main

_LOGGER = logging.getLogger(__name__)


class EditDirective(BaseEditDirective):
    def convert_action(self, action: typing.Dict[str, typing.Any], index: EditIndex) -> typing.Tuple[str, str]:
        op = action.get('Type')
        if isinstance(op, str):
            op = op.lower()

        if op == 'invalidate' or not op:
            sel = action.get("Selection")
            if (sel is None or sel == []) and self.profile == "aerosol":
                return "Invalidate", to_json({
                    "selection": [
                        {
                            "instrument_id": "A11",
                            "variable_id": "Ba",
                            "wavelength": wl,
                        } for wl in ((574.0,) if self.end_epoch < 1439769600 else (467.0, 528.0, 652.0))
                    ] + [
                        {
                            "instrument_id": "A81",
                            "variable_id": var,
                            "wavelength": wl,
                        }
                        for wl in (370.0, 470.0, 520.0, 590.0, 660.0, 880.0, 950.0)
                        for var in ("Ba", "Bac", "X")
                    ] + [
                        {
                            "instrument_id": "S11",
                            "variable_id": var,
                            "wavelength": wl,
                        }
                        for wl in (450.0, 550.0, 700.0)
                        for var in ("Bs", "Bbs")
                    ] + ([
                        {
                            "instrument_id": "A12",
                            "variable_id": "Ba",
                            "wavelength": wl,
                        } for wl in (467.0, 528.0, 652.0)
                    ] if self.start_epoch > 1304640000 and self.end_epoch < 1439769600 else []) + ([
                        {
                            "instrument_id": "A82",
                            "variable_id": var,
                            "wavelength": wl,
                        }
                        for wl in (370.0, 470.0, 520.0, 590.0, 660.0, 880.0, 950.0)
                        for var in ("Ba", "Bac", "X")
                    ] if self.start_epoch > 1414627200 and self.end_epoch < 1478217600 else []),
                }, sort_keys=True)
            elif sel == [{'Variable': "P_wx"}]:
                action["Selection"] = [{'Variable': "P_XM1"}]
            elif sel == [{'Variable': "U1_wx"}] or sel == [{'Variable': "U_wx"}] or sel == [{'Variable': "U1_XM1"}]:
                action["Selection"] = [{'Variable': "U1_XM1"}]
                try:
                    return super().convert_action(action, index)
                except EditConversionFailed:
                    action["Selection"] = [{'Variable': "TD1_XM1"}]

        return super().convert_action(action, index)


station = os.path.basename(__file__).split('.', 1)[0].lower()
assert station in VALID_STATIONS
main(station, EditDirective)
