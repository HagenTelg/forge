#!/usr/bin/env python3

import typing
import os
import logging
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.variant import to_json
from write import EditDirective as BaseEditDirective, EditIndex
from default import main

_LOGGER = logging.getLogger(__name__)


class EditDirective(BaseEditDirective):
    def convert_action(self, action: typing.Dict[str, typing.Any], index: EditIndex) -> typing.Tuple[str, str]:
        op = action.get('Type')
        if isinstance(op, str):
            op = op.lower()

        if op == 'invalidate' or not op:
            sel = action.get("Selection")
            if sel == []:
                return "Invalidate", to_json({
                    "selection": [
                        {
                            "instrument_id": "A11",
                            "variable_id": "Ba",
                            "wavelength": wl,
                        } for wl in (467.0, 528.0, 652.0)
                    ] + [
                        {
                            "instrument_id": "A21",
                            "variable_id": var,
                            "wavelength": wl,
                        }
                        for wl in (670.0, )
                        for var in ("Ba", "Bac", "X")
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
                    ],
                }, sort_keys=True)

        return super().convert_action(action, index)

station = os.path.basename(__file__).split('.', 1)[0].lower()
assert station in VALID_STATIONS
main(station, EditDirective)
