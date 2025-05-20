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
    def convert_action(self, action: typing.Dict[str, typing.Any], index: EditIndex) -> typing.Tuple[str, str]:
        op = action.get('Type')
        if isinstance(op, str):
            op = op.lower()

        if op == 'setcut':
            sel = action.get("Selection")
            if sel == [
                {'Variable': "Bb?[sae][BGRQ0-9]*_A11"},
                {'Variable': "(T|P|U)u?[0-9]*_A11"},
                {'Variable': "Bb?[sae][BGRQ0-9]*_S11"},
                {'Variable': "(T|P|U)u?[0-9]*_S11"},
            ]:
                cut = str(action.get("Cut", "")).lower()
                if cut == "pm10":
                    action["Selection"] = [{'HasFlavors': ["pm1"]}]
                elif cut == "pm1":
                    action["Selection"] = [{'HasFlavors': ["pm10"]}]

        return super().convert_action(action, index)


station = os.path.basename(__file__).split('.', 1)[0].lower()
assert station in VALID_STATIONS
main(station, EditDirective)
