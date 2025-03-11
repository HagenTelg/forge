#!/usr/bin/env python3

import typing
import os
import logging
from forge.const import STATIONS as VALID_STATIONS
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
            if sel == [{'Variable': "P_wx"}]:
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
