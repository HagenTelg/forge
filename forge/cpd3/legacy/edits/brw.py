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
            if sel == [{'Variable': "A11a"}]:
                if self.end_epoch < 1155340800:
                    action["Selection"] = [{'Variable': "BaG_A11"}]
                else:
                    action["Selection"] = [
                        {'Variable': "BaB_A11"},
                        {'Variable': "BaG_A11"},
                        {'Variable': "BaR_A11"},
                    ]
            elif sel == [{'Variable': "N2_N12"}]:
                action["Selection"] = [
                    {'Variable': "N_N12"}
                ]
            elif sel == [{'Variable': "N2_N21"}]:
                action["Selection"] = [{'Variable': "N_N21"}]
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
