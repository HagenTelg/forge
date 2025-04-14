#!/usr/bin/env python3

import typing
import os
import logging
from forge.const import STATIONS as VALID_STATIONS
from write import EditDirective as BaseEditDirective, EditIndex, EditConversionFailed
from default import main

_LOGGER = logging.getLogger(__name__)


class EditDirective(BaseEditDirective):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.profile == 'gradobs':
            self.skip_conversion = True

    def convert_action(self, action: typing.Dict[str, typing.Any], index: EditIndex) -> typing.Tuple[str, str]:
        op = action.get('Type')
        if isinstance(op, str):
            op = op.lower()

        if op == 'invalidate' or not op:
            sel = action.get("Selection")
            if sel == [{'Variable': "P_wx"}]:
                action["Selection"] = [{'Variable': "P_XM1"}]

            if isinstance(sel, list):
                for check in sel:
                    if check in ({'Variable': "U1_wx"}, {'Variable': "U1_XM1"}, {'Variable': "U_wx"},
                                 {'Variable': "TD_wx"}, {'Variable': "TD1_wx"}, ):
                        sel.append({'Variable': "TD1_XM1"})
                        sel.append({'Variable': "U1_XM1"})
                        break

        return super().convert_action(action, index)


station = os.path.basename(__file__).split('.', 1)[0].lower()
assert station in VALID_STATIONS
main(station, EditDirective)
