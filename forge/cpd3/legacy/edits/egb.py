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

        if self.profile == 'met':
            self.profile = 'aerosol'

    def convert_action(self, action: typing.Dict[str, typing.Any], index: EditIndex) -> typing.Tuple[str, str]:
        op = action.get('Type')
        if isinstance(op, str):
            op = op.lower()

        if op == 'invalidate' or not op:
            sel = action.get("Selection")
            if isinstance(sel, list):
                for check in sel:
                    if check == {'Variable': "WS_wx"}:
                        check['Variable'] = "WS1?_XM1"
                    elif check == {'Variable': "WD_wx"}:
                        check['Variable'] = "WD1?_XM1"

            if self.start_epoch and self.start_epoch >= 1427296260 and self.end_epoch and self.end_epoch <= 1440115200 and isinstance(sel, list):
                for check in sel:
                    var = check.get('Variable')
                    if var and isinstance(var, str) and var.endswith("_A12"):
                        check['Variable'] = var[:-4] + "_A11"

        return super().convert_action(action, index)

station = os.path.basename(__file__).split('.', 1)[0].lower()
assert station in VALID_STATIONS
main(station, EditDirective)
