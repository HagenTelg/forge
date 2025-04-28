#!/usr/bin/env python3

import typing
import os
import logging
from forge.const import STATIONS as VALID_STATIONS
from write import EditDirective as BaseEditDirective, EditIndex, EditConversionFailed, Identity
from default import main

_LOGGER = logging.getLogger(__name__)


class EditDirective(BaseEditDirective):
    def __init__(self, identity: Identity, info: typing.Dict[str, typing.Any],
                 modified: typing.Optional[float] = None,
                 allocated_uids: typing.Set[int] = None):
        super().__init__(identity, info, modified, allocated_uids)

        if self.start_epoch and self.end_epoch and int(self.start_epoch) == 1477466829 and int(self.end_epoch) == 1477467738 and not self.comment and identity.priority > 0:
            self.skip_conversion = True

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
            elif sel == [{'Variable': "Xv_G81"}]:
                action["Selection"] = [{'Variable': "X_G81"}]

            if self.end_epoch and self.end_epoch <= 1168473600 and isinstance(sel, list):
                for check in sel:
                    if check == {'Variable': "WS1_XM1"}:
                        check['Variable'] = "WS_X1"
                    elif check == {'Variable': "WD1_XM1"}:
                        check['Variable'] = "WD_X1"

        return super().convert_action(action, index)

station = os.path.basename(__file__).split('.', 1)[0].lower()
assert station in VALID_STATIONS
main(station, EditDirective)
