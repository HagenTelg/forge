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

        if self.author == "WH" and self.end_epoch < 766886400:
            op = self._action.get('Type')
            if isinstance(op, str):
                op = op.lower()

            if op == 'invalidate' or not op:
                sel = self._action.get("Selection")
                if sel in (
                        [{'Variable': "BsB_S12"}],
                        [{'Variable': "BsG_S12"}],
                        [{'Variable': "BsR_S12"}],
                        [{'Variable': "BbsB_S12"}],
                        [{'Variable': "BbsG_S12"}],
                        [{'Variable': "BbsR_S12"}],
                ):
                    self.skip_conversion = True

    def match_index_variables(self, index: EditIndex, vars: typing.List[str]) -> typing.List[typing.Dict[str, typing.Any]]:
        remaining = set(vars)
        result: typing.List[typing.Dict[str, typing.Any]] = list()
        if self.profile == 'radiation':
            for check in list(remaining):
                if re.escape(check) != check:
                    continue
                try:
                    variable_id, instrument_id = check.split('_', 1)
                except ValueError:
                    continue
                if instrument_id != 'R81':
                    continue
                result.append({
                    "instrument_id": instrument_id,
                    "variable_id": variable_id,
                })
                remaining.discard(check)

        if remaining:
            result.extend(super().match_index_variables(index, list(remaining)))
        return result

    def convert_action(self, action: typing.Dict[str, typing.Any], index: EditIndex) -> typing.Tuple[str, str]:
        op = action.get('Type')
        if isinstance(op, str):
            op = op.lower()

        if op == 'invalidate' or not op:
            sel = action.get("Selection")
            if sel == [{'Variable': "BaG_A11"}] and self.end_epoch < 956966400:
                action["Selection"] = [{'Variable': "Ba1_A81"}]
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
