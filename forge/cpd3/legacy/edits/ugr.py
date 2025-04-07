#!/usr/bin/env python3

import typing
import os
import logging
import re
from forge.const import STATIONS as VALID_STATIONS
from write import EditDirective as BaseEditDirective, EditIndex
from default import main

_LOGGER = logging.getLogger(__name__)


class EditDirective(BaseEditDirective):
    _OPTICAL_MATCH = re.compile("(?:Ba|X)([0-9BGR]*_A\d+)")

    def convert_action(self, action: typing.Dict[str, typing.Any], index: EditIndex) -> typing.Tuple[str, str]:
        op = action.get('Type')
        if isinstance(op, str):
            op = op.lower()

        if op == 'invalidate' or not op:
            sel = action.get("Selection")
            if sel == [{'Variable': "A31a"}]:
                action["Selection"] = [
                    {'Variable': "Bac?[0-9BGR]?_A31"},
                    {'Variable': "X[0-9BGR]?_A31"},
                ]
            elif sel == [{'Variable': "A21a"}]:
                action["Selection"] = [
                    {'Variable': "Bac?[0-9BGR]_A21"},
                    {'Variable': "X[0-9BGR]_A21"},
                ]
            elif sel == [{'Variable': "A41a"}]:
                action["Selection"] = [
                    {'Variable': "Bac?[0-9BGR]_A41"},
                    {'Variable': "X[0-9BGR]_A41"},
                ]
            elif sel == [{'Variable': "A42a"}]:
                action["Selection"] = [
                    {'Variable': "Bac?[0-9BGR]_A42"},
                    {'Variable': "X[0-9BGR]_A42"},
                ]
            elif isinstance(sel, list):
                hit_optical = set()
                for check in sel:
                    if isinstance(check, dict) and len(check) == 1:
                        var = self._OPTICAL_MATCH.fullmatch(check.get('Variable', ''))
                        if var:
                            hit_optical.add(var.group(1))
                for add in hit_optical:
                    sel.append({'Variable': f"((Bac?)|X){add}"})

        return super().convert_action(action, index)

station = os.path.basename(__file__).split('.', 1)[0].lower()
assert station in VALID_STATIONS
main(station, EditDirective)
