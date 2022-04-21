import typing
import asyncio
import base64
import time
import struct
import logging
import os
import re
from abc import ABC, abstractmethod
from math import floor, ceil, isfinite
from copy import deepcopy
from pathlib import Path
from forge.const import __version__
from forge.vis import CONFIGURATION
from forge.tasks import background_task
from forge.vis.access import BaseAccessUser
from forge.vis.data.stream import DataStream, RecordStream
from forge.vis.realtime.controller.client import ReadData as RealtimeRead
from forge.vis.realtime.controller.block import DataBlock as RealtimeDataBlock
from forge.vis.export import Export, ExportList
from forge.vis.realtime import Translator as BaseRealtimeTranslator
from forge.vis.realtime.translation import get_translator
from forge.vis.acquisition import Translator as BaseAcquisitionTranslator
from forge.cpd3.identity import Name, Identity
from forge.cpd3.variant import serialize as variant_serialize, deserialize as variant_deserialize
from forge.cpd3.datareader import StandardDataInput, RecordInput
from forge.cpd3.timeinterval import TimeUnit, TimeInterval


_LOGGER = logging.getLogger(__name__)
_interface = CONFIGURATION.get('CPD3.INTERFACE', 'cpd3_forge_interface')
_read_timeout = CONFIGURATION.get('CPD3.READTIMEOUT', 2 * 60 * 60)


def _to_cpd3_selection(selection: typing.List[typing.Dict[str, str]]) -> typing.List[typing.Any]:
    if not isinstance(selection, list):
        return []
    result = []
    for entry in selection:
        if not isinstance(entry, dict):
            continue
        if entry.get('type') != 'variable':
            continue
        variable = entry.get('variable')
        if not variable or not isinstance(variable, str):
            continue
        add = {
            'Variable': variable
        }
        if 'station' in entry:
            add['Station'] = entry['station']
        if 'archive' in entry:
            add['Archive'] = entry['archive']
        if 'flavors' in entry:
            add['Flavors'] = entry['flavors']
        if 'has_flavors' in entry:
            add['HasFlavors'] = entry['has_flavors']
        if 'lacks_flavors' in entry:
            add['LacksFlavors'] = entry['lacks_flavors']
        result.append(add)
    return result


def _from_cpd3_selection(selection: typing.Any) -> typing.List[typing.Dict[str, typing.Any]]:
    def append_add(item, key, add):
        if key not in result:
            item[key] = []
        item[key].append(add)

    def single_or_list(value):
        if isinstance(value, list) and len(value) == 1:
            return value[0]
        return value

    if selection is None:
        return []
    elif isinstance(selection, str):
        parts = selection.split(':')
        if len(parts) == 0:
            return []
        elif len(parts) == 1:
            if len(parts[0]) == 0:
                return []
            return [{'variable': parts[0]}]
        elif len(parts) == 2:
            return [{'archive': parts[0], 'variable': parts[1]}]
        elif len(parts) == 3:
            return [{'station': parts[0], 'archive': parts[1], 'variable': parts[2]}]
        else:
            result = {'station': parts[0], 'archive': parts[1], 'variable': parts[2]}
            for i in range(3, len(parts)):
                flavor = parts[i]
                if flavor.startswith('!') or flavor.startswith('-'):
                    result.pop('flavors')
                    append_add(result, 'lacks_flavors', flavor[1:])
                elif flavor.startswith('='):
                    result.pop('lacks_flavors')
                    result.pop('has_flavors')
                    if len(flavor) == 1:
                        result['flavors'] = ['']
                    else:
                        append_add(result, 'flavors', flavor[1:])
                elif flavor.startswith('+'):
                    result.pop('flavors')
                    append_add(result, 'has_flavors', flavor[1:])
                else:
                    result.pop('flavors')
                    append_add(result, 'has_flavors', flavor)
    elif isinstance(selection, dict):
        return _from_cpd3_selection([selection])

    result: typing.List[typing.Dict[str, typing.Any]] = list()
    for entry in selection:
        converted: typing.Dict[str, typing.Any] = {
            'type': 'variable',
        }
        if 'Station' in entry:
            converted['station'] = single_or_list(entry['Station'])
        if 'Archive' in entry:
            converted['archive'] = single_or_list(entry['Archive'])
        if 'Variable' in entry:
            converted['variable'] = single_or_list(entry['Variable'])
        if 'Flavors' in entry:
            converted['flavors'] = entry['Flavors']
        else:
            if 'HasFlavors' in entry:
                converted['has_flavors'] = entry['HasFlavors']
            if 'LacksFlavors' in entry:
                converted['lacks_flavors'] = entry['LacksFlavors']
        result.append(converted)
    return result


def _to_cpd3_calibration(calibration: typing.List[float]) -> typing.List[float]:
    if not isinstance(calibration, list):
        return []
    result = []
    for coefficient in calibration:
        try:
            coefficient = float(coefficient)
        except (ValueError, TypeError):
            return []
        result.append(coefficient)
    while len(result) > 0 and result[-1] == 0.0:
        result.pop()
    return result


def _from_cpd3_calibration(calibration: typing.Any) -> typing.List[float]:
    if calibration is None:
        return []

    if isinstance(calibration, float) or isinstance(calibration, int):
        return [float(calibration)]
    elif isinstance(calibration, dict):
        return _from_cpd3_calibration(calibration.get('Coefficients'))
    elif not isinstance(calibration, list):
        return []

    result = []
    for coefficient in calibration:
        try:
            coefficient = float(coefficient)
        except (ValueError, TypeError):
            return []
        result.append(coefficient)
    return result


def _selection_to_single_cutsize(selection: typing.List[typing.Dict[str, typing.Any]]) -> typing.Optional[str]:
    if len(selection) != 1:
        return None

    selection = selection[0]
    if selection == {
        'type': 'variable',
        'has_flavors': ['pm10']
    } or selection == {
        'type': 'variable',
        'has_flavors': ['pm10'],
        'lacks_flavors': []
    }:
        return 'pm10'

    if selection == {
        'type': 'variable',
        'has_flavors': ['pm25']
    } or selection == {
        'type': 'variable',
        'has_flavors': ['pm25'],
        'lacks_flavors': []
    }:
        return 'pm25'

    if selection == {
        'type': 'variable',
        'has_flavors': ['pm1']
    } or selection == {
        'type': 'variable',
        'has_flavors': ['pm1'],
        'lacks_flavors': []
    }:
        return 'pm1'

    if selection == {
        'type': 'variable',
        'lacks_flavors': ['pm1', 'pm10', 'pm25'],
        'variable': '((Ba[cfs]*)|(Bb?s)|Be|Ir|L|(N[nbs]?)|(X[cfs]*))[BGRQ0-9]*_.*',
    } or selection == {
        'type': 'variable',
        'lacks_flavors': ['pm1', 'pm10', 'pm25'],
        'has_flavors': [],
        'variable': '((Ba[cfs]*)|(Bb?s)|Be|Ir|L|(N[nbs]?)|(X[cfs]*))[BGRQ0-9]*_.*',
    }:
        return ''
    return None


def _to_cpd3_action(directive: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    op = directive.get('action', 'invalidate')
    if op == 'contaminate':
        return {
            'Type': 'Contaminate',
        }
    elif op == 'calibration':
        return {
            'Type': 'Polynomial',
            'Selection': _to_cpd3_selection(directive.get('selection')),
            'Calibration': _to_cpd3_calibration(directive.get('calibration')),
        }
    elif op == 'recalibrate':
        return {
            'Type': 'Recalibrate',
            'Selection': _to_cpd3_selection(directive.get('selection')),
            'Calibration': _to_cpd3_calibration(directive.get('calibration')),
            'Original': _to_cpd3_calibration(directive.get('reverse_calibration')),
        }
    elif op == 'flow_correction':
        instrument = str(directive.get('instrument', '')).strip()
        if len(instrument) <= 0:
            raise ValueError
        return {
            'Type': 'FlowCorrection',
            'Instrument': str(directive.get('instrument', '')),
            'Calibration': _to_cpd3_calibration(directive.get('calibration')),
            'Original': _to_cpd3_calibration(directive.get('reverse_calibration')),
        }
    elif op == 'cut_size':
        original_size = str(directive.get('cutsize', '')).strip()
        if original_size == '':
            original_selection = [{
                'LacksFlavors': ['pm1', 'pm10', 'pm25'],
                'Variable': '((Ba[cfs]*)|(Bb?s)|Be|Ir|L|(N[nbs]?)|(X[cfs]*))[BGRQ0-9]*_.*',
            }]
        else:
            original_selection = [{
                'HasFlavors': [original_size],
            }]

        modified_size = str(directive.get('modified_cutsize', 'invalidate'))
        if modified_size == 'invalidate':
            return {
                'Type': 'Invalidate',
                'Selection': original_selection,
            }

        # Not a perfect test, but good enough for most use cases
        is_stuck_impactor = ((original_size == 'pm1' and modified_size == 'pm10') or
                             (original_size == 'pm10' and modified_size == 'pm1'))
        return {
            'Type': 'SetCut',
            'ApplyToMetadata': not is_stuck_impactor,
            'Cut': modified_size.lower(),
            'Selection': original_selection,
        }
    else:
        return {
            'Type': 'Invalidate',
            'Selection': _to_cpd3_selection(directive.get('selection')),
        }


def _to_cpd3_trigger(directive: typing.Dict[str, typing.Any]) -> typing.Optional[typing.Any]:
    condition = directive.get('condition', None)
    if not condition:
        return None

    def to_constant(value: typing.Optional[float]) -> typing.Optional[float]:
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        if not isfinite(value):
            return None
        return value

    def to_integer(value: typing.Optional[int]) -> typing.Optional[int]:
        try:
            value = int(value)
        except (TypeError, ValueError):
            return None
        if not isfinite(value):
            return None
        return value

    op = condition.get('type', 'none')
    if op == 'threshold':
        triggers = []
        lower = to_constant(condition.get('lower'))
        upper = to_constant(condition.get('upper'))
        if lower is None:
            if upper is None:
                return None
            for selection in _to_cpd3_selection(condition.get('selection', [])):
                triggers.append({
                    'Type': 'Less',
                    'Right': upper,
                    'Left': {
                        'Value': [selection],
                    }
                })
        elif upper is None:
            for selection in _to_cpd3_selection(condition.get('selection', [])):
                triggers.append({
                    'Type': 'Greater',
                    'Right': lower,
                    'Left': {
                        'Value': [selection],
                    }
                })
        else:
            for selection in _to_cpd3_selection(condition.get('selection', [])):
                triggers.append({
                    'Type': 'Range',
                    'Start': lower,
                    'End': upper,
                    'Value': {
                        'Value': [selection],
                    }
                })
        return triggers
    elif op == 'periodic':
        points = condition.get('points', [])
        interval = condition.get('interval')
        division = condition.get('division')

        intervals = {
            'hour': {
                'Interval': 'Hour',
                'divisions': {
                    'minute': {
                        'MomentUnit': 'Minute',
                        'maximum': 60
                    },
                }
            },
            'day': {
                'Interval': 'Day',
                'divisions': {
                    'minute': {
                        'MomentUnit': 'Minute',
                        'maximum': 60 * 24,
                    },
                    'hour': {
                        'MomentUnit': 'Hour',
                        'maximum': 24,
                    },
                }
            },
        }
        interval_data = intervals.get(interval)
        if interval_data is None:
            return None

        trigger = {
            'Type': 'Periodic',
            'Interval': interval_data['Interval'],
            'Moments': [],
        }
        division_data = interval_data['divisions'].get(division)
        if division_data is None:
            return None
        trigger['MomentUnit'] = division_data['MomentUnit']

        for point in points:
            index = to_integer(point)
            if index is None or index < 0 or index >= division_data['maximum']:
                continue
            trigger['Moments'].append(index)
        return trigger

    return None


def _from_cpd3_trigger(trigger: typing.Any) -> typing.Optional[typing.Dict[str, typing.Any]]:
    if not trigger:
        return None

    def to_constant(value: typing.Any) -> typing.Optional[float]:
        if isinstance(value, dict):
            if value.get('Type').lower() == 'constant':
                value = value['Value']
        if value is None:
            return None
        if not isinstance(value, float):
            raise TypeError
        if not isfinite(value):
            return None
        return value

    def to_variable_selection(value: typing.Any) -> typing.List[typing.Dict[str, typing.Any]]:
        if not isinstance(value, dict):
            return []
        op = value.get('Type')
        if not isinstance(op, str):
            return _from_cpd3_selection(value.get('Value'))
        op = op.lower()
        if op == 'constant':
            return []
        elif op == 'sin':
            return []
        elif op == 'cos':
            return []
        elif op == 'log' or op == 'ln':
            return []
        elif op == 'log10':
            return []
        elif op == 'exp':
            return []
        elif op == 'abs' or op == 'absolute' or op == 'absolutevalue':
            return []
        elif op == 'poly' or op == 'polynomial' or op == 'cal' or op == 'calibration':
            return []
        elif op == 'polyinvert' or op == 'polynomialinvert' or op == 'invertcal' or op == 'invertcalibration':
            return []
        elif op == 'mean':
            return []
        elif op == 'sd' or op == 'standarddeviation':
            return []
        elif op == 'quantile':
            return []
        elif op == 'median':
            return []
        elif op == 'maximum' or op == 'max':
            return []
        elif op == 'slope':
            return []
        elif op == 'length' or op == 'duration' or op == 'elapsed':
            return []
        elif op == 'average' or op == 'smoothed':
            return []
        elif op == 'sum' or op == 'add':
            return []
        elif op == 'difference' or op == 'subtract':
            return []
        elif op == 'power':
            return []
        elif op == 'largest':
            return []
        elif op == 'smallest':
            return []
        elif op == 'first' or op == 'firstvalid' or op == 'valid':
            return []

        return _from_cpd3_selection(value.get('Value'))

    def convert_element(element: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        if element is None:
            return {'type': 'none'}

        if isinstance(element, bool):
            if bool(element):
                return {'type': 'none'}
            raise ValueError

        op = element.get('Type')
        if isinstance(op, str):
            op = op.lower()
        if op == 'range' or op == 'insiderange':
            selection = to_variable_selection(element.get('Value'))
            if len(selection) != 1:
                raise ValueError
            return {
                'type': 'threshold',
                'lower': to_constant(element.get('Start')),
                'upper': to_constant(element.get('End')),
                'selection': selection,
            }
        elif op == 'less' or op == 'lessthan':
            selection = to_variable_selection(element.get('Left'))
            if len(selection) == 1:
                return {
                    'type': 'threshold',
                    'upper': to_constant(element.get('Right')),
                    'selection': selection,
                }
            selection = to_variable_selection(element.get('Right'))
            if len(selection) == 1:
                return {
                    'type': 'threshold',
                    'lower': to_constant(element.get('Left')),
                    'selection': selection,
                }
        elif op == 'greater' or op == 'greaterthan':
            selection = to_variable_selection(element.get('Left'))
            if len(selection) == 1:
                return {
                    'type': 'threshold',
                    'lower': to_constant(element.get('Right')),
                    'selection': selection,
                }
            selection = to_variable_selection(element.get('Right'))
            if len(selection) == 1:
                return {
                    'type': 'threshold',
                    'upper': to_constant(element.get('Left')),
                    'selection': selection,
                }
        elif op == 'periodic' or op == 'moment' or op == 'instant':
            moments = element.get('Moments', [])
            if isinstance(moments, int):
                moments = [moments]
            elif not isinstance(moments, list):
                raise ValueError
            for i in range(len(moments)):
                moments[i] = int(moments[i])

            interval = TimeInterval.from_variant(element.get('Interval'), TimeInterval(TimeUnit.Second, 1, True))
            if interval.count != 1:
                raise ValueError
            momentUnit = TimeInterval.from_variant(element.get('MomentUnit'), TimeInterval(TimeUnit.Second, 1, True))
            if momentUnit.count != 1:
                raise ValueError

            result = {
                'type': 'periodic',
                'points': moments,
            }
            if interval.unit == TimeUnit.Hour:
                result['interval'] = 'hour'
                if momentUnit.unit == TimeUnit.Minute:
                    result['division'] = 'minute'
                    return result
            elif interval.unit == TimeUnit.Day:
                result['interval'] = 'day'
                if momentUnit.unit == TimeUnit.Minute:
                    result['division'] = 'minute'
                    return result
                elif momentUnit.unit == TimeUnit.Hour:
                    result['division'] = 'hour'
                    return result
            raise ValueError

        raise ValueError

    def or_element(target: typing.Dict[str, typing.Any], add: typing.Dict[str, typing.Any]):
        op = target['type']
        if op != add['type']:
            raise ValueError

        if op == 'none':
            return
        elif op == 'threshold':
            if target['lower'] != add['lower']:
                raise ValueError
            if target['upper'] != add['upper']:
                raise ValueError
            target['selection'].extend(add['selection'])
            return
        raise ValueError

    if isinstance(trigger, dict):
        op = trigger.get('Type', '').lower()
        if op == 'or' or op == 'any':
            trigger = trigger.get('Components', [])

    if isinstance(trigger, list):
        condition = convert_element(trigger[0])
        for i in range(1, len(trigger)):
            if not or_element(condition, convert_element(trigger[i])):
                raise ValueError
        return condition

    return convert_element(trigger)


def _new_directive(user: BaseAccessUser, station: str, profile: str,
                   directive: typing.Dict[str, typing.Any]) -> typing.Tuple[Identity, typing.Dict[str, typing.Any]]:
    start = directive.get('start_epoch_ms')
    end = directive.get('end_epoch_ms')
    start = floor(start) if start else None
    end = ceil(end) if end else None

    identity = Identity(station=station, archive='edits', variable=profile,
                        start=(start / 1000.0 if start else None),
                        end=(end / 1000.0 if end else None))

    result = {
        'Author': directive.get('author'),
        'Comment': directive.get('comment'),
        'History': [{
            'Type': 'Created',
            'At': time.time(),
            'Environment': 'forge-vis',
            'Version': __version__,
            'User': user.display_name,
        }],
        'Parameters': {
            'Action': _to_cpd3_action(directive),
            'Trigger': _to_cpd3_trigger(directive),
        },
    }

    return identity, result


def _modify_directive(user: BaseAccessUser, station: str, profile: str,
                      existing: typing.Dict[str, typing.Any], identity: Identity,
                      modification: typing.Dict[str, typing.Any]) -> None:
    history = existing.get('History')
    if not history:
        history = []
        existing['History'] = history

    def add_history(info):
        info['At'] = time.time()
        info['Environment'] = 'forge-vis'
        info['Version'] = __version__
        info['User'] = user.display_name
        history.append(info)

    if identity.name.variable != profile:
        add_history({
            'Type': 'ProfileChanged',
            'OriginalProfile': identity.name.variable,
        })
        identity.name.variable = profile

    start = modification.get('start_epoch_ms')
    end = modification.get('end_epoch_ms')
    start = (floor(start) / 1000.0) if start else None
    end = (ceil(end) / 1000.0) if end else None
    if identity.start != start or identity.end != end:
        add_history({
            'Type': 'BoundsChanged',
            'OriginalBounds': {'Start': identity.start, 'End': identity.end},
            'RevisedBounds': {'Start': start, 'End': end},
        })
        identity.start = start
        identity.end = end

    parameters: typing.Dict[str, typing.Any] = {
        'Action': _to_cpd3_action(modification),
        'Trigger': _to_cpd3_trigger(modification),
    }
    if existing.get('Parameters') != parameters:
        add_history({
            'Type': 'ParametersChanged',
            'OriginalParameters': existing['Parameters'],
        })
        existing['Parameters'] = parameters

    if existing.get('Author') != modification.get('author'):
        add_history({
            'Type': 'AuthorChanged',
            'OriginalAuthor': existing.get('Author'),
        })
        existing['Author'] = modification.get('author')

    if existing.get('Comment') != modification.get('comment'):
        add_history({
            'Type': 'CommentChanged',
            'OriginalComment': existing.get('Comment'),
        })
        existing['Comment'] = modification.get('comment')

    was_enabled = not existing.get('Disabled')
    is_enabled = not modification.get('deleted')
    if was_enabled and not is_enabled:
        add_history({
            'Type': 'Disabled',
        })
        existing['Disabled'] = True
    elif not was_enabled and is_enabled:
        add_history({
            'Type': 'Enabled',
        })
        existing.pop('Disabled')


def _convert_history(history: typing.List[typing.Dict[str, typing.Any]]) -> typing.List[typing.Dict[str, typing.Any]]:
    def format_time(ts: float) -> str:
        if not ts or not isfinite(ts):
            return "∞"
        ts = time.gmtime(ts)
        return f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02}T{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}Z"

    result: typing.List[typing.Dict[str, typing.Any]] = list()
    for entry in history:
        operation = entry.get('Type')
        if operation == 'ProfileChanged':
            operation = f"Profile changed from {entry['OriginalProfile']}"
        elif operation == 'BoundsChanged':
            operation = f"Bounds change from {format_time(entry['OriginalBounds']['Start'])} - {format_time(entry['OriginalBounds']['End'])}"
        elif operation == 'ParametersChanged':
            operation = "Parameters changed"
        elif operation == 'ExtendChanged':
            operation = "Extension changed"
        elif operation == 'AuthorChanged':
            operation = f"Author changed from {entry['OriginalAuthor']}"
        elif operation == 'CommentChanged':
            operation = "Comment changed"
        elif operation == 'PriorityChanged':
            operation = "Priority changed"
        elif operation == 'SystemInternalChanged':
            operation = "System internal flag changed"
        elif operation == 'Disabled':
            operation = "Deleted"
        elif operation == 'Enabled':
            operation = "Restored"
        elif operation == 'Created':
            operation = "Created"

        result.append({
            'time_epoch_ms': round(entry.get('At', 0) * 1000),
            'user': entry.get('User', ''),
            'operation': operation,
        })

    return result


def _convert_directive(profile: str, identity: Identity,
                       original: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    result = {
        '_id': base64.b64encode(identity.serialize()).decode('ascii'),
        'start_epoch_ms': floor(identity.start * 1000) if identity.start else None,
        'end_epoch_ms': ceil(identity.end * 1000) if identity.end else None,
        'author': original.get('Author', ""),
        'comment': original.get('Comment', ""),
        'history': _convert_history(original.get('History', [])),
        'other_type': identity.variable != profile,
        'type': identity.variable.title(),
        'condition': {'type': 'none'},
    }

    try:
        result['modified_epoch_ms'] = result['history'][-1]['time_epoch_ms']
    except IndexError:
        pass

    if original.get('Disabled'):
        result['deleted'] = True

    parameters: typing.Dict[str, typing.Any] = original.get('Parameters', {})

    action: typing.Dict[str, typing.Any] = parameters.get('Action', {})
    op = action.get('Type')
    if isinstance(op, str):
        op = op.lower()
    if op == 'contaminate' or op == 'contam':
        result['action'] = 'contaminate'
    elif op == 'polynomial' or op == 'poly' or op == 'cal' or op == 'calibration':
        result['action'] = 'calibration'
        result['selection'] = _from_cpd3_selection(action.get('Selection'))
        result['calibration'] = _from_cpd3_calibration(action.get('Calibration'))
    elif op == 'recalibrate':
        result['action'] = 'recalibrate'
        result['selection'] = _from_cpd3_selection(action.get('Selection'))
        result['calibration'] = _from_cpd3_calibration(action.get('Calibration'))
        result['reverse_calibration'] = _from_cpd3_calibration(action.get('Original'))
    elif op == 'flowcorrection' or op == 'flowcalibration':
        result['action'] = 'flow_correction'
        result['instrument'] = str(action.get('Instrument'), '')
        result['calibration'] = _from_cpd3_calibration(action.get('Calibration'))
        result['reverse_calibration'] = _from_cpd3_calibration(action.get('Original'))
    elif op == 'setcut' or op == 'cut':
        result['action'] = 'cut_size'
        result['cutsize'] = _selection_to_single_cutsize(_from_cpd3_selection(action.get('Selection')))
        result['modified_cutsize'] = str(action.get('Cut', ''))
    else:
        selection = _from_cpd3_selection(action.get('Selection'))
        single_cutsize = _selection_to_single_cutsize(selection)
        if single_cutsize is not None:
            result['action'] = 'cut_size'
            result['cutsize'] = single_cutsize
            result['modified_cutsize'] = 'invalidate'
        else:
            result['action'] = 'invalidate'
            result['selection'] = selection

    try:
        condition = _from_cpd3_trigger(parameters.get('Trigger', None))
        if condition is not None:
            result['condition'] = condition
    except:
        pass

    return result


def _display_directive(raw: typing.Dict[str, typing.Any]) -> bool:
    if raw.get('SystemInternal'):
        return False

    def is_valid_action(parameters: typing.Dict[str, typing.Any]) -> bool:
        action = parameters.get('Type')
        if action is None:
            return True
        if not isinstance(action, str):
            return False
        action = action.lower()

        def _matches(*args):
            for arg in args:
                if action == arg.lower():
                    return True
            return False

        if _matches("Remove"):
            return False
        elif _matches("Poly", "Polynomial", "Cal", "Calibration"):
            return True
        elif _matches("PolyInvert", "PolynomialInvert", "InvertCal", "InvertCalibration"):
            return False
        elif _matches("Recalibrate"):
            return True
        elif _matches("Wrap", "Modular", "Modulus"):
            return False
        elif _matches("Overlay", "Set"):
            return False
        elif _matches("Meta", "Metadata", "OverlayMeta", "OverlayMetadata"):
            return False
        elif _matches("Serial", "SetSerial"):
            return False
        elif _matches("Flag", "Addflag", "AddFlags"):
            return False
        elif _matches("RemoveFlag", "RemoveFlags"):
            return False
        elif _matches("Contaminate", "Contam"):
            return True
        elif _matches("Uncontaminate", "ClearContam"):
            return False
        elif _matches("FlowCorrection", "FlowCalibration"):
            if len(raw.get('Instrument', '')) > 0:
                return True
            return False
        elif _matches("Spot", "SpotSize"):
            return False
        elif _matches("MultiSpot", "MultiSpotSize", "CLAPSpot"):
            return False
        elif _matches("Unit", "SetUnit", "Duplicate"):
            return False
        elif _matches("UnitReplace", "Translate", "DuplicateTranslate"):
            return False
        elif _matches("Flavors"):
            return False
        elif _matches("SetCut", "Cut"): 
            if _selection_to_single_cutsize(_from_cpd3_selection(parameters.get('Selection'))) is None:
                return False
            return True
        elif _matches("MultiPoly", "MultiPolynomial", "MultiCal", "MultiCalibration"):
            return False
        elif _matches("Arithmetic", "Math", "Function"):
            return False
        elif _matches("ScriptValue", "ScriptValues"):
            return False
        elif _matches("ScriptSegment", "ScriptSegments", "Script"):
            return False
        elif _matches("ScriptDemultiplexer", "Demultiplexer"):
            return False
        elif _matches("ScriptGeneralValue", "ScriptGeneralValues"):
            return False
        elif _matches("ScriptGeneralSegment", "ScriptGeneralSegments"):
            return False
        elif _matches("NOOP", "None"):
            return False

        # Invalidate
        return True

    def is_valid_trigger(parameters: typing.Dict[str, typing.Any]) -> bool:
        try:
            _from_cpd3_trigger(parameters)
        except:
            return False
        return True

    if not is_valid_action(raw.get('Parameters', {}).get('Action', {})):
        return False
    if not is_valid_trigger(raw.get('Parameters', {}).get('Trigger')):
        return False
    return True


async def _write_directive(user: BaseAccessUser, station: str, profile: str,
                           directive: typing.Dict[str, typing.Any]) -> typing.Optional[typing.Dict]:
    if '_id' not in directive:
        try:
            identity, raw = _new_directive(user, station, profile, directive)
        except:
            _LOGGER.debug(f"Error creating directive for {user.display_id} on {station}:{profile}", exc_info=True)
            return None

        target = await asyncio.create_subprocess_exec(_interface, 'directive_create',
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.PIPE)

        target.stdin.write(identity.serialize())
        target.stdin.write(variant_serialize(raw))
        target.stdin.close()

        result = await target.stdout.read()
        await target.wait()
        if target.returncode != 0:
            _LOGGER.warning(f"Error writing directive to database, return code {target.returncode}")
            return None
        if not result:
            return None
        identity = Identity.deserialize(result)

        return _convert_directive(profile, identity, raw)

    try:
        identity = Identity.deserialize(base64.b64decode(directive['_id']))
    except:
        _LOGGER.debug(f"Error reading directive ID for {user.display_id} on {station}:{profile}", exc_info=True)
        return None
    if identity.station != station or identity.archive != 'edits' or len(identity.flavors) != 0:
        _LOGGER.debug(f"Invalid directive ID {identity} for {user.display_id} on {station}:{profile}", exc_info=True)
        return None

    while True:
        target = await asyncio.create_subprocess_exec(_interface, 'directive_rmw',
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.PIPE)

        target.stdin.write(identity.serialize())
        try:
            n = struct.unpack('<I', await target.stdout.readexactly(4))[0]
            existing = await target.stdout.readexactly(n)
            if len(existing) != n:
                _LOGGER.debug(f"Error reading directive {identity} for {user.display_id} on {station}:{profile}")
                target.terminate()
                await target.wait()
                return None
        except:
            _LOGGER.debug(f"Error reading directive {identity} for {user.display_id} on {station}:{profile}",
                          exc_info=True)
            try:
                target.terminate()
                target.stdin.close()
                await target.wait()
            except OSError:
                pass
            return None

        existing = variant_deserialize(existing)
        if not isinstance(existing, dict):
            _LOGGER.debug(f"Error deserializing directive {identity}")
            try:
                target.terminate()
                await target.wait()
            except OSError:
                pass
            return None

        modified_identity = Identity(station=station, archive='edits', variable=identity.variable,
                                     start=identity.start, end=identity.end)
        try:
            _modify_directive(user, station, profile, existing, modified_identity, directive)
        except:
            _LOGGER.debug(f"Error modifying directive for {user.display_id} on {station}:{profile}", exc_info=True)
            try:
                target.terminate()
                target.stdin.close()
                await target.wait()
            except OSError:
                pass
            return None

        target.stdin.write(modified_identity.serialize())
        target.stdin.write(variant_serialize(existing))
        target.stdin.close()

        result = await target.stdout.read()
        await target.wait()
        if target.returncode == 100:
            continue
        if target.returncode != 0:
            _LOGGER.warning(f"Error modifying directive ({identity}) in database, return code {target.returncode}")
            return None

        identity = Identity.deserialize(result)
        break

    return _convert_directive(profile, identity, existing)


async def _queue_pass(station: str, profile: str, start_epoch: int, end_epoch: int, comment: str) -> None:
    reader, writer = await asyncio.open_unix_connection(
        CONFIGURATION.get('CPD3.PASS.SOCKET', '/run/forge-cpd3-pass.socket'))
    header = struct.pack('<BQQ', 0, start_epoch, end_epoch)
    for a in (station, profile, comment):
        raw = a.encode('utf-8')
        header += struct.pack('<I', len(raw))
        header += raw
    writer.write(header)
    await writer.drain()
    await reader.read(1)
    writer.close()


async def _get_latest_passed(station: str, profile: str) -> typing.Optional[int]:
    class Input(StandardDataInput):
        def __init__(self):
            super().__init__()
            self.latest_passed: typing.Optional[int] = None

        def value_ready(self, identity: Identity, value: typing.Any) -> None:
            if not identity.end:
                return
            if not self.latest_passed or self.latest_passed < identity.end:
                self.latest_passed = int(ceil(identity.end))

    async def read_passed(start_epoch: int, end_epoch: int) -> typing.Optional[int]:
        _LOGGER.debug(f"Starting latest passed read for {station} {profile} {start_epoch},{end_epoch}")
        reader = await asyncio.create_subprocess_exec(_interface, 'archive_read',
                                                      str(start_epoch), str(end_epoch),
                                                      f'{station}:passed:{profile}:=',
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)
        await reader.stdout.readexactly(3)

        status = Input()
        while True:
            data = await reader.stdout.read(65536)
            if not data:
                break
            status.incoming_raw(data)
        await reader.wait()
        return status.latest_passed

    current_end = int(time.time())
    current_start = current_end - 366 * 24 * 60 * 60
    last_possible_start = current_start - 10 * 366 * 24 * 60 * 60
    while current_start > last_possible_start:
        latest = await read_passed(current_start, current_end)
        if latest:
            return latest * 1000
        current_end = current_start
        current_start = current_end - 366 * 24 * 60 * 60
    return None


class _ControlledReader(ABC):
    @abstractmethod
    async def readexactly(self, n: int) -> bytes:
        pass

    @abstractmethod
    async def read(self) -> bytes:
        pass

    @abstractmethod
    async def wait(self) -> None:
        pass

    @abstractmethod
    def terminate(self) -> None:
        pass


class _ProcessReader(_ControlledReader):
    def __init__(self, process):
        self.process = process
        self._terminated = False
        self._timeout = background_task(self._run_timeout())

    def _run_termination(self) -> None:
        if not self.process:
            return
        process = self.process
        self.process = None

        try:
            process.terminate()
        except:
            pass

        async def _run_kill():
            await asyncio.sleep(60)
            if not process:
                return
            try:
                process.kill()
            except:
                pass

        async def _wait_process():
            nonlocal process
            try:
                await process.wait()
            except:
                pass
            process = None

        background_task(_wait_process())
        background_task(_run_kill())

    async def _run_timeout(self):
        await asyncio.sleep(_read_timeout)
        self._terminated = True
        self._timeout = None
        self._run_termination()

    def _cancel_timeout(self) -> None:
        if self._timeout is None:
            return
        try:
            self._timeout.cancel()
        except:
            pass
        self._timeout = None

    async def readexactly(self, n: int) -> bytes:
        if self._terminated or not self.process:
            raise asyncio.IncompleteReadError(bytes(), n)
        return await self.process.stdout.readexactly(n)

    async def read(self) -> bytes:
        if self._terminated or not self.process:
            return bytes()
        return await self.process.stdout.read(65536)

    async def wait(self) -> None:
        if self._terminated or not self.process:
            return
        await self.process.wait()
        self.process = None
        self._cancel_timeout()

    def terminate(self) -> None:
        if self._terminated:
            return
        _LOGGER.debug("Terminating reader process")
        self._terminated = True
        self._cancel_timeout()
        self._run_termination()


class _FilteredReader(_ProcessReader):
    def __init__(self, reader, filter):
        super().__init__(filter)
        self.origin = reader

    def _run_termination(self) -> None:
        if not self.process:
            return
        process = self.process
        self.process = None
        origin = self.origin
        self.origin = None

        try:
            process.terminate()
        except:
            pass
        try:
            origin.terminate()
        except:
            pass

        async def _run_kill():
            await asyncio.sleep(60)
            if process:
                try:
                    process.kill()
                except:
                    pass
            if origin:
                try:
                    origin.kill()
                except:
                    pass

        async def _wait_process():
            nonlocal process
            try:
                await process.wait()
            except:
                pass
            process = None

        async def _wait_origin():
            nonlocal origin
            try:
                await origin.wait()
            except:
                pass
            origin = None

        background_task(_wait_process())
        background_task(_wait_origin())
        background_task(_run_kill())

    async def wait(self) -> None:
        if self._terminated:
            return
        if self.process and not self._terminated:
            await self.process.wait()
            self.process = None
        if self.origin and not self._terminated:
            await self.origin.wait()
            self.origin = None
        self._cancel_timeout()


class DataReader(RecordStream):
    PASS_STALL_ARCHIVES = frozenset({"clean", "avgh"})

    class Input(RecordInput):
        def __init__(self, reader: "DataReader",
                     record_buffer: typing.List[typing.Tuple[int, typing.Dict[Name, typing.Any]]]):
            super().__init__()
            self.reader = reader
            self.record_buffer = record_buffer

        def record_ready(self, start: typing.Optional[float], end: typing.Optional[float],
                         record: typing.Dict[Name, typing.Any]) -> None:
            if not start:
                return
            start = round(start * 1000)
            if start < self.reader.clip_start_ms:
                start = self.reader.clip_start_ms
            self.record_buffer.append((start, record))

        def record_break(self, start: float, end: float) -> None:
            start = round(start * 1000)
            if start < self.reader.clip_start_ms:
                start = self.reader.clip_start_ms
            self.record_buffer.append((start, {}))

    def __init__(self, start_epoch_ms: int, end_epoch_ms: int,
                 data: typing.Dict[Name, str],
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send, list(data.values()))
        self.clip_start_ms = start_epoch_ms
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))
        self.data = data

    async def _convert(self, epoch_ms: int, record: typing.Dict[Name, typing.Any]):
        fields: typing.Dict[str, typing.Optional[float]] = dict()

        def convert_value(value):
            if value is None:
                return None
            if isinstance(value, float):
                return value
            if isinstance(value, int):
                return value
            if isinstance(value, list):
                for i in range(len(value)):
                    check = value[i]
                    if i is None:
                        continue
                    try:
                        value[i] = float(check)
                    except (ValueError, TypeError):
                        value[i] = None
                return value
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

        for name, value in record.items():
            target = self.data.get(name)
            if target is None:
                continue
            fields[target] = convert_value(value)
        await self.send_record(epoch_ms, fields)

    def selection_arguments(self) -> typing.List[str]:
        selections: typing.List[str] = list()
        for sel in self.data:
            if len(sel.flavors) == 0:
                arg = ':='
            else:
                arg = ''
                for f in sel.flavors:
                    arg += f':={f}'
            arg = f'{sel.station}:{sel.archive}:{sel.variable}' + arg
            selections.append(arg)
        return selections

    async def create_reader(self) -> _ControlledReader:
        selections = self.selection_arguments()
        _LOGGER.debug(f"Starting data read for {self.start_epoch},{self.end_epoch} with {len(selections)} selections")

        process = await asyncio.create_subprocess_exec(_interface, 'archive_read',
                                                       str(self.start_epoch), str(self.end_epoch),
                                                       *selections,
                                                       stdout=asyncio.subprocess.PIPE,
                                                       stdin=asyncio.subprocess.DEVNULL)

        return _ProcessReader(process)

    class CleanReadyStall(RecordStream.Stall):
        def __init__(self, readers: typing.List[asyncio.StreamReader], writers: typing.List[asyncio.StreamWriter]):
            super().__init__("Passed data update in progress")
            self.readers = readers
            self.writers = writers

        async def block(self):
            for r in self.readers:
                try:
                    await r.read(1)
                except (OSError, EOFError):
                    pass
            for w in self.writers:
                try:
                    w.close()
                except OSError:
                    pass

    def stall_stations(self) -> typing.Set[str]:
        blocking_stations: typing.Set[str] = set()
        for check in self.data.keys():
            if check.archive in self.PASS_STALL_ARCHIVES:
                blocking_stations.add(check.station)
        return blocking_stations

    async def stall(self) -> typing.Optional["DataReader.CleanReadyStall"]:
        blocking_stations = self.stall_stations()

        if len(blocking_stations) == 0:
            return None

        readers: typing.List[asyncio.StreamReader] = list()
        writers: typing.List[asyncio.StreamWriter] = list()

        for station in blocking_stations:
            try:
                reader, writer = await asyncio.open_unix_connection(
                    CONFIGURATION.get('CPD3.PASS.SOCKET', '/run/forge-cpd3-pass.socket'))
                enc = station.encode('utf-8')
                writer.write(struct.pack('<BI', 1, len(enc)))
                writer.write(enc)
                await writer.drain()
                response = await reader.read(1)
                if not response:
                    writer.close()
                    continue
            except (OSError, EOFError):
                continue
            readers.append(reader)
            writers.append(writer)

        if len(readers) == 0:
            return None
        return self.CleanReadyStall(readers, writers)

    async def run(self) -> None:
        reader = await self.create_reader()

        try:
            await reader.readexactly(3)

            buffer: typing.List[typing.Tuple[int, typing.Dict[Name, typing.Any]]] = list()
            converter = self.Input(self, buffer)

            while True:
                data = await reader.read()
                if not data:
                    break
                converter.incoming_raw(data)
                for r in buffer:
                    await self._convert(r[0], r[1])
                buffer.clear()

            converter.flush()
            for r in buffer:
                await self._convert(r[0], r[1])
            await self.flush()
            await reader.wait()
        except asyncio.CancelledError:
            reader.terminate()
            raise


class EditedReader(DataReader):
    def __init__(self, start_epoch_ms: int, end_epoch_ms: int, station: str, profile: str,
                 data: typing.Dict[Name, str],
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        # Limit data request amount, so we don't bog down the system
        if end_epoch_ms - start_epoch_ms > 32 * 24 * 60 * 60 * 1000:
            end_epoch_ms = start_epoch_ms + 32 * 24 * 60 * 60 * 1000

        super().__init__(start_epoch_ms=start_epoch_ms, end_epoch_ms=end_epoch_ms, data=data, send=send)
        self.station = station
        self.profile = profile
        self._generator: typing.Optional[asyncio.subprocess.Process] = None

    async def create_reader(self) -> _ControlledReader:
        selections = self.selection_arguments()
        _LOGGER.debug(f"Starting edited read for {self.station}-{self.profile} {self.start_epoch},{self.end_epoch} with {len(selections)} selections")

        read, write = os.pipe()

        reader = await asyncio.create_subprocess_exec(_interface, 'edited_read',
                                                      str(self.start_epoch), str(self.end_epoch),
                                                      self.station, self.profile,
                                                      stdout=write,
                                                      stdin=asyncio.subprocess.DEVNULL)
        os.close(write)

        filter = await asyncio.create_subprocess_exec(_interface, 'filter',
                                                      *selections,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=read)
        os.close(read)

        return _FilteredReader(reader, filter)

    async def stall(self) -> typing.Optional["DataStream.Stall"]:
        return None


class RealtimeReader(DataReader):
    class Input(DataReader.Input):
        def _valid_time(self, start: typing.Optional[float], end: typing.Optional[float]) -> bool:
            if end and end < self.reader.clip_start_epoch:
                return False
            if start and start >= self.reader.clip_end_epoch:
                return False
            return True

        def record_ready(self, start: typing.Optional[float], end: typing.Optional[float],
                         record: typing.Dict[Name, typing.Any]) -> None:
            if not self._valid_time(start, end):
                return
            super().record_ready(start, end, record)
            if not end:
                return
            end = round(start * 1000)
            self.reader.realtime_start_ms = end

        def record_break(self, start: float, end: float) -> None:
            if not self._valid_time(start, end):
                return
            super().record_break(start, end)

    class RealtimeStream(RealtimeRead):
        def __init__(self, data: "RealtimeReader",  reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            super().__init__(reader, writer, data.station, data.data_name, stream_incoming=True)
            self.data = data
            self.discard_epoch_ms: int = data.realtime_start_ms

            # Realtime records are recorded at the END of the average, so offset them back to the start
            self.record_time_offset: int = round(data.realtime_offset * 1000.0)
            # A bit of slack for network delays
            self.discard_epoch_ms += ceil(data.realtime_offset / 4.0 * 1000.0)

            # Since we only get the current time, add the expected interval to the break threshold
            self.data_break_threshold = ceil((data.Input.TIME_SLACK + data.realtime_offset) * 1000)
            self.data_break_epoch_ms = self.discard_epoch_ms + self.data_break_threshold

        async def block_ready(self, block: RealtimeDataBlock) -> None:
            for record in block.records:
                adjusted_time = record.epoch_ms - self.record_time_offset
                if adjusted_time <= self.discard_epoch_ms:
                    continue
                if adjusted_time > self.data_break_epoch_ms:
                    await self.data.send_record(self.data_break_epoch_ms - 1, {})
                await self.data.send_record(adjusted_time, record.fields)
                self.data_break_epoch_ms = adjusted_time + self.data_break_threshold
            await self.data.flush()

    def __init__(self, start_epoch_ms: int, end_epoch_ms: int, station: str, data_name: str,
                 data: typing.Dict[Name, str],
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]],
                 realtime_offset: float = 60.0):
        now_ms = round(time.time() * 1000)
        if end_epoch_ms < now_ms - 60 * 60 * 1000:
            end_epoch_ms = now_ms - 60 * 60 * 1000
        if end_epoch_ms - start_epoch_ms > 32 * 24 * 60 * 60 * 1000:
            start_epoch_ms = end_epoch_ms - 32 * 24 * 60 * 60 * 1000

        # Apply rounding so we hit the cache better
        rounded_start = floor(start_epoch_ms / (1000 * 60 * 60)) * 1000 * 60 * 60
        rounded_end = ceil(end_epoch_ms / (1000 * 60 * 60)) * 1000 * 60 * 60

        super().__init__(rounded_start, rounded_end, data, send)
        self.station = station
        self.data_name = data_name
        self.realtime_start_ms = start_epoch_ms
        self.realtime_offset = realtime_offset

        self.clip_start_ms = start_epoch_ms
        self.clip_start_epoch = rounded_start / 1000.0
        self.clip_end_epoch = rounded_end / 1000.0

    async def run(self) -> None:
        await super().run()

        socket_name = CONFIGURATION.get('REALTIME.SOCKET', '/run/forge-vis-realtime.socket')
        try:
            reader, writer = await asyncio.open_unix_connection(socket_name)
        except FileNotFoundError:
            _LOGGER.debug(f"Unable to open realtime connection for {self.station} {self.data_name} on {socket_name}")
            return
        _LOGGER.debug(f"Realtime data connection open for {self.station} {self.data_name} on {socket_name}")
        try:
            stream = self.RealtimeStream(self, reader, writer)
            await stream.run()
            _LOGGER.debug(f"Realtime data connection ended for {self.station} {self.data_name}")
        finally:
            try:
                writer.close()
            except OSError:
                pass


class EditReader(DataStream):
    class _Input(StandardDataInput):
        def __init__(self, profile: str, result: typing.List[typing.Dict]):
            super().__init__()
            self.profile = profile
            self.result = result

        def value_ready(self, identity: Identity, value: typing.Any) -> None:
            if not isinstance(value, dict):
                return
            if not _display_directive(value):
                return
            self.result.append(_convert_directive(self.profile, identity, value))

    def __init__(self, station: str, profile: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station
        self.profile = profile
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))

    async def run(self) -> None:
        reader = await asyncio.create_subprocess_exec(_interface, 'directive_read',
                                                      str(self.start_epoch), str(self.end_epoch), self.station,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)
        try:
            await reader.stdout.readexactly(3)

            buffer: typing.List[typing.Dict] = list()
            converter = self._Input(self.profile, buffer)

            while True:
                data = await reader.stdout.read(65536)
                if not data:
                    break
                converter.incoming_raw(data)
                for r in buffer:
                    await self.send(r)
                buffer.clear()

            for r in buffer:
                await self.send(r)
            await reader.wait()
        except asyncio.CancelledError:
            try:
                reader.terminate()
            except:
                pass
            raise


class EditAvailable(DataStream):
    def __init__(self, station: str, profile: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station
        self.profile = profile
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))

    async def run(self) -> None:
        reader = await asyncio.create_subprocess_exec(_interface, 'edited_available',
                                                      str(self.start_epoch), str(self.end_epoch),
                                                      self.station, self.profile,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)
        try:
            while True:
                line = await reader.stdout.readline()
                if not line:
                    break
                line = line.decode('utf-8').strip()
                if not line:
                    continue
                await self.send({
                    'type': 'variable',
                    'variable': line,
                })

            await reader.wait()
        except asyncio.CancelledError:
            try:
                reader.terminate()
            except:
                pass
            raise


class ContaminationReader(DataStream):    
    class _State:
        def __init__(self):
            self.active_contamination: typing.Set[str] = set()
            self.active_start: typing.Optional[int] = None
            self.buffer: typing.List[typing.Dict[str, typing.Any]] = list()

        def complete(self, end_ms: int) -> None:
            if not self.active_start:
                return
            self.buffer.append({
                'start_epoch_ms': self.active_start,
                'end_epoch_ms': end_ms,
                #'flags': list(self.active_contamination),
            })
            self.active_start = None
            self.active_contamination.clear()

        async def flush(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> None:
            for segment in self.buffer:
                await send(segment)
            self.buffer.clear()

        def convert(self, start_ms: int, record: typing.Dict[Name, typing.Any]) -> None:
            contamination_flags: typing.Set[str] = set()
            for value in record.values():
                if not isinstance(value, set):
                    continue
                for flag in value:
                    if flag.startswith('contam') or flag.startswith('Contam'):
                        contamination_flags.add(flag)
            if contamination_flags == self.active_contamination:
                return
            self.complete(start_ms)
            if len(contamination_flags) == 0:
                return
            self.active_contamination = contamination_flags
            self.active_start = start_ms

        def record_break(self, start_ms: int) -> None:
            self.complete(start_ms)

    class Input(RecordInput):
        def __init__(self, reader: "ContaminationReader", state: "ContaminationReader._State"):
            super().__init__()
            self.reader = reader
            self.state = state

        def record_ready(self, start: typing.Optional[float], end: typing.Optional[float],
                         record: typing.Dict[Name, typing.Any]) -> None:
            if not start:
                return
            start = round(start * 1000)
            if start < self.reader.clip_start_ms:
                start = self.reader.clip_start_ms
            self.state.convert(start, record)

        def record_break(self, start: float, end: float) -> None:
            start = round(start * 1000)
            if start < self.reader.clip_start_ms:
                start = self.reader.clip_start_ms
            self.state.record_break(start)

    def __init__(self, start_epoch_ms: int, end_epoch_ms: int,
                 data: typing.Set[Name],
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.clip_start_ms = start_epoch_ms
        self.clip_end_ms = end_epoch_ms
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))
        self.data = data

    def selection_arguments(self) -> typing.List[str]:
        selections = list()
        for sel in self.data:
            selections.append(f'{sel.station}:{sel.archive}:{sel.variable}:-cover:-stats')
        return selections

    async def create_reader(self) -> _ControlledReader:
        selections = self.selection_arguments()            
        _LOGGER.debug(f"Starting contamination read for {self.start_epoch},{self.end_epoch} with {len(selections)} selections")

        process = await asyncio.create_subprocess_exec(_interface, 'archive_read',
                                                       str(self.start_epoch), str(self.end_epoch),
                                                       *selections,
                                                       stdout=asyncio.subprocess.PIPE,
                                                       stdin=asyncio.subprocess.DEVNULL)
        return _ProcessReader(process)

    PASS_STALL_ARCHIVES = DataReader.PASS_STALL_ARCHIVES
    CleanReadyStall = DataReader.CleanReadyStall
    stall = DataReader.stall

    def stall_stations(self) -> typing.Set[str]:
        blocking_stations: typing.Set[str] = set()
        for check in self.data:
            if check.archive in self.PASS_STALL_ARCHIVES:
                blocking_stations.add(check.station)
        return blocking_stations

    async def run(self) -> None:
        reader = await self.create_reader()

        try:
            await reader.readexactly(3)

            state = self._State()
            converter = self.Input(self, state)

            while True:
                data = await reader.read()
                if not data:
                    break
                converter.incoming_raw(data)
                await state.flush(self.send)

            converter.flush()
            state.complete(self.clip_end_ms)
            await state.flush(self.send)
            await reader.wait()
        except asyncio.CancelledError:
            reader.terminate()
            raise


class EditedContaminationReader(ContaminationReader):
    class Input(ContaminationReader.Input):
        def value_ready(self, identity: Identity, value: typing.Any) -> None:
            if 'cover' in identity.flavors or 'stats' in identity.flavors:
                return
            check = Name(station=identity.station, archive=identity.archive, variable=identity.variable)
            if check not in self.reader.data:
                return
            super().value_ready(identity, value)

    def __init__(self, start_epoch_ms: int, end_epoch_ms: int, station: str, profile: str,
                 data: typing.Set[Name],
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        # Limit data request amount, so we don't bog down the system
        if end_epoch_ms - start_epoch_ms > 32 * 24 * 60 * 60 * 1000:
            end_epoch_ms = start_epoch_ms + 32 * 24 * 60 * 60 * 1000

        super().__init__(start_epoch_ms=start_epoch_ms, end_epoch_ms=end_epoch_ms, data=data, send=send)
        self.station = station
        self.profile = profile

    async def create_reader(self) -> _ControlledReader:
        selections = self.selection_arguments()
        _LOGGER.debug(
            f"Starting edited contamination read for {self.station}-{self.profile} {self.start_epoch},{self.end_epoch} with {len(selections)} selections")

        read, write = os.pipe()

        reader = await asyncio.create_subprocess_exec(_interface, 'edited_read',
                                                      str(self.start_epoch), str(self.end_epoch),
                                                      self.station, self.profile,
                                                      stdout=write,
                                                      stdin=asyncio.subprocess.DEVNULL)
        os.close(write)

        filter = await asyncio.create_subprocess_exec(_interface, 'filter',
                                                      *selections,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=read)
        os.close(read)

        return _FilteredReader(reader, filter)

    async def stall(self) -> typing.Optional["DataStream.Stall"]:
        return None


class EventLogReader(DataStream):
    class _Input(StandardDataInput):
        def __init__(self, result: typing.List[typing.Dict]):
            super().__init__()
            self.result = result

        @staticmethod
        def _convert_event(identity: Identity, value: typing.Any) -> typing.Dict[str, typing.Any]:
            result: typing.Dict[str, typing.Any] = {
                'epoch_ms': floor(identity.start * 1000) if identity.start else None,
                'message': value.get('Text', ""),
            }

            if identity.variable == 'acquisition':
                if value.get('Source') == 'EXTERNAL':
                    result['type'] = "User"
                    result['author'] = value.get('Author', "")
                else:
                    result['type'] = "Instrument"
                    result['author'] = value.get('Source', "")
                    result['acquisition'] = True

            return result

        def value_ready(self, identity: Identity, value: typing.Any) -> None:
            if not isinstance(value, dict):
                return
            if not _display_directive(value):
                return
            self.result.append(self._convert_event(identity, value))

    def __init__(self, station: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))

    async def run(self) -> None:
        reader = await asyncio.create_subprocess_exec(_interface, 'archive_read',
                                                      str(self.start_epoch), str(self.end_epoch),
                                                      f'{self.station}:events:acquisition:=',
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)
        try:
            await reader.stdout.readexactly(3)

            buffer: typing.List[typing.Dict] = list()
            converter = self._Input(buffer)

            while True:
                data = await reader.stdout.read(65536)
                if not data:
                    break
                converter.incoming_raw(data)
                for r in buffer:
                    await self.send(r)
                buffer.clear()

            for r in buffer:
                await self.send(r)
            await reader.wait()
        except asyncio.CancelledError:
            try:
                reader.terminate()
            except:
                pass
            raise


class RealtimeTranslator(BaseRealtimeTranslator):
    class Key:
        def __init__(self, variable: str, flavors: typing.Optional[typing.Set[str]] = None):
            self.variable = variable
            self.flavors: typing.Set[str] = {flavor.lower() for flavor in flavors} if flavors else set()

        def __eq__(self, other):
            if not isinstance(other, RealtimeTranslator.Key):
                return NotImplemented
            return self.variable == other.variable and self.flavors == other.flavors

        def __hash__(self):
            flavors = None
            if len(self.flavors) == 1:
                flavors = next(x for x in self.flavors)
            return hash((self.variable, flavors))

        def __repr__(self):
            return f"Key({self.variable}, {self.flavors})"

    class Target:
        def __init__(self, data_name: str, field: str):
            self.data_name = data_name
            self.field = field

    def __init__(self, data: typing.Dict[str, typing.Dict["RealtimeTranslator.Key", str]],
                 realtime_offset: float = 60.0):
        super().__init__()
        self.data = data
        self.realtime_offset = realtime_offset

        self._dispatch: typing.Dict["RealtimeTranslator.Key", typing.List["RealtimeTranslator.Target"]] = dict()
        for data_name, field_lookup in self.data.items():
            for key, field in field_lookup.items():
                targets = self._dispatch.get(key)
                if targets is None:
                    targets: typing.List["RealtimeTranslator.Target"] = list()
                    self._dispatch[key] = targets
                targets.append(self.Target(data_name, field))

    def realtime_targets(self, key: "RealtimeTranslator.Key") -> typing.List["RealtimeTranslator.Target"]:
        return self._dispatch.get(key, [])

    def reader(self, start_epoch_ms: int, end_epoch_ms: int, station: str, data_name: str,
               send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[RealtimeReader]:
        contents = self.data.get(data_name)
        if not contents:
            _LOGGER.debug(f"No realtime data defined for {data_name}")
            return None
        reader_data: typing.Dict[Name, str] = dict()
        for key, field in contents.items():
            reader_data[Name(station, 'raw', key.variable, key.flavors)] = field
        return RealtimeReader(start_epoch_ms, end_epoch_ms, station, data_name, reader_data, send,
                              realtime_offset=self.realtime_offset)

    def __deepcopy__(self, memo):
        y = type(self)(deepcopy(self.data), realtime_offset=self.realtime_offset)
        memo[id(self)] = y
        return y

    def detach(self):
        return deepcopy(self)

    class Data(dict):
        def __init__(self, profile: str, *args, archive: str = 'realtime', **kwargs):
            super().__init__(*args, **kwargs)
            self.profile = profile
            self.archive = archive

        def _translated_reader(self, record: str) -> typing.Callable[[str, int, int, typing.Callable],
                                                                     typing.Optional[DataStream]]:
            def lookup(station: str, start_epoch_ms: int, end_epoch_ms: int,
                       send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
                translator: typing.Optional[RealtimeTranslator] = get_translator(station)
                if translator is None or not isinstance(translator, RealtimeTranslator):
                    _LOGGER.debug(f"No realtime translator for {station}")
                    return None
                data_name = f'{self.profile}-{self.archive}-{record}'
                return translator.reader(start_epoch_ms, end_epoch_ms, station, data_name, send)
            return lookup

        def __getitem__(self, key) -> typing.Callable[[str, int, int, typing.Callable], typing.Optional[DataStream]]:
            return self._translated_reader(key)

        def get(self, key, default=None) -> typing.Callable[[str, int, int, typing.Callable],
                                                            typing.Optional[DataStream]]:
            return self._translated_reader(key)

        def __deepcopy__(self, memo):
            y = type(self)(self.profile, self, archive=self.archive)
            memo[id(self)] = y
            return y

    @classmethod
    def assemble_translator(cls, profile_data: typing.Dict[str, typing.Dict[str, typing.Union[typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]], "RealtimeTranslator.Data"]]],
                            realtime_offset = 60.0) -> "RealtimeTranslator":
        data: typing.Dict[str, typing.Dict["RealtimeTranslator.Key", str]] = dict()
        for profile_name, profile_archives in profile_data.items():
            for archive_name, archive_records in profile_archives.items():
                if not isinstance(archive_records, cls.Data):
                    continue
                for record_name, record_data in archive_records.items():
                    data_name = f'{profile_name}-{archive_name}-{record_name}'
                    data[data_name] = record_data
        return cls(data, realtime_offset=realtime_offset)


class AcquisitionTranslator(BaseAcquisitionTranslator):
    class Variable:
        def __init__(self, variable: str, flavors: typing.Optional[typing.Set[str]] = None):
            self.variable = variable
            self.flavors: typing.Optional[typing.Set[str]] = {flavor.lower() for flavor in flavors} if flavors else None

        def __eq__(self, other):
            if not isinstance(other, AcquisitionTranslator.Variable):
                return NotImplemented
            return self.variable == other.variable and self.flavors == other.flavors

        def __hash__(self):
            flavors = None
            if self.flavors is not None and len(self.flavors) == 1:
                flavors = next(x for x in self.flavors)
            return hash((self.variable, flavors))

        def __repr__(self):
            return f"Variable({self.variable}, {self.flavors})"

    class Interface:
        def __init__(self, display_type: str,
                     variable_map: typing.Dict["AcquisitionTranslator.Variable", str] = None,
                     command_map: typing.Dict[str, str] = None,
                     zstate_notifications: typing.Dict[str, str] = None,
                     zstate_set_warning: typing.Set[str] = None,
                     flags_notifications: typing.Dict[str, str] = None,
                     flags_set_warning: typing.Set[str] = None):
            self.display_type = display_type

            if variable_map is None:
                variable_map = dict()
            self.variable_map: typing.Dict["AcquisitionTranslator.Variable", str] = variable_map

            if command_map is None:
                command_map = dict()
            self.command_map: typing.Dict[str, str] = command_map

            if zstate_notifications is None:
                zstate_notifications = dict()
            self.zstate_notifications: typing.Dict[str, str] = zstate_notifications

            if zstate_set_warning is None:
                zstate_set_warning = set()
            self.zstate_set_warning: typing.Set[str] = zstate_set_warning

            if flags_notifications is None:
                flags_notifications = dict()
            self.flags_notifications: typing.Dict[str, str] = flags_notifications

            if flags_set_warning is None:
                flags_set_warning = set()
            self.flags_set_warning: typing.Set[str] = flags_set_warning

        def matches(self, interface_name: str, interface_info: typing.Dict[str, typing.Any]) -> bool:
            return False

        def display_information(self, interface_info: typing.Dict[str, typing.Any]) -> typing.Any:
            source = interface_info.get('Source')
            if source is None:
                source = {}
            serial_number = source.get('SerialNumber')
            if serial_number is None:
                display_string = str(interface_info.get('WindowTitle', '')).split('#', 2)
                if len(display_string) < 1:
                    display_string = str(interface_info.get('MenuEntry', '')).split('#', 2)
                if len(display_string) > 1:
                    serial_number = display_string[1]
            return {
                'type': self.display_type,
                'serial_number': serial_number,
                'display_id': source.get('Name'),
                'display_letter': interface_info.get('MenuCharacter'),
            }

        def display_state(self, state: typing.Optional[typing.Dict[str, typing.Any]]) -> typing.Dict[str, typing.Any]:
            if state is None:
                return {}
            return {
                'communicating': (str(state.get('Status')).lower() != 'nocommunications'),
                'bypassed': (len(state.get('BypassFlags')) > 0),
            }

        def translate_command(self, command: str, data: typing.Any) -> typing.Any:
            target = self.command_map.get(command)
            if not target:
                return None
            result = dict()
            if callable(target):
                target, value = target(data)
                result[target] = value
            else:
                result[target] = True
            return result

        def value_translator(self, name: Name) -> typing.Tuple[
                typing.Optional[str], typing.Optional[typing.Callable[[typing.Any], typing.Any]]]:
            return None, None

        def activate(self, source: str, info: typing.Optional[typing.Dict[str, typing.Any]]) -> "AcquisitionTranslator.ActiveInterface":
            active = AcquisitionTranslator.ActiveInterface(self, source)
            active.update_interface_information(info)
            return active

    class ActiveInterface:
        def __init__(self, interface: "AcquisitionTranslator.Interface", source: str):
            self.interface = interface
            self.source = source

        def matches(self, interface_info: typing.Dict[str, typing.Any]) -> bool:
            return self.interface.matches(interface_info)

        def display_information(self, interface_info: typing.Dict[str, typing.Any]) -> typing.Any:
            return self.interface.display_information(interface_info)

        def update_instrument_state(self, state: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
            pass

        def update_interface_information(self, info: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
            pass

        def display_state(self, state: typing.Optional[typing.Dict[str, typing.Any]]) -> typing.Dict[str, typing.Any]:
            return self.interface.display_state(state)

        def translate_command(self, command: str, data: typing.Any) -> typing.Any:
            return self.interface.translate_command(command, data)

        def value_translator(self, name: Name) -> typing.Tuple[typing.Optional[str], typing.Optional[typing.Callable[[typing.Any], typing.Any]]]:
            return None, None

        def translator_override(self, name: Name) -> typing.Optional[typing.Callable[[typing.Any, typing.Any], None]]:
            return None

    class Component(Interface):
        def __init__(self, component_type: str, display_type: str, **kwargs):
            super().__init__(display_type, **kwargs)
            self.component_type = component_type

        def matches(self, interface_name: str, interface_info: typing.Dict[str, typing.Any]) -> bool:
            source_info = interface_info.get('Source')
            if source_info is None:
                return False
            return source_info.get('Component') == self.component_type

    class Nephelometer(Component):
        def value_translator(self, name: Name) -> typing.Tuple[
                typing.Optional[str], typing.Optional[typing.Callable[[typing.Any], typing.Any]]]:
            if name.variable.startswith('ZSPANCHECK_'):
                def translator(value: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
                    if value is None:
                        return {}

                    result: typing.Dict[str, typing.Any] = dict()

                    def select_angle(target, angle, index, angles) -> bool:
                        delta_target = abs(angle - target)
                        if delta_target > 10.0:
                            return False
                        if index is None:
                            return True
                        return delta_target < abs(angles[index] - target)

                    angles = value.get('Angles', [0.0, 90.0])
                    index_total = None
                    index_back = None
                    for i in range(len(angles)):
                        angle = angles[i]
                        if select_angle(0.0, angle, index_total, angles):
                            index_total = i
                        if select_angle(90.0, angle, index_back, angles):
                            index_back = i

                    def set_path(value: typing.Any, *path: str) -> None:
                        target = result
                        for i in range(len(path)-1):
                            p = path[i]
                            next_target = target.get(p)
                            if next_target is None:
                                next_target = dict()
                                target[p] = next_target
                            target = next_target
                        target[path[-1]] = value

                    for color_code in ('B', 'G', 'R'):
                        color_data = result.get(color_code)
                        if color_data is None:
                            continue

                        k2 = color_data.get('K2')
                        if isfinite(k2):
                            set_path(k2, 'calibration', 'K2', color_code)
                        k4 = color_data.get('K4')
                        if isfinite(k4):
                            set_path(k4, 'calibration', 'K4', color_code)

                        color_result = color_data.get('Results')
                        if color_result is None or not isinstance(color_result, list):
                            continue

                        if index_total is not None and index_total < len(color_result):
                            angle_data = color_result[index_total]
                            set_path(angle_data.get('PCT'), 'percent_error', 'total', color_code)
                            set_path(angle_data.get('Cc'), 'sensitivity_factor', 'total', color_code)

                            m = angle_data.get('CalM')
                            if isfinite(m):
                                set_path(m, 'calibration', 'M', 'total', color_code)
                            c = angle_data.get('CalC')
                            if isfinite(c):
                                set_path(c, 'calibration', 'C', 'total', color_code)
                        if index_back is not None and index_back < len(color_result):
                            angle_data = color_result[index_back]
                            set_path(angle_data.get('PCT'), 'percent_error', 'back', color_code)
                            set_path(angle_data.get('Cc'), 'sensitivity_factor', 'back', color_code)

                            m = angle_data.get('CalM')
                            if isfinite(m):
                                set_path(m, 'calibration', 'M', 'back', color_code)
                            c = angle_data.get('CalC')
                            if isfinite(c):
                                set_path(c, 'calibration', 'C', 'back', color_code)
                    return result
                return 'spancheck_result', translator
            return super().value_translator(name)

    class TSI3563Nephelometer(Nephelometer):
        def translate_command(self, command: str, data: typing.Any) -> typing.Any:
            if command == 'set_parameters':
                parameters: typing.Dict[str, typing.Any] = dict()

                for name in ('SKB', 'SKG', 'SKR'):
                    base = data.get(name)
                    if base is None:
                        continue
                    for k in ('K2', 'K4'):
                        value = base.get(k)
                        try:
                            value = float(value)
                        except (ValueError, TypeError):
                            continue
                        parameters[name + k.upper()] = value

                for name in ('B', 'SMZ', 'SP', 'STB', 'STZ', 'SVB', 'SVG', 'SVR'):
                    value = data.get(name)
                    try:
                        value = int(value)
                    except (ValueError, TypeError):
                        continue
                    parameters[name] = value

                for name in ('SMB', 'H'):
                    value = data.get(name)
                    if not isinstance(value, bool):
                        continue
                    parameters[name] = value

                # for name in ('V'):
                #     value = data.get(name)
                #     if not isinstance(value, str):
                #         continue
                #     if len(value) == 0:
                #         continue
                #     parameters[name] = value.lower()

                return {'SetParameters': parameters}
            return super().translate_command(command, data)

    class _LovePIDActive(ActiveInterface):
        def __init__(self, interface: "AcquisitionTranslator.Interface", source: str):
            super().__init__(interface, source)
            self._value: typing.List[typing.Optional[float]] = list()
            self._raw: typing.List[typing.Optional[float]] = list()
            self._setpoint: typing.List[typing.Optional[float]] = list()
            self._control: typing.List[typing.Optional[float]] = list()
            self._address: typing.List[int] = list()
            self._variable: typing.List[typing.Optional[str]] = list()

        def update_interface_information(self, info: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
            source = info.get('Source')
            if source is None:
                return
            self._address = source.get('AddressIndex', self._address)
            self._variable = source.get('VariableIndex', self._variable)

        def display_information(self, interface_info: typing.Dict[str, typing.Any]) -> typing.Any:
            result = super().display_information(interface_info)
            result['address'] = self._address
            result['variable'] = self._variable
            return result

        _CHANNEL_MATCH = re.compile(r'^(ZIN|ZSP|ZPCT)([A-Fa-f0-9]{1,2})$')

        def translator_override(self, name: Name) -> typing.Optional[typing.Callable[[typing.Any, typing.Any], None]]:
            try:
                index = self._variable.index(name.variable)
                while index >= len(self._variable):
                    self._variable.append(None)

                def update(result, value) -> None:
                    self._variable[index] = value
                    result[self.source]['value'] = self._variable

                return update
            except ValueError:
                pass

            variable_split = name.variable.split('_', 2)
            if len(variable_split) < 2:
                return None
            variable_source = variable_split[0]
            interface_source = variable_split[1]

            if interface_source != self.source:
                return None

            channel_matched = self._CHANNEL_MATCH.match(variable_source)
            if not channel_matched:
                return None

            address = int(channel_matched.group(2), 16)
            try:
                index = self._address.index(address)
            except ValueError:
                return None

            input_code = channel_matched.group(1)
            if input_code == 'ZIN':
                target = self._raw
                field = 'raw'
            elif input_code == 'ZSP':
                target = self._setpoint
                field = 'setpoint'
            elif input_code == 'ZPCT':
                target = self._control
                field = 'control'
            else:
                return None

            while index >= len(target):
                target.append(None)

            def update(result, value) -> None:
                target[index] = value
                result[self.source][field] = target

            return update

    class LovePID(Component):
        def activate(self, source: str,
                     info: typing.Optional[typing.Dict[str, typing.Any]]) -> "AcquisitionTranslator.ActiveInterface":
            active = AcquisitionTranslator._LovePIDActive(self, source)
            active.update_interface_information(info)
            return active

        def translate_command(self, command: str, data: typing.Any) -> typing.Any:
            if command == 'set_analog_chanel':
                return {
                    'SetAnalog': {
                        'Channel': data.get('channel'),
                        'Value': data.get('value'),
                    }
                }
            return super().translate_command(command, data)

    class _AnalogInputActive(ActiveInterface):
        def __init__(self, interface: "AcquisitionTranslator.Interface", source: str):
            super().__init__(interface, source)
            self._value: typing.List[typing.Optional[float]] = list()
            self._variable: typing.List[typing.Optional[str]] = list()

        def update_interface_information(self, info: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
            super().update_interface_information(info)
            source = info.get('Source')
            if source is None:
                return
            self._variable = source.get('VariableIndex', self._variable)

        def display_information(self, interface_info: typing.Dict[str, typing.Any]) -> typing.Any:
            result = super().display_information(interface_info)
            result['variable'] = self._variable
            return result

        def translator_override(self, name: Name) -> typing.Optional[typing.Callable[[typing.Any, typing.Any], None]]:
            try:
                index = self._variable.index(name.variable)
                while index >= len(self._variable):
                    self._variable.append(None)

                def update(result, value) -> None:
                    self._variable[index] = value
                    result[self.source]['value'] = self._variable

                return update
            except ValueError:
                pass

            return None

    class AnalogInput(Component):
        def activate(self, source: str,
                     info: typing.Optional[typing.Dict[str, typing.Any]]) -> "AcquisitionTranslator.ActiveInterface":
            active = AcquisitionTranslator._AnalogInputActive(self, source)
            active.update_interface_information(info)
            return active

    class _AnalogInputOutputActive(_AnalogInputActive):
        def __init__(self, interface: "AcquisitionTranslator.Interface", source: str):
            super().__init__(interface, source)
            self._output: typing.List[typing.Optional[str]] = list()

        def update_interface_information(self, info: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
            super().update_interface_information(info)
            source = info.get('Source')
            if source is None:
                return
            self._output = source.get('OutputIndex', self._output)

        def display_information(self, interface_info: typing.Dict[str, typing.Any]) -> typing.Any:
            result = super().display_information(interface_info)
            result['output'] = self._output
            return result

    class AnalogInputOutput(AnalogInput):
        def activate(self, source: str,
                     info: typing.Optional[typing.Dict[str, typing.Any]]) -> "AcquisitionTranslator.ActiveInterface":
            active = AcquisitionTranslator._AnalogInputOutputActive(self, source)
            active.update_interface_information(info)
            return active

        def translate_command(self, command: str, data: typing.Any) -> typing.Any:
            if command == 'set_analog_chanel':
                return {
                    'SetAnalog': {
                        'Channel': data.get('channel'),
                        'Value': data.get('value'),
                    }
                }
            return super().translate_command(command, data)

    class _AnalogInputOutputDigitalActive(_AnalogInputOutputActive):
        def __init__(self, interface: "AcquisitionTranslator.Interface", source: str):
            super().__init__(interface, source)
            self._digital: typing.List[typing.Optional[str]] = list()

        def update_interface_information(self, info: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
            super().update_interface_information(info)
            source = info.get('Source')
            if source is None:
                return
            self._digital = source.get('DigitalIndex', self._output)

        def display_information(self, interface_info: typing.Dict[str, typing.Any]) -> typing.Any:
            result = super().display_information(interface_info)
            result['digital'] = self._digital
            return result

    class AnalogInputOutputDigital(AnalogInputOutput):
        def activate(self, source: str,
                     info: typing.Optional[typing.Dict[str, typing.Any]]) -> "AcquisitionTranslator.ActiveInterface":
            active = AcquisitionTranslator._AnalogInputOutputDigitalActive(self, source)
            active.update_interface_information(info)
            return active

        def translate_command(self, command: str, data: typing.Any) -> typing.Any:
            if command == 'set_digital_output':
                try:
                    data = int(data)
                except:
                    return None
                return {
                    'SetDigital': {
                        'Mask': 0xFFFF_FFFF,
                        'Value': data,
                    }
                }
            return super().translate_command(command, data)

    class ImpactorCycle(Component):
        def matches(self, interface_name: str, interface_info: typing.Dict[str, typing.Any]) -> bool:
            if interface_name != 'IMPACTOR':
                return False
            return super().matches(interface_name, interface_info)

        @staticmethod
        def to_size(flags) -> str:
            if 'pm1' in flags:
                return 'PM1'
            elif 'pm25' in flags:
                return 'PM2.5'
            elif 'pm10' in flags:
                return 'PM10'
            return 'Whole'

        def value_translator(self, name: Name) -> typing.Tuple[
                typing.Optional[str], typing.Optional[typing.Callable[[typing.Any], typing.Any]]]:
            if name.variable.startswith('ZLAST'):
                def translator(value: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
                    if value is None:
                        return {}

                    return {
                        'size': self.to_size(value.get('Flavors')),
                        'epoch_ms': round(value.get('Time', 0) * 1000.0)
                    }
                return 'active', translator
            elif name.variable.startswith('ZNEXT'):
                def translator(value: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
                    if value is None:
                        return {}

                    return {
                        'size': self.to_size(value.get('Flavors')),
                        'epoch_ms': round(value.get('Time', 0) * 1000.0)
                    }
                return 'next', translator
            return super().value_translator(name)

    def __init__(self, interfaces: typing.List["AcquisitionTranslator.Interface"] = None):
        if interfaces is None:
            interfaces = []
        self.interfaces: typing.List["AcquisitionTranslator.Interface"] = interfaces

    def __deepcopy__(self, memo):
        y = type(self)(interfaces=deepcopy(self.interfaces))
        memo[id(self)] = y
        return y

    @staticmethod
    def pitot_shim(target: typing.Callable[[typing.Any, typing.Any], None]) -> typing.Callable[[typing.Any, typing.Any], None]:
        def shim(result, value):
            result['_pitot']['pitot'] = value
            return target(result, value)
        return shim

    @staticmethod
    def spancheck_state_shim(target: typing.Callable[[typing.Any, typing.Any], None]) -> typing.Callable[[typing.Any, typing.Any], None]:
        def shim(result, value):
            if isinstance(value, dict):
                current_state = value.get('Current')
                state: typing.Dict[str, typing.Any] = dict()
                if current_state == 'GasAirFlush':
                    state['id'] = 'gas_air_flush'
                elif current_state == 'GasFlush':
                    state['id'] = 'gas_flush'
                elif current_state == 'GasSample':
                    state['id'] = 'gas_sample'
                elif current_state == 'AirFlush':
                    state['id'] = 'air_flush'
                elif current_state == 'AirSample':
                    state['id'] = 'air_sample'
                else:
                    state['id'] = 'inactive'

                next_time = value.get('EndTime')
                if isfinite(next_time):
                    state['next_epoch_ms'] = round(next_time * 1000)

                result['_spancheck']['state'] = state
            return target(result, value)
        return shim

    @staticmethod
    def spancheck_results_shim(name: Name, target: typing.Callable[[typing.Any, typing.Any], None]) -> typing.Callable[[typing.Any, typing.Any], None]:
        variable_split = name.variable.split('_', 2)
        if len(variable_split) < 2:
            return target
        interface_source = variable_split[1]

        def shim(result, value):
            if isinstance(value, dict):
                results = result['_spancheck'].get('percent_error')
                if results is None:
                    results = dict()
                    result['_spancheck']['percent_error'] = results
                results[interface_source] = value.get('PCT')
            return target(result, value)
        return shim

    def translator_shim(self, name: Name, target: typing.Callable[[typing.Any, typing.Any], None]) -> typing.Callable[[typing.Any, typing.Any], None]:
        if name.variable == 'Pd_P01':
            return self.pitot_shim(target)
        elif name.variable.startswith('ZSPANCHECKSTATE_'):
            return self.spancheck_state_shim(target)
        elif name.variable.startswith('ZSPANCHECK_'):
            return self.spancheck_results_shim(name, target)
        return target

    def detach(self):
        return deepcopy(self)


def editing_get(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    profile = mode_name.split('-', 1)[0]
    return EditReader(station, profile, start_epoch_ms, end_epoch_ms, send)


def editing_available(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                      send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    profile = mode_name.split('-', 1)[0]
    return EditAvailable(station, profile, start_epoch_ms, end_epoch_ms, send)


def editing_writable(user: BaseAccessUser, station: str, mode_name: str,
                     directive: typing.Dict[str, typing.Any]) -> bool:
    profile = mode_name.split('-', 1)[0]
    if '_id' in directive:
        try:
            identity = Identity.deserialize(base64.b64decode(directive['_id']))
        except:
            return False
        if identity.station != station:
            # Require doing it from the station targeted
            return False
        if identity.variable == profile:
            # Already checked
            return True
        return user.allow_mode(station, identity.variable + '-editing', write=True)
    return True


def editing_save(user: BaseAccessUser, station: str, mode_name: str,
                 directive: typing.Dict[str, typing.Any]) -> typing.Awaitable[typing.Optional[dict]]:
    profile = mode_name.split('-', 1)[0]
    return _write_directive(user, station, profile, directive)


def editing_pass(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                 comment: typing.Optional[str] = None) -> typing.Awaitable[None]:
    profile = mode_name.split('-', 1)[0]
    start_epoch = int(floor(start_epoch_ms / 1000.0))
    end_epoch = int(ceil(end_epoch_ms / 1000.0))
    if not comment:
        comment = ''
    return _queue_pass(station, profile, start_epoch, end_epoch, comment)


def eventlog_get(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return EventLogReader(station, start_epoch_ms, end_epoch_ms, send)


def data_get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
             send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, profile_data)


def latest_passed(station: str, mode_name: str) -> typing.Awaitable[typing.Optional[int]]:
    profile = mode_name.split('-', 1)[0]
    return _get_latest_passed(station, profile)


def data_profile_get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
                     send: typing.Callable[[typing.Dict], typing.Awaitable[None]],
                     lookup: typing.Dict[str, typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]]]) -> typing.Optional[DataStream]:
    components = data_name.split('-', 2)
    if len(components) != 3:
        return None
    profile = components[0]
    archive = components[1]
    record = components[2]

    result = lookup.get(profile)
    if not result:
        _LOGGER.debug(f"No information for profile in {data_name}")
        return None
    result = result.get(archive)
    if not result:
        _LOGGER.debug(f"No information for archive in {data_name}")
        return None
    result = result.get(record)
    if not result:
        _LOGGER.debug(f"No information for record in {data_name}")
        return None
    return result(station, start_epoch_ms, end_epoch_ms, send)


class DataExport(Export):
    def __init__(self, start_epoch_ms: int, end_epoch_ms: int, directory: str, export_mode: str,
                 data: typing.Set[Name], limit_flavors: bool = False):
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))
        self.directory = directory
        self.export_mode = export_mode
        self.data = data
        self.limit_flavors = limit_flavors

    def export_file_name(self) -> typing.Optional[str]:
        station = None
        for sel in self.data:
            station = sel.station
            if station:
                break
        ts = time.gmtime(self.start_epoch)
        if station:
            station = station.lower()
            return f"{station}_{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.csv"
        else:
            return f"export_{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.csv"

    async def __call__(self) -> Export.Result:
        selections = list()
        for sel in self.data:
            arg = ''
            if len(sel.flavors) == 0:
                if self.limit_flavors:
                    arg = ':='
            else:
                for f in sel.flavors:
                    arg += f':={f}'
            arg = f'{sel.station}:{sel.archive}:{sel.variable}' + arg
            selections.append(arg)
        _LOGGER.debug(f"Starting data export for {self.start_epoch},{self.end_epoch} with {len(selections)} selections")

        target_file = self.export_file_name()
        if target_file:
            target_file = (Path(self.directory) / target_file).open('wb')
        else:
            target_file = asyncio.subprocess.DEVNULL
        exporter = await asyncio.create_subprocess_exec(_interface, 'export', self.export_mode,
                                                        str(self.start_epoch), str(self.end_epoch),
                                                        *selections,
                                                        stdout=target_file,
                                                        stdin=asyncio.subprocess.DEVNULL)
        await exporter.communicate()
        return Export.Result()


class DataExportList(ExportList):
    class Entry(ExportList.Entry):
        def __init__(self, key: str, display: str, data: typing.Callable[[str, int, int], Export],
                     time_limit_days: typing.Optional[int] = 366):
            super().__init__(key, display)
            self.data = data
            if time_limit_days:
                self.time_limit_ms: typing.Optional[int] = time_limit_days * 86400 * 1000
            else:
                self.time_limit_ms: typing.Optional[int] = None

        def __deepcopy__(self, memo):
            y = type(self)(self.key, self.display, self.data)
            y.time_limit_ms = self.time_limit_ms
            memo[id(self)] = y
            return y

    def __init__(self, exports: typing.Optional[typing.List["DataExportList.Entry"]] = None):
        super().__init__(exports)

    def create_export(self, station: str, export_key: str,
                      start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        for export in self.exports:
            if export.key == export_key:
                if export.time_limit_ms and (end_epoch_ms - start_epoch_ms) > export.time_limit_ms:
                    return None
                return export.data(station, start_epoch_ms, end_epoch_ms, directory)
        return None


def export_profile_lookup(station: str, mode_name: str, lookup) -> typing.Optional[DataExportList]:
    components = mode_name.split('-', 2)
    if len(components) != 2:
        return None
    profile = components[0]
    archive = components[1]

    return lookup.get(profile, {}).get(archive)


def export_get(station: str, mode_name: str, export_key: str,
               start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, profile_export)


def export_available(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, profile_export)


def export_profile_get(station: str, mode_name: str, export_key: str,
                       start_epoch_ms: int, end_epoch_ms: int, directory: str,
                       lookup: typing.Dict[str, typing.Dict[str, DataExportList]]) -> typing.Optional[DataExportList]:
    components = mode_name.split('-', 2)
    if len(components) != 2:
        return None
    profile = components[0]
    archive = components[1]

    result = lookup.get(profile)
    if not result:
        _LOGGER.debug(f"No information for profile in {mode_name}")
        return None
    result = result.get(archive)
    if not result:
        _LOGGER.debug(f"No information for archive in {mode_name}")
        return None
    return result.create_export(station, export_key, start_epoch_ms, end_epoch_ms, directory)


def detach(*profiles):
    result = dict()
    for profile in profiles:
        result.update(deepcopy(profile))
    return result


aerosol_export: typing.Dict[str, DataExportList] = {
    'raw': DataExportList([
        DataExportList.Entry('extensive', "Extensive", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'raw', 'T_S11'),
                Name(station, 'raw', 'P_S11'),
                Name(station, 'raw', 'U_S11'),
                Name(station, 'raw', 'BsB_S11'),
                Name(station, 'raw', 'BsG_S11'),
                Name(station, 'raw', 'BsR_S11'),
                Name(station, 'raw', 'BbsB_S11'),
                Name(station, 'raw', 'BbsG_S11'),
                Name(station, 'raw', 'BbsR_S11'),
                Name(station, 'raw', 'BaB_A11'),
                Name(station, 'raw', 'BaG_A11'),
                Name(station, 'raw', 'BaR_A11'),
                Name(station, 'raw', 'N_N71'),
                Name(station, 'raw', 'N_N61'),
            },
        )),
        DataExportList.Entry('scattering', "Scattering", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'raw', 'T_S11'),
                Name(station, 'raw', 'P_S11'),
                Name(station, 'raw', 'U_S11'),
                Name(station, 'raw', 'BsB_S11'),
                Name(station, 'raw', 'BsG_S11'),
                Name(station, 'raw', 'BsR_S11'),
                Name(station, 'raw', 'BbsB_S11'),
                Name(station, 'raw', 'BbsG_S11'),
                Name(station, 'raw', 'BbsR_S11'),
            },
        )),
        DataExportList.Entry('absorption', "Absorption", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'raw', 'Q_A11'),
                Name(station, 'raw', 'L_A11'),
                Name(station, 'raw', 'Fn_A11'),
                Name(station, 'raw', 'BaB_A11'),
                Name(station, 'raw', 'BaG_A11'),
                Name(station, 'raw', 'BaR_A11'),
                Name(station, 'raw', 'IrB_A11'),
                Name(station, 'raw', 'IrG_A11'),
                Name(station, 'raw', 'IrR_A11'),
            },
        )),
        DataExportList.Entry('counts', "Counts", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'N_N71'),
                Name(station, 'raw', 'N_N61'),
            },
        )),
        DataExportList.Entry('aethalometer', "Aethalometer", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
                [Name(station, 'raw', f'Ba{i + 1}_A81') for i in range(7)] +
                [Name(station, 'raw', f'X{i + 1}_A81') for i in range(7)] +
                [Name(station, 'raw', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
                [Name(station, 'raw', f'Ir{i + 1}_A81') for i in range(7)]
            ),
        )),
    ]),
    'clean': DataExportList([
        DataExportList.Entry('intensive', "Intensive", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'clean', 'N_XI'),
                Name(station, 'clean', 'BsB_XI'),
                Name(station, 'clean', 'BsG_XI'),
                Name(station, 'clean', 'BsR_XI'),
                Name(station, 'clean', 'BaB_XI'),
                Name(station, 'clean', 'BaG_XI'),
                Name(station, 'clean', 'BaR_XI'),
                Name(station, 'clean', 'ZSSAG_XI'),
                Name(station, 'clean', 'ZBfrG_XI'),
                Name(station, 'clean', 'ZAngBsG_XI'),
                Name(station, 'clean', 'ZRFEG_XI'),
                Name(station, 'clean', 'ZGG_XI'),
            },
        )),
        DataExportList.Entry('scattering', "Scattering", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'clean', 'T_S11'),
                Name(station, 'clean', 'P_S11'),
                Name(station, 'clean', 'U_S11'),
                Name(station, 'clean', 'BsB_S11'),
                Name(station, 'clean', 'BsG_S11'),
                Name(station, 'clean', 'BsR_S11'),
                Name(station, 'clean', 'BbsB_S11'),
                Name(station, 'clean', 'BbsG_S11'),
                Name(station, 'clean', 'BbsR_S11'),
            },
        )),
        DataExportList.Entry('absorption', "Absorption", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'basic', {
                Name(station, 'clean', 'Q_A11'),
                Name(station, 'clean', 'L_A11'),
                Name(station, 'clean', 'Fn_A11'),
                Name(station, 'clean', 'BaB_A11'),
                Name(station, 'clean', 'BaG_A11'),
                Name(station, 'clean', 'BaR_A11'),
                Name(station, 'clean', 'IrB_A11'),
                Name(station, 'clean', 'IrG_A11'),
                Name(station, 'clean', 'IrR_A11'),
            },
        )),
        DataExportList.Entry('counts', "Counts", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'clean', 'N_N71'),
                Name(station, 'clean', 'N_N61'),
            },
        )),
        DataExportList.Entry('aethalometer', "Aethalometer", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', set(
                [Name(station, 'clean', f'Ba{i + 1}_A81') for i in range(7)] +
                [Name(station, 'clean', f'X{i + 1}_A81') for i in range(7)] +
                [Name(station, 'clean', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
                [Name(station, 'clean', f'Ir{i + 1}_A81') for i in range(7)]
            ),
        )),
    ]),
    'avgh': DataExportList([
        DataExportList.Entry('intensive', "Intensive", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'average', {
                Name(station, 'avgh', 'N_XI'),
                Name(station, 'avgh', 'BsB_XI'),
                Name(station, 'avgh', 'BsG_XI'),
                Name(station, 'avgh', 'BsR_XI'),
                Name(station, 'avgh', 'BaB_XI'),
                Name(station, 'avgh', 'BaG_XI'),
                Name(station, 'avgh', 'BaR_XI'),
                Name(station, 'avgh', 'ZSSAG_XI'),
                Name(station, 'avgh', 'ZBfrG_XI'),
                Name(station, 'avgh', 'ZAngBsG_XI'),
                Name(station, 'avgh', 'ZRFEG_XI'),
                Name(station, 'avgh', 'ZGG_XI'),
            },
        ), time_limit_days=None),
        DataExportList.Entry('scattering', "Scattering", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'average', {
                Name(station, 'avgh', 'T_S11'),
                Name(station, 'avgh', 'P_S11'),
                Name(station, 'avgh', 'U_S11'),
                Name(station, 'avgh', 'BsB_S11'),
                Name(station, 'avgh', 'BsG_S11'),
                Name(station, 'avgh', 'BsR_S11'),
                Name(station, 'avgh', 'BbsB_S11'),
                Name(station, 'avgh', 'BbsG_S11'),
                Name(station, 'avgh', 'BbsR_S11'),
            },
        ), time_limit_days=None),
        DataExportList.Entry('absorption', "Absorption", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'average', {
                Name(station, 'avgh', 'Q_A11'),
                Name(station, 'avgh', 'L_A11'),
                Name(station, 'avgh', 'Fn_A11'),
                Name(station, 'avgh', 'BaB_A11'),
                Name(station, 'avgh', 'BaG_A11'),
                Name(station, 'avgh', 'BaR_A11'),
                Name(station, 'avgh', 'IrB_A11'),
                Name(station, 'avgh', 'IrG_A11'),
                Name(station, 'avgh', 'IrR_A11'),
            },
        ), time_limit_days=None),
        DataExportList.Entry('counts', "Counts", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'average', {
                Name(station, 'avgh', 'N_N71'),
                Name(station, 'avgh', 'N_N61'),
            },
        ), time_limit_days=None),
        DataExportList.Entry('aethalometer', "Aethalometer", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'average', set(
                [Name(station, 'avgh', f'Ba{i + 1}_A81') for i in range(7)] +
                [Name(station, 'avgh', f'X{i + 1}_A81') for i in range(7)] +
                [Name(station, 'avgh', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
                [Name(station, 'avgh', f'Ir{i + 1}_A81') for i in range(7)]
            ),
        ), time_limit_days=None),
    ]),
}

ozone_export: typing.Dict[str, DataExportList] = {
    'raw': DataExportList([
        DataExportList.Entry('basic', "Basic", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'raw', 'X_G81'),
                Name(station, 'raw', 'T1_G81'),
                Name(station, 'raw', 'T2_G81'),
                Name(station, 'raw', 'P_G81'),
                Name(station, 'raw', 'P1_G81'),                
                Name(station, 'raw', 'Q1_G81'),
                Name(station, 'raw', 'Q2_G81'),
                Name(station, 'raw', 'C1_G81'),
                Name(station, 'raw', 'C2_G81'),
                Name(station, 'raw', 'WS1_XM1'),
                Name(station, 'raw', 'WD1_XM1'),
            },
        )),
    ]),
    'clean': DataExportList([
        DataExportList.Entry('basic', "Basic", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'clean', 'X_G81'),
                Name(station, 'clean', 'T1_G81'),
                Name(station, 'clean', 'T2_G81'),
                Name(station, 'clean', 'P_G81'),
                Name(station, 'clean', 'P1_G81'),                
                Name(station, 'clean', 'Q1_G81'),
                Name(station, 'clean', 'Q2_G81'),
                Name(station, 'clean', 'C1_G81'),
                Name(station, 'clean', 'C2_G81'),
                Name(station, 'clean', 'WS1_XM1'),
                Name(station, 'clean', 'WD1_XM1'),
            },
        )),
    ]),
    'avgh': DataExportList([
        DataExportList.Entry('basic', "Basic", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
                Name(station, 'avgh', 'X_G81'),
                Name(station, 'avgh', 'T1_G81'),
                Name(station, 'avgh', 'T2_G81'),
                Name(station, 'avgh', 'P_G81'),
                Name(station, 'avgh', 'P1_G81'),                
                Name(station, 'avgh', 'Q1_G81'),
                Name(station, 'avgh', 'Q2_G81'),
                Name(station, 'avgh', 'C1_G81'),
                Name(station, 'avgh', 'C2_G81'),
                Name(station, 'avgh', 'WS1_XM1'),
                Name(station, 'avgh', 'WD1_XM1'),
            },
        )),
    ]),
}

met_export: typing.Dict[str, DataExportList] = {
    'raw': DataExportList([
        DataExportList.Entry('ambient', "Ambient", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplt', {
                Name(station, 'raw', 'WS1_XM1'), Name(station, 'raw', 'WD1_XM1'),
                Name(station, 'raw', 'WS2_XM1'), Name(station, 'raw', 'WD2_XM1'),
                Name(station, 'raw', 'WS3_XM1'), Name(station, 'raw', 'WD3_XM1'),
                Name(station, 'raw', 'T1_XM1'), Name(station, 'raw', 'U1_XM1'), Name(station, 'raw', 'TD1_XM1'),
                Name(station, 'raw', 'T2_XM1'), Name(station, 'raw', 'U2_XM1'), Name(station, 'raw', 'TD2_XM1'),
                Name(station, 'raw', 'T3_XM1'), Name(station, 'raw', 'U3_XM1'), Name(station, 'raw', 'TD3_XM1'),
                Name(station, 'raw', 'P_XM1'),
                Name(station, 'raw', 'WI_XM1'),
            },
        )),
    ]),
    'clean': DataExportList([
        DataExportList.Entry('ambient', "Ambient", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplt', {
                Name(station, 'clean', 'WS1_XM1'), Name(station, 'clean', 'WD1_XM1'),
                Name(station, 'clean', 'WS2_XM1'), Name(station, 'clean', 'WD2_XM1'),
                Name(station, 'clean', 'WS3_XM1'), Name(station, 'clean', 'WD3_XM1'),
                Name(station, 'clean', 'T1_XM1'), Name(station, 'clean', 'U1_XM1'), Name(station, 'clean', 'TD1_XM1'),
                Name(station, 'clean', 'T2_XM1'), Name(station, 'clean', 'U2_XM1'), Name(station, 'clean', 'TD2_XM1'),
                Name(station, 'clean', 'T3_XM1'), Name(station, 'clean', 'U3_XM1'), Name(station, 'clean', 'TD3_XM1'),
                Name(station, 'clean', 'P_XM1'),
                Name(station, 'clean', 'WI_XM1'),
            },
        )),
    ]),
    'avgh': DataExportList([
        DataExportList.Entry('ambient', "Ambient", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
            start_epoch_ms, end_epoch_ms, directory, 'unsplt', {
                Name(station, 'avgh', 'WS1_XM1'), Name(station, 'avgh', 'WD1_XM1'),
                Name(station, 'avgh', 'WS2_XM1'), Name(station, 'avgh', 'WD2_XM1'),
                Name(station, 'avgh', 'WS3_XM1'), Name(station, 'avgh', 'WD3_XM1'),
                Name(station, 'avgh', 'T1_XM1'), Name(station, 'avgh', 'U1_XM1'), Name(station, 'avgh', 'TD1_XM1'),
                Name(station, 'avgh', 'T2_XM1'), Name(station, 'avgh', 'U2_XM1'), Name(station, 'avgh', 'TD2_XM1'),
                Name(station, 'avgh', 'T3_XM1'), Name(station, 'avgh', 'U3_XM1'), Name(station, 'avgh', 'TD3_XM1'),
                Name(station, 'avgh', 'P_XM1'),
                Name(station, 'avgh', 'WI_XM1'),
            },
        )),
    ]),
}

profile_export: typing.Dict[str, typing.Dict[str, DataExportList]] = {
    'aerosol': aerosol_export,
    'ozone': ozone_export,
    'met': met_export,    
}


acquisition_translator = AcquisitionTranslator(interfaces=[
    AcquisitionTranslator.Component('acquire_ad_cpcmagic200', 'admagiccpc', variable_map={
        AcquisitionTranslator.Variable('N'): 'N',
        AcquisitionTranslator.Variable('C1'): 'Clower',
        AcquisitionTranslator.Variable('C2'): 'Cupper',
        AcquisitionTranslator.Variable('Q'): 'Q',
        AcquisitionTranslator.Variable('ZQ'): 'Qinstrument',
        AcquisitionTranslator.Variable('P'): 'P',
        AcquisitionTranslator.Variable('Pd'): 'Pd',
        AcquisitionTranslator.Variable('V'): 'V',
        AcquisitionTranslator.Variable('Tu'): 'Tinlet',
        AcquisitionTranslator.Variable('T1'): 'Tconditioner',
        AcquisitionTranslator.Variable('T2'): 'Tinitiator',
        AcquisitionTranslator.Variable('T3'): 'Tmoderator',
        AcquisitionTranslator.Variable('T4'): 'Toptics',
        AcquisitionTranslator.Variable('T5'): 'Theatsink',
        AcquisitionTranslator.Variable('T6'): 'Tpcb',
        AcquisitionTranslator.Variable('T7'): 'Tcabinet',
        AcquisitionTranslator.Variable('Uu'): 'Uinlet',
        AcquisitionTranslator.Variable('TDu'): 'TDinlet',
    }, flags_notifications={
        'ConditionerTemperatureOutOfRange': 'conditioner_temperature_out_of_range',
        'InitiatorTemperatureOutOfRange': 'initiator_temperature_out_of_range',
        'ModeratorTemperatureOutOfRange': 'moderator_temperature_out_of_range',
        'OpticsTemperatureOutOfRange': 'optics_temperature_out_of_range',
        'LaserOff': 'laser_off',
        'PumpOff': 'pump_off',
        'RHDataStale': 'rh_data_stale',
        'I2CCommunicationFailure': 'i2c_communication_error',
        'RHSensorError': 'rh_sensor_error',
        'Overheat': 'overheat',
        'ModeratorInAbsoluteMode': 'moderator_in_absolute_mode',
        'WaterPumpActivated': 'water_pump_activated',
        'InvalidFlashRecord': 'invalid_flash_record',
        'FlashFull': 'flash_full',
        'FRAMDataInvalid': 'fram_data_invalid',
    }, flags_set_warning={
        'ConditionerTemperatureOutOfRange',
        'InitiatorTemperatureOutOfRange',
        'ModeratorTemperatureOutOfRange',
        'OpticsTemperatureOutOfRange',
        'LaserOff',
        'PumpOff',
        'RHSensorError',
        'Overheat',
    }),

    AcquisitionTranslator.AnalogInputOutputDigital('acquire_azonix_umac1050', 'azonixumac1050', variable_map={
        AcquisitionTranslator.Variable('T'): 'T',
        AcquisitionTranslator.Variable('V'): 'V',
        AcquisitionTranslator.Variable('ZINPUTS'): 'raw',
        AcquisitionTranslator.Variable('ZOUTPUTS'): 'output',
        AcquisitionTranslator.Variable('ZDIGITAL'): 'digital',
    }),

    AcquisitionTranslator.Component('acquire_bmi_cpc1710', 'bmi1710cpc', variable_map={
        AcquisitionTranslator.Variable('N'): 'N',
        AcquisitionTranslator.Variable('C'): 'C',
        AcquisitionTranslator.Variable('Tu'): 'Tinlet',
        AcquisitionTranslator.Variable('T1'): 'Tsaturatorbottom',
        AcquisitionTranslator.Variable('T2'): 'Tsaturatortop',
        AcquisitionTranslator.Variable('T3'): 'Tcondenser',
        AcquisitionTranslator.Variable('T4'): 'Toptics',
        AcquisitionTranslator.Variable('Q1'): 'Qsample',
        AcquisitionTranslator.Variable('Q2'): 'Qsaturator',
        AcquisitionTranslator.Variable('PCT1'): 'PCTsaturatorbottom',
        AcquisitionTranslator.Variable('PCT2'): 'PCTsaturatortop',
        AcquisitionTranslator.Variable('PCT3'): 'PCTcondenser',
        AcquisitionTranslator.Variable('PCT4'): 'PCToptics',
        AcquisitionTranslator.Variable('PCT5'): 'PCTsaturatorpump',
    }, flags_notifications={
        'EEPROMError': 'eeprom_error',
        'ConfigurationError': 'configuration_error',
        'RTCReset': 'rtc_reset',
        'RTCError': 'rtc_error',
        'SDCardError': 'sdcard_error',
        'SDCardFormatError': 'sdcard_format_error',
        'SDCardFull': 'sdcard_full',
        'SaturatorPumpWarning': 'saturator_pump_warning',
        'LiquidLow': 'liquid_low',
        'TemperatureControlError': 'temperature_control_error',
        'Overheating': 'overheating',
        'OpticsThermistorError': 'optics_thermistor_error',
        'CondenserThermistorError': 'condenser_thermistor_error',
        'SaturatorTopThermistorError': 'saturator_top_thermistor_error',
        'SaturatorBottomThermistorError': 'saturator_bottom_thermistor_error',
        'InletThermistorError': 'inlet_thermistor_error',
    }, flags_set_warning={
        'EEPROMError',
        'ConfigurationError',
        'SaturatorPumpWarning',
        'LiquidLow',
        'TemperatureControlError',
        'Overheating',
        'OpticsThermistorError',
        'CondenserThermistorError',
        'SaturatorTopThermistorError',
        'SaturatorBottomThermistorError',
        'InletThermistorError',
    }),

    AcquisitionTranslator.Component('acquire_bmi_cpc1720', 'bmi1720cpc', variable_map={
        AcquisitionTranslator.Variable('N'): 'N',
        AcquisitionTranslator.Variable('C'): 'C',
        AcquisitionTranslator.Variable('Tu'): 'Tinlet',
        AcquisitionTranslator.Variable('T1'): 'Tsaturatorbottom',
        AcquisitionTranslator.Variable('T2'): 'Tsaturatortop',
        AcquisitionTranslator.Variable('T3'): 'Tcondenser',
        AcquisitionTranslator.Variable('T4'): 'Toptics',
        AcquisitionTranslator.Variable('Q1'): 'Qsample',
        AcquisitionTranslator.Variable('Q2'): 'Qsaturator',
        AcquisitionTranslator.Variable('P'): 'P',
        AcquisitionTranslator.Variable('PCT1'): 'PCTsaturatorbottom',
        AcquisitionTranslator.Variable('PCT2'): 'PCTsaturatortop',
        AcquisitionTranslator.Variable('PCT3'): 'PCTcondenser',
        AcquisitionTranslator.Variable('PCT4'): 'PCToptics',
        AcquisitionTranslator.Variable('PCT5'): 'PCTsaturatorpump',
        AcquisitionTranslator.Variable('ZP'): 'RAWP',
        AcquisitionTranslator.Variable('ZQ1'): 'RAWQsample',
        AcquisitionTranslator.Variable('ZQ2'): 'RAWQsaturator',
    }, flags_notifications={
        'EEPROMError': 'eeprom_error',
        'ConfigurationError': 'configuration_error',
        'RTCReset': 'rtc_reset',
        'RTCError': 'rtc_error',
        'SDCardError': 'sdcard_error',
        'SDCardFormatError': 'sdcard_format_error',
        'SDCardFull': 'sdcard_full',
        'SaturatorPumpWarning': 'saturator_pump_warning',
        'LiquidLow': 'liquid_low',
        'TemperatureControlError': 'temperature_control_error',
        'Overheating': 'overheating',
        'OpticsThermistorError': 'optics_thermistor_error',
        'CondenserThermistorError': 'condenser_thermistor_error',
        'SaturatorTopThermistorError': 'saturator_top_thermistor_error',
        'SaturatorBottomThermistorError': 'saturator_bottom_thermistor_error',
        'InletThermistorError': 'inlet_thermistor_error',
    }, flags_set_warning={
        'EEPROMError',
        'ConfigurationError',
        'SaturatorPumpWarning',
        'LiquidLow',
        'TemperatureControlError',
        'Overheating',
        'OpticsThermistorError',
        'CondenserThermistorError',
        'SaturatorTopThermistorError',
        'SaturatorBottomThermistorError',
        'InletThermistorError',
    }),

    AcquisitionTranslator.AnalogInputOutputDigital('acquire_campbell_cr1000gmd', 'campbellcr1000gmd', variable_map={
        AcquisitionTranslator.Variable('T'): 'T',
        AcquisitionTranslator.Variable('V'): 'V',
        AcquisitionTranslator.Variable('ZINPUTS'): 'raw',
        AcquisitionTranslator.Variable('ZOUTPUTS'): 'output',
        AcquisitionTranslator.Variable('ZDIGITAL'): 'digital',
    }),

    AcquisitionTranslator.Component('acquire_csd_pops', 'csdpops', variable_map={
        AcquisitionTranslator.Variable('Nb'): 'dN',
        AcquisitionTranslator.Variable('Ns'): 'Dp',
        AcquisitionTranslator.Variable('N'): 'N',
        AcquisitionTranslator.Variable('C'): 'C',
        AcquisitionTranslator.Variable('Q'): 'Q',
        AcquisitionTranslator.Variable('P'): 'P',
        AcquisitionTranslator.Variable('T1'): 'Tpressure',
        AcquisitionTranslator.Variable('T2'): 'Tlaser',
        AcquisitionTranslator.Variable('T3'): 'Tinternal',
        AcquisitionTranslator.Variable('I'): 'baseline',
        AcquisitionTranslator.Variable('Ig'): 'baseline_stddev',
        AcquisitionTranslator.Variable('Im'): 'baseline_threshold',
        AcquisitionTranslator.Variable('Igm'): 'baseline_stddevmax',
        AcquisitionTranslator.Variable('A'): 'Alaser',
        AcquisitionTranslator.Variable('V'): 'Vsupply',
        AcquisitionTranslator.Variable('ZMEANWIDTH'): 'peak_width',
        AcquisitionTranslator.Variable('ZLASERMON'): 'laser_monitor',
        AcquisitionTranslator.Variable('ZLASERFB'): 'laser_feedback',
        AcquisitionTranslator.Variable('ZPUMPFB'): 'pump_feedback',
        AcquisitionTranslator.Variable('ZPUMPTIME'): 'pump_on_time',
    }, flags_notifications={
        'TooManyParticles': 'too_many_particles',
        'TimingUncertainty': 'timing_uncertainty',
    }, flags_set_warning={
        'TooManyParticles', 'TimingUncertainty',
    }),

    AcquisitionTranslator.Nephelometer('acquire_ecotech_nephaurora', 'ecotechnephelometer', variable_map={
        AcquisitionTranslator.Variable('ZBsB'): 'BsB',
        AcquisitionTranslator.Variable('ZBsG'): 'BsG',
        AcquisitionTranslator.Variable('ZBsR'): 'BsR',
        AcquisitionTranslator.Variable('ZBbsB'): 'BbsB',
        AcquisitionTranslator.Variable('ZBbsG'): 'BbsG',
        AcquisitionTranslator.Variable('ZBbsR'): 'BbsR',
        AcquisitionTranslator.Variable('BswB'): 'BswB',
        AcquisitionTranslator.Variable('BswG'): 'BswG',
        AcquisitionTranslator.Variable('BswR'): 'BswR',
        AcquisitionTranslator.Variable('BbswB'): 'BbswB',
        AcquisitionTranslator.Variable('BbswG'): 'BbswG',
        AcquisitionTranslator.Variable('BbswR'): 'BbswR',
        AcquisitionTranslator.Variable('BswdB'): 'BswdB',
        AcquisitionTranslator.Variable('BswdG'): 'BswdG',
        AcquisitionTranslator.Variable('BswdR'): 'BswdR',
        AcquisitionTranslator.Variable('BbswdB'): 'BbswdB',
        AcquisitionTranslator.Variable('BbswdG'): 'BbswdG',
        AcquisitionTranslator.Variable('BbswdR'): 'BbswdR',
        AcquisitionTranslator.Variable('CsB'): 'CsB',
        AcquisitionTranslator.Variable('CsG'): 'CsG',
        AcquisitionTranslator.Variable('CsR'): 'CsR',
        AcquisitionTranslator.Variable('CbsB'): 'CbsB',
        AcquisitionTranslator.Variable('CbsG'): 'CbsG',
        AcquisitionTranslator.Variable('CbsR'): 'CbsR',
        AcquisitionTranslator.Variable('CfB'): 'CfB',
        AcquisitionTranslator.Variable('CfG'): 'CfG',
        AcquisitionTranslator.Variable('CfR'): 'CfR',
        AcquisitionTranslator.Variable('CrB'): 'CrB',
        AcquisitionTranslator.Variable('CrG'): 'CrG',
        AcquisitionTranslator.Variable('CrR'): 'CrR',
        AcquisitionTranslator.Variable('CbrB'): 'CbrB',
        AcquisitionTranslator.Variable('CbrG'): 'CbrG',
        AcquisitionTranslator.Variable('CbrR'): 'CbrR',
        AcquisitionTranslator.Variable('Cd'): 'Cd',
        AcquisitionTranslator.Variable('T'): 'Tsample',
        AcquisitionTranslator.Variable('U'): 'Usample',
        AcquisitionTranslator.Variable('P'): 'Psample',
        AcquisitionTranslator.Variable('Tx'): 'Tcell',
    }, flags_notifications={
        'InconsistentZero': 'inconsistent_zero',
        'BackscatterFault': 'backscatter_fault',
        'BackscatterDigitalFault': 'backscatter_digital_fault',
        'ShutterFault': 'shutter_fault',
        'LightSourceFault': 'light_source_fault',
        'PressureFault': 'pressure_sensor_fault',
        'EnclosureTemperatureFault': 'enclosure_temperature_fault',
        'SampleTemperatureFault': 'sample_temperature_fault',
        'RHFault': 'rh_fault',
        'PMTFault': 'pmt_fault',
        'WarmupFault': 'warmup_fault',
        'BackscattterHighWarning': 'backscatter_high_warning',
        'SystemFault': 'system_fault',
    }, flags_set_warning={
        'InconsistentZero',
        'BackscatterFault',
        'BackscatterDigitalFault',
        'ShutterFault',
        'LightSourceFault',
        'PressureFault',
        'EnclosureTemperatureFault',
        'SampleTemperatureFault',
        'RHFault',
        'PMTFault',
        'WarmupFault',
        'BackscattterHighWarning',
        'SystemFault',
    }, zstate_notifications={
        'Blank': 'blank',
        'Zero': 'zero',
        'Spancheck': 'spancheck',
        'Calibration': 'calibration',
        'Warmup': 'warmup',
    }, zstate_set_warning={
        'Warmup',
    }, command_map={
        'start_zero': 'StartZero',
        'start_spancheck': 'StartSpancheck',
        'stop_spancheck': 'StopSpancheck',
        'apply_spancheck_calibration': 'ApplySpancheckCalibration',
        'reboot': 'Reboot',
    }),

    AcquisitionTranslator.Component('acquire_gmd_cpcpulse', 'tsi3760cpc', variable_map={
        AcquisitionTranslator.Variable('N'): 'N',
        AcquisitionTranslator.Variable('C'): 'C',
    }),

    AcquisitionTranslator.Component('acquire_gmd_clap3w', 'clap', variable_map={
        AcquisitionTranslator.Variable('BaB'): 'BaB',
        AcquisitionTranslator.Variable('BaG'): 'BaG',
        AcquisitionTranslator.Variable('BaR'): 'BaR',
        AcquisitionTranslator.Variable('IrB'): 'IrB',
        AcquisitionTranslator.Variable('IrG'): 'IrG',
        AcquisitionTranslator.Variable('IrR'): 'IrR',
        AcquisitionTranslator.Variable('IfB'): 'IfB',
        AcquisitionTranslator.Variable('IfG'): 'IfG',
        AcquisitionTranslator.Variable('IfR'): 'IfR',
        AcquisitionTranslator.Variable('IpB'): 'IpB',
        AcquisitionTranslator.Variable('IpG'): 'IpG',
        AcquisitionTranslator.Variable('IpR'): 'IpR',
        AcquisitionTranslator.Variable('Q'): 'Q',
        AcquisitionTranslator.Variable('VQ'): 'Vflow',
        AcquisitionTranslator.Variable('T1'): 'Tsample',
        AcquisitionTranslator.Variable('T2'): 'Tcase',
        AcquisitionTranslator.Variable('Fn'): 'Fn',
    }, flags_notifications={
        'FlowError': 'flow_error',
        'LampError': 'led_error',
        'TemperatureOutOfRange': 'temperature_out_of_range',
        'CaseTemperatureUnstable': 'case_temperature_control_error',
        'NonWhiteFilter': 'filter_was_not_white',
    }, flags_set_warning={
        'FlowError', 'LampError', 'TemperatureOutOfRange', 'NonWhiteFilter',
    }, zstate_notifications={
        'Normalize': 'wait_spot_stability',
        'SpotAdvance': 'spot_advancing',
        'FilterBaselineStart': 'filter_baseline',
        'FilterBaseline': 'filter_baseline',
        'FilterChangeStart': 'filter_change',
        'FilterChange': 'filter_change',
        'WhiteFilterBaselineStart': 'filter_baseline',
        'WhiteFilterBaseline': 'filter_baseline',
        'WhiteFilterChangeStart': 'white_filter_change',
        'WhiteFilterChange': 'white_filter_change',
        'BypassedNormalize': 'bypass_wait_spot_stability',
        'BypassedFilterBaselineStart': 'bypass_filter_baseline',
        'BypassedFilterBaseline': 'bypass_filter_baseline',
        'BypassedFilterChangeStart': 'filter_change',
        'BypassedFilterChange': 'filter_change',
        'BypassedWhiteFilterBaselineStart': 'bypass_filter_baseline',
        'BypassedWhiteFilterBaseline': 'bypass_filter_baseline',
        'BypassedWhiteFilterChangeStart': 'white_filter_change',
        'BypassedWhiteFilterChange': 'white_filter_change',
        'RequireFilterChange': 'need_filter_change',
        'RequireWhiteFilterChange': 'need_white_filter_change',
        'BypassedRequireFilterChange': 'need_filter_change',
        'BypassedRequireWhiteFilterChange': 'need_white_filter_change',
    }, zstate_set_warning={
        'FilterChangeStart', 'FilterChange', 'WhiteFilterChangeStart', 'WhiteFilterChange',
        'BypassedFilterChangeStart', 'BypassedFilterChange',
        'BypassedWhiteFilterChangeStart', 'BypassedWhiteFilterChange',
        'RequireFilterChange', 'RequireWhiteFilterChange',
        'BypassedRequireFilterChange', 'BypassedRequireWhiteFilterChange'
    }, command_map={
        'spot_advance': 'AdvanceSpot',
        'filter_change_start': 'StartFilterChange',
        'filter_change_end': 'StopFilterChange',
        'white_filter_change': 'StartWhiteFilterChange',
    }),

    AcquisitionTranslator.Component('acquire_grimm_opc110x', 'grimm110xopc', variable_map={
        AcquisitionTranslator.Variable('Nb'): 'dN',
        AcquisitionTranslator.Variable('Ns'): 'Dp',
        AcquisitionTranslator.Variable('N'): 'N',
        AcquisitionTranslator.Variable('Q'): 'Q',
        AcquisitionTranslator.Variable('ZXPM1'): 'X1',
        AcquisitionTranslator.Variable('ZXPM10'): 'X10',
        AcquisitionTranslator.Variable('ZXPM25'): 'X25',
        AcquisitionTranslator.Variable('PCT1'): 'PCTbattery',
        AcquisitionTranslator.Variable('PCT2'): 'PCTpump',
    }, zstate_notifications={
        'SelfTestFault': 'self_test_failure',
        'MemoryCardFault': 'memory_card_fault',
        'NozzleFault': 'nozzle_fault',
        'BatteryDrained': 'battery_drained',
        'BatteryLow': 'battery_low',
        'PumpCurrentHigh': 'pump_current_high',
        'FlowError': 'flow_error',
        'PumpLow': 'pump_low',
        'PumpHigh': 'pump_high',
    }, zstate_set_warning={
        'SelfTestFault',
        'NozzleFault',
        'PumpCurrentHigh',
        'FlowError',
        'PumpLow',
        'PumpHigh',
    }),

    AcquisitionTranslator.LovePID('acquire_love_pid', 'lovepid'),

    AcquisitionTranslator.Component('acquire_magee_aethalometer33', 'mageeae33', variable_map=dict(
        [(AcquisitionTranslator.Variable(f'X{i+1}'), f'X{i+1}') for i in range(7)] +
        [(AcquisitionTranslator.Variable(f'ZFACTOR{i+1}'), f'k{i+1}') for i in range(7)] +
        [(AcquisitionTranslator.Variable(f'Ba{i+1}'), f'Ba{i+1}') for i in range(7)] +
        [(AcquisitionTranslator.Variable(f'Bas{i+1}'), f'Bas{i+1}') for i in range(7)] +
        [(AcquisitionTranslator.Variable(f'Ir{i+1}'), f'Ir{i+1}') for i in range(7)] +
        [(AcquisitionTranslator.Variable(f'Irs{i+1}'), f'Irs{i+1}') for i in range(7)] +
        [(AcquisitionTranslator.Variable(f'If{i+1}'), f'If{i+1}') for i in range(7)] +
        [(AcquisitionTranslator.Variable(f'Ip{i+1}'), f'Ip{i+1}') for i in range(7)] +
        [(AcquisitionTranslator.Variable(f'Ips{i+1}'), f'Ips{i+1}') for i in range(7)] +
        [
            (AcquisitionTranslator.Variable('Q1'), 'Q1'),
            (AcquisitionTranslator.Variable('Q2'), 'Q2'),
            (AcquisitionTranslator.Variable('T1'), 'Tcontroller'),
            (AcquisitionTranslator.Variable('T2'), 'Tsupply'),
            (AcquisitionTranslator.Variable('T3'), 'Tled'),
        ]
    ), flags_notifications={
        'SpotAdvanced': 'spot_advancing',
        'NotMeasuring': 'not_measuring',
        'Calibrating': 'calibrating',
        'Stopped': 'stopped',
        'FlowOutOfRange': 'flow_out_of_range',
        'FlowCheckHistory': 'flow_check_history',
        'LEDCalibration': 'led_calibration',
        'LEDCalibrationError': 'led_calibration_error',
        'LEDError': 'led_error',
        'ChamberError': 'chamber_error',
        'TapeLow': 'tape_low',
        'TapeCritical': 'tape_critical',
        'TapeError': 'tape_error',
        'StabilityTest': 'stability_test',
        'CleanAirTest': 'clear_air_test',
        'ChangeTapeTest': 'change_tape_test',
        'ControllerNotReady': 'controller_not_ready',
        'ControllerBusy': 'controller_busy',
        'DetectorInitializationError': 'detector_initialization_error',
        'DetectorStopped': 'detector_stopped',
        'DetectorLEDCalibration': 'detector_led_calibration',
        'DetectorFastLEDCalibration': 'detector_fast_led_calibration',
        'DetectorReadNDF0': 'detector_read_ndf0',
        'DetectorReadNDF1': 'detector_read_ndf1',
        'DetectorReadNDF2': 'detector_read_ndf2',
        'DetectorReadNDF3': 'detector_read_ndf3',
        'DetectorReadNDFError': 'detector_read_ndf_error',
    }, flags_set_warning={
        'NotMeasuring', 'Calibrating', 'Stopped', 'FlowOutOfRange', 'LEDCalibration', 'LEDCalibrationError',
        'LEDError', 'ChamberError', 'TapeError', 'ControllerNotReady', 'ControllerBusy', 'DetectorInitializationError',
        'DetectorStopped', 'DetectorLEDCalibration', 'DetectorFastLEDCalibration', 'DetectorReadNDF0',
        'DetectorReadNDF1', 'DetectorReadNDF2', 'DetectorReadNDF3', 'DetectorReadNDFError',
    }, command_map={
        'spot_advance': 'AdvanceSpot',
    }),

    AcquisitionTranslator.Component('acquire_purpleair_pa2', 'purpleair', variable_map={
        AcquisitionTranslator.Variable('Ipa'): 'IBsa',
        AcquisitionTranslator.Variable('Ipb'): 'IBsb',
        AcquisitionTranslator.Variable('ZXa'): 'Xa',
        AcquisitionTranslator.Variable('ZXb'): 'Xb',
        AcquisitionTranslator.Variable('T'): 'T',
        AcquisitionTranslator.Variable('U'): 'U',
        AcquisitionTranslator.Variable('P'): 'P',
    }),

    AcquisitionTranslator.Component('acquire_teledyne_t640', 'teledynet640', variable_map={
        AcquisitionTranslator.Variable('ZXPM1'): 'X1',
        AcquisitionTranslator.Variable('ZXPM10'): 'X10',
        AcquisitionTranslator.Variable('ZXPM25'): 'X25',
        AcquisitionTranslator.Variable('U1'): 'Usample',
        AcquisitionTranslator.Variable('T1'): 'Tsample',
        AcquisitionTranslator.Variable('T2'): 'Tambient',
        AcquisitionTranslator.Variable('T3'): 'Tasc',
        AcquisitionTranslator.Variable('T4'): 'Tled',
        AcquisitionTranslator.Variable('T5'): 'Tbox',
        AcquisitionTranslator.Variable('P'): 'Pambient',
        AcquisitionTranslator.Variable('Q1'): 'Qsample',
        AcquisitionTranslator.Variable('Q2'): 'Qbypass',
        AcquisitionTranslator.Variable('ZSPAN'): 'spandev',
        AcquisitionTranslator.Variable('PCT1'): 'PCTpump',
        AcquisitionTranslator.Variable('PCT2'): 'PCTvalve',
        AcquisitionTranslator.Variable('PCT3'): 'PCTact',
    }, flags_notifications={
        'BoxTemperatureWarning': 'box_temperature_warning',
        'FlowAlarm': 'flow_alarm',
        'SystemFaultWarning': 'system_fault_warning',
        'SystemResetWarning': 'system_reset_warning',
        'TemperatureAlarm': 'temperature_alarm',
        'SystemServiceWarning': 'system_service_warning',
        'OPCInstrumentWarning': 'opc_instrument_warning',
        'SampleTemperatureWarning': 'sample_temperature_warning',
    }, flags_set_warning={
        'BoxTemperatureWarning', 'FlowAlarm', 'SystemFaultWarning', 'TemperatureAlarm', 'SystemServiceWarning',
        'OPCInstrumentWarning', 'SampleTemperatureWarning',
    }),

    AcquisitionTranslator.Component('acquire_thermo_ozone49', 'thermo49', variable_map={
        AcquisitionTranslator.Variable('ZX'): 'X',
        AcquisitionTranslator.Variable('T1'): 'Tsample',
        AcquisitionTranslator.Variable('T2'): 'Tlamp',
        AcquisitionTranslator.Variable('T3'): 'Tozonator',
        AcquisitionTranslator.Variable('P'): 'Psample',
        AcquisitionTranslator.Variable('ZINSTFLAGS'): 'bitflags',
        AcquisitionTranslator.Variable('Q1'): 'Qa',
        AcquisitionTranslator.Variable('Q2'): 'Qb',
        AcquisitionTranslator.Variable('Q3'): 'Qozonator',
        AcquisitionTranslator.Variable('C1'): 'Ca',
        AcquisitionTranslator.Variable('C2'): 'Cb',
    }, flags_notifications={
        'SampleTemperatureLowAlarm': 'alarm_sample_temperature_low',
        'SampleTemperatureHighAlarm': 'alarm_sample_temperature_high',
        'LampTemperatureLowAlarm': 'alarm_lamp_temperature_low',
        'LampTemperatureHighAlarm': 'alarm_lamp_temperature_high',
        'OzonatorTemperatureLowAlarm': 'alarm_ozonator_temperature_low',
        'OzonatorTemperatureHighAlarm': 'alarm_ozonator_temperature_high',
        'PressureLowAlarm': 'alarm_pressure_low',
        'PressureHighAlarm': 'alarm_pressure_high',
        'FlowALowAlarm': 'alarm_flow_a_low',
        'FlowAHighAlarm': 'alarm_flow_a_high',
        'FlowBLowAlarm': 'alarm_flow_b_low',
        'FlowBHighAlarm': 'alarm_flow_b_high',
        'IntensityALowAlarm': 'alarm_intensity_a_low',
        'IntensityAHighAlarm': 'alarm_intensity_a_high',
        'IntensityBLowAlarm': 'alarm_intensity_b_low',
        'IntensityBHighAlarm': 'alarm_intensity_b_high',
        'OzoneLowAlarm': 'alarm_ozone_low',
        'OzoneHighAlarm': 'alarm_ozone_high',
    }),

    AcquisitionTranslator.Component('acquire_thermo_ozone49iq', 'thermo49iq', variable_map={
        AcquisitionTranslator.Variable('X'): 'X',
        AcquisitionTranslator.Variable('T1'): 'Tsample',
        AcquisitionTranslator.Variable('T2'): 'Tlamp',
        AcquisitionTranslator.Variable('P1'): 'Psample',
        AcquisitionTranslator.Variable('P2'): 'Ppump',
        AcquisitionTranslator.Variable('Q'): 'Q',
        AcquisitionTranslator.Variable('C1'): 'Ca',
        AcquisitionTranslator.Variable('C1g'): 'Cag',
        AcquisitionTranslator.Variable('C2'): 'Cb',
        AcquisitionTranslator.Variable('C2g'): 'Cbg',
        AcquisitionTranslator.Variable('VA1'): 'Alamp',
        AcquisitionTranslator.Variable('VA2'): 'Aheater',
    }, flags_notifications={
        'IntensityAHighAlarm': 'alarm_intensity_a_high',
        'IntensityBHighAlarm': 'alarm_intensity_b_high',
        'LampTemperatureShortAlarm': 'lamp_temperature_short',
        'LampTemperatureOpenAlarm': 'lamp_temperature_open',
        'SampleTemperatureShortAlarm': 'sample_temperature_short',
        'SampleTemperatureOpenAlarm': 'sample_temperature_open',
        'LampConnectionAlarm': 'lamp_connection_alarm',
        'LampShortAlarm': 'lamp_short',
        'CommunicationsAlarm': 'communications_alarm',
        'PowerSupplyAlarm': 'power_supply_alarm',
        'LampCurrentAlarm': 'lamp_current_alarm',
        'LampTemperatureAlarm': 'lamp_temperature_alarm',
        'SampleTemperatureAlarm': 'sample_temperature_alarm',
    }, flags_set_warning={
        'LampTemperatureShortAlarm',
        'LampTemperatureOpenAlarm',
        'SampleTemperatureShortAlarm',
        'SampleTemperatureOpenAlarm',
        'LampConnectionAlarm',
        'LampShortAlarm',
        'CommunicationsAlarm',
        'PowerSupplyAlarm',
        'LampCurrentAlarm',
    }),

    AcquisitionTranslator.Component('acquire_tsi_cpc377x', 'tsi377xcpc', variable_map={
        AcquisitionTranslator.Variable('N'): 'N',
        AcquisitionTranslator.Variable('C'): 'C',
        AcquisitionTranslator.Variable('T1'): 'Tsaturator',
        AcquisitionTranslator.Variable('T2'): 'Tcondenser',
        AcquisitionTranslator.Variable('T3'): 'Toptics',
        AcquisitionTranslator.Variable('T4'): 'Tcabinet',
        AcquisitionTranslator.Variable('Q'): 'Qsample',
        AcquisitionTranslator.Variable('Qu'): 'Qinlet',
        AcquisitionTranslator.Variable('P'): 'P',
        AcquisitionTranslator.Variable('Pd1'): 'PDnozzle',
        AcquisitionTranslator.Variable('Pd2'): 'PDorifice',
        AcquisitionTranslator.Variable('A'): 'Alaser',
        AcquisitionTranslator.Variable('ZND'): 'Ndisplay',
        AcquisitionTranslator.Variable('ZQ'): 'Qdisplay',
        AcquisitionTranslator.Variable('ZLIQUID'): 'liquid_level',
    }, flags_notifications={
        'SaturatorTemperatureError': 'saturator_temperature_out_of_range',
        'CondenserTemperatureError': 'condenser_temperature_out_of_range',
        'OpticsTemperatureError': 'optics_temperature_error',
        'InletFlowError': 'inlet_flow_error',
        'SampleFlowError': 'sample_flow_error',
        'LaserPowerError': 'laser_power_error',
        'LiquidLow': 'liquid_low',
        'ConcentrationOutOfRange': 'concentration_out_of_range',
    }, flags_set_warning={
        'SaturatorTemperatureError',
        'CondenserTemperatureError',
        'InletFlowError',
        'SampleFlowError',
        'LaserPowerError',
        'LiquidLow',
    }),

    AcquisitionTranslator.Component('acquire_tsi_cpc3010', 'tsi3010cpc', variable_map={
        AcquisitionTranslator.Variable('N'): 'N',
        AcquisitionTranslator.Variable('C'): 'C',
        AcquisitionTranslator.Variable('T1'): 'Tsaturator',
        AcquisitionTranslator.Variable('T2'): 'Tcondenser',
    }, flags_notifications={
        'LiquidLow': 'liquid_low',
        'LowVacuum': 'vacuum_low',
        'InstrumentNotReady': 'not_ready',
    }, flags_set_warning={
        'LiquidLow',
        'LowVacuum',
        'InstrumentNotReady',
    }, command_map={
        'fill': 'Fill',
    }),

    AcquisitionTranslator.Component('acquire_tsi_mfm4xxx', 'tsimfm', variable_map={
        AcquisitionTranslator.Variable('Q'): 'Q',
        AcquisitionTranslator.Variable('T'): 'T',
        AcquisitionTranslator.Variable('U'): 'U',
        AcquisitionTranslator.Variable('P'): 'P',
    }),

    AcquisitionTranslator.TSI3563Nephelometer('acquire_tsi_neph3563', 'tsi3563nephelometer', variable_map={
        AcquisitionTranslator.Variable('ZBsB'): 'BsB',
        AcquisitionTranslator.Variable('ZBsG'): 'BsG',
        AcquisitionTranslator.Variable('ZBsR'): 'BsR',
        AcquisitionTranslator.Variable('ZBbsB'): 'BbsB',
        AcquisitionTranslator.Variable('ZBbsG'): 'BbsG',
        AcquisitionTranslator.Variable('ZBbsR'): 'BbsR',
        AcquisitionTranslator.Variable('BswB'): 'BswB',
        AcquisitionTranslator.Variable('BswG'): 'BswG',
        AcquisitionTranslator.Variable('BswR'): 'BswR',
        AcquisitionTranslator.Variable('BbswB'): 'BbswB',
        AcquisitionTranslator.Variable('BbswG'): 'BbswG',
        AcquisitionTranslator.Variable('BbswR'): 'BbswR',
        AcquisitionTranslator.Variable('BswdB'): 'BswdB',
        AcquisitionTranslator.Variable('BswdG'): 'BswdG',
        AcquisitionTranslator.Variable('BswdR'): 'BswdR',
        AcquisitionTranslator.Variable('BbswdB'): 'BbswdB',
        AcquisitionTranslator.Variable('BbswdG'): 'BbswdG',
        AcquisitionTranslator.Variable('BbswdR'): 'BbswdR',
        AcquisitionTranslator.Variable('CsB'): 'CsB',
        AcquisitionTranslator.Variable('CsG'): 'CsG',
        AcquisitionTranslator.Variable('CsR'): 'CsR',
        AcquisitionTranslator.Variable('CbsB'): 'CbsB',
        AcquisitionTranslator.Variable('CbsG'): 'CbsG',
        AcquisitionTranslator.Variable('CbsR'): 'CbsR',
        AcquisitionTranslator.Variable('CdB'): 'CdB',
        AcquisitionTranslator.Variable('CdG'): 'CdG',
        AcquisitionTranslator.Variable('CdR'): 'CdR',
        AcquisitionTranslator.Variable('CbdB'): 'CbdB',
        AcquisitionTranslator.Variable('CbdG'): 'CbdG',
        AcquisitionTranslator.Variable('CbdR'): 'CbdR',
        AcquisitionTranslator.Variable('CfB'): 'CfB',
        AcquisitionTranslator.Variable('CfG'): 'CfG',
        AcquisitionTranslator.Variable('CfR'): 'CfR',
        AcquisitionTranslator.Variable('T'): 'Tsample',
        AcquisitionTranslator.Variable('U'): 'Usample',
        AcquisitionTranslator.Variable('P'): 'Psample',
        AcquisitionTranslator.Variable('Tu'): 'Tinlet',
        AcquisitionTranslator.Variable('Uu'): 'Uinlet',
        AcquisitionTranslator.Variable('Al'): 'Al',
        AcquisitionTranslator.Variable('Vl'): 'Vl',
        AcquisitionTranslator.Variable('F2'): 'modestring',
        AcquisitionTranslator.Variable('ZRTIME'): 'modetime',
        AcquisitionTranslator.Variable('ZPARAMETERS'): 'parameters',
    }, flags_notifications={
        'BackscatterDisabled': 'backscatter_disabled',
        'LampPowerError': 'lamp_power_error',
        'ValveFault': 'valve_fault',
        'ChopperFault': 'chopper_fault',
        'ShutterFault': 'shutter_fault',
        'HeaterUnstable': 'heater_unstable',
        'PressureOutOfRange': 'pressure_out_of_range',
        'TemperatureOutOfRange': 'sample_temperature_out_of_range',
        'InletTemperatureOutOfRange': 'inlet_temperature_out_of_range',
        'RHOutOfRange': 'rh_out_of_range',
    }, flags_set_warning={
        'LampPowerError', 'ChopperFault', 'ShutterFault', 'HeaterUnstable', 'PressureOutOfRange',
        'TemperatureOutOfRange', 'InletTemperatureOutOfRange',
    }, zstate_notifications={
        'Blank': 'blank',
        'Zero': 'zero',
        'Spancheck': 'spancheck',
    }, command_map={
        'start_zero': 'StartZero',
        'start_spancheck': 'StartSpancheck',
        'stop_spancheck': 'StopSpancheck',
    }),

    AcquisitionTranslator.Component('acquire_vaisala_pwdx2', 'vaisalapwdx2', variable_map={
        AcquisitionTranslator.Variable('ZWZ1Min'): 'WZ',
        AcquisitionTranslator.Variable('ZWZ10Min'): 'WZ10Min',
        AcquisitionTranslator.Variable('ZWXInstant2'): 'WX',
        AcquisitionTranslator.Variable('ZWX15Min2'): 'WX15Min',
        AcquisitionTranslator.Variable('ZWX1Hour2'): 'WX1Hour',
        AcquisitionTranslator.Variable('ZWXNWS'): 'nws_code',
        AcquisitionTranslator.Variable('T1'): 'Tambient',
        AcquisitionTranslator.Variable('T2'): 'Tinternal',
        AcquisitionTranslator.Variable('T3'): 'Tdrd',
        AcquisitionTranslator.Variable('I'): 'I',
        AcquisitionTranslator.Variable('V1'): 'Vled',
        AcquisitionTranslator.Variable('V2'): 'Vambient',
        AcquisitionTranslator.Variable('V3'): 'Vsupply',
        AcquisitionTranslator.Variable('V4'): 'Vpositive',
        AcquisitionTranslator.Variable('V5'): 'Vnegative',
        AcquisitionTranslator.Variable('C1'): 'Csignal',
        AcquisitionTranslator.Variable('C2'): 'Coffset',
        AcquisitionTranslator.Variable('C3'): 'Cdrift',
        AcquisitionTranslator.Variable('C4'): 'Cdrd',
        AcquisitionTranslator.Variable('ZBsp'): 'BsRx',
        AcquisitionTranslator.Variable('ZBsx'): 'BsTx',
        AcquisitionTranslator.Variable('ZBspd'): 'BsRxChange',
        AcquisitionTranslator.Variable('ZBsxd'): 'BsTxChange',
    }, flags_notifications={
        'HardwareError': 'hardware_error',
        'HardwareWarning': 'hardware_error',
        'BackscatterAlarm': 'backscatter_range',
        'BackscatterWarning': 'backscatter_range',
        'TransmitterError': 'transmitter_range',
        'PowerError': 'power_range',
        'OffsetError': 'offset_range',
        'SignalError': 'signal_error',
        'ReceiverError': 'receiver_range',
        'DataRAMError': 'data_ram_error',
        'EEPROMError': 'eeprom_error',
        'TemperatureError': 'temperature_range',
        'RainError': 'rain_range',
        'LuminanceError': 'luminance_range',
        'TransmitterLow': 'transmitter_range',
        'ReceiverSaturated': 'receiver_range',
        'OffsetDrifted': 'offset_drift',
        'VisiblityNotCalibrated': 'visibility_not_calibrated',
    }, flags_set_warning={
        'HardwareError',
        'HardwareWarning',
        'TransmitterError',
        'PowerError',
        'OffsetError',
        'SignalError',
        'ReceiverError',
        'DataRAMError',
        'EEPROMError',
        'TransmitterLow',
    }),

    AcquisitionTranslator.Component('acquire_vaisala_wmt700', 'vaisalawmt700', variable_map={
        AcquisitionTranslator.Variable('WS'): 'WS',
        AcquisitionTranslator.Variable('WD'): 'WD',
        AcquisitionTranslator.Variable('T1'): 'Tsonic',
        AcquisitionTranslator.Variable('T2'): 'Ttransducer',
        AcquisitionTranslator.Variable('V1'): 'Vsupply',
        AcquisitionTranslator.Variable('V2'): 'Vheater',
    }, flags_notifications={
        'TemperatureSensor1Failure': 'temperature_sensor_1_failure',
        'TemperatureSensor2Failure': 'temperature_sensor_2_failure',
        'TemperatureSensor3Failure': 'temperature_sensor_3_failure',
        'HeaterFailure': 'heater_failure',
        'HighSupplyVoltage': 'supply_voltage_high',
        'LowSupplyVoltage': 'supply_voltage_low',
        'WindSpeedHigh': 'wind_speed_high',
        'SonicTemperatureOutOfRange': 'sonic_temperature_out_of_range',
        'WindMeasurementSuspect': 'low_wind_validity',
        'BlockedSensor': 'blocked_sensor',
        'HighNoise': 'high_noise_level',
    }, flags_set_warning={
        'TemperatureSensor1Failure',
        'TemperatureSensor3Failure',
        'HeaterFailure',
        'HighSupplyVoltage',
        'LowSupplyVoltage',
        'SonicTemperatureOutOfRange',
        'BlockedSensor',
    }),

    AcquisitionTranslator.Component('acquire_vaisala_wxt5xx', 'vaisalawxt5xx', variable_map={
        AcquisitionTranslator.Variable('WS'): 'WS',
        AcquisitionTranslator.Variable('WD'): 'WD',
        AcquisitionTranslator.Variable('WI'): 'WI',
        AcquisitionTranslator.Variable('T1'): 'Tambient',
        AcquisitionTranslator.Variable('T2'): 'Tinternal',
        AcquisitionTranslator.Variable('T3'): 'Theater',
        AcquisitionTranslator.Variable('T4'): 'Taux',
        AcquisitionTranslator.Variable('U1'): 'Uambient',
        AcquisitionTranslator.Variable('P'): 'P',
        AcquisitionTranslator.Variable('VA'): 'R',
        AcquisitionTranslator.Variable('Ld'): 'Ld',
        AcquisitionTranslator.Variable('V1'): 'Vsupply',
        AcquisitionTranslator.Variable('V2'): 'Vreference',
        AcquisitionTranslator.Variable('V3'): 'Vheater',
    }, flags_notifications={
        'HeaterOn': 'heater_on',
    }),

    AcquisitionTranslator.ImpactorCycle('control_cycle', 'impactor_cycle'),
])


aerosol_data = {
    'raw': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'F1_N71'),
                Name(station, 'raw', 'F1_N61'),
                Name(station, 'raw', 'F1_S11'),
                Name(station, 'raw', 'F1_A11'),
            }, send
        ),

        'cnc': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'N_N71'): 'cnc',
                Name(station, 'raw', 'N_N61'): 'cnc',
            }, send
        ),
        
        'scattering-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11'): 'BsB',
                Name(station, 'raw', 'BsG_S11'): 'BsG',
                Name(station, 'raw', 'BsR_S11'): 'BsR',
                Name(station, 'raw', 'BbsB_S11'): 'BbsB',
                Name(station, 'raw', 'BbsG_S11'): 'BbsG',
                Name(station, 'raw', 'BbsR_S11'): 'BbsR',
            }, send
        ),
        'scattering-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm10'}): 'BbsR',
            }, send
        ),
        'scattering-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm25'}): 'BbsR',
            }, send
        ),
        'scattering-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm1'}): 'BbsR',
            }, send
        ),
        
        'absorption-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BaB_A11'): 'BaB',
                Name(station, 'raw', 'BaG_A11'): 'BaG',
                Name(station, 'raw', 'BaR_A11'): 'BaR',
            }, send
        ),
        'absorption-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'absorption-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'absorption-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'aethalometer': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, dict(
                [(Name(station, 'raw', f'Ba{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
                [(Name(station, 'raw', f'X{i+1}_A81'), f'X{i+1}') for i in range(7)] +
                [(Name(station, 'raw', f'ZFACTOR{i+1}_A81'), f'CF{i+1}') for i in range(7)] +
                [(Name(station, 'raw', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)]
            ), send
        ),

        'intensive-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11'): 'BsB',
                Name(station, 'raw', 'BsG_S11'): 'BsG',
                Name(station, 'raw', 'BsR_S11'): 'BsR',
                Name(station, 'raw', 'BbsB_S11'): 'BbsB',
                Name(station, 'raw', 'BbsG_S11'): 'BbsG',
                Name(station, 'raw', 'BbsR_S11'): 'BbsR',
                Name(station, 'raw', 'BaB_A11'): 'BaB',
                Name(station, 'raw', 'BaG_A11'): 'BaG',
                Name(station, 'raw', 'BaR_A11'): 'BaR',
            }, send
        ),
        'intensive-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm10'}): 'BbsR',
                Name(station, 'raw', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'intensive-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm25'}): 'BbsR',
                Name(station, 'raw', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'intensive-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm1'}): 'BbsR',
                Name(station, 'raw', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'WS1_XM1'): 'WS',
                Name(station, 'raw', 'WD1_XM1'): 'WD',
            }, send
        ),
        'flow': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Q_Q11'): 'sample',
                Name(station, 'raw', 'Q_Q11', {'pm10'}): 'sample',
                Name(station, 'raw', 'Q_Q11', {'pm1'}): 'sample',
                Name(station, 'raw', 'Q_Q11', {'pm25'}): 'sample',
                Name(station, 'raw', 'Pd_P01'): 'pitot',
            }, send
        ),
        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T_V51'): 'Tinlet', Name(station, 'raw', 'U_V51'): 'Uinlet',
                Name(station, 'raw', 'T_V01'): 'Taux', Name(station, 'raw', 'U_V01'): 'Uaux',
                Name(station, 'raw', 'T1_XM1'): 'Tambient',
                Name(station, 'raw', 'U1_XM1'): 'Uambient',
                Name(station, 'raw', 'TD1_XM1'): 'TDambient',

                Name(station, 'raw', 'T_V11'): 'Tsample', Name(station, 'raw', 'U_V11'): 'Usample',
                Name(station, 'raw', 'T_V11', {'pm10'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm10'}): 'Usample',
                Name(station, 'raw', 'T_V11', {'pm1'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm1'}): 'Usample',
                Name(station, 'raw', 'T_V11', {'pm25'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm25'}): 'Usample',

                Name(station, 'raw', 'Tu_S11'): 'Tnephinlet', Name(station, 'raw', 'Uu_S11'): 'Unephinlet',
                Name(station, 'raw', 'Tu_S11', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm10'}): 'Unephinlet',
                Name(station, 'raw', 'Tu_S11', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm1'}): 'Unephinlet',
                Name(station, 'raw', 'Tu_S11', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm25'}): 'Unephinlet',

                Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
                Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
                Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
                Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',
            }, send
        ),
        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_XM1'): 'ambient',
                Name(station, 'raw', 'Pd_P01'): 'pitot',
                Name(station, 'raw', 'Pd_P12'): 'vacuum',
                Name(station, 'raw', 'Pd_P12', {'pm10'}): 'vacuum',
                Name(station, 'raw', 'Pd_P12', {'pm1'}): 'vacuum',
                Name(station, 'raw', 'Pd_P12', {'pm25'}): 'vacuum',
            }, send
        ),
        'samplepressure-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_S11'): 'neph',
                Name(station, 'raw', 'Pd_P11'): 'impactor',
            }, send
        ),
        'samplepressure-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_S11', {'pm10'}): 'neph',
                Name(station, 'raw', 'Pd_P11', {'pm10'}): 'impactor',
            }, send
        ),
        'samplepressure-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_S11', {'pm25'}): 'neph',
                Name(station, 'raw', 'Pd_P11', {'pm25'}): 'impactor',
            }, send
        ),
        'samplepressure-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
                Name(station, 'raw', 'Pd_P11', {'pm1'}): 'impactor',
            }, send
        ),

        'nephzero': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BswB_S11'): 'BswB',
                Name(station, 'raw', 'BswG_S11'): 'BswG',
                Name(station, 'raw', 'BswR_S11'): 'BswR',
                Name(station, 'raw', 'BbswB_S11'): 'BbswB',
                Name(station, 'raw', 'BbswG_S11'): 'BbswG',
                Name(station, 'raw', 'BbswR_S11'): 'BbswR',
            }, send
        ),
        'nephstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'CfG_S11'): 'CfG',
                Name(station, 'raw', 'CfG_S11', {'pm10'}): 'CfG',
                Name(station, 'raw', 'CfG_S11', {'pm1'}): 'CfG',
                Name(station, 'raw', 'CfG_S11', {'pm25'}): 'CfG',
                Name(station, 'raw', 'Vl_S11'): 'Vl',
                Name(station, 'raw', 'Vl_S11', {'pm10'}): 'Vl',
                Name(station, 'raw', 'Vl_S11', {'pm1'}): 'Vl',
                Name(station, 'raw', 'Vl_S11', {'pm25'}): 'Vl',
                Name(station, 'raw', 'Al_S11'): 'Al',
                Name(station, 'raw', 'Al_S11', {'pm10'}): 'Al',
                Name(station, 'raw', 'Al_S11', {'pm1'}): 'Al',
                Name(station, 'raw', 'Al_S11', {'pm25'}): 'Al',
            }, send
        ),
        
        'clapstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'IrG_A11'): 'IrG',
                Name(station, 'raw', 'IrG_A11', {'pm10'}): 'IrG',
                Name(station, 'raw', 'IrG_A11', {'pm1'}): 'IrG',
                Name(station, 'raw', 'IrG_A11', {'pm25'}): 'IrG',
                Name(station, 'raw', 'IfG_A11'): 'IfG',
                Name(station, 'raw', 'IfG_A11', {'pm10'}): 'IfG',
                Name(station, 'raw', 'IfG_A11', {'pm1'}): 'IfG',
                Name(station, 'raw', 'IfG_A11', {'pm25'}): 'IfG',
                Name(station, 'raw', 'IpG_A11'): 'IpG',
                Name(station, 'raw', 'IpG_A11', {'pm10'}): 'IpG',
                Name(station, 'raw', 'IpG_A11', {'pm1'}): 'IpG',
                Name(station, 'raw', 'IpG_A11', {'pm25'}): 'IpG',
                Name(station, 'raw', 'Q_A11'): 'Q',
                Name(station, 'raw', 'Q_A11', {'pm10'}): 'Q',
                Name(station, 'raw', 'Q_A11', {'pm1'}): 'Q',
                Name(station, 'raw', 'Q_A11', {'pm25'}): 'Q',
                Name(station, 'raw', 'T1_A11'): 'Tsample',
                Name(station, 'raw', 'T1_A11', {'pm10'}): 'Tsample',
                Name(station, 'raw', 'T1_A11', {'pm1'}): 'Tsample',
                Name(station, 'raw', 'T1_A11', {'pm25'}): 'Tsample',
                Name(station, 'raw', 'T2_A11'): 'Tcase',
                Name(station, 'raw', 'T2_A11', {'pm10'}): 'Tcase',
                Name(station, 'raw', 'T2_A11', {'pm1'}): 'Tcase',
                Name(station, 'raw', 'T2_A11', {'pm25'}): 'Tcase',
                Name(station, 'raw', 'Fn_A11'): 'spot',
            }, send
        ),

        'aethalometerstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T1_A81'): 'Tcontroller',
                Name(station, 'raw', 'T2_A81'): 'Tsupply',
                Name(station, 'raw', 'T3_A81'): 'Tled',
            }, send
        ),

        'cpcstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Q_Q71'): 'Qsample',
                Name(station, 'raw', 'Q_Q61'): 'Qsample',
                Name(station, 'raw', 'Q_Q72'): 'Qdrier',
                Name(station, 'raw', 'Q_Q62'): 'Qdrier',
            }, send
        ),

        'umacstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T_X1'): 'T',
                Name(station, 'raw', 'V_X1'): 'V',
            }, send
        ),
    },
    
    'clean': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'F1_N71'),
                Name(station, 'clean', 'F1_N61'),
                Name(station, 'clean', 'F1_S11'),
                Name(station, 'clean', 'F1_A11'),
            }, send
        ),

        'cnc': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'N_N71'): 'cnc',
                Name(station, 'clean', 'N_N61'): 'cnc',
            }, send
        ),

        'scattering-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11'): 'BsB',
                Name(station, 'clean', 'BsG_S11'): 'BsG',
                Name(station, 'clean', 'BsR_S11'): 'BsR',
                Name(station, 'clean', 'BbsB_S11'): 'BbsB',
                Name(station, 'clean', 'BbsG_S11'): 'BbsG',
                Name(station, 'clean', 'BbsR_S11'): 'BbsR',
            }, send
        ),
        'scattering-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm10'}): 'BbsR',
            }, send
        ),
        'scattering-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm25'}): 'BbsR',
            }, send
        ),
        'scattering-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm1'}): 'BbsR',
            }, send
        ),

        'absorption-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BaB_A11'): 'BaB',
                Name(station, 'clean', 'BaG_A11'): 'BaG',
                Name(station, 'clean', 'BaR_A11'): 'BaR',
            }, send
        ),
        'absorption-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'absorption-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),
        'absorption-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'aethalometer': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, dict(
                [(Name(station, 'clean', f'Ba{i + 1}_A81'), f'Ba{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'X{i + 1}_A81'), f'X{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'ZFACTOR{i + 1}_A81'), f'CF{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'Ir{i + 1}_A81'), f'Ir{i + 1}') for i in range(7)]
            ), send
        ),

        'intensive-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11'): 'BsB',
                Name(station, 'clean', 'BsG_S11'): 'BsG',
                Name(station, 'clean', 'BsR_S11'): 'BsR',
                Name(station, 'clean', 'BbsB_S11'): 'BbsB',
                Name(station, 'clean', 'BbsG_S11'): 'BbsG',
                Name(station, 'clean', 'BbsR_S11'): 'BbsR',
                Name(station, 'clean', 'BaB_A11'): 'BaB',
                Name(station, 'clean', 'BaG_A11'): 'BaG',
                Name(station, 'clean', 'BaR_A11'): 'BaR',
            }, send
        ),
        'intensive-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm10'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'intensive-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm25'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'intensive-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm1'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'WS1_XM1'): 'WS',
                Name(station, 'clean', 'WD1_XM1'): 'WD',
            }, send
        ),
    },
    
    'avgh': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'F1_N71'),
                Name(station, 'avgh', 'F1_N61'),
                Name(station, 'avgh', 'F1_S11'),
                Name(station, 'avgh', 'F1_A11'),
            }, send
        ),

        'cnc': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'N_N71'): 'cnc',
                Name(station, 'avgh', 'N_N61'): 'cnc',
            }, send
        ),

        'scattering-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11'): 'BsB',
                Name(station, 'avgh', 'BsG_S11'): 'BsG',
                Name(station, 'avgh', 'BsR_S11'): 'BsR',
                Name(station, 'avgh', 'BbsB_S11'): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11'): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11'): 'BbsR',
            }, send
        ),
        'scattering-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm10'}): 'BbsR',
            }, send
        ),
        'scattering-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm25'}): 'BbsR',
            }, send
        ),
        'scattering-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm1'}): 'BbsR',
            }, send
        ),

        'absorption-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BaB_A11'): 'BaB',
                Name(station, 'avgh', 'BaG_A11'): 'BaG',
                Name(station, 'avgh', 'BaR_A11'): 'BaR',
            }, send
        ),
        'absorption-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'absorption-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'absorption-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'aethalometer': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, dict(
                [(Name(station, 'avgh', f'Ba{i + 1}_A81'), f'Ba{i + 1}') for i in range(7)] +
                [(Name(station, 'avgh', f'X{i + 1}_A81'), f'X{i + 1}') for i in range(7)] +
                [(Name(station, 'avgh', f'ZFACTOR{i + 1}_A81'), f'CF{i + 1}') for i in range(7)] +
                [(Name(station, 'avgh', f'Ir{i + 1}_A81'), f'Ir{i + 1}') for i in range(7)]
            ), send
        ),

        'intensive-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11'): 'BsB',
                Name(station, 'avgh', 'BsG_S11'): 'BsG',
                Name(station, 'avgh', 'BsR_S11'): 'BsR',
                Name(station, 'avgh', 'BbsB_S11'): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11'): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11'): 'BbsR',
                Name(station, 'avgh', 'BaB_A11'): 'BaB',
                Name(station, 'avgh', 'BaG_A11'): 'BaG',
                Name(station, 'avgh', 'BaR_A11'): 'BaR',
            }, send
        ),
        'intensive-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm10'}): 'BbsR',
                Name(station, 'avgh', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'intensive-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm25'}): 'BbsR',
                Name(station, 'avgh', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'intensive-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm1'}): 'BbsR',
                Name(station, 'avgh', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'WS1_XM1'): 'WS',
                Name(station, 'avgh', 'WD1_XM1'): 'WD',
            }, send
        ),
    },

    'editing': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: EditedContaminationReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'F1_N71'),
                Name(station, 'clean', 'F1_N61'),
                Name(station, 'clean', 'F1_S11'),
                Name(station, 'clean', 'F1_A11'),
            }, send
        ),

        'cnc': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'N_N71'): 'cnc',
                Name(station, 'clean', 'N_N61'): 'cnc',
            }, send
        ),

        'scattering-whole': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11'): 'BsB',
                Name(station, 'clean', 'BsG_S11'): 'BsG',
                Name(station, 'clean', 'BsR_S11'): 'BsR',
                Name(station, 'clean', 'BbsB_S11'): 'BbsB',
                Name(station, 'clean', 'BbsG_S11'): 'BbsG',
                Name(station, 'clean', 'BbsR_S11'): 'BbsR',
            }, send
        ),
        'scattering-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm10'}): 'BbsR',
            }, send
        ),
        'scattering-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm25'}): 'BbsR',
            }, send
        ),
        'scattering-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm1'}): 'BbsR',
            }, send
        ),

        'absorption-whole': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BaB_A11'): 'BaB',
                Name(station, 'clean', 'BaG_A11'): 'BaG',
                Name(station, 'clean', 'BaR_A11'): 'BaR',
            }, send
        ),
        'absorption-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'absorption-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'absorption-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'aethalometer': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
                [(Name(station, 'clean', f'Ba{i + 1}_A81'), f'Ba{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'X{i + 1}_A81'), f'X{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'ZFACTOR{i + 1}_A81'), f'CF{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'Ir{i + 1}_A81'), f'Ir{i + 1}') for i in range(7)]
            ), send
        ),

        'intensive-whole': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11'): 'BsB',
                Name(station, 'clean', 'BsG_S11'): 'BsG',
                Name(station, 'clean', 'BsR_S11'): 'BsR',
                Name(station, 'clean', 'BbsB_S11'): 'BbsB',
                Name(station, 'clean', 'BbsG_S11'): 'BbsG',
                Name(station, 'clean', 'BbsR_S11'): 'BbsR',
                Name(station, 'clean', 'BaB_A11'): 'BaB',
                Name(station, 'clean', 'BaG_A11'): 'BaG',
                Name(station, 'clean', 'BaR_A11'): 'BaR',
            }, send
        ),
        'intensive-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm10'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'intensive-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm25'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'intensive-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm1'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'WS1_XM1'): 'WS',
                Name(station, 'clean', 'WD1_XM1'): 'WD',
            }, send
        ),
    },

    'realtime': RealtimeTranslator.Data('aerosol', {
        'cnc': {
            RealtimeTranslator.Key('N_N71'): 'cnc',
            RealtimeTranslator.Key('N_N61'): 'cnc',
        },

        'scattering-whole': {
            RealtimeTranslator.Key('BsB_S11'): 'BsB',
            RealtimeTranslator.Key('BsG_S11'): 'BsG',
            RealtimeTranslator.Key('BsR_S11'): 'BsR',
            RealtimeTranslator.Key('BbsB_S11'): 'BbsB',
            RealtimeTranslator.Key('BbsG_S11'): 'BbsG',
            RealtimeTranslator.Key('BbsR_S11'): 'BbsR',
        },
        
        'scattering-pm10': {
            RealtimeTranslator.Key('BsB_S11', {'pm10'}): 'BsB',
            RealtimeTranslator.Key('BsG_S11', {'pm10'}): 'BsG',
            RealtimeTranslator.Key('BsR_S11', {'pm10'}): 'BsR',
            RealtimeTranslator.Key('BbsB_S11', {'pm10'}): 'BbsB',
            RealtimeTranslator.Key('BbsG_S11', {'pm10'}): 'BbsG',
            RealtimeTranslator.Key('BbsR_S11', {'pm10'}): 'BbsR',
        },
        'scattering-pm25': {
            RealtimeTranslator.Key('BsB_S11', {'pm25'}): 'BsB',
            RealtimeTranslator.Key('BsG_S11', {'pm25'}): 'BsG',
            RealtimeTranslator.Key('BsR_S11', {'pm25'}): 'BsR',
            RealtimeTranslator.Key('BbsB_S11', {'pm25'}): 'BbsB',
            RealtimeTranslator.Key('BbsG_S11', {'pm25'}): 'BbsG',
            RealtimeTranslator.Key('BbsR_S11', {'pm25'}): 'BbsR',
        },
        'scattering-pm1': {
            RealtimeTranslator.Key('BsB_S11', {'pm1'}): 'BsB',
            RealtimeTranslator.Key('BsG_S11', {'pm1'}): 'BsG',
            RealtimeTranslator.Key('BsR_S11', {'pm1'}): 'BsR',
            RealtimeTranslator.Key('BbsB_S11', {'pm1'}): 'BbsB',
            RealtimeTranslator.Key('BbsG_S11', {'pm1'}): 'BbsG',
            RealtimeTranslator.Key('BbsR_S11', {'pm1'}): 'BbsR',
        },

        'absorption-whole': {
            RealtimeTranslator.Key('BaB_A11'): 'BaB',
            RealtimeTranslator.Key('BaG_A11'): 'BaG',
            RealtimeTranslator.Key('BaR_A11'): 'BaR',
        },
        'absorption-pm10': {
            RealtimeTranslator.Key('BaB_A11', {'pm10'}): 'BaB',
            RealtimeTranslator.Key('BaG_A11', {'pm10'}): 'BaG',
            RealtimeTranslator.Key('BaR_A11', {'pm10'}): 'BaR',
        },
        'absorption-pm25': {
            RealtimeTranslator.Key('BaB_A11', {'pm25'}): 'BaB',
            RealtimeTranslator.Key('BaG_A11', {'pm25'}): 'BaG',
            RealtimeTranslator.Key('BaR_A11', {'pm25'}): 'BaR',
        },
        'absorption-pm1': {
            RealtimeTranslator.Key('BaB_A11', {'pm1'}): 'BaB',
            RealtimeTranslator.Key('BaG_A11', {'pm1'}): 'BaG',
            RealtimeTranslator.Key('BaR_A11', {'pm1'}): 'BaR',
        },

        'aethalometer': dict(
            [(RealtimeTranslator.Key(f'Ba{i + 1}_A81'), f'Ba{i + 1}') for i in range(7)] +
            [(RealtimeTranslator.Key(f'X{i + 1}_A81'), f'X{i + 1}') for i in range(7)] +
            [(RealtimeTranslator.Key(f'ZFACTOR{i + 1}_A81'), f'CF{i + 1}') for i in range(7)] +
            [(RealtimeTranslator.Key(f'Ir{i + 1}_A81'), f'Ir{i + 1}') for i in range(7)]
        ),

        'intensive-whole': {
            RealtimeTranslator.Key('BsB_S11'): 'BsB',
            RealtimeTranslator.Key('BsG_S11'): 'BsG',
            RealtimeTranslator.Key('BsR_S11'): 'BsR',
            RealtimeTranslator.Key('BbsB_S11'): 'BbsB',
            RealtimeTranslator.Key('BbsG_S11'): 'BbsG',
            RealtimeTranslator.Key('BbsR_S11'): 'BbsR',
            RealtimeTranslator.Key('BaB_A11'): 'BaB',
            RealtimeTranslator.Key('BaG_A11'): 'BaG',
            RealtimeTranslator.Key('BaR_A11'): 'BaR',
        },
        'intensive-pm10': {
            RealtimeTranslator.Key('BsB_S11', {'pm10'}): 'BsB',
            RealtimeTranslator.Key('BsG_S11', {'pm10'}): 'BsG',
            RealtimeTranslator.Key('BsR_S11', {'pm10'}): 'BsR',
            RealtimeTranslator.Key('BbsB_S11', {'pm10'}): 'BbsB',
            RealtimeTranslator.Key('BbsG_S11', {'pm10'}): 'BbsG',
            RealtimeTranslator.Key('BbsR_S11', {'pm10'}): 'BbsR',
            RealtimeTranslator.Key('BaB_A11', {'pm10'}): 'BaB',
            RealtimeTranslator.Key('BaG_A11', {'pm10'}): 'BaG',
            RealtimeTranslator.Key('BaR_A11', {'pm10'}): 'BaR',
        },
        'intensive-pm25': {
            RealtimeTranslator.Key('BsB_S11', {'pm25'}): 'BsB',
            RealtimeTranslator.Key('BsG_S11', {'pm25'}): 'BsG',
            RealtimeTranslator.Key('BsR_S11', {'pm25'}): 'BsR',
            RealtimeTranslator.Key('BbsB_S11', {'pm25'}): 'BbsB',
            RealtimeTranslator.Key('BbsG_S11', {'pm25'}): 'BbsG',
            RealtimeTranslator.Key('BbsR_S11', {'pm25'}): 'BbsR',
            RealtimeTranslator.Key('BaB_A11', {'pm25'}): 'BaB',
            RealtimeTranslator.Key('BaG_A11', {'pm25'}): 'BaG',
            RealtimeTranslator.Key('BaR_A11', {'pm25'}): 'BaR',
        },
        'intensive-pm1': {
            RealtimeTranslator.Key('BsB_S11', {'pm1'}): 'BsB',
            RealtimeTranslator.Key('BsG_S11', {'pm1'}): 'BsG',
            RealtimeTranslator.Key('BsR_S11', {'pm1'}): 'BsR',
            RealtimeTranslator.Key('BbsB_S11', {'pm1'}): 'BbsB',
            RealtimeTranslator.Key('BbsG_S11', {'pm1'}): 'BbsG',
            RealtimeTranslator.Key('BbsR_S11', {'pm1'}): 'BbsR',
            RealtimeTranslator.Key('BaB_A11', {'pm1'}): 'BaB',
            RealtimeTranslator.Key('BaG_A11', {'pm1'}): 'BaG',
            RealtimeTranslator.Key('BaR_A11', {'pm1'}): 'BaR',
        },

        'wind': {
            RealtimeTranslator.Key('WS1_XM1'): 'WS',
            RealtimeTranslator.Key('WD1_XM1'): 'WD',
        },
        'flow': {
            RealtimeTranslator.Key('Q_Q11'): 'sample',
            RealtimeTranslator.Key('Q_Q11', {'pm10'}): 'sample',
            RealtimeTranslator.Key('Q_Q11', {'pm1'}): 'sample',
            RealtimeTranslator.Key('Q_Q11', {'pm25'}): 'sample',
            RealtimeTranslator.Key('Pd_P01'): 'pitot',
        },
        'temperature': {
            RealtimeTranslator.Key('T_V51'): 'Tinlet', RealtimeTranslator.Key('U_V51'): 'Uinlet',
            RealtimeTranslator.Key('T_V01'): 'Taux', RealtimeTranslator.Key('U_V01'): 'Uaux',
            RealtimeTranslator.Key('T1_XM1'): 'Tambient',
            RealtimeTranslator.Key('U1_XM1'): 'Uambient',
            RealtimeTranslator.Key('TD1_XM1'): 'TDambient',

            RealtimeTranslator.Key('T_V11'): 'Tsample', RealtimeTranslator.Key('U_V11'): 'Usample',
            RealtimeTranslator.Key('T_V11', {'pm10'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm10'}): 'Usample',
            RealtimeTranslator.Key('T_V11', {'pm1'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm1'}): 'Usample',
            RealtimeTranslator.Key('T_V11', {'pm25'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm25'}): 'Usample',

            RealtimeTranslator.Key('Tu_S11'): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11'): 'Unephinlet',
            RealtimeTranslator.Key('Tu_S11', {'pm10'}): 'Tnephinlet',
            RealtimeTranslator.Key('Uu_S11', {'pm10'}): 'Unephinlet',
            RealtimeTranslator.Key('Tu_S11', {'pm1'}): 'Tnephinlet',
            RealtimeTranslator.Key('Uu_S11', {'pm1'}): 'Unephinlet',
            RealtimeTranslator.Key('Tu_S11', {'pm25'}): 'Tnephinlet',
            RealtimeTranslator.Key('Uu_S11', {'pm25'}): 'Unephinlet',

            RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
            RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
            RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
            RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',
        },
        'pressure': {
            RealtimeTranslator.Key('P_XM1'): 'ambient',
            RealtimeTranslator.Key('Pd_P01'): 'pitot',
            RealtimeTranslator.Key('Pd_P12'): 'vacuum',
            RealtimeTranslator.Key('Pd_P12', {'pm10'}): 'vacuum',
            RealtimeTranslator.Key('Pd_P12', {'pm1'}): 'vacuum',
            RealtimeTranslator.Key('Pd_P12', {'pm25'}): 'vacuum',
        },
        'samplepressure-whole': {
            RealtimeTranslator.Key('P_S11'): 'neph',
            RealtimeTranslator.Key('Pd_P11'): 'impactor',
        },
        'samplepressure-pm10': {
            RealtimeTranslator.Key('P_S11', {'pm10'}): 'neph',
            RealtimeTranslator.Key('Pd_P11', {'pm10'}): 'impactor',
        },
        'samplepressure-pm25': {
            RealtimeTranslator.Key('P_S11', {'pm25'}): 'neph',
            RealtimeTranslator.Key('Pd_P11', {'pm25'}): 'impactor',
        },
        'samplepressure-pm1': {
            RealtimeTranslator.Key('P_S11', {'pm1'}): 'neph',
            RealtimeTranslator.Key('Pd_P11', {'pm1'}): 'impactor',
        },

        'nephzero': {
            RealtimeTranslator.Key('BswB_S11'): 'BswB',
            RealtimeTranslator.Key('BswG_S11'): 'BswG',
            RealtimeTranslator.Key('BswR_S11'): 'BswR',
            RealtimeTranslator.Key('BbswB_S11'): 'BbswB',
            RealtimeTranslator.Key('BbswG_S11'): 'BbswG',
            RealtimeTranslator.Key('BbswR_S11'): 'BbswR',
        },
        'nephstatus': {
            RealtimeTranslator.Key('CfG_S11'): 'CfG',
            RealtimeTranslator.Key('CfG_S11', {'pm10'}): 'CfG',
            RealtimeTranslator.Key('CfG_S11', {'pm1'}): 'CfG',
            RealtimeTranslator.Key('CfG_S11', {'pm25'}): 'CfG',
            RealtimeTranslator.Key('Vl_S11'): 'Vl',
            RealtimeTranslator.Key('Vl_S11', {'pm10'}): 'Vl',
            RealtimeTranslator.Key('Vl_S11', {'pm1'}): 'Vl',
            RealtimeTranslator.Key('Vl_S11', {'pm25'}): 'Vl',
            RealtimeTranslator.Key('Al_S11'): 'Al',
            RealtimeTranslator.Key('Al_S11', {'pm10'}): 'Al',
            RealtimeTranslator.Key('Al_S11', {'pm1'}): 'Al',
            RealtimeTranslator.Key('Al_S11', {'pm25'}): 'Al',
        },

        'clapstatus': {
            RealtimeTranslator.Key('IrG_A11'): 'IrG',
            RealtimeTranslator.Key('IrG_A11', {'pm10'}): 'IrG',
            RealtimeTranslator.Key('IrG_A11', {'pm1'}): 'IrG',
            RealtimeTranslator.Key('IrG_A11', {'pm25'}): 'IrG',
            RealtimeTranslator.Key('IfG_A11'): 'IfG',
            RealtimeTranslator.Key('IfG_A11', {'pm10'}): 'IfG',
            RealtimeTranslator.Key('IfG_A11', {'pm1'}): 'IfG',
            RealtimeTranslator.Key('IfG_A11', {'pm25'}): 'IfG',
            RealtimeTranslator.Key('IpG_A11'): 'IpG',
            RealtimeTranslator.Key('IpG_A11', {'pm10'}): 'IpG',
            RealtimeTranslator.Key('IpG_A11', {'pm1'}): 'IpG',
            RealtimeTranslator.Key('IpG_A11', {'pm25'}): 'IpG',
            RealtimeTranslator.Key('Q_A11'): 'Q',
            RealtimeTranslator.Key('Q_A11', {'pm10'}): 'Q',
            RealtimeTranslator.Key('Q_A11', {'pm1'}): 'Q',
            RealtimeTranslator.Key('Q_A11', {'pm25'}): 'Q',
            RealtimeTranslator.Key('T1_A11'): 'Tsample',
            RealtimeTranslator.Key('T1_A11', {'pm10'}): 'Tsample',
            RealtimeTranslator.Key('T1_A11', {'pm1'}): 'Tsample',
            RealtimeTranslator.Key('T1_A11', {'pm25'}): 'Tsample',
            RealtimeTranslator.Key('T2_A11'): 'Tcase',
            RealtimeTranslator.Key('T2_A11', {'pm10'}): 'Tcase',
            RealtimeTranslator.Key('T2_A11', {'pm1'}): 'Tcase',
            RealtimeTranslator.Key('T2_A11', {'pm25'}): 'Tcase',
            RealtimeTranslator.Key('Fn_A11'): 'spot',
        },

        'aethalometerstatus': {
            RealtimeTranslator.Key('T1_A81'): 'Tcontroller',
            RealtimeTranslator.Key('T2_A81'): 'Tsupply',
            RealtimeTranslator.Key('T3_A81'): 'Tled',
        },

        'cpcstatus': {
            RealtimeTranslator.Key('Q_Q71'): 'Qsample',
            RealtimeTranslator.Key('Q_Q61'): 'Qsample',
            RealtimeTranslator.Key('Q_Q72'): 'Qdrier',
            RealtimeTranslator.Key('Q_Q62'): 'Qdrier',
        },

        'umacstatus': {
            RealtimeTranslator.Key('T_X1'): 'T',
            RealtimeTranslator.Key('V_X1'): 'V',
        },
    }),
}

ozone_data = {
    'raw': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'F1_G81'),
            }, send
        ),

        'ozone': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'X_G81'): 'ozone',
            }, send
        ),

        'status': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T1_G81'): 'Tsample',
                Name(station, 'raw', 'T2_G81'): 'Tlamp',
                Name(station, 'raw', 'P_G81'): 'Psample',
                Name(station, 'raw', 'P1_G81'): 'Psample',
            }, send
        ),

        'cells': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Q1_G81'): 'Qa',
                Name(station, 'raw', 'Q2_G81'): 'Qb',
                Name(station, 'raw', 'C1_G81'): 'Ca',
                Name(station, 'raw', 'C2_G81'): 'Cb',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'WS1_XM1'): 'WS',
                Name(station, 'raw', 'WD1_XM1'): 'WD',
            }, send
        ),
    },
    
    'clean': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'F1_G81'),
            }, send
        ),

        'ozone': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'X_G81'): 'ozone',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'WS1_XM1'): 'WS',
                Name(station, 'clean', 'WD1_XM1'): 'WD',
            }, send
        ),
    },
    
    'avgh': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'F1_G81'),
            }, send
        ),

        'ozone': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'X_G81'): 'ozone',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'WS1_XM1'): 'WS',
                Name(station, 'avgh', 'WD1_XM1'): 'WD',
            }, send
        ),
    },

    'editing': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: EditedContaminationReader(
            start_epoch_ms, end_epoch_ms, station, 'ozone', {
                Name(station, 'clean', 'F1_G81'),
            }, send
        ),

        'ozone': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'ozone', {
                Name(station, 'clean', 'X_G81'): 'ozone',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'WS1_XM1'): 'WS',
                Name(station, 'clean', 'WD1_XM1'): 'WD',
            }, send
        ),
    },

    'realtime': RealtimeTranslator.Data('ozone', {
        'ozone': {
            RealtimeTranslator.Key('X_G81'): 'ozone',
        },

        'status': {
            RealtimeTranslator.Key('T1_G81'): 'Tsample',
            RealtimeTranslator.Key('T2_G81'): 'Tlamp',
            RealtimeTranslator.Key('P_G81'): 'Psample',
            RealtimeTranslator.Key('P1_G81'): 'Psample',
        },

        'cells': {
            RealtimeTranslator.Key('Q1_G81'): 'Qa',
            RealtimeTranslator.Key('Q2_G81'): 'Qb',
            RealtimeTranslator.Key('C1_G81'): 'Ca',
            RealtimeTranslator.Key('C2_G81'): 'Cb',
        },

        'wind': {
            RealtimeTranslator.Key('WS1_XM1'): 'WS',
            RealtimeTranslator.Key('WD1_XM1'): 'WD',
        },
    }),
}

met_data = {
    'raw': {
        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'WS1_XM1'): 'WSambient', Name(station, 'raw', 'WD1_XM1'): 'WDambient',
                Name(station, 'raw', 'WS2_XM1'): 'WS2', Name(station, 'raw', 'WD2_XM1'): 'WD2',
                Name(station, 'raw', 'WS3_XM1'): 'WS3', Name(station, 'raw', 'WD3_XM1'): 'WD3',
            }, send
        ),

        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'U1_XM1'): 'Uambient',
                Name(station, 'raw', 'T1_XM1'): 'Tambient',
                Name(station, 'raw', 'TD1_XM1'): 'TDambient',

                Name(station, 'raw', 'U2_XM1'): 'U2',
                Name(station, 'raw', 'T2_XM1'): 'T2',
                Name(station, 'raw', 'TD2_XM1'): 'TD2',

                Name(station, 'raw', 'U3_XM1'): 'U3',
                Name(station, 'raw', 'T3_XM1'): 'T3',
                Name(station, 'raw', 'TD3_XM1'): 'TD3',
            }, send
        ),

        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_XM1'): 'ambient',
            }, send
        ),

        'precipitation': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'WI_XM1'): 'precipitation',
            }, send
        ),

        'tower': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T2_XM1'): 'Tmiddle',
                Name(station, 'raw', 'T3_XM1'): 'Ttop',
            }, send
        ),
    },
    
    'clean': {
        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'WS1_XM1'): 'WSambient', Name(station, 'clean', 'WD1_XM1'): 'WDambient',
                Name(station, 'clean', 'WS2_XM1'): 'WS2', Name(station, 'clean', 'WD2_XM1'): 'WD2',
                Name(station, 'clean', 'WS3_XM1'): 'WS3', Name(station, 'clean', 'WD3_XM1'): 'WD3',
            }, send
        ),

        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'U1_XM1'): 'Uambient',
                Name(station, 'clean', 'T1_XM1'): 'Tambient',
                Name(station, 'clean', 'TD1_XM1'): 'TDambient',

                Name(station, 'clean', 'U2_XM1'): 'U2',
                Name(station, 'clean', 'T2_XM1'): 'T2',
                Name(station, 'clean', 'TD2_XM1'): 'TD2',

                Name(station, 'clean', 'U3_XM1'): 'U3',
                Name(station, 'clean', 'T3_XM1'): 'T3',
                Name(station, 'clean', 'TD3_XM1'): 'TD3',
            }, send
        ),

        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'P_XM1'): 'ambient',
            }, send
        ),

        'precipitation': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'WI_XM1'): 'precipitation',
            }, send
        ),
    },
    
    'avgh': {
        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'WS1_XM1'): 'WSambient', Name(station, 'avgh', 'WD1_XM1'): 'WDambient',
                Name(station, 'avgh', 'WS2_XM1'): 'WS2', Name(station, 'avgh', 'WD2_XM1'): 'WD2',
                Name(station, 'avgh', 'WS3_XM1'): 'WS3', Name(station, 'avgh', 'WD3_XM1'): 'WD3',
            }, send
        ),

        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'U1_XM1'): 'Uambient',
                Name(station, 'avgh', 'T1_XM1'): 'Tambient',
                Name(station, 'avgh', 'TD1_XM1'): 'TDambient',

                Name(station, 'avgh', 'U2_XM1'): 'U2',
                Name(station, 'avgh', 'T2_XM1'): 'T2',
                Name(station, 'avgh', 'TD2_XM1'): 'TD2',

                Name(station, 'avgh', 'U3_XM1'): 'U3',
                Name(station, 'avgh', 'T3_XM1'): 'T3',
                Name(station, 'avgh', 'TD3_XM1'): 'TD3',
            }, send
        ),

        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'P_XM1'): 'ambient',
            }, send
        ),

        'precipitation': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'WI_XM1'): 'precipitation',
            }, send
        ),
    },
    
    'editing': {
        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'WS1_XM1'): 'WSambient', Name(station, 'clean', 'WD1_XM1'): 'WDambient',
                Name(station, 'clean', 'WS2_XM1'): 'WS2', Name(station, 'clean', 'WD2_XM1'): 'WD2',
                Name(station, 'clean', 'WS3_XM1'): 'WS3', Name(station, 'clean', 'WD3_XM1'): 'WD3',
            }, send
        ),

        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'U1_XM1'): 'Uambient',
                Name(station, 'clean', 'T1_XM1'): 'Tambient',
                Name(station, 'clean', 'TD1_XM1'): 'TDambient',

                Name(station, 'clean', 'U2_XM1'): 'U2',
                Name(station, 'clean', 'T2_XM1'): 'T2',
                Name(station, 'clean', 'TD2_XM1'): 'TD2',

                Name(station, 'clean', 'U3_XM1'): 'U3',
                Name(station, 'clean', 'T3_XM1'): 'T3',
                Name(station, 'clean', 'TD3_XM1'): 'TD3',
            }, send
        ),

        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'P_XM1'): 'ambient',
            }, send
        ),

        'precipitation': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'WI_XM1'): 'precipitation',
            }, send
        ),

        'tower': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'T2_XM1'): 'Tmiddle',
                Name(station, 'clean', 'T3_XM1'): 'Ttop',
            }, send
        ),
    },

    'realtime': RealtimeTranslator.Data('met'),
}

profile_data = {
    'aerosol': aerosol_data,
    'ozone': ozone_data,
    'met': met_data,
}

realtime_translator = RealtimeTranslator.assemble_translator(profile_data)
