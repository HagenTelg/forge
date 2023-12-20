import typing
import logging
from netCDF4 import Variable
from numpy import uint64

_LOGGER = logging.getLogger(__name__)


def parse_flags(contents: Variable) -> typing.Dict[int, str]:
    flag_meanings = getattr(contents, 'flag_meanings', "").strip().split()
    if not flag_meanings:
        return dict()
    flag_masks = getattr(contents, 'flag_masks', None)
    if flag_masks is None:
        _LOGGER.warning("Flags variable (%s) with no flags masks", contents.name)
        return dict()
    result = dict()
    for i in range(len(flag_meanings)):
        flag_name = flag_meanings[i]
        if len(flag_meanings) == 1 and i == 0:
            flag_bits = int(flag_masks)
        elif i < len(flag_masks):
            flag_bits = int(flag_masks[i])
        else:
            break
        result[flag_bits] = flag_name
    return result


def declare_flag(var: Variable, flag_name: str, preferred_bit: typing.Optional[int] = None) -> int:
    existing_flags = parse_flags(var)
    taken_mask = 0
    for bit, name in existing_flags.items():
        if name == flag_name:
            return bit
        taken_mask |= bit

    if preferred_bit and (taken_mask & preferred_bit) == 0:
        selected_bit = preferred_bit
    else:
        for i in range(64):
            check_bit = 1 << i
            if (taken_mask & check_bit) == 0:
                selected_bit = check_bit
                break
        else:
            raise ValueError("No available bit in variable (%s) for flag %s", var.name, flag_name)

    existing_flags[selected_bit] = flag_name
    bits: typing.List[int] = list(existing_flags.keys())
    bits.sort()

    var.valid_range = [uint64(0), uint64(taken_mask)]
    var.flag_masks = [uint64(v) for v in bits]
    var.flag_meanings = " ".join([existing_flags[b].replace(" ", "_") for b in bits])

    return selected_bit
