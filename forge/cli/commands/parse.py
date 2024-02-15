import typing
import enum
from io import StringIO


class GroupType(enum.Enum):
    PLAIN = enum.auto()
    SPECIAL = enum.auto()


_SPECIAL_BRACKET: typing.Dict[str, str] = {
    '(': ')',
    '{': '}',
    '[': ']',
}


def split_grouped_parts(raw: str) -> typing.List[typing.Tuple[GroupType, str]]:
    result: typing.List[typing.Tuple[GroupType, str]] = list()
    current_contents = StringIO()
    bracket_stack: typing.List[str] = list()

    idx = 0
    while idx < len(raw):
        ch = raw[idx]
        if ch == '\\':
            current_contents.write(ch)
            idx += 1
            if idx < len(raw):
                current_contents.write(raw[idx])
                idx += 1
            continue

        close_bracket = _SPECIAL_BRACKET.get(ch)
        if close_bracket:
            if len(bracket_stack) == 0:
                add = current_contents.getvalue()
                if add:
                    result.append((GroupType.PLAIN, add))
                    current_contents.truncate(0)
                    current_contents.seek(0)
            bracket_stack.append(close_bracket)
            current_contents.write(ch)
        elif len(bracket_stack) != 0 and ch == bracket_stack[-1]:
            current_contents.write(ch)
            bracket_stack.pop()
            if len(bracket_stack) == 0:
                result.append((GroupType.SPECIAL, current_contents.getvalue()))
                current_contents.truncate(0)
                current_contents.seek(0)
        else:
            current_contents.write(ch)

        idx += 1

    remaining = current_contents.getvalue()
    if remaining:
        if len(bracket_stack) == 0:
            result.append((GroupType.PLAIN, remaining))
        else:
            result.append((GroupType.SPECIAL, remaining))

    if not result:
        result.append((GroupType.PLAIN, ""))

    return result


def split_tagged_regex(raw: str) -> typing.List[typing.Tuple[str, str]]:
    result: typing.List[typing.Tuple[str, str]] = list()
    trailing_tag: typing.Optional[str] = None
    trailing_value: str = ""
    for group_type, contents in split_grouped_parts(raw):
        if group_type == GroupType.PLAIN:
            values = contents.split(",")
            if trailing_tag is not None:
                trailing_value += values[0]
                values = values[1:]
                result.append((trailing_tag, trailing_value))
                trailing_tag = None
                trailing_value = ""
                if not values:
                    continue

            for plain in values[:-1]:
                tag_value = plain.split(":", 1)
                if len(tag_value) == 2:
                    result.append((tag_value[0].strip(), tag_value[1]))
                else:
                    result.append(("", plain))
            tag_value = values[-1].split(":", 1)
            if len(tag_value) == 2:
                trailing_tag = tag_value[0].strip()
                trailing_value = tag_value[1]
            else:
                trailing_tag = ""
                trailing_value = tag_value[0]
        elif group_type == GroupType.SPECIAL:
            if trailing_tag is None:
                trailing_tag = ""
            trailing_value += contents
        else:
            assert False

    if trailing_tag is not None:
        result.append((trailing_tag, trailing_value))

    return result
