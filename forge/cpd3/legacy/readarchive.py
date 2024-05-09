import typing
import subprocess
from forge.cpd3.archive.selection import Selection
from forge.cpd3.datareader import Identity, deserialize_archive_value


def read_archive(selections: typing.Iterable[Selection]) -> typing.List[typing.Tuple[Identity, typing.Any, float]]:
    p = subprocess.Popen(['cpd3_forge_interface', 'lossless_read'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    for sel in selections:
        p.stdin.write(sel.serialize())
    p.stdin.close()
    data = p.stdout.read()
    p.wait()

    result: typing.List[typing.Tuple[Identity, typing.Any, float]] = list()
    data = bytearray(data)
    while data:
        ident, value, modified, _ = deserialize_archive_value(data)
        result.append((ident, value, modified))
    return result
