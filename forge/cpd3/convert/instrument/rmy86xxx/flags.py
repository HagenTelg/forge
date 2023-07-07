import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'abnormal_status': CPD3Flag("AbnormalStatus", "Non-zero status code reported (insufficient samples or measurement error)"),
}
