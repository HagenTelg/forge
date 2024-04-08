import typing
from json import loads as from_json
from netCDF4 import Dataset
from forge.cpd3.identity import Identity


def convert_data_passed(station: str, root: Dataset) -> typing.List[typing.Tuple[Identity, typing.Any, float]]:
    result_passed: typing.List[typing.Tuple[Identity, typing.Any, float]] = list()

    passed = root.groups["passed"]
    pass_start = passed.variables["start_time"]
    pass_end = passed.variables["end_time"]
    pass_at = passed.variables["pass_time"]
    raw_profile = passed.variables["profile"]
    comment = passed.variables["comment"]
    raw_auxiliary = passed.variables["auxiliary_data"]

    profile_map: typing.Dict[int, str] = dict()
    for name, value in raw_profile.datatype.enum_dict.items():
        profile_map[value] = str(name)

    for i in range(len(pass_start)):
        ident = Identity(
            station=station,
            archive='passed',
            variable=profile_map[int(raw_profile[i])],
            start=float(pass_start[i]) / 1000.0,
            end=float(pass_end[i]) / 1000.0,
        )

        pass_run_at = float(pass_at[i]) / 1000.0

        passed_data: typing.Dict[str, typing.Any] = {
            "Information": {
                "At": pass_run_at,
                "By": "forge.pass",
            }
        }
        pass_comment = str(comment[i])
        if pass_comment:
            passed_data["Comment"] = pass_comment

        aux = str(raw_auxiliary[i])
        if aux:
            aux = from_json(aux)
            passed_data["Information"]["Auxiliary"] = aux

        result_passed.append((ident, passed_data, pass_run_at))

    return result_passed
