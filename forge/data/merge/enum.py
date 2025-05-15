import typing
import netCDF4
import numpy as np


class MergeEnum:
    def __init__(self, name: str):
        self.name = name
        self.dtype: typing.Optional[netCDF4.EnumType] = None
        self.values: typing.Dict[str, int] = dict()

    def incorporate_structure(self, contents: netCDF4.EnumType) -> None:
        if not self.values:
            for name, value in contents.enum_dict.items():
                self.values[name] = int(value)
        else:
            for name, value in contents.enum_dict.items():
                value = int(value)
                if name in self.values:
                    continue
                next_value = 0
                hit_value = False
                for check in self.values.values():
                    next_value = max(next_value, check+1)
                    if check == value:
                        hit_value = True
                if hit_value:
                    value = next_value
                self.values[name] = value

    @property
    def storage_dtype(self):
        value_min = min(0, *self.values.values())
        value_max = max(0, *self.values.values())

        for check_type in (np.uint8, np.int8, np.uint16, np.int16, np.uint32, np.int32, np.uint64):
            ti = np.iinfo(check_type)
            if ti.min <= value_min and ti.max >= value_max:
                return check_type
        return np.int64

    def declare_structure(self, root: netCDF4.Dataset) -> None:
        self.dtype = root.createEnumType(self.storage_dtype, self.name, self.values)

    @staticmethod
    def _apply_map(source_enum: typing.Dict[str, int], dest_enum: typing.Dict[str, int],
                   data: np.ndarray, dtype) -> typing.Optional[np.ndarray]:
        for name, value in source_enum.items():
            if dest_enum.get(name) == value:
                continue
            break
        else:
            return None

        converted = np.full(data.shape, min(dest_enum.values()), dtype=dtype)
        for name, value in source_enum.items():
            converted[data == value] = dest_enum[name]
        return converted

    @staticmethod
    def map_variables(source: netCDF4.Variable, destination: netCDF4.Variable,
                      data: np.ndarray) -> typing.Optional[np.ndarray]:
        return MergeEnum._apply_map(
            source.datatype.enum_dict,
            destination.datatype.enum_dict,
            data,
            destination.dtype
        )

    def apply(self, source: netCDF4.Variable, data: np.ndarray, copy: bool = True) -> np.ndarray:
        target_dtype = self.storage_dtype
        mapped = self._apply_map(
            source.datatype.enum_dict,
            self.values,
            data,
            target_dtype
        )
        if mapped is not None:
            return mapped
        return data.astype(target_dtype, copy=copy)
