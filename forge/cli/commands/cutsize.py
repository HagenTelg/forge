import typing
import asyncio
import logging
import argparse
import time
import numpy as np
from netCDF4 import Dataset, Variable, Dimension, EnumType, VLType
from math import nan, isfinite
from forge.data.attrs import copy as copy_attrs
from forge.data.values import create_and_copy_variable
from forge.data.structure.timeseries import setup_cutsize, cutsize_coordinate, cutsize_variable
from ..execute import Execute, ExecuteStage
from . import ParseCommand, ParseArguments


_LOGGER = logging.getLogger(__name__)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["cut", "cut-size"]
    HELP: str = "select data by cut size"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return not cmd.is_first and not cmd.is_last

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        parser.add_argument('cut',
                            help="cut size to include",
                            nargs='*')
        parser.add_argument('--min',
                            dest='min_size',
                            help="minimum size in um to select")
        parser.add_argument('--max',
                            dest='max_size',
                            help="maximum size in um to select")
        parser.add_argument('--exclude',
                            dest='exclude', action='append',
                            help="cut size to exclude")

        parser.add_argument('--no-squash',
                            dest='no_squash', action='store_true',
                            help="disable squashing single cut sizes into constants")
        parser.add_argument('--remove-time',
                            dest='remove_time', action='store_true',
                            help="remove cut size timed data instead of invalidating")
        parser.add_argument('--invalidate-dimension',
                            dest='keep_dimension', action='store_true',
                            help="invalidate cut size dimensioned data instead of removing")

        parser.add_argument('--require-cut',
                            dest='require_cut', action='store_true',
                            help="remove all data without a cut size set")

    @classmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: Execute,
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        cls.no_extra_args(parser, extra_args)

        def to_size(arg) -> float:
            try:
                v = float(arg)
                if v > 0.0:
                    return v
            except (ValueError, TypeError):
                pass
            check = str(arg).strip().lower()
            if check == "pm1":
                return 1.0
            elif check == "pm10":
                return 10.0
            elif check == "pm25" or check == "pm2.5":
                return 2.5
            elif check == "whole" or check == "inf" or check == "none" or check == "undef":
                return nan
            parser.error(f"Invalid cut size '{arg}'")

        include: typing.Set[float] = set()
        if args.cut:
            for a in args.cut:
                for c in a.split(','):
                    include.add(to_size(c))

        exclude: typing.Set[float] = set()
        if args.exclude:
            for a in args.exclude:
                for c in a.split(','):
                    exclude.add(to_size(c))

        min_size = 0.0
        if args.min_size:
            min_size = to_size(args.min_size)

        max_size = nan
        if args.max_size:
            max_size = to_size(args.max_size)

        execute.install(_CutSizeSelectStage(
            execute,
            include, exclude, min_size, max_size,
            args.no_squash, args.remove_time, args.keep_dimension,
            args.require_cut,
        ))


class _CutSizeSelectorContext:
    def __init__(self, stage: "_CutSizeSelectStage", cut_variable: Variable, cut_dimension: typing.Optional[Dimension],
                 selection: np.ndarray):
        self.stage = stage
        self.source_variable: typing.Optional[Variable] = None
        self.source_dimension: typing.Optional[Dimension] = None
        self._selected_count: int = 0
        self.squash: bool = False
        self.selection: typing.Union[bool, np.ndarray] = False

        if not np.issubdtype(cut_variable.dtype, np.floating):
            _LOGGER.debug("Invalid cut size data type")
            self.selection = False
            return

        if len(cut_variable.dimensions) == 0:
            if cut_dimension is not None:
                _LOGGER.debug("Invalid dimension for constant cut size")
                self.selection = False
            else:
                self.selection = bool(selection[0])
                self.source_variable = cut_variable
        elif len(cut_variable.dimensions) == 1 and cut_dimension is not None and cut_variable.dimensions[0] == cut_dimension.name:
            self.source_variable = cut_variable
            self.source_dimension = cut_dimension

            self._selected_count = np.count_nonzero(selection)
            if self._selected_count == 0:
                self.selection = False
            elif self._selected_count == cut_dimension.size:
                if self._selected_count == 1 and not self.stage.keep_dimension:
                    self.squash = True
                self.selection = True
            else:
                if self._selected_count == 1 and not self.stage.keep_dimension:
                    self.squash = True
                self.selection = selection
        elif len(cut_variable.dimensions) == 1 and cut_dimension is None and cut_variable.dimensions[0] == 'time':
            self.source_variable = cut_variable

            self._selected_count = np.count_nonzero(selection)
            selected_sizes = cut_variable[:].data[selection]
            sized_selected_data = np.isfinite(selected_sizes)
            unique_count = np.unique(selected_sizes[sized_selected_data]).shape[0]
            if np.any(np.invert(sized_selected_data)):
                unique_count += 1
            if self._selected_count == 0:
                self.selection = False
            elif self._selected_count == cut_variable.shape[0]:
                if unique_count == 1 and self.stage.remove_time:
                    self.squash = True
                self.selection = True
            else:
                if unique_count == 1 and self.stage.remove_time:
                    self.squash = True
                self.selection = selection
        else:
            _LOGGER.debug("Invalid shape for cut size variable")
            self.selection = False

    @property
    def total_selection(self) -> bool:
        return isinstance(self.selection, bool)

    @property
    def full_passthrough(self) -> bool:
        if self.source_variable is None:
            return True
        if self.total_selection and self.selection:
            return True
        return False

    @property
    def have_dimension(self) -> bool:
        return self.source_dimension is not None and not self.squash

    @property
    def _selected_cut_size_data(self) -> np.ndarray:
        return self.source_variable[:].data[self.selection]

    def create(self, destination: Dataset) -> None:
        if self.squash:
            var = destination.createVariable("cut_size", "f8", (), fill_value=nan)
            setup_cutsize(var)
            var.coverage_content_type = "referenceInformation"
            var[0] = float(self._selected_cut_size_data[0])
            return

        if self.source_dimension is not None:
            if self.stage.keep_dimension:
                var = cutsize_coordinate(destination, self.source_dimension.size)
                var[:] = self.source_variable[:].data
            else:
                var = cutsize_coordinate(destination, self._selected_count)
                var[:] = self._selected_cut_size_data
        else:
            var = cutsize_variable(destination)
            if self.stage.remove_time:
                var[:] = self._selected_cut_size_data
            else:
                var[:] = self.source_variable[:].data

    def _offset_selection(self, dimension_offset: int) -> typing.Tuple:
        return tuple(([slice(None)] * dimension_offset) + [self.selection])

    def _copy_removed(self, source_variable: Variable, destination_variable: Variable,
                      dimension_offset: int = 0) -> None:
        selected_data = source_variable[:].data[self._offset_selection(dimension_offset)]
        if isinstance(destination_variable.datatype, VLType):
            for idx in np.ndindex(selected_data.shape):
                destination_variable[idx] = selected_data[idx]
        else:
            destination_variable[:] = selected_data

    def _copy_invalidated(self, source_variable: Variable, destination_variable: Variable,
                          dimension_offset: int = 0) -> None:
        if isinstance(destination_variable.datatype, VLType):
            for idx in np.ndindex(source_variable.shape):
                if not self.selection[idx[dimension_offset]]:
                    continue
                destination_variable[idx] = source_variable[idx]
        else:
            # Time already shaped by cut size create, so this will be correctly sized and filled
            raw = destination_variable[:].data
            sel = self._offset_selection(dimension_offset)
            raw[sel] = source_variable[:].data[sel]
            destination_variable[:] = raw

    def _apply_dimension(self, source_variable: Variable, destination: Dataset, dimension_idx: int) -> None:
        if self.squash:
            var = create_and_copy_variable(
                source_variable, destination,
                dimensions=source_variable.dimensions[:dimension_idx] + source_variable.dimensions[dimension_idx+1:],
                copy_values=False,
            )
            ancillary_variables = set(getattr(source_variable, 'ancillary_variables', "").split())
            ancillary_variables.add('cut_size')
            var.ancillary_variables = " ".join(sorted(ancillary_variables))

            selected_data = source_variable[:].data[self._offset_selection(dimension_idx)]
            selected_data = np.reshape(selected_data, (
                *selected_data.shape[:dimension_idx], *selected_data.shape[dimension_idx+1:],
            ))
            if isinstance(var.datatype, VLType):
                for idx in np.ndindex(selected_data.shape):
                    var[idx] = selected_data[idx]
            else:
                var[...] = selected_data
            return

        var = create_and_copy_variable(source_variable, destination, copy_values=False)
        if not self.stage.keep_dimension:
            self._copy_removed(source_variable, var, dimension_idx)
        else:
            self._copy_invalidated(source_variable, var, dimension_idx)

    def _apply_sibling(self, source_variable: Variable, destination: Dataset) -> None:
        var = create_and_copy_variable(source_variable, destination, copy_values=False)
        if self.stage.remove_time:
            self._copy_removed(source_variable, var)
        else:
            self._copy_invalidated(source_variable, var)

    def apply(self, source_variable: Variable, destination: Dataset) -> None:
        if source_variable.name == 'time' and len(source_variable.dimensions) == 1 and source_variable.dimensions[0] == 'time':
            if self.source_dimension is None and self.stage.remove_time:
                self._apply_sibling(source_variable, destination)
                return
            create_and_copy_variable(source_variable, destination)
            return

        if source_variable.name == 'cut_size':
            return

        if 'time' not in source_variable.dimensions:
            create_and_copy_variable(source_variable, destination)
            return

        try:
            cut_dimension = source_variable.dimensions.index('cut_size')
        except ValueError:
            cut_dimension = None

        if cut_dimension is not None:
            self._apply_dimension(source_variable, destination, cut_dimension)
            return

        if 'cut_size' in getattr(source_variable, 'ancillary_variables', "").split() and self.source_dimension is None:
            self._apply_sibling(source_variable, destination)
            return

        if not self.stage.require_cut and not self.stage.remove_time:
            create_and_copy_variable(source_variable, destination)
        elif source_variable.name == 'system_flags':
            # Not explicitly split, but always pass it through
            if self.source_dimension is not None:
                create_and_copy_variable(source_variable, destination)
            else:
                self._apply_sibling(source_variable, destination)


class _CutSizeSelectStage(ExecuteStage):
    def __init__(self, execute: Execute, include: typing.Set[float], exclude: typing.Set[float],
                 min_size: float, max_size: float, disable_squash: bool = False,
                 remove_time: bool = False, keep_dimension: bool = False,
                 require_cut: bool = False):
        super().__init__(execute)
        self.include = include
        self.exclude = exclude
        self.min_size = min_size
        self.max_size = max_size
        self.disable_squash = disable_squash
        self.remove_time = remove_time
        self.keep_dimension = keep_dimension
        self.require_cut = require_cut

    def _select_size(self, size_data: np.ndarray) -> np.ndarray:
        cut_selected_data = np.isfinite(size_data)
        whole_air_data = np.invert(cut_selected_data)

        if isfinite(self.min_size):
            min_selector = np.any((
                whole_air_data,
                size_data >= self.min_size
            ), axis=0)
        else:
            min_selector = whole_air_data

        if isfinite(self.max_size):
            max_selector = size_data <= self.max_size
        else:
            max_selector = None

        if self.include:
            include_selector = np.any((
                *[(size_data == s if isfinite(s) else whole_air_data) for s in self.include],
            ), axis=0)
        else:
            include_selector = None

        if self.exclude:
            exclude_selector = np.all((
                *[(np.all(
                    (cut_selected_data, size_data != s), axis=0) if isfinite(s) else np.isfinite(size_data)
                   ) for s in self.exclude],
            ), axis=0)
        else:
            exclude_selector = None

        return np.all((
            min_selector,
            *((max_selector,) if max_selector is not None else ()),
            *((include_selector,) if include_selector is not None else ()),
            *((exclude_selector,) if exclude_selector is not None else ()),
        ), axis=0)

    def _process_file(self, input_file: Dataset, output_file: Dataset) -> None:
        def _process_group(
                source: Dataset,
                destination: Dataset,
                parent_context: typing.Optional[_CutSizeSelectorContext] = None,
        ) -> None:
            copy_attrs(source, destination)

            time_dimension = source.dimensions.get('time')
            if time_dimension is not None:
                if time_dimension.isunlimited() or self.remove_time:
                    destination.createDimension('time', None)
                else:
                    destination.createDimension('time', time_dimension.size)

            cut_var = source.variables.get('cut_size')
            cut_dim = source.dimensions.get('cut_size')
            if cut_var is not None:
                context = _CutSizeSelectorContext(
                    self, cut_var, cut_dim,
                    self._select_size(cut_var[:].data)
                )
                if self.disable_squash:
                    context.squash = False
            else:
                context = parent_context

            def passthrough_dimensions():
                for name, source_dimension in source.dimensions.items():
                    if name == 'time':
                        continue
                    if source_dimension.isunlimited():
                        destination.createDimension(name, None)
                    else:
                        destination.createDimension(name, source_dimension.size)

            def passthrough_variables():
                for source_variable in source.variables.values():
                    create_and_copy_variable(source_variable, destination)

            def process_contained_groups():
                for name, source_group in source.groups.items():
                    destination_group = destination.createGroup(name)
                    _process_group(source_group, destination_group, context)

            if context is None:
                passthrough_dimensions()
                if self.require_cut:
                    for source_variable in source.variables.values():
                        if 'time' in source_variable.dimensions:
                            continue
                        create_and_copy_variable(source_variable, destination)
                else:
                    passthrough_variables()
                process_contained_groups()
                return

            if context.full_passthrough:
                passthrough_dimensions()
                passthrough_variables()
                process_contained_groups()
                return

            if cut_var is not None:
                context.create(destination)

            for name, source_dimension in source.dimensions.items():
                if name == 'time':
                    continue
                if source_dimension == context.source_dimension:
                    continue
                if source_dimension.isunlimited():
                    destination.createDimension(name, None)
                else:
                    destination.createDimension(name, source_dimension.size)

            for source_variable in source.variables.values():
                context.apply(source_variable, destination)

            process_contained_groups()

        _process_group(input_file, output_file)

    async def __call__(self) -> None:
        begin_time = time.monotonic()

        with self.data_replacement() as output_path:
            for input_file in self.data_file_progress("Selecting cut size data"):
                output_file = Dataset(str(output_path / input_file.name), 'w', format='NETCDF4')
                input_file = Dataset(str(input_file), 'r')
                try:
                    self._process_file(input_file, output_file)
                finally:
                    input_file.close()
                    output_file.close()

        _LOGGER.debug("Cut size selection completed in %.3f seconds", time.monotonic() - begin_time)
