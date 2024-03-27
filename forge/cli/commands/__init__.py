import typing
import asyncio
import argparse
from abc import ABC, abstractmethod
from ..arguments import ParseArguments

if typing.TYPE_CHECKING:
    from ..execute import Execute


class ParseCommand(ABC):
    COMMANDS: typing.List[str] = None
    HELP: str = None

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return True

    @classmethod
    @abstractmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        pass

    @classmethod
    def no_extra_args(cls, parser: argparse.ArgumentParser, extra_args: typing.List[str]) -> None:
        if extra_args:
            parser.error(f"Unrecognized arguments: {', '.join(extra_args)}")

    @classmethod
    @abstractmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        pass


def available_commands() -> typing.List[typing.Type[ParseCommand]]:
    result: typing.List[typing.Type[ParseCommand]] = list()

    from .export import Command as ExportCommand
    result.append(ExportCommand)
    from .get import Command as GetCommand
    result.append(GetCommand)
    from .average import Command as AverageCommand
    result.append(AverageCommand)
    from .netcdf import Command as NetCDFCommand
    result.append(NetCDFCommand)
    from .importcmd import Command as ImportCommand
    result.append(ImportCommand)
    from .edit import Command as EditCommand
    result.append(EditCommand)
    from .contamination import Command as ContaminationCommand
    result.append(ContaminationCommand)
    from .cutsize import Command as CutSizeCommand
    result.append(CutSizeCommand)
    from .select import Command as SelectCommand
    result.append(SelectCommand)

    return result
