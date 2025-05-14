import typing
import argparse
import os
from .execute import Execute


class ParseArguments:
    class SubCommand:
        def __init__(self, args: typing.List[str], parse: "ParseArguments", idx: int):
            self.args = args
            self.parse = parse
            self.idx = idx

        @property
        def is_first(self) -> bool:
            return self.idx == 0

        @property
        def is_last(self) -> bool:
            return self.idx == len(self.parse.sub_commands) - 1

    def __init__(self, raw: typing.List[str]):
        self.prog = "forge-data-command"
        if len(raw) > 0:
            self.prog = os.path.basename(raw[0])

        command_args: typing.List[typing.List[str]] = [list()]
        for arg_idx in range(1, len(raw)):
            arg = raw[arg_idx]
            if arg == '--' or arg == '|':
                command_args.append(list())
                continue
            elif arg == '\\|':
                command_args[-1].append('|')
                continue
            elif arg == '---':
                command_args[-1].append('--')
                continue
            elif arg == '----':
                command_args[-1].extend(raw[arg_idx + 1:])
                break
            command_args[-1].append(arg)

        self.sub_commands: typing.List[ParseArguments.SubCommand] = list()
        for args in command_args:
            self.sub_commands.append(self.SubCommand(args, self, len(self.sub_commands)))

    def __call__(self) -> Execute:
        assert len(self.sub_commands) != 0

        from .commands import available_commands, ParseCommand

        all_commands = available_commands()

        exec = Execute()
        for cmd in self.sub_commands:
            if cmd.is_first:
                parser = argparse.ArgumentParser(
                    prog=self.prog,
                    description="Forge data command processor."
                )
                parser.add_argument('--debug',
                                    dest='debug', action='store_true',
                                    help="enable debug output")
                parser.add_argument('--temp-dir',
                                    dest='temp_dir',
                                    help="temporary file root directory")
                group = parser.add_mutually_exclusive_group()
                group.add_argument('--archive-host',
                                   dest='archive_tcp_server',
                                   help="archive server host")
                group.add_argument('--archive-socket',
                                   dest='archive_unix_socket',
                                   help="archive_archive server Unix socket")
                parser.add_argument('--archive-port',
                                    dest='archive_tcp_port',
                                    type=int,
                                    help="archive server port")
            else:
                parser = argparse.ArgumentParser(
                    prog=f"COMMAND{cmd.idx + 1}",
                )

            subparsers = parser.add_subparsers(dest='command')

            parse_lookup: typing.Dict[str, typing.Tuple[typing.Type[ParseCommand], argparse.ArgumentParser]] = dict()
            for parse in all_commands:
                if not parse.available(cmd, exec):
                    continue
                subcommand_names = parse.COMMANDS
                if len(subcommand_names) > 1:
                    cmd_subparse = subparsers.add_parser(subcommand_names[0],
                                                         aliases=subcommand_names[1:],
                                                         help=parse.HELP)
                else:
                    cmd_subparse = subparsers.add_parser(subcommand_names[0],
                                                         help=parse.HELP)
                parse.install(cmd, exec, cmd_subparse)
                for name in subcommand_names:
                    assert name not in parse_lookup
                    parse_lookup[name] = (parse, cmd_subparse)

            args, extra_args = parser.parse_known_args(cmd.args)

            if cmd.is_first:
                if args.debug:
                    from forge.log import set_debug_logger
                    set_debug_logger()
                if args.archive_tcp_server:
                    exec.set_archive_tcp(args.archive_tcp_server, args.archive_tcp_port)
                elif args.archive_unix_socket:
                    exec.set_archive_unix(args.archive_unix_socket)
                if args.temp_dir:
                    from pathlib import Path
                    temp_dir = Path(args.temp_dir)
                    if not temp_dir.is_dir():
                        parser.error("invalid temporary directory")
                    exec.temp_dir_root = temp_dir

            if not args.command:
                parser.error("no command specified")
            parse, cmd_subparse = parse_lookup[args.command]
            parse.instantiate(cmd, exec, cmd_subparse, args, extra_args)

        return exec
