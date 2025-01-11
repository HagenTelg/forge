import typing
import asyncio
import shutil
from pathlib import Path


async def cleanup_working_directory(working_directory: Path) -> None:
    def rm_file(file: Path) -> None:
        if file.name == '.' or file.name == '..':
            return

        if file.is_dir():
            try:
                shutil.rmtree(str(file), ignore_errors=True)
            except OSError:
                pass
            return

        try:
            file.unlink(missing_ok=True)
        except OSError:
            pass

    for file in working_directory.iterdir():
        await asyncio.get_event_loop().run_in_executor(None, rm_file, file)
