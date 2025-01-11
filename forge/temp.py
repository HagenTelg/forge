import typing
import asyncio
import os
import shutil
from tempfile import TemporaryDirectory


class WorkingDirectory(TemporaryDirectory):
    async def __aenter__(self) -> str:
        return super().__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await asyncio.get_event_loop().run_in_executor(None, super().__exit__,
                                                              exc_type, exc_val, exc_tb)

    async def make_empty(self) -> None:
        if os.supports_dir_fd:
            dir_fd = os.open(self.name, os.O_RDONLY)
            try:
                def rmdir(name: str) -> None:
                    shutil.rmtree(name, ignore_errors=True, dir_fd=dir_fd)

                def rmfile(name: str) -> None:
                    os.remove(name, dir_fd=dir_fd)

                for file in os.scandir(dir_fd):
                    if file.is_dir():
                        await asyncio.get_event_loop().run_in_executor(None, rmdir, file.name)
                    else:
                        await asyncio.get_event_loop().run_in_executor(None, rmfile, file.name)
            finally:
                os.close(dir_fd)
        else:
            for file in os.scandir(self.name):
                file_path = os.path.join(self.name, file.name)
                if file.is_dir():
                    await asyncio.get_event_loop().run_in_executor(None, shutil.rmtree, file_path)
                else:
                    await asyncio.get_event_loop().run_in_executor(None, os.remove, file_path)
