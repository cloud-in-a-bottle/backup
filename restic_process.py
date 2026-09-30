"""Settle owned restic subprocesses before releasing files or operation locks."""

from __future__ import annotations

import asyncio
import os
import signal

from operations import drain


class ReadError(ValueError):
    """Fixed error codes; subprocess output never enters an exception."""
    pass


async def read(args, env, *, limit, timeout):
    """Read bounded stdout, suppress stderr, and settle the process on every exit."""
    proc = await asyncio.create_subprocess_exec(
        "restic", *args, env=env, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL, start_new_session=True,
    )
    try:
        async with asyncio.timeout(timeout):
            output = bytearray()
            while chunk := await proc.stdout.read(65536):
                if len(output) + len(chunk) > limit:
                    raise ReadError("size")
                output.extend(chunk)
            if await proc.wait() != 0:
                raise ReadError("exit")
            return bytes(output)
    except TimeoutError:
        raise ReadError("timeout") from None
    finally:
        if proc.returncode is None or not proc.stdout.at_eof():
            await kill_and_drain(proc)


def kill_group(proc: asyncio.subprocess.Process) -> None:
    # Every caller starts a new session. Backend helpers (SSH/rclone) must not
    # retain inherited pipe descriptors or keep writing after restic is killed.
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


async def finish_communication(task: asyncio.Task) -> None:
    async def finish() -> None:
        try:
            await task
        except Exception:
            # The caller already has the operation's original failure. Retrieve
            # the terminal exception without rendering private process output.
            pass

    await drain(finish())


async def kill_and_drain(proc: asyncio.subprocess.Process) -> None:
    kill_group(proc)

    async def read_to_eof(stream: asyncio.StreamReader | None) -> None:
        if stream is not None:
            while await stream.read(65536):
                pass

    async def finish() -> None:
        # Reading to EOF also unpauses full pipe transports. Waiting alone can
        # hang indefinitely even after the process has already exited.
        await asyncio.gather(read_to_eof(proc.stdout), read_to_eof(proc.stderr), proc.wait())

    await drain(finish())
