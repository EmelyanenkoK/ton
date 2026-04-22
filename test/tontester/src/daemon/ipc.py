import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast, final

from pydantic import BaseModel

from tl import JSONSerializable

from .storage import TestMetadata

logger = logging.getLogger(__name__)


class PingResponse(BaseModel):
    status: str
    message: str


class RegisterResponse(BaseModel):
    status: str
    run_id: str
    prometheus_url: str
    grafana_url: str


class DaemonInfo(BaseModel):
    prometheus_url: str
    grafana_url: str


type RegisterCallback = Callable[[str, TestMetadata], Awaitable[None]]
type CompleteCallback = Callable[[str], Awaitable[None]]


@final
class IPCServer:
    def __init__(
        self,
        socket_path: Path,
        info_provider: Callable[[], DaemonInfo],
        on_register: RegisterCallback,
        on_complete: CompleteCallback,
    ):
        self.socket_path: Path = socket_path
        self._info_provider: Callable[[], DaemonInfo] = info_provider
        self._on_register: RegisterCallback = on_register
        self._on_complete: CompleteCallback = on_complete
        self._server: asyncio.Server | None = None
        self._active_writers: set[asyncio.StreamWriter] = set()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._active_writers.add(writer)
        run_id: str | None = None
        try:
            line = await reader.readline()
            if not line:
                return

            hello = cast(JSONSerializable, json.loads(line.decode()))
            if not isinstance(hello, dict):
                return

            command = hello.get("command")
            if command == "ping":
                response = PingResponse(status="ok", message="pong")
                writer.write((response.model_dump_json() + "\n").encode())
                await writer.drain()
                return

            run_id_raw = hello.get("run_id")
            if not isinstance(run_id_raw, str):
                return
            run_id = run_id_raw

            metadata_raw = hello.get("metadata", {})
            if not isinstance(metadata_raw, dict):
                return

            metadata = TestMetadata.model_validate(metadata_raw)
            await self._on_register(run_id, metadata)

            info = self._info_provider()
            response = RegisterResponse(
                status="ok",
                run_id=run_id,
                prometheus_url=info.prometheus_url,
                grafana_url=info.grafana_url,
            )
            writer.write((response.model_dump_json() + "\n").encode())
            await writer.drain()

            # Keep connection alive until the client disconnects.
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break

        except Exception:
            logger.exception("IPC client error")
        finally:
            self._active_writers.discard(writer)
            if run_id is not None:
                await self._on_complete(run_id)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self) -> None:
        if self.socket_path.exists():
            self.socket_path.unlink()
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        self._server = await asyncio.start_unix_server(
            self.handle_client, path=str(self.socket_path)
        )

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            # Force-close any still-connected clients so wait_closed() returns.
            for writer in list(self._active_writers):
                if not writer.is_closing():
                    try:
                        writer.close()
                    except Exception:
                        pass
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("IPC server wait_closed() timed out; forcing shutdown")
        if self.socket_path.exists():
            self.socket_path.unlink()


@final
class IPCClient:
    def __init__(self, socket_path: Path):
        self.socket_path: Path = socket_path
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect_and_register(self, run_id: str, metadata: TestMetadata) -> RegisterResponse:
        self._reader, self._writer = await asyncio.open_unix_connection(str(self.socket_path))

        hello = {"run_id": run_id, "metadata": metadata.model_dump()}
        self._writer.write((json.dumps(hello) + "\n").encode())
        await self._writer.drain()

        line = await self._reader.readline()
        response = RegisterResponse.model_validate_json(line)
        if response.status != "ok":
            raise RuntimeError(f"Failed to register run: {response}")
        return response

    async def disconnect(self) -> None:
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._reader = None
            self._writer = None

    async def ping(self) -> PingResponse:
        reader, writer = await asyncio.open_unix_connection(str(self.socket_path))
        try:
            writer.write((json.dumps({"command": "ping"}) + "\n").encode())
            await writer.drain()

            line = await reader.readline()
            return PingResponse.model_validate_json(line)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
