import asyncio
import logging
import signal
from pathlib import Path
from typing import final

import uvicorn
from pydantic import ValidationError

from .api import create_app
from .config import ConfigWatcher, DaemonConfig
from .container import ScrapeTarget, ServiceManager
from .ipc import DaemonInfo, IPCServer
from .sqlite_storage import SQLiteStorage
from .storage import TestMetadata

logger = logging.getLogger(__name__)


@final
class DashboardDaemon:
    def __init__(self, config_path: Path, socket_path: Path, frontend_dir: Path):
        self.config_path: Path = config_path
        self.socket_path: Path = socket_path
        self.frontend_dir: Path = frontend_dir

        self._storage: SQLiteStorage = SQLiteStorage()
        self._ipc_server: IPCServer = IPCServer(
            socket_path,
            info_provider=self._daemon_info,
            on_register=self._on_run_register,
            on_complete=self._on_run_complete,
        )

        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._config_change_event: asyncio.Event = asyncio.Event()
        self._current_config: DaemonConfig | None = None
        self._new_config: DaemonConfig | None = None

        self._service_manager: ServiceManager | None = None
        self._scrape_lock: asyncio.Lock = asyncio.Lock()

        self._config_watcher: ConfigWatcher = ConfigWatcher(config_path, self._on_config_change)

    def _daemon_info(self) -> DaemonInfo:
        cfg = self._current_config
        if cfg is None:
            return DaemonInfo(prometheus_url="", grafana_url="")
        return DaemonInfo(
            prometheus_url=f"http://{cfg.host}:{cfg.prometheus_port}",
            grafana_url=f"http://{cfg.host}:{cfg.grafana_port}",
        )

    def _on_config_change(self, new_config: DaemonConfig | ValidationError | None) -> None:
        if new_config is None:
            self._shutdown_event.set()
        elif isinstance(new_config, ValidationError):
            logger.error(f"Invalid configuration: {new_config}")
        elif new_config != self._new_config:
            self._new_config = new_config
            self._config_change_event.set()

    async def _on_run_register(self, run_id: str, metadata: TestMetadata) -> None:
        await self._storage.register_run(run_id, metadata)
        await self._sync_scrape_targets()
        logger.info(f"Registered run {run_id} with {len(metadata.nodes)} node(s)")

    async def _on_run_complete(self, run_id: str) -> None:
        await self._storage.update_run_status(run_id, "completed")
        await self._sync_scrape_targets()
        logger.info(f"Completed run {run_id}")

    async def _sync_scrape_targets(self) -> None:
        async with self._scrape_lock:
            if self._service_manager is None:
                return
            runs = await self._storage.list_active_runs()
            groups: list[ScrapeTarget] = []
            for run in runs:
                for node in run.metadata.nodes:
                    groups.append(
                        ScrapeTarget(
                            targets=[node.address],
                            labels={"run_id": run.run_id, "node": node.name},
                        )
                    )
            await self._service_manager.apply_scrape_targets(groups)

    async def _http_server_manager(self, initial_config: DaemonConfig) -> None:
        current_config = initial_config
        while True:
            app = create_app(self._storage, self._daemon_info, str(self.frontend_dir))
            uvicorn_config = uvicorn.Config(
                app,
                host=current_config.host,
                port=current_config.dashboard_port,
                log_level="info",
            )
            uvicorn_server = uvicorn.Server(uvicorn_config)
            server_task = asyncio.create_task(uvicorn_server.serve())
            config_task = asyncio.create_task(self._config_change_event.wait())
            shutdown_task = asyncio.create_task(self._shutdown_event.wait())

            _ = await asyncio.wait(
                [server_task, config_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            uvicorn_server.should_exit = True
            if not server_task.done():
                await server_task
            for task in (config_task, shutdown_task):
                if not task.done():
                    _ = task.cancel()

            if self._shutdown_event.is_set():
                break

            if self._config_change_event.is_set():
                self._config_change_event.clear()
                if self._new_config is not None:
                    current_config = self._new_config
                    self._current_config = current_config

    async def run(self) -> None:
        initial_config = self._config_watcher.get_current_config()
        if initial_config is None:
            raise RuntimeError(f"No configuration found at {self.config_path}")
        if isinstance(initial_config, ValidationError):
            raise RuntimeError("Invalid configuration") from initial_config

        self._current_config = initial_config
        self._new_config = initial_config

        services_dir = self.socket_path.parent / "services"
        services_dir.mkdir(parents=True, exist_ok=True)
        self._service_manager = ServiceManager(
            instance_dir=services_dir,
            prometheus_port=initial_config.prometheus_port,
            grafana_port=initial_config.grafana_port,
        )

        started_services = False
        try:
            await self._service_manager.start_all()
            started_services = True
        except RuntimeError as e:
            logger.warning(f"Could not start services: {e}")

        await self._sync_scrape_targets()

        await self._ipc_server.start()

        http_task = asyncio.create_task(self._http_server_manager(initial_config))
        watch_task = asyncio.create_task(self._config_watcher.watch())

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self._shutdown_event.set)
        loop.add_signal_handler(signal.SIGINT, self._shutdown_event.set)

        try:
            _ = await self._shutdown_event.wait()
        finally:
            self._config_watcher.stop()

            if not http_task.done():
                await http_task

            _ = watch_task.cancel()
            try:
                await watch_task
            except asyncio.CancelledError:
                pass

            await self._ipc_server.stop()

            if started_services:
                assert self._service_manager is not None
                await self._service_manager.stop_all()
                await self._service_manager.cleanup()

            self._storage.close()
