from collections.abc import Callable

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .ipc import DaemonInfo
from .storage import NodeTarget, RunMetadata, StorageBackend


class RunInfo(BaseModel):
    run_id: str
    status: str
    start_time: str
    end_time: str | None
    description: str
    git_branch: str
    git_commit_id: str
    nodes: list[NodeTarget]


def _run_to_info(run: RunMetadata) -> RunInfo:
    return RunInfo(
        run_id=run.run_id,
        status=run.status,
        start_time=run.start_time.isoformat(),
        end_time=run.end_time.isoformat() if run.end_time else None,
        description=run.metadata.description,
        git_branch=run.metadata.git_branch,
        git_commit_id=run.metadata.git_commit_id,
        nodes=run.metadata.nodes,
    )


def create_app(
    storage: StorageBackend,
    info_provider: Callable[[], DaemonInfo],
    frontend_dir: str,
) -> FastAPI:
    app = FastAPI(title="TON Dashboard Daemon")

    @app.get("/api/info")
    async def get_info() -> DaemonInfo:  # pyright: ignore[reportUnusedFunction]
        return info_provider()

    @app.get("/api/runs")
    async def list_runs() -> list[RunInfo]:  # pyright: ignore[reportUnusedFunction]
        runs = await storage.list_runs(limit=100)
        return [_run_to_info(run) for run in runs]

    @app.get("/api/runs/{run_id}")
    async def get_run(run_id: str) -> RunInfo:  # pyright: ignore[reportUnusedFunction]
        run = await storage.get_run_metadata(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return _run_to_info(run)

    @app.get("/health")
    async def health() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        return {"status": "ok"}

    @app.get("/grafana")
    async def grafana_redirect() -> RedirectResponse:  # pyright: ignore[reportUnusedFunction]
        return RedirectResponse(f"{info_provider().grafana_url}/d/ton-overview?refresh=5s")

    @app.get("/prometheus")
    async def prom_redirect() -> RedirectResponse:  # pyright: ignore[reportUnusedFunction]
        return RedirectResponse(info_provider().prometheus_url)

    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")

    return app
