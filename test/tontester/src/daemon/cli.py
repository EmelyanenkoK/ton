import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import cast

from .config import DaemonConfig
from .daemon import DashboardDaemon
from .ipc import IPCClient

logger = logging.getLogger(__name__)


def get_default_paths() -> tuple[Path, Path, Path]:
    integration_dir = Path(__file__).resolve().parents[3] / "integration"
    dashboard_dir = integration_dir / ".dashboard"
    config_path = dashboard_dir / "config.json"
    socket_path = dashboard_dir / "daemon.sock"
    frontend_dir = Path(__file__).parent / "frontend"
    return config_path, socket_path, frontend_dir


async def _start() -> None:
    config_path, socket_path, frontend_dir = get_default_paths()

    if socket_path.exists():
        try:
            _ = await IPCClient(socket_path).ping()
            logger.error(f"Daemon is already running at {socket_path}")
            sys.exit(1)
        except Exception:
            # Stale socket, will be replaced by IPCServer on startup.
            pass

    if not config_path.exists():
        config_path.parent.mkdir(parents=True, exist_ok=True)
        default = DaemonConfig()
        _ = config_path.write_text(default.model_dump_json(indent=2) + "\n")
        logger.info(f"Wrote default config to {config_path}")

    if not frontend_dir.exists():
        logger.error(f"Frontend directory not found at {frontend_dir}")
        sys.exit(1)

    daemon = DashboardDaemon(config_path, socket_path, frontend_dir)
    logger.info("Starting dashboard daemon...")
    logger.info(f"Config: {config_path}")
    logger.info(f"Socket: {socket_path}")

    try:
        await daemon.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


async def _stop() -> None:
    config_path, _, _ = get_default_paths()
    if not config_path.exists():
        logger.error("No daemon config found; nothing to stop")
        sys.exit(1)
    # Config deletion is interpreted by the daemon as a shutdown signal.
    config_path.unlink()
    logger.info(f"Removed {config_path}; daemon will shut down on next config poll")


async def _status() -> None:
    config_path, socket_path, _ = get_default_paths()

    if not socket_path.exists():
        logger.error("Daemon is not running (no socket)")
        sys.exit(1)

    try:
        _ = await IPCClient(socket_path).ping()
    except Exception as e:
        logger.error(f"Error connecting to daemon: {e}")
        sys.exit(1)

    logger.info("Daemon is running")
    logger.info(f"Config: {config_path}")
    logger.info(f"Socket: {socket_path}")


async def _info() -> None:
    _, socket_path, _ = get_default_paths()
    if not socket_path.exists():
        logger.error("Daemon is not running")
        sys.exit(1)
    # Fetch urls by reading config (cheap; avoids HTTP roundtrip).
    config_path, _, _ = get_default_paths()
    data = config_path.read_text()
    config = DaemonConfig.model_validate_json(data)
    out = {
        "prometheus_url": f"http://{config.host}:{config.prometheus_port}",
        "grafana_url": f"http://{config.host}:{config.grafana_port}",
        "dashboard_url": f"http://{config.host}:{config.dashboard_port}",
        "socket": str(socket_path),
    }
    print(json.dumps(out, indent=2))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        prog="daemon",
        description="tontester dashboard daemon",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    _ = subparsers.add_parser("start", help="Run the daemon in the foreground")
    _ = subparsers.add_parser("stop", help="Stop the daemon by removing its config")
    _ = subparsers.add_parser("status", help="Check daemon status")
    _ = subparsers.add_parser("info", help="Print URLs and socket path for the daemon")

    args = parser.parse_args()
    command = cast(str, args.command)

    match command:
        case "start":
            asyncio.run(_start())
        case "stop":
            asyncio.run(_stop())
        case "status":
            asyncio.run(_status())
        case "info":
            asyncio.run(_info())
        case _:
            parser.error(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
