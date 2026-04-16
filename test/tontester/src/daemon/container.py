import asyncio
import json
import logging
import os
import shutil
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from typing import final, override

import yaml
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ContainerConfig(BaseModel):
    name: str
    image: str
    port_mappings: dict[int, int]
    volumes: dict[Path, str] = {}
    environment: dict[str, str] = {}
    command: list[str] | None = None


class ContainerController(ABC):
    def __init__(self, instance_dir: Path, network_name: str):
        self.instance_dir: Path = instance_dir
        self.network_name: str = network_name
        self.instance_dir.mkdir(parents=True, exist_ok=True)
        self._container_id: str | None = None
        self._is_running: bool = False

    async def _run_command(self, command: list[str], check: bool = True) -> str:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        if check and process.returncode != 0:
            assert process.returncode is not None
            raise subprocess.CalledProcessError(process.returncode, command, stdout, stderr)

        return stdout.decode().strip()

    @abstractmethod
    def get_config(self) -> ContainerConfig:
        pass

    @property
    def is_running(self) -> bool:
        return self._is_running

    async def _remove_stale(self, name: str) -> None:
        _ = await self._run_command(["podman", "rm", "-f", name], check=False)

    async def start(self) -> None:
        config = self.get_config()

        await self._remove_stale(config.name)

        command = ["podman", "run", "-d", "--rm", "--name", config.name]
        command.extend(["--user", f"{os.getuid()}:{os.getgid()}"])
        command.extend(["--userns", "keep-id"])
        command.extend(["--network", self.network_name])
        command.extend(["--add-host", "host.containers.internal:host-gateway"])

        for host_port, container_port in config.port_mappings.items():
            command.extend(["-p", f"127.0.0.1:{host_port}:{container_port}"])

        for host_path, container_path in config.volumes.items():
            host_path.mkdir(parents=True, exist_ok=True)
            command.extend(["-v", f"{host_path}:{container_path}:Z"])

        for key, value in config.environment.items():
            command.extend(["-e", f"{key}={value}"])

        command.append(config.image)
        if config.command:
            command.extend(config.command)

        self._container_id = await self._run_command(command)
        self._is_running = True
        logger.info(f"Started container {config.name}")

    async def reload(self) -> None:
        if self._is_running and self._container_id:
            _ = await self._run_command(
                ["podman", "kill", "-s", "HUP", self._container_id], check=False
            )

    async def stop(self) -> None:
        if not self._container_id:
            return

        if self._is_running:
            self._is_running = False
            _ = await self._run_command(["podman", "stop", self._container_id], check=False)
            logger.info(f"Stopped container {self.get_config().name}")

    async def remove(self) -> None:
        await self.stop()
        if self._container_id:
            _ = await self._run_command(["podman", "rm", self._container_id], check=False)
            self._container_id = None


class ScrapeTarget(BaseModel):
    targets: list[str]
    labels: dict[str, str] = {}


@final
class PrometheusController(ContainerController):
    def __init__(self, instance_dir: Path, network_name: str, port: int):
        self.port: int = port
        self.config_dir: Path = instance_dir / "prometheus-config"
        self.data_dir: Path = instance_dir / "prometheus-data"
        super().__init__(instance_dir / "prometheus", network_name)

    @property
    def _targets_file(self) -> Path:
        return self.config_dir / "targets.json"

    @override
    def get_config(self) -> ContainerConfig:
        self._ensure_config()
        self._ensure_data_dir()

        return ContainerConfig(
            name="tontester-prometheus",
            image="docker.io/prom/prometheus:latest",
            port_mappings={self.port: 9090},
            volumes={
                self.config_dir: "/etc/prometheus",
                self.data_dir: "/prometheus",
            },
            command=[
                "--config.file=/etc/prometheus/prometheus.yml",
                "--storage.tsdb.path=/prometheus",
                "--storage.tsdb.retention.time=7d",
                "--web.console.libraries=/usr/share/prometheus/console_libraries",
                "--web.console.templates=/usr/share/prometheus/consoles",
                "--web.enable-lifecycle",
            ],
        )

    def _ensure_data_dir(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _ensure_config(self) -> None:
        self.config_dir.mkdir(parents=True, exist_ok=True)

        config = {
            "global": {
                "scrape_interval": "5s",
                "evaluation_interval": "5s",
            },
            "scrape_configs": [
                {
                    "job_name": "ton-validator",
                    "file_sd_configs": [
                        {
                            "files": ["/etc/prometheus/targets.json"],
                            "refresh_interval": "5s",
                        }
                    ],
                }
            ],
        }
        _ = (self.config_dir / "prometheus.yml").write_text(
            yaml.safe_dump(config, default_flow_style=False)
        )

        if not self._targets_file.exists():
            _ = self._targets_file.write_text("[]")

    async def update_scrape_targets(self, groups: list[ScrapeTarget]) -> None:
        payload = [group.model_dump() for group in groups]
        tmp = self._targets_file.with_suffix(".json.tmp")
        _ = tmp.write_text(json.dumps(payload, indent=2))
        _ = tmp.replace(self._targets_file)
        logger.info(f"Updated prometheus targets: {payload}")


@final
class GrafanaController(ContainerController):
    def __init__(self, instance_dir: Path, network_name: str, port: int):
        self.port: int = port
        self.data_dir: Path = instance_dir / "grafana-data"
        self.provisioning_dir: Path = instance_dir / "grafana-provisioning"
        super().__init__(instance_dir / "grafana", network_name)

    @override
    def get_config(self) -> ContainerConfig:
        self._ensure_provisioning()
        self._ensure_data_dir()

        return ContainerConfig(
            name="tontester-grafana",
            image="docker.io/grafana/grafana:latest",
            port_mappings={self.port: 3000},
            volumes={
                self.data_dir: "/var/lib/grafana",
                self.provisioning_dir: "/etc/grafana/provisioning",
            },
            environment={
                "GF_SECURITY_ADMIN_PASSWORD": "admin",
                "GF_USERS_ALLOW_SIGN_UP": "false",
                "GF_AUTH_ANONYMOUS_ENABLED": "true",
                "GF_AUTH_ANONYMOUS_ORG_ROLE": "Admin",
                "GF_AUTH_DISABLE_LOGIN_FORM": "true",
            },
        )

    def _ensure_data_dir(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _ensure_provisioning(self) -> None:
        datasources_dir = self.provisioning_dir / "datasources"
        datasources_dir.mkdir(parents=True, exist_ok=True)

        datasource_config = {
            "apiVersion": 1,
            "deleteDatasources": [{"name": "Prometheus", "orgId": 1}],
            "datasources": [
                {
                    "name": "Prometheus",
                    "uid": "prometheus",
                    "type": "prometheus",
                    "access": "proxy",
                    "url": "http://tontester-prometheus:9090",
                    "isDefault": True,
                    "editable": True,
                }
            ],
        }
        _ = (datasources_dir / "prometheus.yml").write_text(yaml.safe_dump(datasource_config))

        dashboards_dir = self.provisioning_dir / "dashboards"
        dashboards_dir.mkdir(parents=True, exist_ok=True)

        dashboard_provider = {
            "apiVersion": 1,
            "providers": [
                {
                    "name": "default",
                    "orgId": 1,
                    "folder": "",
                    "type": "file",
                    "disableDeletion": False,
                    "updateIntervalSeconds": 10,
                    "options": {"path": "/etc/grafana/provisioning/dashboards/definitions"},
                }
            ],
        }
        _ = (dashboards_dir / "dashboard.yml").write_text(yaml.safe_dump(dashboard_provider))

        definitions_dir = dashboards_dir / "definitions"
        definitions_dir.mkdir(parents=True, exist_ok=True)

        _ = (definitions_dir / "ton-overview.json").write_text(
            json.dumps(_default_dashboard(), indent=2)
        )


def _default_dashboard() -> dict[str, object]:
    def panel(
        panel_id: int,
        title: str,
        targets: str | list[tuple[str, str]],
        x: int,
        y: int,
        unit: str = "",
    ) -> dict[str, object]:
        if isinstance(targets, str):
            targets = [(targets, "{{node}} ({{instance}})")]
        p: dict[str, object] = {
            "id": panel_id,
            "title": title,
            "type": "timeseries",
            "datasource": {"type": "prometheus", "uid": "prometheus"},
            "gridPos": {"h": 8, "w": 12, "x": x, "y": y},
            "targets": [
                {"expr": expr, "refId": chr(ord("A") + i), "legendFormat": legend}
                for i, (expr, legend) in enumerate(targets)
            ],
            "options": {
                "legend": {"displayMode": "list", "placement": "bottom"},
                "tooltip": {"mode": "multi"},
            },
        }
        if unit:
            p["fieldConfig"] = {"defaults": {"unit": unit}, "overrides": []}
        return p

    run_filter = '{run_id=~"$run_id", node=~"$node"}'

    # Sum away `kind` label on app-level QUIC metrics so ratios line-match cleanly.
    def app_rate(metric: str) -> str:
        return f"sum without (kind) (rate({metric}{run_filter}[1m]))"

    def rate(metric: str) -> str:
        return f"rate({metric}{run_filter}[1m])"

    panels = [
        panel(1, "Up", f"up{run_filter}", 0, 0),
        panel(2, "Scrape duration", f"scrape_duration_seconds{run_filter}", 12, 0, unit="s"),
        panel(3, "ADNL ingress bytes/s", rate("ton_adnl_net_udp_ingress_bytes_total"), 0, 8, unit="Bps"),
        panel(4, "ADNL egress bytes/s", rate("ton_adnl_net_udp_egress_bytes_total"), 12, 8, unit="Bps"),
        panel(5, "ADNL peers", f"ton_adnl_peers{run_filter}", 0, 16),
        panel(
            6,
            "Exporter collection duration",
            f"ton_exporter_last_collection_duration_seconds{run_filter}",
            12,
            16,
            unit="s",
        ),
        # --- QUIC / ngtcp2 overhead investigation ---
        panel(
            10,
            "QUIC TX bytes/s: app vs ngtcp2 vs UDP",
            [
                (app_rate("ton_quic_app_send_bytes_total"), "{{node}} app_send"),
                (rate("ton_quic_summary_tx_bytes_total"), "{{node}} ngtcp2_tx"),
                (rate("ton_quic_udp_egress_bytes_total"), "{{node}} udp_egress"),
            ],
            0,
            24,
            unit="Bps",
        ),
        panel(
            11,
            "QUIC RX bytes/s: app vs stream vs UDP",
            [
                (app_rate("ton_quic_app_deliver_bytes_total"), "{{node}} app_deliver"),
                (rate("ton_quic_summary_stream_bytes_received_total"), "{{node}} stream_rx"),
                (rate("ton_quic_udp_ingress_bytes_total"), "{{node}} udp_ingress"),
            ],
            12,
            24,
            unit="Bps",
        ),
        panel(
            12,
            "QUIC TX expansion ratio (bytes)",
            [
                (
                    f"{rate('ton_quic_summary_tx_bytes_total')} "
                    f"/ {app_rate('ton_quic_app_send_bytes_total')}",
                    "{{node}} ngtcp2/app",
                ),
                (
                    f"{rate('ton_quic_udp_egress_bytes_total')} "
                    f"/ {rate('ton_quic_summary_tx_bytes_total')}",
                    "{{node}} UDP/ngtcp2",
                ),
                (
                    f"{rate('ton_quic_udp_egress_bytes_total')} "
                    f"/ {app_rate('ton_quic_app_send_bytes_total')}",
                    "{{node}} UDP/app",
                ),
            ],
            0,
            32,
        ),
        panel(
            13,
            "QUIC RX expansion ratio (bytes)",
            [
                (
                    f"{rate('ton_quic_summary_stream_bytes_received_total')} "
                    f"/ {app_rate('ton_quic_app_deliver_bytes_total')}",
                    "{{node}} stream/app",
                ),
                (
                    f"{rate('ton_quic_udp_ingress_bytes_total')} "
                    f"/ {rate('ton_quic_summary_stream_bytes_received_total')}",
                    "{{node}} UDP/stream",
                ),
                (
                    f"{rate('ton_quic_udp_ingress_bytes_total')} "
                    f"/ {app_rate('ton_quic_app_deliver_bytes_total')}",
                    "{{node}} UDP/app",
                ),
            ],
            12,
            32,
        ),
        panel(
            14,
            "UDP packets/s",
            [
                (rate("ton_quic_udp_egress_packets_total"), "{{node}} egress"),
                (rate("ton_quic_udp_ingress_packets_total"), "{{node}} ingress"),
            ],
            0,
            40,
            unit="pps",
        ),
        panel(
            15,
            "Avg UDP packet size",
            [
                (
                    f"{rate('ton_quic_udp_egress_bytes_total')} "
                    f"/ {rate('ton_quic_udp_egress_packets_total')}",
                    "{{node}} egress",
                ),
                (
                    f"{rate('ton_quic_udp_ingress_bytes_total')} "
                    f"/ {rate('ton_quic_udp_ingress_packets_total')}",
                    "{{node}} ingress",
                ),
            ],
            12,
            40,
            unit="bytes",
        ),
        panel(
            16,
            "ngtcp2 packets/s (sent / recv / lost)",
            [
                (rate("ton_quic_summary_pkt_sent_total"), "{{node}} sent"),
                (rate("ton_quic_summary_pkt_recv_total"), "{{node}} recv"),
                (rate("ton_quic_summary_pkt_lost_total"), "{{node}} lost"),
            ],
            0,
            48,
            unit="pps",
        ),
        panel(
            17,
            "UDP packets per syscall (batching)",
            [
                (
                    f"{rate('ton_quic_udp_egress_packets_total')} "
                    f"/ {rate('ton_quic_udp_egress_syscalls_total')}",
                    "{{node}} egress",
                ),
                (
                    f"{rate('ton_quic_udp_ingress_packets_total')} "
                    f"/ {rate('ton_quic_udp_ingress_syscalls_total')}",
                    "{{node}} ingress",
                ),
            ],
            12,
            48,
        ),
        panel(
            18,
            "QUIC RTT",
            [
                (f"ton_quic_summary_latest_rtt_seconds{run_filter}", "{{node}} latest"),
                (f"ton_quic_summary_min_rtt_seconds{run_filter}", "{{node}} min"),
                (f"ton_quic_summary_rttvar_seconds{run_filter}", "{{node}} var"),
            ],
            0,
            56,
            unit="s",
        ),
        panel(
            19,
            "QUIC congestion state",
            [
                (f"ton_quic_summary_cwnd_bytes{run_filter}", "{{node}} cwnd"),
                (f"ton_quic_summary_bytes_in_flight{run_filter}", "{{node}} in_flight"),
                (f"ton_quic_summary_unacked_bytes{run_filter}", "{{node}} unacked"),
                (f"ton_quic_summary_unsent_bytes{run_filter}", "{{node}} unsent"),
            ],
            12,
            56,
            unit="bytes",
        ),
        # --- TL-breakdown traffic panels ---
        # RLDP answers carry the actual response body's TL (query wrappers are mostly overlay.query,
        # so the bytes panel for answers is where you see which payload types dominate the wire).
        panel(
            20,
            "RLDP answer bytes/s by TL",
            [
                (
                    f"sum by (tl) (rate(ton_rldp_app_deliver_bytes_by_tl_total{{"
                    f'run_id=~"$run_id",node=~"$node",kind="answer"}}[1m]))',
                    "{{tl}}",
                )
            ],
            0,
            64,
            unit="Bps",
        ),
        panel(
            21,
            "RLDP2 answer bytes/s by TL",
            [
                (
                    f"sum by (tl) (rate(ton_rldp2_app_deliver_bytes_by_tl_total{{"
                    f'run_id=~"$run_id",node=~"$node",kind="answer"}}[1m]))',
                    "{{tl}}",
                )
            ],
            12,
            64,
            unit="Bps",
        ),
        # Overlay broadcasts: split by overlay type (consensus / shard / fast-sync) and inner TL
        # so it's obvious which broadcast kind carries the bulk of the traffic.
        panel(
            22,
            "Overlay broadcasts sent bytes/s by type + TL",
            [
                (
                    f"sum by (type, tl) (rate(ton_overlay_broadcasts_sent_bytes_by_tl_total{{"
                    f'run_id=~"$run_id",node=~"$node"}}[1m]))',
                    "{{type}} {{tl}}",
                )
            ],
            0,
            72,
            unit="Bps",
        ),
        panel(
            23,
            "Overlay broadcasts received bytes/s by type + TL",
            [
                (
                    f"sum by (type, tl) (rate(ton_overlay_broadcasts_received_bytes_by_tl_total{{"
                    f'run_id=~"$run_id",node=~"$node"}}[1m]))',
                    "{{type}} {{tl}}",
                )
            ],
            12,
            72,
            unit="Bps",
        ),
        # Overlay direct (non-broadcast) messages — useful for seeing catchain / dht / etc payloads.
        panel(
            24,
            "Overlay messages sent bytes/s by type + TL",
            [
                (
                    f"sum by (type, tl) (rate(ton_overlay_messages_sent_bytes_by_tl_total{{"
                    f'run_id=~"$run_id",node=~"$node"}}[1m]))',
                    "{{type}} {{tl}}",
                )
            ],
            0,
            80,
            unit="Bps",
        ),
        panel(
            25,
            "Overlay messages received bytes/s by type + TL",
            [
                (
                    f"sum by (type, tl) (rate(ton_overlay_messages_received_bytes_by_tl_total{{"
                    f'run_id=~"$run_id",node=~"$node"}}[1m]))',
                    "{{type}} {{tl}}",
                )
            ],
            12,
            80,
            unit="Bps",
        ),
    ]

    def var(name: str, label: str) -> dict[str, object]:
        return {
            "name": name,
            "label": label,
            "type": "query",
            "datasource": {"type": "prometheus", "uid": "prometheus"},
            "query": {
                "query": f"label_values(up, {name})",
                "refId": "PrometheusVariableQueryEditor",
            },
            "refresh": 2,  # on time range change
            "sort": 1,
            "includeAll": True,
            "allValue": ".*",
            "multi": True,
            "current": {"text": "All", "value": "$__all", "selected": True},
        }

    return {
        "uid": "ton-overview",
        "title": "TON Validator Overview",
        "tags": ["ton"],
        "timezone": "browser",
        "schemaVersion": 38,
        "version": 2,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "templating": {"list": [var("run_id", "Run"), var("node", "Node")]},
        "panels": panels,
    }


@final
class ServiceManager:
    def __init__(self, instance_dir: Path, prometheus_port: int, grafana_port: int):
        self.instance_dir: Path = instance_dir
        self.network_name: str = "tontester"
        self.prometheus: PrometheusController = PrometheusController(
            instance_dir, self.network_name, prometheus_port
        )
        self.grafana: GrafanaController = GrafanaController(
            instance_dir, self.network_name, grafana_port
        )

    async def _ensure_network(self) -> None:
        exists = await asyncio.create_subprocess_exec(
            "podman",
            "network",
            "exists",
            self.network_name,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        _ = await exists.wait()
        if exists.returncode != 0:
            create = await asyncio.create_subprocess_exec(
                "podman",
                "network",
                "create",
                self.network_name,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            _ = await create.wait()
            if create.returncode == 0:
                logger.info(f"Created podman network: {self.network_name}")

    async def start_all(self) -> None:
        if shutil.which("podman") is None:
            raise RuntimeError("Podman is not installed or not in PATH")

        await self._ensure_network()
        await self.prometheus.start()
        await self.grafana.start()

        logger.info(f"Prometheus: http://localhost:{self.prometheus.port}")
        logger.info(f"Grafana:    http://localhost:{self.grafana.port}")

    async def stop_all(self) -> None:
        _ = await asyncio.gather(
            self.prometheus.stop(),
            self.grafana.stop(),
            return_exceptions=True,
        )

    async def cleanup(self) -> None:
        _ = await asyncio.gather(
            self.prometheus.remove(),
            self.grafana.remove(),
            return_exceptions=True,
        )

    async def apply_scrape_targets(self, groups: list[ScrapeTarget]) -> None:
        await self.prometheus.update_scrape_targets(groups)
