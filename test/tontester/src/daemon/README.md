# tontester dashboard daemon

Long-running local process that boots a Prometheus + Grafana pair (via `podman`)
and discovers scrape targets from tontester test runs over a Unix socket.

Result: you start a tontester network and immediately see per-validator
Prometheus metrics in Grafana — no manual compose/podman/prom config wrangling.

## Components

```
 test harness (tontester Network)
        │  unix socket
        ▼
 daemon  ─────────────►  file_sd  ─────►  prometheus (podman)
   │                                          │
   │                                          ▼
   └─► fastapi dashboard    ◄────────  grafana (podman)
```

- `daemon/daemon.py` — `DashboardDaemon` orchestrator. Starts IPC server,
  HTTP API, and the Prometheus/Grafana containers.
- `daemon/container.py` — `PrometheusController`, `GrafanaController`,
  `ServiceManager`. Manages container lifecycle; writes a file_sd
  `targets.json` that Prometheus re-reads every 5 s.
- `daemon/ipc.py` — Unix-socket protocol. `IPCClient.connect_and_register`
  holds the connection open; the daemon marks the run "completed" and drops
  its scrape targets when the client disconnects.
- `daemon/storage.py` + `sqlite_storage.py` — in-memory SQLite keyed by
  `run_id`, recording `TestMetadata` (description, git info, list of
  `NodeTarget`).
- `daemon/config.py` — `ConfigWatcher` uses `watchfiles` to auto-reload
  `config.json`. Deleting the config tells the daemon to shut down.
- `daemon/api.py` — FastAPI: `/api/info`, `/api/runs`, `/api/runs/{id}`,
  `/health`, plus `/grafana` and `/prometheus` redirects. Serves the
  SPA from `daemon/frontend/`.
- `daemon/cli.py` — `start`, `stop`, `status`, `info` commands.

## Usage

### 1. Start the daemon

```sh
uv run daemon start
```

First run writes a default config to `test/integration/.dashboard/config.json`:

```json
{
  "host": "127.0.0.1",
  "dashboard_port": 8080,
  "prometheus_port": 9090,
  "grafana_port": 3000
}
```

Editing the file reloads the HTTP server; deleting it shuts the daemon down.

### 2. Run a tontester network with `enable_dashboard=True`

```python
async with Network(install, working_dir, enable_dashboard=True) as network:
    ...  # create_full_node, run, etc.
    await network.register_with_dashboard()  # call after nodes are launched
```

`Network.register_with_dashboard()` opens the Unix socket, sends the list of
`NodeTarget(name, host.containers.internal:<port>)` to the daemon, and
receives back the Grafana / Prometheus URLs.

See `test/integration/test_metrics.py` for a full example.

### 3. View metrics

- Dashboard:  http://127.0.0.1:8080
- Prometheus: http://127.0.0.1:9090
- Grafana:    http://127.0.0.1:3000 (anonymous admin)

The Grafana instance is auto-provisioned with a Prometheus datasource and a
minimal "TON Validator Overview" dashboard.

## How scrape target discovery works

Each `FullNode` allocates an exporter port and launches `validator-engine`
with `--exporter-address 0.0.0.0:<port>`. From inside the Prometheus
container, the host is reached via `host.containers.internal` (provided by
`--add-host=...:host-gateway`).

When the test connects over IPC, the daemon writes
`services/prometheus-config/targets.json`:

```json
[
  {"targets": ["host.containers.internal:9101"], "labels": {"run_id": "abc", "node": "node-0"}},
  {"targets": ["host.containers.internal:9102"], "labels": {"run_id": "abc", "node": "node-1"}}
]
```

Prometheus' `file_sd_configs` picks this up every 5 seconds (no reload
needed). On disconnect the daemon rewrites the file without that run's
entries.

## CLI

```sh
uv run daemon start     # foreground, streams logs to stdout
uv run daemon status    # pings the socket
uv run daemon info      # prints the URLs
uv run daemon stop      # removes the config -> triggers graceful shutdown
```

`Ctrl-C`/`SIGTERM`/`SIGINT` also trigger a graceful shutdown (stops and
removes the containers).
