"""Local testing stand for Prometheus metrics from a tontester network run.

Run the tontester daemon first (in another terminal):

    uv run daemon start

Then run this test:

    uv run test/integration/test_metrics.py

While the network is running, browse to:
    http://127.0.0.1:8080  - dashboard + run list
    http://127.0.0.1:9090  - Prometheus
    http://127.0.0.1:3000  - Grafana (anonymous admin access enabled)
"""

import asyncio
import logging
import shutil
from pathlib import Path

from tontester.install import Install
from tontester.network import FullNode, Network
from tontester.zerostate import SimplexConsensusConfig


async def main():
    repo_root = Path(__file__).resolve().parents[2]
    working_dir = repo_root / "test/integration/.network"
    shutil.rmtree(working_dir, ignore_errors=True)
    working_dir.mkdir(exist_ok=True)

    install = Install(repo_root / "build", repo_root)
    install.tonlibjson.client_set_verbosity_level(3)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s][%(asctime)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H-%M-%S",
    )

    async with Network(
        install,
        working_dir,
        enable_dashboard=True,
        description="metrics stand",
    ) as network:
        dht = network.create_dht_node()

        network.config.mc_consensus = SimplexConsensusConfig()
        network.config.shard_consensus = SimplexConsensusConfig()
        network.config.shard_validators = 2

        nodes: list[FullNode] = []
        for _ in range(2):
            node = network.create_full_node()
            node.make_initial_validator()
            node.announce_to(dht)
            nodes.append(node)

        async with asyncio.TaskGroup() as start_group:
            _ = start_group.create_task(dht.run())
            for node in nodes:
                _ = start_group.create_task(node.run())

        response = await network.register_with_dashboard()
        assert response is not None
        print(f"Grafana:    {response.grafana_url}")
        print(f"Prometheus: {response.prometheus_url}")
        for node in nodes:
            print(f"  metrics: http://127.0.0.1:{node.metrics_port}/metrics")

        await network.wait_mc_block(seqno=1)

        # Let the network produce blocks for a while so metrics accumulate.
        await asyncio.sleep(3000)


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), 10 * 60))
