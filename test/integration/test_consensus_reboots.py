import asyncio
import logging
import shutil
from pathlib import Path

from tontester.install import Install
from tontester.network import FullNode, Network
from tontester.zerostate import SimplexConsensusConfig

from tonlib import TonlibClient, TonlibError


async def wait_for_mc_seqno(client: TonlibClient, seqno: int, timeout_s: float = 90.0):
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        try:
            info = await client.get_masterchain_info()
            assert info.last is not None
            if info.last.seqno >= seqno:
                return info.last
        except TonlibError as exc:
            if exc.result.code == 500 and exc.result.message in {
                "LITE_SERVER_NETWORKtimeout for adnl query query",
                "LITE_SERVER_NETWORK",
            }:
                await asyncio.sleep(0.2)
                continue
            raise

        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"Timed out waiting for MC seqno >= {seqno}")
        await asyncio.sleep(0.2)


async def restart_node(node: FullNode):
    await node.stop()
    await node.run()


async def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    working_dir = repo_root / "test/integration/.network"
    shutil.rmtree(working_dir, ignore_errors=True)
    working_dir.mkdir(exist_ok=True)

    install = Install(repo_root / "build", repo_root)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s][%(asctime)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H-%M-%S",
    )
    log = logging.getLogger(__name__)

    async with Network(install, working_dir) as network:
        dht = network.create_dht_node()

        network.config.mc_consensus = SimplexConsensusConfig(
            target_block_rate_ms=200,
            slots_per_leader_window=2,
            first_block_timeout_ms=1000,
            max_leader_window_desync=250,
        )
        network.config.shard_consensus = SimplexConsensusConfig(
            target_block_rate_ms=1000,
            slots_per_leader_window=1,
            first_block_timeout_ms=1000,
            max_leader_window_desync=250,
        )

        nodes: list[FullNode] = []
        for _ in range(4):
            node = network.create_full_node()
            node.make_initial_validator()
            node.announce_to(dht)
            nodes.append(node)

        async with asyncio.TaskGroup() as start_group:
            _ = start_group.create_task(dht.run())
            for node in nodes:
                _ = start_group.create_task(node.run())

        observer = nodes[0]
        restarted = nodes[3]
        observer_client = await observer.tonlib_client()

        await wait_for_mc_seqno(observer_client, 25)
        initial_mc = await observer_client.get_masterchain_info()
        assert initial_mc.last is not None

        # Covers a real validator-engine stop/start with a stable external observer:
        # stop one validator process, require the surviving quorum to keep producing MC blocks,
        # then restart the stopped validator and require it to catch up and stay on the live tip.
        down_target = initial_mc.last.seqno + 8
        await restarted.stop()
        await wait_for_mc_seqno(observer_client, down_target)

        after_downtime = await observer_client.get_masterchain_info()
        assert after_downtime.last is not None
        log.info(
            "Observer reached MC seqno %s while node-3 stayed offline",
            after_downtime.last.seqno,
        )

        await restart_node(restarted)

        # After the reboot, the observer must keep advancing and the restarted process
        # must catch back up to that newer frontier through its own tonlib endpoint.
        post_restart_target = after_downtime.last.seqno + 8
        await wait_for_mc_seqno(observer_client, post_restart_target)
        restarted_client = await restarted.tonlib_client()
        await wait_for_mc_seqno(restarted_client, post_restart_target)

        # Once catch-up is complete, both the stable observer and the restarted node should
        # continue observing later masterchain blocks instead of stalling at the sync frontier.
        steady_target = post_restart_target + 4
        await wait_for_mc_seqno(observer_client, steady_target)
        await wait_for_mc_seqno(restarted_client, steady_target)
        log.info("Restarted validator caught up and followed the live tip through MC seqno %s", steady_target)


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), 5 * 60))
