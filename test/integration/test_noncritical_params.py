import asyncio
import logging
import shutil
from pathlib import Path

from tonapi import ton_api
from tontester.install import Install
from tontester.network import FullNode, Network
from tontester.zerostate import SimplexConsensusConfig

from tonlib import TonlibClient

SHARD_FULL = 0x8000_0000_0000_0000 - (2**64)


def get_mc_target_rate_override(target_rate_ms: int):
    return ton_api.Consensus_noncriticalParamsOverrideList(
        overrides=[
            ton_api.Consensus_noncriticalParamsOverride(
                workchain=-1,
                shard=SHARD_FULL,
                from_seqno=0,
                to_seqno=(1 << 31) - 1,
                override=ton_api.Consensus_simplex_noncriticalParams(
                    flags=1,
                    target_rate_ms=target_rate_ms,
                ),
            )
        ],
    )


async def set_mc_target_rate(node: FullNode, target_rate_ms: int):
    await node.engine_console.set_consensus_noncritical_params_overrides(
        get_mc_target_rate_override(target_rate_ms)
    )


async def measure_block_rate(
    client: TonlibClient, workchain: int, shard: int, from_seqno: int, to_seqno: int
):
    async def get_gen_utime(seqno: int):
        block = await client.lookup_block(workchain, shard, seqno)
        header = await client.get_block_header(block)
        return header.gen_utime

    utimes = await asyncio.gather(
        *(get_gen_utime(seqno) for seqno in range(from_seqno, to_seqno + 1))
    )
    deltas = [utimes[i + 1] - utimes[i] for i in range(len(utimes) - 1)]
    return sum(deltas) / len(deltas)


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
    l = logging.getLogger(__name__)

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

        node = network.create_full_node()
        node.make_initial_validator()
        node.announce_to(dht)

        async with asyncio.TaskGroup() as tg:
            _ = tg.create_task(dht.run())
            _ = tg.create_task(node.run())

        # First, measure baseline block rate
        await network.wait_mc_block(seqno=25)
        client = await node.tonlib_client()

        fast_rate = await measure_block_rate(client, -1, SHARD_FULL, 5, 25)
        l.info(f"Baseline MC rate (200ms target): {fast_rate:.2f}s avg")
        assert abs(fast_rate - 0.2) < 0.05, f"Expected baseline MC rate ~0.2s, got {fast_rate:.2f}s"

        # Then, let's slow down the network.
        await set_mc_target_rate(node, 1000)

        last_block = (await client.get_masterchain_info()).last
        assert last_block is not None
        await network.wait_mc_block(last_block.seqno + 8)
        slow_rate = await measure_block_rate(
            client, -1, SHARD_FULL, last_block.seqno + 2, last_block.seqno + 8
        )
        l.info(f"MC rate with 1000ms target: {slow_rate:.2f}s avg")
        assert abs(slow_rate - 1.0) < 0.1, f"Expected MC slow_rate ~1s, got {slow_rate:.2f}s"

        # Check the getter
        overrides = await node.engine_console.get_consensus_noncritical_params_overrides()
        assert overrides.to_dict() == get_mc_target_rate_override(1000).to_dict()

        # And speed it up again
        await set_mc_target_rate(node, 200)

        last_block = (await client.get_masterchain_info()).last
        assert last_block is not None
        await network.wait_mc_block(last_block.seqno + 22)
        sped_up_rate = await measure_block_rate(
            client, -1, SHARD_FULL, last_block.seqno + 2, last_block.seqno + 22
        )
        l.info(f"MC rate with 200ms target: {sped_up_rate:.2f}s avg")
        assert abs(sped_up_rate - 0.2) < 0.05, (
            f"Expected MC sped_up_rate ~0.2s, got {sped_up_rate:.2f}s"
        )

        # Check that shardchain was unaffected.
        last_block = (await client.get_masterchain_info()).last
        assert last_block is not None
        shard_last_block = (await client.get_shards(last_block)).shards[0]
        shard_rate = await measure_block_rate(client, 0, SHARD_FULL, 2, shard_last_block.seqno)
        l.info(f"Shard rate: {shard_rate:.2f}s avg")
        assert abs(shard_rate - 1.0) < 0.1, f"Expected shard_rate ({shard_rate:.2f}s) ~ 1s"


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), 5 * 60))
