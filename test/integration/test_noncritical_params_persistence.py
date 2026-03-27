import asyncio
import logging
import shutil
from pathlib import Path

from tonapi import ton_api
from tontester.install import Install
from tontester.key import Key
from tontester.network import FullNode, Network
from tontester.zerostate import SimplexConsensusConfig

from tonlib import EngineConsoleClient, RemoteError, TonlibClient

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


def get_shard_target_rate_override(target_rate_ms: int):
    return ton_api.Consensus_noncriticalParamsOverrideList(
        overrides=[
            ton_api.Consensus_noncriticalParamsOverride(
                workchain=0,
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


async def restart_node(node: FullNode):
    await node.stop()
    node._client = None
    node._engine_console = None
    await node.run()


def make_unauthorized_engine_console(node: FullNode) -> EngineConsoleClient:
    unauthorized_key = Key.new(node._install)
    return EngineConsoleClient(
        node._tonlib,
        node._tonlib_event_loop,
        ton_api.EngineConsoleClient_config(
            address=node._engine_console_addr.address,
            server_public_key=node._engine_console_server_key.public_key,
            client_private_key=unauthorized_key.private_key,
        ),
    )


async def wait_and_measure_mc_rate(network: Network, client: TonlibClient, blocks: int):
    last_block = (await client.get_masterchain_info()).last
    assert last_block is not None
    await network.wait_mc_block(last_block.seqno + blocks)
    return await measure_block_rate(
        client, -1, SHARD_FULL, last_block.seqno + 2, last_block.seqno + blocks
    )


async def wait_and_measure_shard_rate(network: Network, client: TonlibClient, blocks: int):
    last_mc = (await client.get_masterchain_info()).last
    assert last_mc is not None
    last_shard = (await client.get_shards(last_mc)).shards[0]
    await network.wait_block(0, SHARD_FULL, last_shard.seqno + blocks)
    return await measure_block_rate(
        client, 0, SHARD_FULL, last_shard.seqno + 1, last_shard.seqno + blocks
    )


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

        await network.wait_mc_block(seqno=25)
        client = await node.tonlib_client()

        # Use an engine-console client with the wrong keypair to verify control
        # queries reject unauthorized callers before touching consensus state.
        unauthorized_console = make_unauthorized_engine_console(node)
        try:
            try:
                _ = await unauthorized_console.get_consensus_noncritical_params_overrides()
                raise AssertionError("Unauthorized control query unexpectedly succeeded")
            except RemoteError as exc:
                assert exc.message == "not authorized"
        finally:
            unauthorized_console.close()

        # Persist a masterchain override, measure live block timing, restart the
        # node, and verify both the stored payload and runtime behavior survive.
        mc_override = get_mc_target_rate_override(1000)
        await node.engine_console.set_consensus_noncritical_params_overrides(mc_override)
        persisted = await node.engine_console.get_consensus_noncritical_params_overrides()
        assert persisted.to_dict() == mc_override.to_dict()

        slowed_mc_rate = await wait_and_measure_mc_rate(network, client, 8)
        l.info(f"MC rate before restart with persisted 1000ms override: {slowed_mc_rate:.2f}s")
        assert abs(slowed_mc_rate - 1.0) < 0.15

        await restart_node(node)
        client = await node.tonlib_client()

        persisted_after_restart = (
            await node.engine_console.get_consensus_noncritical_params_overrides()
        )
        assert persisted_after_restart.to_dict() == mc_override.to_dict()

        slowed_after_restart = await wait_and_measure_mc_rate(network, client, 8)
        l.info(f"MC rate after restart with persisted 1000ms override: {slowed_after_restart:.2f}s")
        assert abs(slowed_after_restart - 1.0) < 0.15

        # Switch to a shard-only override and compare shard/masterchain block
        # rates to prove the override is reloaded and shard-scoped correctly.
        shard_override = get_shard_target_rate_override(2000)
        await node.engine_console.set_consensus_noncritical_params_overrides(shard_override)
        shard_persisted = await node.engine_console.get_consensus_noncritical_params_overrides()
        assert shard_persisted.to_dict() == shard_override.to_dict()

        shard_rate = await wait_and_measure_shard_rate(network, client, 6)
        mc_rate = await wait_and_measure_mc_rate(network, client, 12)
        l.info(f"Shard rate with 2000ms shard override: {shard_rate:.2f}s")
        l.info(f"MC rate while shard override is active: {mc_rate:.2f}s")
        assert abs(shard_rate - 2.0) < 0.2
        assert abs(mc_rate - 0.2) < 0.05

        # Replace the persisted JSON with malformed data, restart, and verify
        # the engine falls back to empty overrides and baseline block timing.
        await node.stop()
        node._client = None
        node._engine_console = None
        overrides_path = node._directory / "noncritical-params-overrides.json"
        overrides_path.write_text("{ definitely not valid json }\n", encoding="utf-8")
        await node.run()
        client = await node.tonlib_client()

        malformed_reload = await node.engine_console.get_consensus_noncritical_params_overrides()
        assert malformed_reload.to_dict()["overrides"] == []

        restored_mc_rate = await wait_and_measure_mc_rate(network, client, 12)
        restored_shard_rate = await wait_and_measure_shard_rate(network, client, 6)
        l.info(f"MC rate after malformed override fallback: {restored_mc_rate:.2f}s")
        l.info(f"Shard rate after malformed override fallback: {restored_shard_rate:.2f}s")
        assert abs(restored_mc_rate - 0.2) < 0.05
        assert abs(restored_shard_rate - 1.0) < 0.15


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), 5 * 60))
