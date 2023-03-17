from najapy.cache.redis import BlockingRedisPool
from najapy.common.async_base import Utils
from najapy.event.async_event import DistributedEvent


async def test_distributed_event(p2: BlockingRedisPool):
    channel_name = "test_channel"
    event_name = "test_event"

    e = DistributedEvent(
        p2, channel_name, 1
    )

    await Utils.sleep(0.5)

    async def call_func(num1, num2):
        Utils.log.info("test_distributed_event call success.")
        assert num1 + 1 == 1
        assert num2 + 2 == 3

    e.add_listener(event_name, call_func)

    await e.dispatch(event_name, 0, 1)
