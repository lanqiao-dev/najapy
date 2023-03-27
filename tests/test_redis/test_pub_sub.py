import asyncio
from typing import Optional

import pytest
import pytest_asyncio
import redis
from redis.typing import EncodableT

from najapy.cache.redis import CacheClient, PeriodCounter
from najapy.common.async_base import Utils


@pytest_asyncio.fixture()
async def pubsub(r: CacheClient):
    p = r.pubsub()
    yield p
    await p.close()


async def wait_for_message(pubsub, timeout=0.2, ignore_subscribe_messages=False):
    now = Utils.loop_time()
    timeout = now + timeout
    while now < timeout:
        message = await pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages
        )
        if message is not None:
            return message
        await Utils.sleep(0.01)
        now = Utils.loop_time()

    return None


def make_message(
        type, channel: Optional[str], data: EncodableT, pattern: Optional[str] = None
):
    return {
        "type": type,
        "channel": channel and channel.encode("utf-8") or None,
        "data": data.encode("utf-8") if isinstance(data, str) else data,
        "pattern": pattern and pattern.encode("utf-8") or None,
    }


@pytest.mark.redis_basic_comm
async def test_published_message_to_channel(r: CacheClient, pubsub):
    p = pubsub
    # 订阅频道
    await p.subscribe("foo")
    assert await wait_for_message(p) == make_message("subscribe", "foo", 1)

    # 频道发送消息
    assert await r.publish("foo", "test message") == 1

    message = await wait_for_message(p)
    assert isinstance(message, dict)
    assert message == make_message("message", "foo", "test message")


STOPWORD = "STOP"


async def reader(channel: redis.client.PubSub, counter: PeriodCounter):
    while True:
        await counter.incr()
        message = await channel.get_message(ignore_subscribe_messages=True, timeout=None)
        if message is not None:
            Utils.log.info(f"(Reader) Message Received: {message}")
            if message["data"].decode() == STOPWORD:
                Utils.log.info("(Reader) STOP")
                break


@pytest.mark.redis_basic_comm
async def test_pub_sub(r: CacheClient, pubsub, create_period_counter):
    c = await create_period_counter(c_time=100)

    await pubsub.subscribe("channel:1")

    future = asyncio.create_task(reader(pubsub, c))

    await Utils.sleep(0.5)

    await r.publish("channel:1", "Hello")
    await r.publish("channel:1", "World")
    await r.publish("channel:1", STOPWORD)
    await future

    # await Utils.sleep(6)

    total = await c.incr()
    Utils.log.info(f"[test_pub_sub]: {total=}")
    assert 3 <= total < 6

