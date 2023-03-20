import asyncio
import os
from unittest import mock

import pytest
import pytest_asyncio
import redis
from najapy.cache.redis import RedisDelegate
from redis.asyncio import Connection

from najapy.common.async_base import Utils


class TestRedisAutoReleaseConnectionPool:
    @pytest_asyncio.fixture
    async def r(self, create_redis) -> redis.Redis:
        """This is necessary since r and r2 create ConnectionPools behind the scenes"""
        r = await create_redis()
        r.auto_close_connection_pool = True
        yield r

    @staticmethod
    async def create_two_conn(r: redis.Redis):
        if not r.single_connection_client:  # Single already initialized connection
            r.connection = await r.connection_pool.get_connection("_")
        return await r.connection_pool.get_connection("_")

    async def test_auto_disconnect_redis_created_pool(self, r: redis.Redis):
        new_conn = await self.create_two_conn(r)
        assert new_conn != r.connection
        await r.close()


class DummyConnection(Connection):
    description_format = "DummyConnection<>"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def can_read_destructive(self, timeout: float = 0):
        return False


async def test_connection_creation(create_pool):
    connection_kwargs = {"foo": "bar", "biz": "baz"}

    pool = await create_pool(connection_kwargs=connection_kwargs, connection_class=DummyConnection)

    connection = await pool.get_connection("_")
    assert isinstance(connection, DummyConnection)
    assert connection.kwargs.get("connection_kwargs") == connection_kwargs


async def test_disconnect(create_pool):
    async with await create_pool() as pool:
        await pool.get_connection("_")
        await pool.disconnect()


async def test_connection_creation_by_with(create_pool):
    connection_kwargs = {"foo": "bar", "biz": "baz"}

    async with await create_pool(
            connection_kwargs=connection_kwargs,
            connection_class=DummyConnection
    ) as pool:
        connection = await pool.get_connection("_")
        assert isinstance(connection, DummyConnection)
        assert connection.kwargs.get("connection_kwargs") == connection_kwargs


async def test_multiple_connections(create_pool):
    async with await create_pool() as pool:
        c1 = await pool.get_connection("_")
        c2 = await pool.get_connection("_")
        assert c1 != c2


async def test_multiple_connections_by_client(create_pool):
    """一个连接池中获取多个连接
    """
    pool = await create_pool()
    c1 = await pool.get_client()
    c2 = await pool.get_client()

    assert c1.connection != c2.connection


async def test_max_connections(create_pool):
    pool = await create_pool(max_connections=2, timeout=1)

    await pool.get_connection("_")
    await pool.get_connection("_")

    with pytest.raises(redis.ConnectionError):
        await pool.get_connection("_")


async def test_max_connections_by_client(create_pool):
    """测试最大连接数
    """
    pool = await create_pool(max_connections=3, timeout=1)
    await pool.get_client()
    await pool.get_client()
    await pool.get_client()

    with pytest.raises(redis.ConnectionError):
        await pool.get_client()


async def test_max_connections_by_client_2(create_pool):
    """测试最大连接数
    """
    pool = await create_pool(max_connections=32, timeout=1)

    async def get_client(pool):
        await pool.get_client()

    tasks = []
    for i in range(32):
        tasks.append(get_client(pool))

    await asyncio.wait(tasks)

    with pytest.raises(redis.ConnectionError):
        await pool.get_client()


async def test_connection_pool_blocks_until_timeout(create_pool):
    """When out of connections, block for timeout seconds, then raise"""
    async with await create_pool(
            max_connections=1, timeout=1,
    ) as pool:
        c1 = await pool.get_connection("_")

        start = Utils.loop_time()
        with pytest.raises(redis.ConnectionError):
            await pool.get_connection("_")
        # we should have waited at least 0.1 seconds
        assert Utils.loop_time() - start >= 1
        await c1.disconnect()


async def test_connection_pool_blocks_until_timeout_by_client(create_pool):
    """超过最大连接数且超出timeout后将报错
    """
    pool = await create_pool(max_connections=1, timeout=1)
    c1 = await pool.get_client()

    start = Utils.loop_time()
    with pytest.raises(redis.ConnectionError):
        await pool.get_client()
    # we should have waited at least 0.1 seconds
    assert Utils.loop_time() - start >= 1
    await c1.close()


async def test_connection_pool_blocks_until_conn_available(create_pool):
    """
    When out of connections, block until another connection is released
    to the pool
    """
    async with await create_pool(
            max_connections=1, timeout=2
    ) as pool:
        c1 = await pool.get_connection("_")

        async def target():
            await Utils.sleep(0.1)
            await pool.release(c1)

        start = Utils.loop_time()
        await asyncio.gather(target(), pool.get_connection("_"))
        assert Utils.loop_time() - start >= 0.1


async def test_connection_pool_blocks_until_conn_available_client(create_pool):
    """超过最大连接数直到释放连接
    """
    pool = await create_pool(max_connections=1, timeout=1)
    c1 = await pool.get_client()

    async def target():
        await Utils.sleep(0.1)
        await c1.close()

    start = Utils.loop_time()
    await asyncio.gather(target(), pool.get_client())
    assert Utils.loop_time() - start >= 0.1


async def test_reuse_previously_released_connection(create_pool):
    async with await create_pool() as pool:
        c1 = await pool.get_connection("_")
        await pool.release(c1)
        c2 = await pool.get_connection("_")
        assert c1 == c2


async def test_reuse_previously_released_connection_by_client(create_pool):
    """释放连接后前后获取的连接为同一个
    """
    pool = await create_pool()
    c1 = await pool.get_client()
    c1_conn = c1.connection
    await c1.close()
    c2 = await pool.get_client()

    assert c1_conn == c2.connection


async def test_reuse_previously_released_connection_by_client_with(create_pool):
    """释放连接后前后获取的连接为同一个 使用with模式
    """
    pool = await create_pool()
    async with await pool.get_client() as cache:
        c1_conn = cache.connection

    async with await pool.get_client() as cache:
        c2_conn = cache.connection

    assert c1_conn == c2_conn


async def test_repr_contains_db_info_tcp(create_pool):
    """tcp连接信息是否正确"""
    pool = await create_pool(client_name="test-client")
    expected = (
        "BlockingRedisPool<Connection<"
        "host=localhost,port=6379,db=9,client_name=test-client>>"
    )
    assert repr(pool) == expected


class TestHealthCheck:
    interval = 60

    def assert_interval_advanced(self, connection):
        diff = connection.next_health_check - Utils.loop_time()
        assert self.interval >= diff > (self.interval - 1)

    async def test_health_check_runs(self, create_pool):
        pool = await create_pool(health_check_interval=self.interval)
        c1 = await pool.get_client()

        if c1.connection:
            c1.connection.next_health_check = Utils.loop_time() - 1
            await c1.connection.check_health()
            self.assert_interval_advanced(c1.connection)

    async def test_health_check_in_pipeline(self, create_pool):
        pool = await create_pool(health_check_interval=self.interval)
        c1 = await pool.get_client()

        async with c1.pipeline(transaction=False) as pipe:
            pipe.connection = await pipe.connection_pool.get_connection("_")
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                    pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = await pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    async def test_health_check_in_transaction(self, create_pool):
        pool = await create_pool(health_check_interval=self.interval)
        c1 = await pool.get_client()

        async with c1.pipeline(transaction=True) as pipe:
            pipe.connection = await pipe.connection_pool.get_connection("_")
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                    pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = await pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    async def wait_for_message(self, pubsub, timeout=0.2, ignore_subscribe_messages=False):
        now = asyncio.get_running_loop().time()
        timeout = now + timeout
        while now < timeout:
            message = await pubsub.get_message(
                ignore_subscribe_messages=ignore_subscribe_messages
            )
            if message is not None:
                return message
            await asyncio.sleep(0.01)
            now = asyncio.get_running_loop().time()
        return None

    async def test_health_check_in_pubsub_before_subscribe(self, create_pool):
        """A health check happens before the first [p]subscribe"""
        pool = await create_pool(health_check_interval=self.interval)
        c1 = await pool.get_client()

        p = c1.pubsub()
        p.connection = await p.connection_pool.get_connection("_")
        p.connection.next_health_check = 0
        with mock.patch.object(
                p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            assert not p.subscribed
            await p.subscribe("foo")
            # the connection is not yet in pubsub mode, so the normal
            # ping/pong within connection.send_command should check
            # the health of the connection
            m.assert_any_call("PING", check_health=False)
            self.assert_interval_advanced(p.connection)

            subscribe_message = await self.wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
