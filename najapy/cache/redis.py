from typing import Optional, Type

from redis.asyncio import Connection, BlockingConnectionPool, Redis
from redis.asyncio import ConnectionPool

from najapy.common.async_base import AsyncContextManager, Utils
from najapy.common.base import catch_error
from najapy.event.async_event import DistributedEvent


class RedisDelegate:
    """Redis功能组件
    对外暴露redis连接池与客户端
    """

    def __init__(self):
        self._redis_pool = None

    @property
    def redis_pool(self):

        return self._redis_pool

    def set_redis_key_prefix(self, value):
        self._redis_pool.key_prefix = value

    async def async_init_redis(
            self,
            host, port=6379, db=0, password=None,
            min_connections: Optional[int] = 1,
            max_connections: Optional[int] = 32,
            timeout: Optional[int] = 20,
            **kwargs
    ):
        """初始化redis连接池并对外提供连接池"""

        self._redis_pool = await BlockingRedisPool(
            host, port, db=db, password=password,
            min_connections=min_connections,
            max_connections=max_connections,
            timeout=timeout,
            **kwargs
        ).initialize()

        return self._redis_pool

    async def async_close_redis(self):

        if self._redis_pool is not None:
            await self._redis_pool.disconnect()
            self._redis_pool = None

    async def cache_health(self):

        result = False

        async with await self.get_cache_client() as cache:
            result = bool(await cache.time())
            Utils.log.info(f"Redis Pool Idle Current Connect Nums: {cache.pool.pool.qsize()}/{cache.pool.max_connections}")

        return result

    async def get_cache_client(self, *args, **kwargs):
        """提供redis客户端
        """
        client = None

        if self._redis_pool is not None:
            client = await self._redis_pool.get_client(*args, **kwargs)

        return client

    def event_dispatcher(self, channel_name, channel_count):
        """提供redis广播总线
        """
        return DistributedEvent(self._redis_pool, channel_name, channel_count)


class _PoolMixin(AsyncContextManager):

    def __init__(self, min_connections=0):

        self._name = Utils.uuid1()[:8]
        self._key_prefix = None
        self._min_connections = min_connections if min_connections else 0

    async def _context_release(self):
        pass

    def get_safe_key(self, key, *args, **kwargs):

        if self._key_prefix:
            _key = f'{self._key_prefix}:{key}'
        else:
            _key = key

        if args or kwargs:
            _key = f'{_key}:{Utils.params_sign(*args, **kwargs)}'

        return _key

    @property
    def key_prefix(self):
        return self._key_prefix

    @key_prefix.setter
    def key_prefix(self, value):
        self._key_prefix = value

    async def get_client(self, *args, **kwargs):
        return await CacheClient(self, *args, **kwargs)


class RedisPool(ConnectionPool, _PoolMixin):
    """非阻塞式连接池
    """

    def __init__(
            self,
            host,
            port=6379,
            db=0,
            password=None,
            connection_class: Type[Connection] = Connection,
            min_connections: Optional[int] = 1,
            max_connections: Optional[int] = 32,
            **kwargs
    ):
        super(RedisPool, self).__init__(
            connection_class=connection_class,
            host=host,
            port=port,
            db=db,
            password=password,
            max_connections=max_connections,
            **kwargs
        )

        _PoolMixin.__init__(self, min(min_connections, max_connections))

    async def _init_connection(self):
        with catch_error():
            connections = []
            for _ in range(self._min_connections):
                connections.append(
                    await self.get_connection("_")
                )

            for connection in connections:
                await self.release(connection)

    async def initialize(self):

        await self._init_connection()

        config = self.connection_kwargs

        Utils.log.info(
            f"Redis Pool [{config[r'host']}:{config[r'port']}] ({self._name}) initialized: "
            f"{self._min_connections}/{self.max_connections}"
        )

        return self


class BlockingRedisPool(BlockingConnectionPool, _PoolMixin):
    """阻塞式连接池
    """

    def __init__(
            self,
            host, port=6379, db=0, password=None,
            connection_class: Type[Connection] = Connection,
            min_connections: Optional[int] = 1,
            max_connections: Optional[int] = 32,
            timeout: Optional[int] = 20,
            **kwargs
    ):

        super().__init__(
            connection_class=connection_class,
            host=host,
            port=port,
            db=db,
            password=password,
            max_connections=max_connections,
            timeout=timeout,
            **kwargs
        )

        _PoolMixin.__init__(self, min(min_connections, max_connections))

    async def _init_connection(self):
        with catch_error():
            connections = []
            for _ in range(self._min_connections):
                connections.append(
                    await self.get_connection("_")
                )

            for connection in connections:
                await self.release(connection)

    async def initialize(self):

        await self._init_connection()

        config = self.connection_kwargs

        Utils.log.info(
            f"Redis Pool [{config[r'host']}:{config[r'port']}] ({self._name}) initialized: "
            f"{self.pool.qsize()}/{self.max_connections}"
        )

        return self


class CacheClient(Redis, AsyncContextManager):
    """Redis客户端对象，使用with进行上下文管理
    single_connection_client is True: 一个CacheClient对象即RedisPool中的一个Connection
    remarks:
        1.decode_responses=True会造成意想不到的结果，所以除使用get_obj & set_obj外，使用Redis的方法时需将结果进行解码
    """

    def __init__(self, pool, *args, **kwargs):
        super().__init__(
            connection_pool=pool,
            single_connection_client=True,
            *args,
            **kwargs
        )

        self._pool = pool

    async def _close_conn(self):
        await self.close()

    async def _context_release(self):
        await self._close_conn()

    async def release(self):
        await self._close_conn()

    def get_safe_key(self, key):
        return self._pool.get_safe_key(key)

    def allocate_lock(self, key, expire=60, *, sleep=0.1, blocking=False, blocking_timeout=None, thread_local=True):
        """获取redis分布式锁
        默认非阻塞
        """
        return self.lock(
            self.get_safe_key(key),
            timeout=expire,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local
        )

    async def get_obj(self, name):
        result = await super().get(name)
        return Utils.pickle_loads(result) if result else result

    async def set_obj(self, name, value, ex=3600, nx=False, xx=False):
        value = Utils.pickle_dumps(value)
        return await super().set(name, value, ex=ex, nx=nx, xx=xx)

    @property
    def pool(self):
        return self._pool

    def get_pub_sub(self, **kwargs):
        return self.pubsub(**kwargs)


class ShareCache(AsyncContextManager):
    """共享缓存，使用with进行上下文管理

    基于分布式锁实现的一个缓存共享逻辑，保证在分布式环境下，同一时刻业务逻辑只执行一次，其运行结果会通过缓存被共享
    该共享缓存中的分布式锁采用阻塞式，阻塞时间取决于业务逻辑执行时间，默认为60秒
    """

    def __init__(self, redis_client, share_key, lock_expire=60, lock_blocking_timeout=60):
        """
        redis_client： CacheClient对象
        share_key: 共享缓存key值
        lock_expire: 分布式锁的生命周期
        lock_blocking_timeout: 获取分布式锁的最大阻塞时间
        """

        self._redis_client = redis_client
        self._share_key = redis_client.get_safe_key(share_key)

        self._lock = self._redis_client.allocate_lock(
            f'share_cache:{share_key}',
            expire=lock_expire, blocking=True, blocking_timeout=lock_blocking_timeout
        )

        self.result = None

    async def _context_release(self):

        await self.release()

    async def get(self):

        result = await self._redis_client.get_obj(self._share_key)

        if result is None:

            if await self._lock.acquire():
                result = await self._redis_client.get_obj(self._share_key)

        return result

    async def set(self, value, expire=None):

        return await self._redis_client.set_obj(self._share_key, value, expire)

    async def delete(self):
        """非必要不需进行手动删除"""
        await self._redis_client.delete(self._share_key)

    async def release(self):

        if self._lock:
            await self._lock.release()

        await self._redis_client.close()
        self._redis_client = self._lock = None


class PeriodCounter:
    MIN_EXPIRE = 60

    def __init__(self, redis_client, key_prefix, time_slice: int):
        self._redis_client = redis_client

        self._key_prefix = key_prefix
        self._time_slice = time_slice

    def _get_key(self) -> str:
        timestamp = Utils.timestamp()

        time_period = Utils.math.floor(timestamp / self._time_slice)

        return self._redis_client.get_safe_key(f'{self._key_prefix}:{time_period}')

    async def _execute(self, key: str, val: int) -> int:
        res = None

        async with self._redis_client as cache:
            pipeline = cache.pipeline(transaction=False)
            await pipeline.incrby(key, val)
            await pipeline.expire(key, max(self._time_slice, self.MIN_EXPIRE))
            res, _ = await pipeline.execute()

        return res

    async def incr(self, val: int = 1):
        return await self._execute(self._get_key(), val)

    async def decr(self, val: int = 1) -> int:
        return await self._execute(self._get_key(), -val)

    async def release(self):
        await self._redis_client.close()
