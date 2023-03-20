from typing import Callable, TypeVar
from urllib.parse import urlparse, parse_qs, unquote

import pytest_asyncio
from redis import Connection, ConnectionPool, Redis

from najapy.cache.redis import RedisDelegate, PeriodCounter
from packaging.version import Version

import pytest
import redis
from redis.asyncio.connection import URL_QUERY_ARGUMENT_PARSERS

from najapy.common.async_base import Utils

REDIS_INFO = {}
default_redis_url = "redis://localhost:6379/9?password=myredis"

_DecoratedTest = TypeVar("_DecoratedTest", bound="Callable")
_TestDecorator = Callable[[_DecoratedTest], _DecoratedTest]

POOL_KEY_PREFIX = "test_redis"


def pytest_addoption(parser):
    parser.addoption(
        "--redis-url",
        default=default_redis_url,
        action="store",
        help="Redis connection string, defaults to `%(default)s`",
    )


def _get_redis_params(url):
    """redis-py V4.5.1 中url解析有bug,所以重写解析方法
    """
    kwargs = {}

    for name, value in parse_qs(url.query).items():
        if value and len(value) > 0:
            value = unquote(value[0])
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    kwargs[name] = parser(value)
                except (TypeError, ValueError):
                    raise ValueError(f"Invalid value for `{name}` in connection URL.")
            else:
                kwargs[name] = value

    if url.username:
        kwargs["username"] = unquote(url.username)
    if url.password:
        kwargs["password"] = unquote(url.password)

    if url.hostname:
        kwargs["host"] = unquote(url.hostname)
    if url.port:
        kwargs["port"] = int(url.port)

    # If there's a path argument, use it as the db argument if a
    # querystring value wasn't specified
    if url.path and "db" not in kwargs:
        try:
            kwargs["db"] = int(unquote(url.path).replace("/", ""))
        except (AttributeError, ValueError):
            pass

    return kwargs


def _get_info(redis_url):
    url = urlparse(redis_url)
    url_kwargs = _get_redis_params(url)
    url_kwargs["connection_class"] = Connection

    pool = ConnectionPool(**url_kwargs)
    client = Redis(connection_pool=pool)
    info = client.info()
    try:
        client.execute_command("DPING")
        info["enterprise"] = True
    except redis.ResponseError:
        info["enterprise"] = False
    client.connection_pool.disconnect()
    return info


def pytest_sessionstart(session):
    redis_url = session.config.getoption("--redis-url")
    info = _get_info(redis_url)
    try:
        version = info["redis_version"]
        arch_bits = info["arch_bits"]
        cluster_enabled = info["cluster_enabled"]
        enterprise = info["enterprise"]
    except redis.ConnectionError:
        version = "10.0.0"
        arch_bits = 64
        cluster_enabled = False
        enterprise = False

    REDIS_INFO["version"] = version
    REDIS_INFO["arch_bits"] = arch_bits
    REDIS_INFO["cluster_enabled"] = cluster_enabled
    REDIS_INFO["enterprise"] = enterprise
    # store REDIS_INFO in config so that it is available from "condition strings"
    session.config.REDIS_INFO = REDIS_INFO


@pytest_asyncio.fixture()
async def create_redis(request):
    """创建CacheClient对象
    """
    teardown_clients = []

    async def client_factory(
            url: str = request.config.getoption("--redis-url"),
            *args,
            **kwargs,
    ):
        url = urlparse(url)
        url_kwargs = _get_redis_params(url)
        rd = RedisDelegate()
        pool = await rd.async_init_redis(
            **url_kwargs
        )
        pool.key_prefix = POOL_KEY_PREFIX
        client = await rd.get_cache_client(*args, **kwargs)

        async def teardown():
            await client.flushdb()
            await client.release()

        teardown_clients.append(teardown)
        return client

    yield client_factory

    for teardown in teardown_clients:
        await teardown()


@pytest_asyncio.fixture()
async def r(create_redis):
    return await create_redis()


@pytest_asyncio.fixture()
async def create_pool(request):
    """创建连接池对象,该连接池为阻塞式
    """
    teardown_clients = []

    async def pool_factor(
            url: str = request.config.getoption("--redis-url"),
            *args,
            **kwargs,
    ):
        url = urlparse(url)
        url_kwargs = _get_redis_params(url)

        url_kwargs.update(**kwargs)

        rd = RedisDelegate()

        pool = await rd.async_init_redis(
            **url_kwargs
        )
        pool.key_prefix = "test_pool"

        async def teardown():
            await pool.disconnect()

        teardown_clients.append(teardown)
        return pool

    yield pool_factor

    for teardown in teardown_clients:
        await teardown()


@pytest_asyncio.fixture()
async def p(create_pool):
    return await create_pool()


@pytest_asyncio.fixture()
async def create_pool_2(request):
    """创建连接池对象,该连接池为阻塞式
    """

    async def pool_factor(
            url: str = request.config.getoption("--redis-url"),
            *args,
            **kwargs,
    ):
        url = urlparse(url)
        url_kwargs = _get_redis_params(url)

        url_kwargs.update(**kwargs)

        pool = await RedisDelegate().async_init_redis(
            **url_kwargs
        )
        pool.key_prefix = "test_pool"

        return pool

    yield pool_factor


@pytest_asyncio.fixture()
async def p2(create_pool_2):
    return await create_pool_2()


@pytest_asyncio.fixture()
async def create_period_counter(r, request):
    """创建计数器"""
    async def c_factor(
            **kwargs
    ):
        c_time = kwargs.get("c_time")
        c = PeriodCounter(r, Utils.uuid1(), c_time)
        return c

    yield c_factor


@pytest_asyncio.fixture()
async def c(create_period_counter):
    return await create_period_counter()


def skip_if_server_version_lt(min_version: str) -> _TestDecorator:
    redis_version = REDIS_INFO.get("version", "0")
    check = Version(redis_version) < Version(min_version)
    return pytest.mark.skipif(check, reason=f"Redis version required >= {min_version}")


def skip_if_server_version_gte(min_version: str) -> _TestDecorator:
    redis_version = REDIS_INFO.get("version", "0")
    check = Version(redis_version) >= Version(min_version)
    return pytest.mark.skipif(check, reason=f"Redis version required < {min_version}")


def skip_if_redis_enterprise() -> _TestDecorator:
    check = REDIS_INFO.get("enterprise", False) is True
    return pytest.mark.skipif(check, reason="Redis enterprise")


def skip_ifnot_redis_enterprise() -> _TestDecorator:
    check = REDIS_INFO.get("enterprise", False) is False
    return pytest.mark.skipif(check, reason="Not running in redis enterprise")
