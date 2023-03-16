import asyncio
from unittest import mock

import pytest
from redis import InvalidResponse
from redis.asyncio.connection import PythonParser, BaseParser

from tests.test_redis.mocks import MockStream


async def test_invalid_response(create_redis):
    r = await create_redis()

    raw = b"x"
    fake_stream = MockStream(raw + b"\r\n")

    parser: BaseParser = r.connection._parser
    with mock.patch.object(parser, "_stream", fake_stream):
        with pytest.raises(InvalidResponse) as cm:
            await parser.read_response()
    if isinstance(parser, PythonParser):
        assert str(cm.value) == f"Protocol Error: {raw!r}"
    else:
        assert (
            str(cm.value) == f'Protocol error, got "{raw.decode()}" as reply type byte'
        )
    await r.connection.disconnect()


async def test_can_run_concurrent_commands(r):
    if getattr(r, "connection", None) is not None:
        # Concurrent commands are only supported on pooled or cluster connections
        # since there is no synchronization on a single connection.
        pytest.skip("pool only")
    assert await r.ping() is True
    assert all(await asyncio.gather(*(r.ping() for _ in range(10))))
