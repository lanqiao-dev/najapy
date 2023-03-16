import time

import pytest

from najapy.common.async_base import AsyncCirculator
from najapy.common.buffer import QueueBuffer

pytestmark = pytest.mark.asyncio


class TestBuffer:
    async def test_queue_buffer_1(self):
        async def _handle_data(data):
            assert type(data) == list
            assert len(data) == 20

        buffer = QueueBuffer(
            _handle_data, 20, data_limit=20
        )

        async for i in AsyncCirculator(max_times=20):
            buffer.append(i)

    async def test_queue_buffer_2(self):
        async def _handle_data(data):
            assert type(data) == list
            assert len(data) == 1

        buffer = QueueBuffer(
            _handle_data, 2, timeout=1
        )

        buffer.start()

        async for i in AsyncCirculator(max_times=2):
            buffer.append(i)

        time.sleep(2)
