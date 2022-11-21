import pytest

from najapy.common.async_base import AsyncCirculatorForSecond
from najapy.common.buffer import QueueBuffer

pytestmark = pytest.mark.asyncio


class TestBuffer:
    async def test_queue_buffer_1(self):
        async def _handle_data(data):
            pass

        buffer = QueueBuffer(
            _handle_data, 100
        )

        async for i in AsyncCirculatorForSecond(max_times=2):
            buffer.append(i)

        assert buffer.size() == 2

    async def test_queue_buffer_2(self):
        async def _handle_data(data):
            pass

        buffer = QueueBuffer(
            _handle_data, 1
        )

        async for i in AsyncCirculatorForSecond(max_times=2):
            buffer.append(i)

        assert buffer.size() == 0
