import asyncio

import pytest

from najapy.common.pool import ObjectPool

DUMMY_OBJ_NAME = "dummy_obj_1"


class DummyObj:
    def __init__(self, name):
        self.name = name


class DummyObjPool(ObjectPool):
    def __init__(self, *args, **kwargs):
        super(DummyObjPool, self).__init__(*args, **kwargs)

    async def _create_obj(self):
        obj = DummyObj(DUMMY_OBJ_NAME)
        print(f"\nCreate {id(obj)}")
        return obj

    async def _delete_obj(self, obj):
        print(f"\nDelete {id(obj)}")
        del obj


@pytest.fixture
async def pool():
    pool = DummyObjPool(maxsize=3)

    await pool.open()

    yield pool

    await pool.close()


class TestObjectPool:
    @staticmethod
    async def test_size(pool):
        assert pool.size == 3

    @staticmethod
    async def test_get(pool):
        async with pool.get() as obj:
            assert obj.name == DUMMY_OBJ_NAME

        assert pool.size == 3

    @staticmethod
    def test_get_nowait(pool):
        with pool.get_nowait() as obj:
            assert obj.name == DUMMY_OBJ_NAME

        assert pool.size == 3

    @staticmethod
    async def test_full_pool(pool):
        with pytest.raises(asyncio.QueueFull):
            pool._queue.put_nowait(DummyObj(name=DUMMY_OBJ_NAME))

    @staticmethod
    async def test_empty_pool(pool):
        async with pool.get() as obj:
            await asyncio.sleep(0.2)

        async with pool.get() as obj:
            await asyncio.sleep(0.2)

        async with pool.get() as obj:
            await asyncio.sleep(0.2)

        async with pool.get() as obj: pass
