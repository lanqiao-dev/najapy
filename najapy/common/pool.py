from asyncio import Queue
from contextlib import asynccontextmanager, contextmanager

from najapy.common.async_base import Utils


class ObjectPool:
    """先进先出的对象池"""
    def __init__(self, maxsize):
        self._queue = Queue(maxsize=maxsize)

    async def _create_obj(self):
        raise NotImplementedError

    async def _delete_obj(self, obj):
        raise NotImplementedError

    async def open(self):
        """打开对象池，将对象放入池中"""
        while not self._queue.full():
            self._queue.put_nowait(
                await self._create_obj()
            )

        Utils.log.info(f"ObjectPool {type(self)} Initialized: {self._queue.qsize()}")

    async def close(self):
        """关闭对象池，将池中的对象进行删除"""
        if self._queue.empty():
            return

        Utils.log.info(f"ObjectPool {type(self)} Delete: {self._queue.qsize()}")
        while not self._queue.empty():
            await self._delete_obj(
                self._queue.get_nowait()
            )

    @property
    def size(self):
        return self._queue.qsize()

    @asynccontextmanager
    async def get(self):
        """异步获取对象池中的对象"""
        obj = await self._queue.get()

        try:
            yield obj
        except Exception as err:
            raise err
        finally:
            self._queue.put_nowait(obj)

    @contextmanager
    def get_nowait(self):
        """同步获取对象池中的对象"""
        obj = self._queue.get_nowait()

        try:
            yield obj
        except Exception as err:
            raise err
        finally:
            self._queue.put_nowait(obj)
