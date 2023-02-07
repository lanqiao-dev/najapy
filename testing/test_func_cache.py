import time
import pytest

from najapy.cache.base import FuncCache
from najapy.common.async_base import Utils

pytestmark = pytest.mark.asyncio


class TestFuncCache:

    async def test_func_cache_1(self):
        @FuncCache(ttl=6)
        async def func_1(str1, str2, str3=None, str4=None):
            time.sleep(0.5)
            return f'{str1}-{str2}-{str3}-{str4}'

        t1 = Utils.timestamp(msec=True)
        a = await func_1('a', 'c', str3='b', str4='d')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 >= 500

        t1 = Utils.timestamp(msec=True)
        b = await func_1('a', 'c', str3='b', str4='d')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 < 500

        assert a == b

    async def test_func_cache_3(self):
        @FuncCache(ttl=6, includes=['str3', 'str4'])
        async def func_1(str1, str2, str3=None, str4=None):
            time.sleep(0.5)
            return f'{str1}-{str2}-{str3}-{str4}'

        t1 = Utils.timestamp(msec=True)
        a = await func_1('a', 'c', str3='b', str4='d')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 >= 500

        t1 = Utils.timestamp(msec=True)
        b = await func_1('a', 'c', str3='b', str4='d')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 < 500

        assert a == b

    async def test_func_cache_4(self):
        @FuncCache(ttl=6, excludes=['str3', 'str4'])
        async def func_1(str1, str2, str3=None, str4=None):
            time.sleep(0.5)
            return f'{str1}-{str2}-{str3}-{str4}'

        t1 = Utils.timestamp(msec=True)
        a = await func_1('a', 'c', str3='b', str4='d')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 >= 500

        t1 = Utils.timestamp(msec=True)
        b = await func_1('a', 'c', str3='b', str4='d')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 < 500

        assert a == b

    async def test_func_cache_5(self):
        @FuncCache(ttl=6, includes=['str3', 'str4'])
        async def func_1(str1, str2, str3=None, str4=None):
            time.sleep(0.5)
            return f'{str1}-{str2}-{str3}-{str4}'

        t1 = Utils.timestamp(msec=True)
        a = await func_1('a', 'c', str3='b', str4='d')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 >= 500

        t1 = Utils.timestamp(msec=True)
        b = await func_1('a', 'c', str3='e', str4='f')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 >= 500

        assert a != b

    async def test_func_cache_6(self):
        @FuncCache(ttl=6, excludes=['str3'])
        async def func_1(str1, str2, str3=None, str4=None):
            time.sleep(0.5)
            return f'{str1}-{str2}-{str3}-{str4}'

        t1 = Utils.timestamp(msec=True)
        a = await func_1('a', 'c', str3='b', str4='d')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 >= 500

        t1 = Utils.timestamp(msec=True)
        b = await func_1('a', 'c', str3='b', str4='f')
        t2 = Utils.timestamp(msec=True)
        assert t2 - t1 >= 500

        assert a != b
