import asyncio
from typing import Optional, List

from cachetools import TTLCache

from najapy.common.async_base import Utils


class StackCache:
    """堆栈缓存

    使用运行内存作为高速缓存，可有效提高并发的处理能力

    """

    def __init__(self, maxsize=0xff, ttl=60):
        self._cache = TTLCache(maxsize, ttl)

    def has(self, key):
        return key in self._cache

    def get(self, key, default=None):
        return self._cache.get(key, default)

    def set(self, key, val):
        if val is None:
            return

        self._cache[key] = val

    def incr(self, key, val=1):
        res = self.get(key, 0) + val

        self.set(key, res)

        return res

    def decr(self, key, val=1):
        res = self.get(key, 0) - val

        self.set(key, res)

        return res

    def delete(self, key):
        del self._cache[key]

    def size(self):
        return len(self._cache)

    def clear(self):
        return self._cache.clear()

    @property
    def cache(self):
        return self._cache

    def __getitem__(self, key):
        self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self.delete(key)


class FuncCache:
    """函数缓存

    使用堆栈缓存实现的函数缓存，在有效期内函数签名一致就会命中缓存
    includes: 可从关键字参数中指定某些参数作为缓存key值
    excludes: 可从关键字参数中指定某些参数不作为缓存key值
    """

    def __init__(self, maxsize=0xff, ttl=10,
                 includes: Optional[List[str]] = None,
                 excludes: Optional[List[str]] = None
                 ):

        self._cache = StackCache(maxsize, ttl)
        self._includes = includes or []
        self._excludes = excludes or []

    def _get_func_sign(self, func, *args, **kwargs):
        if not self._includes and not self._excludes:
            return Utils.params_sign(func, *args, **kwargs)

        if self._includes:
            sign_kwargs = {key: val for key, val in kwargs.items() if key in self._includes}
            return Utils.params_sign(func, **sign_kwargs)

        if self._excludes:
            sign_kwargs = {key: val for key, val in kwargs.items() if key not in self._excludes}
            return Utils.params_sign(func, *args, **sign_kwargs)

    def __call__(self, func):

        @Utils.func_wraps(func)
        async def _wrapper(*args, **kwargs):
            func_sign = self._get_func_sign(func, *args, **kwargs)
            result = self._cache.get(func_sign)

            if result is None:

                result = await Utils.awaitable_wrapper(
                    func(*args, **kwargs)
                )

                if result is not None:
                    self._cache.set(func_sign, result)

            return result

        return _wrapper


class ShareFuture:
    """共享Future装饰器

    同一时刻并发调用函数时，使用该装饰器的函数签名一致的调用，会共享计算结果

    """

    def __init__(self):

        self._future = {}

    def __call__(self, func):

        @Utils.func_wraps(func)
        async def _wrapper(*args, **kwargs):

            future = None

            func_sign = Utils.params_sign(func, *args, **kwargs)

            if func_sign in self._future:

                future = asyncio.Future()

                self._future[func_sign].append(future)

            else:

                future = Utils.create_task(
                    func(*args, **kwargs)
                )

                if future is None:
                    TypeError(r'Not Coroutine Object')

                self._future[func_sign] = [future]

                future.add_done_callback(
                    Utils.func_partial(self._clear_future, func_sign)
                )

            return await future

        return _wrapper

    def _clear_future(self, func_sign, _):

        if func_sign not in self._future:
            return

        futures = self._future.pop(func_sign)

        result = futures.pop(0).result()

        for future in futures:
            future.set_result(Utils.deepcopy(result))
