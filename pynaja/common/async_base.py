import asyncio
import functools
import inspect
import traceback
import types
from contextvars import Context

from pynaja.common.base import _Utils, Ignore, FuncWrapper as _FuncWrapper


class Utils(_Utils):
    """异步基础工具类

    集成常用的异步工具函数

    """

    sleep = staticmethod(asyncio.sleep)

    @staticmethod
    def is_coroutine_function(func):

        if isinstance(func, functools.partial):
            return asyncio.iscoroutinefunction(func.func)
        else:
            return asyncio.iscoroutinefunction(func)

    @staticmethod
    async def awaitable_wrapper(obj):
        """自适应awaitable对象
        """

        if inspect.isawaitable(obj):
            return await obj
        else:
            return obj

    @staticmethod
    @types.coroutine
    def wait_frame(count=10):
        """暂停指定帧数
        """

        for _ in range(max(1, count)):
            yield

    @staticmethod
    def loop_time():
        """获取当前loop内部时钟
        """

        loop = asyncio.events.get_event_loop()

        return loop.time()

    @classmethod
    def call_soon(cls, callback, *args, **kwargs):
        """延时调用(能隔离上下文)
        """

        loop = asyncio.events.get_event_loop()

        if kwargs:

            return loop.call_soon(
                async_adapter(
                    cls.func_partial(
                        callback,
                        *args,
                        **kwargs
                    )
                ),
                context=Context()
            )

        else:

            return loop.call_soon(
                async_adapter(callback),
                *args,
                context=Context()
            )

    @classmethod
    def call_soon_threadsafe(cls, callback, *args, **kwargs):
        """延时调用(线程安全，能隔离上下文)
        """

        loop = asyncio.events.get_event_loop()

        if kwargs:

            return loop.call_soon_threadsafe(
                async_adapter(
                    cls.func_partial(
                        callback,
                        *args,
                        **kwargs
                    )
                ),
                context=Context()
            )

        else:

            return loop.call_soon_threadsafe(
                async_adapter(callback),
                *args,
                context=Context()
            )

    @classmethod
    def call_later(cls, delay, callback, *args, **kwargs):
        """延时指定秒数调用(能隔离上下文)
        """

        loop = asyncio.events.get_event_loop()

        if kwargs:

            return loop.call_later(
                delay,
                async_adapter(
                    cls.func_partial(
                        callback,
                        *args,
                        **kwargs
                    )
                ),
                context=Context()
            )

        else:

            return loop.call_later(
                delay,
                async_adapter(callback),
                *args,
                context=Context()
            )

    @classmethod
    def call_at(cls, when, callback, *args, **kwargs):
        """指定时间调用(能隔离上下文)
        """

        loop = asyncio.events.get_event_loop()

        if kwargs:

            return loop.call_at(
                when,
                async_adapter(
                    cls.func_partial(
                        callback,
                        *args,
                        **kwargs
                    )
                ),
                context=Context()
            )

        else:

            return loop.call_at(
                when,
                async_adapter(callback),
                *args,
                context=Context()
            )

    @staticmethod
    def create_task(coro):
        """将协程对象包装成task对象(兼容Future接口)
        """

        if asyncio.iscoroutine(coro):
            return asyncio.create_task(coro)
        else:
            return None

    @staticmethod
    def run_until_complete(future):
        """运行事件循环直到future结束
        """

        loop = asyncio.events.get_event_loop()

        return loop.run_until_complete(future)


class AsyncCirculator:
    """异步循环器

    提供一个循环体内的代码重复执行管理逻辑，可控制超时时间、执行间隔(LoopFrame)和最大执行次数

    async for index in AsyncCirculator():
        pass

    其中index为执行次数，从1开始

    """

    def __init__(self, timeout=0, interval=0xff, max_times=0):

        if timeout > 0:
            self._expire_time = Utils.loop_time() + timeout
        else:
            self._expire_time = 0

        self._interval = interval
        self._max_times = max_times

        self._current = 0

    def __aiter__(self):

        return self

    async def __anext__(self):

        if self._current > 0:

            if (self._max_times > 0) and (self._max_times <= self._current):
                raise StopAsyncIteration()

            if (self._expire_time > 0) and (self._expire_time <= Utils.loop_time()):
                raise StopAsyncIteration()

            await self._sleep()

        self._current += 1

        return self._current

    async def _sleep(self):

        await Utils.wait_frame(self._interval)


class AsyncCirculatorForSecond(AsyncCirculator):

    def __init__(self, timeout=0, interval=1, max_times=0):
        super().__init__(timeout, interval, max_times)

    async def _sleep(self):
        await Utils.sleep(self._interval)


def async_adapter(func):
    """异步函数适配装饰器

    使异步函数可以在同步函数中调用，即非阻塞式的启动异步函数，同时会影响上下文资源的生命周期

    """

    if not Utils.is_coroutine_function(func):
        return func

    @Utils.func_wraps(func)
    def _wrapper(*args, **kwargs):
        return Utils.create_task(
            func(*args, **kwargs)
        )

    return _wrapper


class AsyncContextManager:
    """异步上下文资源管理器

    子类通过实现_context_release接口，方便的实现with语句管理上下文资源释放

    """

    async def __aenter__(self):

        await self._context_initialize()

        return self

    async def __aexit__(self, exc_type, exc_value, _traceback):

        await self._context_release()

        if exc_type is Ignore:

            return not exc_value.throw()

        elif exc_value:

            _Utils.log.exception(traceback.format_exc())

            return True

    async def _context_initialize(self):

        pass

    async def _context_release(self):

        raise NotImplementedError()


class FutureWithTimeout(asyncio.Future):
    """带超时功能的Future
    """

    def __init__(self, delay, default=None):
        super().__init__()

        self._timeout_handle = Utils.call_later(
            delay,
            self.set_result,
            default
        )

        self.add_done_callback(self._clear_timeout)

    def _clear_timeout(self, *_):
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
            self._timeout_handle = None


class FuncWrapper(_FuncWrapper):
    """非阻塞异步函数包装器

    将多个同步或异步函数包装成一个可调用对象

    """

    def __call__(self, *args, **kwargs):

        for func in self._callables:
            Utils.call_soon(func, *args, **kwargs)


class MultiTasks:
    """多任务并发管理器

    提供协程的多任务并发的解决方案

    tasks = MultiTasks()
    tasks.append(func1())
    tasks.append(func2())
    ...
    tasks.append(funcN())
    await tasks

    多任务中禁止使用上下文资源共享的对象(如mysql和redis等)
    同时需要注意类似这种不能同时为多个协程提供服务的对象会造成不可预期的问题

    """

    def __init__(self, *args):

        self._coro_list = list(args)
        self._task_list = []

    def __await__(self):

        if len(self._coro_list) > 0:
            self._task_list = [Utils.create_task(coro) for coro in self._coro_list]
            self._coro_list.clear()
            yield from asyncio.gather(*self._task_list).__await__()

        return [task.result() for task in self._task_list]

    def __len__(self):

        return self._coro_list.__len__()

    def __iter__(self):

        for task in self._task_list:
            yield task.result()

    def __getitem__(self, item):

        return self._task_list.__getitem__(item).result()

    def append(self, coro):

        return self._coro_list.append(coro)

    def extend(self, coro_list):

        return self._coro_list.extend(coro_list)

    def clear(self):

        self._coro_list.clear()
        self._task_list.clear()
