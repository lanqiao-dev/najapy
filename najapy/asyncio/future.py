# -*- coding: utf-8 -*-

import asyncio
from concurrent.futures import ThreadPoolExecutor

from najapy.common.async_base import Utils
from najapy.common.interface import RunnableInterface, TaskInterface


class ThreadPool(RunnableInterface):
    """线程池，桥接线程与协程
    """

    def __init__(self, max_workers=None):

        self._executor = ThreadPoolExecutor(max_workers)

    async def run(self, _callable, *args, **kwargs):
        """线程转协程，不支持协程函数
        """

        loop = asyncio.events.get_event_loop()

        if kwargs:

            return await loop.run_in_executor(
                self._executor,
                Utils.func_partial(
                    _callable,
                    *args,
                    **kwargs
                )
            )

        else:

            return await loop.run_in_executor(
                self._executor,
                _callable,
                *args,
            )


class ThreadWorker:
    """通过线程转协程实现普通函数非阻塞的装饰器
    """

    def __init__(self, max_workers=None):

        self._thread_pool = ThreadPool(max_workers)

    def __call__(self, func):

        @Utils.func_wraps(func)
        def _wrapper(*args, **kwargs):
            return self._thread_pool.run(func, *args, **kwargs)

        return _wrapper


class SubProcess(TaskInterface):
    """子进程管理，通过command方式启动子进程
    """

    @classmethod
    async def create(cls, program, *args, stdin=None, stdout=None, stderr=None, **kwargs):

        inst = cls(program, *args, stdin, stdout, stderr, **kwargs)
        await inst.start()

        return inst

    def __init__(self, program, *args, stdin=None, stdout=None, stderr=None, **kwargs):

        self._program = program
        self._args = args
        self._kwargs = kwargs

        self._stdin = asyncio.subprocess.DEVNULL if stdin is None else stdin
        self._stdout = asyncio.subprocess.DEVNULL if stdout is None else stdout
        self._stderr = asyncio.subprocess.DEVNULL if stderr is None else stderr

        self._process = None
        self._process_id = None

    @property
    def pid(self):

        return self._process_id

    @property
    def process(self):

        return self._process

    @property
    def stdin(self):

        return self._process.stdin if self._process is not None else None

    @property
    def stdout(self):

        return self._process.stdout if self._process is not None else None

    @property
    def stderr(self):

        return self._process.stderr if self._process is not None else None

    def is_running(self):

        return self._process is not None and self._process.returncode is None

    async def start(self):

        if self.is_running():
            return False

        self._process = await asyncio.create_subprocess_exec(
            self._program, *self._args,
            stdin=self._stdin,
            stdout=self._stdout,
            stderr=self._stderr,
            **self._kwargs
        )

        self._process_id = self._process.pid

        return True

    async def stop(self):

        if not self.is_running():
            return False

        self._process.kill()
        await self._process.wait()

        return True

    def kill(self):

        if not self.is_running():
            return False

        self._process.kill()

        return True

    async def wait(self, timeout=None):

        if not self.is_running():
            return

        try:
            await asyncio.wait_for(self._process.wait(), timeout=timeout)
        except Exception as err:
            Utils.log.error(err)
        finally:
            await self.stop()
