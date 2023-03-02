from typing import Callable

from najapy.common.async_base import Utils
from najapy.common.task import IntervalTask


class _BufferAbs:
    def __init__(self):
        self._buffer = []

    def _consume_buffer(self):
        raise NotImplementedError

    def _get_buffer(self):
        if not self._buffer:
            return None

        buffer, self._buffer = self._buffer, []

        return buffer

    def size(self):
        return len(self._buffer)

    def append(self, data):
        self._buffer.append(data)
        self._consume_buffer()

    def extent(self, data_list):
        self._buffer.extend(data_list)
        self._consume_buffer()


class DadaQueue(_BufferAbs):
    def __init__(self, handler: Callable, task_limit=1, data_limit=1):
        super(DadaQueue, self).__init__()

        self._handler = handler
        self._tasks = set()
        self._task_limit = task_limit
        self._data_limit = data_limit

    def _consume_buffer(self):
        while len(self._tasks) < self._task_limit:
            if not self._create_task():
                break

    def _create_task(self):
        result = False

        if len(self._buffer) > 0:
            datas = self._buffer[0: self._data_limit]
            for data in datas:
                if len(self._buffer) <= 0:
                    break
                self._buffer.remove(data)

            task = Utils.create_task(
                self._handler(datas)
            )

            task.add_done_callback(self._task_done)

            self._tasks.add(task)

            result = True

        return result

    def _task_done(self, task):
        if task in self._tasks:
            self._tasks.remove(task)

        self._consume_buffer()


class QueueBuffer(_BufferAbs):
    def __init__(self, handler: Callable, size_limit, *, timeout=1, task_limit=1, data_limit=1):
        """
        handler: 处理数据的callable
        size_limit: buffer最大尺寸
        timeout: 超时时间，即每间隔timeout时间处理一次buffer中的数据
        task_limit: 处理数据最大任务数
        data_limit: 每个任务批量处理的最大数据量
        """
        super().__init__()

        self._size_limit = size_limit
        self._data_queue = DadaQueue(handler, task_limit, data_limit=data_limit)

        self._interval_task = IntervalTask.create(timeout, self._do_consume_buffer)

    def _consume_buffer(self):
        if self.size() >= self._size_limit:
            self._do_consume_buffer()

    def _do_consume_buffer(self):
        buffer = self._get_buffer()

        if buffer:
            self._data_queue.extent(buffer)

    def data_queue_size(self):
        return self._data_queue.size()

    def start(self):
        self._interval_task.start()

    def stop(self):
        self._interval_task.stop()
