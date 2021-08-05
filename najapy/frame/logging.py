# -*- coding: utf-8 -*-

import queue
import logging
from threading import Thread
from datetime import timedelta

from loguru._file_sink import FileSink

from najapy.common.async_base import Utils


class LogFileRotator:

    @classmethod
    def make(cls, _size=500, _time=r'00:00'):

        return cls(_size, _time).should_rotate

    def __init__(self, _size, _time):

        _size = _size * (1024 ** 2)
        _time = Utils.split_int(_time, r':')

        now_time = Utils.today()

        self._size_limit = _size
        self._time_limit = now_time.replace(hour=_time[0], minute=_time[1])

        if now_time >= self._time_limit:
            self._time_limit += timedelta(days=1)

    def should_rotate(self, message, file):

        file.seek(0, 2)

        if file.tell() + len(message) > self._size_limit:
            return True

        if message.record[r'time'].timestamp() > self._time_limit.timestamp():
            self._time_limit += timedelta(days=1)
            return True

        return False


DEFAULT_LOG_FILE_ROTATOR = LogFileRotator.make()


class LogInterceptor(logging.Handler):
    """日志拦截器
    """

    def emit(self, record):
        Utils.log.opt(
            depth=6,
            exception=record.exc_info
        ).log(
            record.levelname,
            record.getMessage()
        )


DEFAULT_LOG_INTERCEPTOR = LogInterceptor()


class QueuedFileSink(FileSink):
    """日志文件队列
    """

    def __init__(self, path, *, buffer_size=0, buffer_block=False, **kwargs):

        super().__init__(path, **kwargs)

        self._buffer = queue.Queue(buffer_size)
        self._buffer_size = buffer_size
        self._buffer_block = buffer_block

        self._worker = Thread(target=self._queued_writer, daemon=True)
        self._running = True

        self._worker.start()

    def write(self, message):

        try:
            self._buffer.put(message, block=self._buffer_block)
        except queue.Full as _:
            print(f'Log queued writer overflow: {self._buffer_size}')

    def stop(self):

        self._running = False
        self._worker.join(10)

        super().stop()

    def _queued_writer(self):

        while self._running:

            try:
                message = self._buffer.get(block=True, timeout=1)
                if message:
                    super().write(message)
            except queue.Empty as _:
                pass


def init_logger(
        level, handler=None,
        file_path=None, file_rotation=DEFAULT_LOG_FILE_ROTATOR, file_retention=0xff,
        debug=False
):
    level = level.upper()

    if handler or file_path:

        Utils.log.remove()

        if handler:
            Utils.log.add(
                handler,
                level=level,
                enqueue=True,
                backtrace=debug
            )

        if file_path:
            Utils.log.add(
                QueuedFileSink(
                    Utils.path.join(file_path, f'runtime_{{time}}_{Utils.getpid()}.log'),
                    rotation=file_rotation,
                    retention=file_retention
                ),
                level=level,
                enqueue=True,
                backtrace=debug
            )

    else:

        Utils.log.level(level)

    logging.getLogger().addHandler(DEFAULT_LOG_INTERCEPTOR)
