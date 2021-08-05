"""任务工具集"""
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from najapy.common.async_base import Utils

TIMEZONE = pytz.timezone(r'Asia/Shanghai')


class TaskInterface:
    """Task接口定义
    """

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def is_running(self):
        raise NotImplementedError()


class TaskAbstract(TaskInterface):
    """任务基类
    """

    def __init__(self, scheduler=None):

        global TIMEZONE

        self._scheduler = AsyncIOScheduler(
            job_defaults={
                r'coalesce': False,
                r'max_instances': 1,
                r'misfire_grace_time': 10
            },
            timezone=TIMEZONE
        ) if scheduler is None else scheduler

    @property
    def scheduler(self):

        return self._scheduler

    @staticmethod
    def _func_wrapper(func, *args, **kwargs):

        if Utils.is_coroutine_function(func):

            if args or kwargs:
                return Utils.func_partial(func, *args, **kwargs)
            else:
                return func

        async def _wrapper():
            return func(*args, **kwargs)

        return _wrapper

    def is_running(self):

        return self._scheduler.running

    def start(self):

        return self._scheduler.start()

    def stop(self):

        return self._scheduler.shutdown()

    def add_job(self):

        raise NotImplementedError()

    def remove_job(self, job_id):

        return self._scheduler.remove_job(job_id)

    def remove_all_jobs(self):

        return self._scheduler.remove_all_jobs()


class IntervalTask(TaskAbstract):
    """间隔任务类
    """

    @classmethod
    def create(cls, interval, func, *args, **kwargs):
        inst = cls()

        inst.add_job(interval, func, *args, **kwargs)

        return inst

    def add_job(self, interval, func, *args, **kwargs):
        return self._scheduler.add_job(
            self._func_wrapper(func, *args, **kwargs),
            r'interval', seconds=interval
        )


class CronTask(TaskAbstract):
    """定时任务类
    """

    @classmethod
    def create(cls, crontab, func, *args, **kwargs):
        inst = cls()

        inst.add_job(crontab, func, *args, **kwargs)

        return inst

    def add_job(self, crontab, func, *args, **kwargs):
        return self._scheduler.add_job(
            self._func_wrapper(func, *args, **kwargs),
            CronTrigger.from_crontab(crontab, TIMEZONE)
        )
