# -*- coding: utf-8 -*-


class FunctorInterface:
    """仿函数接口定义
    """

    def __call__(self):
        raise NotImplementedError()


class RunnableInterface:
    """Runnable接口定义
    """

    def run(self, *args, **kwargs):
        raise NotImplementedError()


class TaskInterface:
    """Task接口定义
    """

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def is_running(self):
        raise NotImplementedError()


class ObjectFactoryInterface:
    """对象工厂类接口定义
    """

    def create(self):
        raise NotImplementedError()
