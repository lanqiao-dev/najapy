import traceback


class SingletonMetaclass(type):
    """单例的元类实现
    """

    def __init__(cls, _what, _bases=None, _dict=None):

        super().__init__(_what, _bases, _dict)

        cls._instance = None

    def __call__(cls, *args, **kwargs):

        result = None

        try:

            if cls._instance is not None:
                result = cls._instance
            else:
                result = cls._instance = super().__call__(*args, **kwargs)

        except Exception as _:

            traceback.print_exc()

        return result


class Singleton(metaclass=SingletonMetaclass):
    """单例基类
    """
    pass
