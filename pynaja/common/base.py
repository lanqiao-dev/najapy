import copy
import functools
import hashlib
import math
import pickle
import platform
import random
import sys
import textwrap
import itertools
import os
import time
import traceback
import uuid
import weakref
import zlib
from contextlib import contextmanager
from datetime import datetime, timedelta
from contextvars import ContextVar

import jwt
import pytz
import loguru
import ujson

from pynaja.common.metaclass import Singleton


class _Utils(Singleton):
    """基础工具基类

    集成常用的工具函数

    """
    _BYTES_TYPES = (bytes, type(None))
    _STRING_TYPES = (str, type(None))

    math = math
    random = random
    textwrap = textwrap
    itertools = itertools

    path = os.path

    log = loguru.logger

    deepcopy = staticmethod(copy.deepcopy)

    func_wraps = staticmethod(functools.wraps)
    func_partial = staticmethod(functools.partial)

    randint = staticmethod(random.randint)
    randstr = staticmethod(random.sample)

    getenv = staticmethod(os.getenv)
    getpid = staticmethod(os.getpid)
    getppid = staticmethod(os.getppid)

    @classmethod
    def environment(cls):

        return {
            r'python': sys.version,
            r'system': [platform.system(), platform.release(), platform.version(), platform.machine()],
        }

    @classmethod
    def basestring(cls, val):

        if isinstance(val, cls._STRING_TYPES):
            return val

        if isinstance(val, bytes):
            return val.decode(r'utf-8')

        raise TypeError(
            r'Expected str, bytes, or None; got %r' % type(val)
        )

    @classmethod
    def json_encode(cls, val, **kwargs):

        return ujson.dumps(val, **kwargs)

    @classmethod
    def json_decode(cls, val, **kwargs):

        return ujson.loads(cls.basestring(val), **kwargs)

    @classmethod
    def pickle_dumps(cls, val):

        stream = pickle.dumps(val)

        result = zlib.compress(stream)

        return result

    @classmethod
    def pickle_loads(cls, val):

        stream = zlib.decompress(val)

        result = pickle.loads(stream)

        return result

    @classmethod
    def today(cls, origin=False):

        result = datetime.now()

        return result.replace(hour=0, minute=0, second=0, microsecond=0) if origin else result

    @classmethod
    def yesterday(cls, origin=False):

        result = datetime.now() - timedelta(days=1)

        return result.replace(hour=0, minute=0, second=0, microsecond=0) if origin else result

    @classmethod
    def timestamp(cls, msec=False):

        if msec:
            return int(time.time() * 1000)
        else:
            return int(time.time())

    @classmethod
    def time2stamp(cls, time_str, format_type=r'%Y-%m-%d %H:%M:%S', timezone=None):

        if timezone is None:
            return int(datetime.strptime(time_str, format_type).timestamp())
        else:
            return int(datetime.strptime(time_str, format_type).replace(tzinfo=pytz.timezone(timezone)).timestamp())

    @classmethod
    def stamp2time(cls, time_int=None, format_type=r'%Y-%m-%d %H:%M:%S', timezone=None):

        if time_int is None:
            time_int = cls.timestamp()

        if timezone is None:
            return time.strftime(format_type, datetime.fromtimestamp(time_int).timetuple())
        else:
            return time.strftime(format_type, datetime.fromtimestamp(time_int, pytz.timezone(timezone)).timetuple())

    @classmethod
    def split_int(cls, val, sep=r',', minsplit=0, maxsplit=-1):

        result = [int(item.strip()) for item in val.split(sep, maxsplit) if item.strip().isdigit()]

        fill = minsplit - len(result)

        if fill > 0:
            result.extend(0 for _ in range(fill))

        return result

    @classmethod
    def split_str(cls, val, sep=r'|', minsplit=0, maxsplit=-1):

        if val:
            result = [item.strip() for item in val.split(sep, maxsplit)]
        else:
            result = []

        fill = minsplit - len(result)

        if fill > 0:
            result.extend(r'' for _ in range(fill))

        return result

    @classmethod
    def uuid1(cls, node=None, clock_seq=None):

        return uuid.uuid1(node, clock_seq).hex

    @classmethod
    def utf8(cls, val):

        if isinstance(val, cls._BYTES_TYPES):
            return val

        if isinstance(val, str):
            return val.encode(r'utf-8')

        raise TypeError(
            r'Expected str, bytes, or None; got %r' % type(val)
        )

    @classmethod
    def md5(cls, val):

        val = cls.utf8(val)

        return hashlib.md5(val).hexdigest()

    @classmethod
    def md5_u32(cls, val):

        val = cls.utf8(val)

        return int(hashlib.md5(val).hexdigest(), 16) >> 96

    @classmethod
    def md5_u64(cls, val):

        val = cls.utf8(val)

        return int(hashlib.md5(val).hexdigest(), 16) >> 64

    @classmethod
    def params_join(cls, params, filters=[]):

        if filters:
            params = {key: val for key,
                                   val in params.items() if key not in filters}

        return r'&'.join(f'{key}={val}' for key, val in sorted(params.items(), key=lambda x: x[0]))

    @classmethod
    def params_sign(cls, *args, **kwargs):

        result = []

        if args:
            result.extend(str(val) for val in args)

        if kwargs:
            result.append(cls.params_join(kwargs))

        return cls.md5(r'&'.join(result))

    @staticmethod
    def chunks_content_n(arr, n):
        """
        in - > [1, 2, 3, 4, 5, 6, 7, 8] , 2
        out -> [[1, 2], [3, 4], [5, 6], [7, 8]]
        """
        return [arr[i:i + n] for i in range(0, len(arr), n)]

    @staticmethod
    def chunks_count_m(arr, n):
        """
        in - > [1, 2, 3, 4, 5, 6, 7, 8] , 2
        out -> [[1, 2, 3, 4], [5, 6, 7, 8]]
        """
        length = len(arr)

        p = length // n

        # 尽量把原来的 lst 列表中的数字等分成 n 份

        partitions = []

        if n == 1:
            partitions.append(arr)
            return partitions

        for i in range(n - 1):

            partitions.append(arr[i * p:i * p + p])

        else:

            partitions.append(arr[i * p + p:])

        return partitions

    @staticmethod
    def list_dict_index_by_key(list_dict_data, key):
        new_data = {}
        for data in list_dict_data:
            new_data[data[key]] = data

        return new_data

    @staticmethod
    def list_dict_group_by_key(list_dict_data, key):
        """
        key: [value_list]
        """
        new_data = {}
        for data in list_dict_data:
            data_list = new_data.setdefault(data[key], [])
            data_list.append(data)

        return new_data

    @classmethod
    def jwt_encode(cls, val, key, algorithms=r'HS256'):

        result = jwt.encode(val, key, algorithms)

        return cls.basestring(result)

    @classmethod
    def jwt_decode(cls, val, key, algorithms=None):
        if not algorithms:
            algorithms = [r'HS256']

        val = cls.utf8(val)

        return jwt.decode(val, key, algorithms)


@contextmanager
def catch_error():
    """异常捕获，打印error级日志

    通过with语句捕获异常，代码更清晰，还可以使用Ignore异常安全的跳出with代码块

    """

    try:

        yield

    except Ignore as err:

        if err.throw():
            raise err

    except Exception as err:

        _Utils.log.exception(err)


class ContextManager:
    """上下文资源管理器

    子类通过实现_context_release接口，方便的实现with语句管理上下文资源释放

    """

    def __del__(self):

        self._context_release()

    def __enter__(self):

        self._context_initialize()

        return self

    def __exit__(self, exc_type, exc_value, _traceback):

        self._context_release()

        if exc_type is Ignore:

            return not exc_value.throw()

        elif exc_value:

            _Utils.log.exception(traceback.format_exc())

            return True

    def _context_initialize(self):

        pass

    def _context_release(self):

        raise NotImplementedError()


class BaseError(Exception):

    def __init__(self, data=None):
        super().__init__()

        self._data = data

    @property
    def data(self):
        return self._data

    def __repr__(self):
        return repr(self._data)


class Ignore(BaseError):
    """可忽略的异常

    用于with语句块跳出，或者需要跳出多层逻辑的情况

    """

    def __init__(self, data=None, layers=1):

        super().__init__(data)

        self._layers = layers

        if self._data:
            _Utils.log.warning(self._data)

    def throw(self):

        if self._layers > 0:
            self._layers -= 1

        return self._layers > 0


class WeakContextVar:
    """弱引用版的上下文资源共享器
    """

    _instances = {}

    def __new__(cls, name):

        if name in cls._instances:
            inst = cls._instances[name]
        else:
            inst = cls._instances[name] = super().__new__(cls)

        return inst

    def __init__(self, name):

        self._context_var = ContextVar(name, default=None)

    def get(self):

        ref = self._context_var.get()

        return None if ref is None else ref()

    def set(self, value):

        return self._context_var.set(weakref.ref(value))


class FuncWrapper:
    """函数包装器

    将多个函数包装成一个可调用对象

    """

    def __init__(self):

        self._callables = set()

    def __call__(self, *args, **kwargs):

        for func in self._callables:
            try:
                func(*args, **kwargs)
            except Exception as err:
                _Utils.log.error(err)

    @property
    def is_valid(self):

        return len(self._callables) > 0

    def add(self, func):

        if func in self._callables:
            return False
        else:
            self._callables.add(func)
            return True

    def remove(self, func):

        if func in self._callables:
            self._callables.remove(func)
            return True
        else:
            return False
