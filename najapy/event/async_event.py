import redis

from najapy.common.async_base import Utils, FutureWithTimeout, FuncWrapper
from najapy.common.base import catch_error
from najapy.event.event import EventDispatcher as _EventDispatcher


class EventDispatcher(_EventDispatcher):
    """支持异步函数的事件总线
    """

    def _gen_observer(self):
        return FuncWrapper()


class DistributedEvent(EventDispatcher):
    """Redis实现的消息广播总线
    """

    def __init__(self, redis_pool, channel_name, channel_count):

        super().__init__()

        self._redis_pool = redis_pool

        self._channels = [f'event_bus_{Utils.md5_u32(channel_name)}_{index}' for index in range(channel_count)]

        for channel in self._channels:
            Utils.create_task(self._event_listener(channel))

    async def _event_listener(self, channel):
        with catch_error():
            cache = await self._redis_pool.get_client()

            async with cache.get_pub_sub() as pub_sub:
                await pub_sub.subscribe(channel)
                Utils.log.info(f'event bus channel({channel}) receiver created.')

                while True:
                    try:
                        message = await pub_sub.get_message(
                            ignore_subscribe_messages=True, timeout=None
                        )
                        if message:
                            await self._event_assigner(message)

                    except redis.ConnectionError:
                        try:
                            await pub_sub.connection.disconnect()
                            await pub_sub.connection.connect()
                        except redis.ConnectionError:
                            Utils.log.warning('Could not reconnect, trying again in 1 second')
                            await Utils.sleep(1)

    async def _event_assigner(self, message):
        data = Utils.pickle_loads(message[r'data'])

        _type = data.get(r'type', r'')
        args = data.get(r'args', [])
        kwargs = data.get(r'kwargs', {})

        if _type in self._observers:
            self._observers[_type](*args, **kwargs)

    def _gen_observer(self):
        return FuncWrapper()

    async def dispatch(self, _type, *args, **kwargs):

        channel = self._channels[Utils.md5_u32(_type) % len(self._channels)]
        Utils.log.info(f"dispatch channel({channel}):{_type}")

        message = {
            r'type': _type,
            r'args': args,
            r'kwargs': kwargs,
        }

        async with await self._redis_pool.get_client() as cache:
            return await cache.publish(channel, Utils.pickle_dumps(message))

    def gen_event_waiter(self, event_type, delay_time):
        return EventWaiter(self, event_type, delay_time)


class EventWaiter(FutureWithTimeout):
    """带超时的临时消息接收器
    """

    def __init__(self, dispatcher, event_type, delay_time):

        super().__init__(delay_time)

        self._dispatcher = dispatcher
        self._event_type = event_type

        self._dispatcher.add_listener(self._event_type, self._event_handler)

    def set_result(self, result):

        if self.done():
            return

        super().set_result(result)

        self._dispatcher.remove_listener(self._event_type, self._event_handler)

    def _event_handler(self, *args, **kwargs):

        if not self.done():
            self.set_result({r'args': args, r'kwargs': kwargs})
