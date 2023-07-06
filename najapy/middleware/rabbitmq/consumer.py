import asyncio
from typing import Optional, Callable

import aio_pika
import aiormq
from aio_pika import RobustConnection
from aio_pika.abc import AbstractRobustChannel, AbstractRobustQueue, TimeoutType


class Consumer(RobustConnection):
    """RabbitMq消费者"""

    def __init__(self, url, **kwargs):
        super(Consumer, self).__init__(url, **kwargs)

        self._channel: Optional[AbstractRobustChannel] = None
        self._queue: Optional[AbstractRobustQueue] = None

        self._channel_qos_config = None

        self._queue_name = None
        self._queue_config = None

        self._consume_func: Optional[Callable] = None
        self._consume_no_ack = None

    @property
    def current_channel(self):
        return self._channel

    @property
    def current_queue(self):
        return self._queue

    @property
    def queue_name(self):
        return self._queue_name

    def config(self,
               queue_name,
               consume_func: Callable,
               consume_no_ack=False,
               *,
               channel_qos_config: Optional[dict] = None,
               queue_config: Optional[dict] = None
               ):
        """

        @param queue_name:
        @param consume_func:
        @param consume_no_ack:
        @param channel_qos_config: {
                    prefetch_count: int = 0, 信道中未确认（ACK）的消息可缓存的数量，为0代表无限制，该数值决定了消费速度
                    prefetch_size: int = 0, 信道中未确认的消息可缓存的大小
                    global_: bool = False,  是否将此Qos设置应用于整个信道
                    timeout: TimeoutType = None, 设置Qos的超时时间，在指定时间内未完成,会返回错误
                }
        @param queue_config: {
                    durable: 持久性。如果true,表示在Broker重启后队列仍然存在
                    exclusive: 使队列变为独占。独占队列只能被当前连接访问,在连接关闭时被删除。其他连接的被动声明独占队列是不允许的。
                    passive: True-只是检查队列是否存在。如果队列存在,则直接返回这个已经存在的队列,如果队列不存在,则抛出aio_pika.exceptions.ChannelClosed
                            False-即使这个队列已经存在,也会重新声明这个队列.新的声明会覆盖之前声明,如果新的声明参数与之前不同,则新的参数会生效.如果队列不存在,将会创建一个新的队列
                    auto_delete: 关闭channel时自动删除队列
                    arguments:
                    timeout:
                }
        @return:
        """
        self._channel_qos_config = channel_qos_config if channel_qos_config else {r"prefetch_count": 1}

        self._queue_name = queue_name
        self._queue_config = queue_config if queue_config else {}

        self._consume_func = consume_func
        self._consume_no_ack = consume_no_ack

    async def connect(self, timeout: TimeoutType = None) -> None:
        self._RobustConnection__channels.clear()
        await super(Consumer, self).connect(timeout)
        await self.ready()

        self._channel = await self.channel()

        await self._channel.set_qos(**self._channel_qos_config)

        self._queue = await self._channel.declare_queue(
            self._queue_name, **self._queue_config
        )

        if self._consume_func is not None:
            await self._queue.consume(self._consume_func, no_ack=self._consume_no_ack)

    async def close(
            self, exc: Optional[aiormq.abc.ExceptionType] = asyncio.CancelledError,
    ) -> None:
        await self._channel.close(exc)
        await super().close(exc)

    async def get(self, *, no_ack=False, timeout=1):
        await self._queue.get(no_ack=no_ack, timeout=timeout)


class ConsumerForExchange(Consumer):
    def __init__(self, url, **kwargs):
        super().__init__(url, **kwargs)

        self._exchange: Optional[aio_pika.abc.AbstractExchange] = None

        self._exchange_name = None
        self._routing_key = None

    @property
    def current_exchange(self) -> aio_pika.abc.AbstractExchange:
        return self._exchange

    def config(self,
               exchange_name,
               consume_func: Callable,
               routing_key=None,
               queue_config: Optional[dict] = None,
               consume_no_ack=False,
               *,
               channel_qos_config: Optional[dict] = None,
               ):
        queue_name = None

        if queue_config is None:
            queue_config = {"exclusive": True}
        elif "name" in queue_config:
            queue_name = queue_config.pop("name")

        super(ConsumerForExchange, self).config(
            queue_name, consume_func, consume_no_ack,
            channel_qos_config=channel_qos_config, queue_config=queue_config
        )

        self._exchange_name = exchange_name
        self._routing_key = routing_key

    async def connect(self, timeout: TimeoutType = None, ensure_exchange=True) -> None:
        """
        @param timeout: 执行的超时时间
        @param ensure_exchange: True-如果指定的exchange不存在,则会创建该exchange；False-检查exchange是否存在,存在则直接返回该exchange对象,不存在则返回None
        """
        self._exchange = None

        await super(ConsumerForExchange, self).connect(timeout)

        self._exchange = await self._channel.get_exchange(self._exchange_name, ensure=ensure_exchange)

        await self._queue.bind(self._exchange, self._routing_key)
