import asyncio
from typing import Optional, Union

import aiormq
from aio_pika import RobustConnection, Message
from aio_pika.abc import TimeoutType, AbstractRobustChannel, AbstractExchange, ExchangeType


class Producer(RobustConnection):
    """RabbitMq生产者"""

    def __init__(self, url, **kwargs):
        super(Producer, self).__init__(url, **kwargs)

        self._channel: Optional[AbstractRobustChannel] = None
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def current_channel(self) -> AbstractRobustChannel:
        return self._channel

    async def connect(self,
                      *,
                      channel_number: int = None,
                      publisher_confirms: bool = True,
                      on_return_raises: bool = False,
                      timeout: TimeoutType = None
                      ):
        """
        channel_number: 指定通道号,默认自动分配
        publisher_confirms: 是否开启发布确认。
                            - True:publish后会返回
                                    DeliveredMessage(
                                        delivery=<Basic.Ack ...>,
                                        header=<pika.spec.BasicProperties ...>,
                                        body=b'hello',
                                        channel=<Channel 1>
                                    );
                            - False: publish后会返回None
        on_return_raises: 消息与routing key不匹配消息发送失败是否抛出异常,True:抛出DeliveryError异常
        timeout: 连接rabbitMq服务的超时时间
        """
        await super().connect(timeout)

        await self.ready()

        if self._channel is None:
            self._channel = await self.channel(
                channel_number=channel_number,
                publisher_confirms=publisher_confirms,
                on_return_raises=on_return_raises
            )

    async def close(self, exc: Optional[aiormq.abc.ExceptionType] = asyncio.CancelledError):
        await self._channel.close()
        await super().close(exc)

    async def publish(self, message: Union[bytes, Message], routing_key=r"", **kwargs) -> \
            Optional[aiormq.abc.ConfirmationFrameType]:
        """
        @param message:
        @param routing_key:
        @param mandatory: 是否设置为强制交付模式。如果为`True`,没有队列绑定到路由键,会返回一个`ReturnFrame`。默认为True
        @param immediate: 是否立即发布模式。如果为`True`,则会在交换机消费之前返回一个`ReturnFrame`。默认为False
        @param timeout:超时时间。如果在指定时间内未发送成功,会返回`TimeoutError`。默认为None
        @return:
        """
        async with self._lock:
            return await self._channel.default_exchange.publish(
                message if isinstance(message, Message) else Message(message),
                routing_key,
                **kwargs
            )


class ProducerWithExchange(Producer):
    """RabbitMq交换机生产者"""

    def __init__(self, url, **kwargs):
        super(ProducerWithExchange, self).__init__(url, **kwargs)

        self._exchange: Optional[AbstractExchange] = None

        self._exchange_name: Optional[str] = None
        self._exchange_type: Optional[ExchangeType] = None
        self._exchange_config: Optional[dict] = None

    @property
    def current_exchange(self) -> AbstractExchange:
        return self._exchange

    def config(self,
               exchange_name: str,
               exchange_type: ExchangeType = ExchangeType.FANOUT,
               *,
               exchange_config=None
               ):
        """
        @param exchange_name:
        @param exchange_type:
                "fanout": 忽略 routing key, broadcast 给所有绑定的队列
                "direct": 根据 routing key 完全匹配来路由
                "topic": routing key 使用通配符 # 和 *,然后匹配绑定队列的 binding key
                "headers": 忽略 routing_key,匹配消息属性来进行路由
                "x-delayed-message": 延迟消息类型
                    eg: exchange_declare(exchange="msgs",
                                        type="x-delayed-message",
                                        arguments={"x-delayed-type":"direct"}
                                        )
                "x-consistent-hash": 一致性哈希类型的Exchange, 与x-delayed-message类似，使用`arguments`参数指定
                "x-modulus-hash": 模镜像Exchange, 与x-delayed-message类似，使用`arguments`参数指定
        @param exchange_config: {
                durable: 是否持久化，broker重启后exchange依然存在,默认为 False
                auto_delete: 是否持久化，broker重启后exchange依然存在,默认为 False
                internal: 设置是否为内部 exchange,默认为 False
                passive: 被动声明,检查是否存在,默认为 False;
                        - 当声明一个已经存在的 exchange 时,passive 为 True 的声明不会返回错误。
                        - 同时也不会重新声明已经存在的 exchange。
                        - 只会检查这个 exchange 是否存在。
                        - 如果 exchange 存在,返回这个已经存在的 exchange。
                        - 如果 exchange 不存在,则返回 ChannelClosed 错误
                timeout: 超时控制,默认为 None
                robust: 指定声明 exchange 的时候是否需要异常处理,默认为 true
                        - True:在声明 exchange 时候进行异常处理和重试
                        - False:在声明 exchange 时候直接抛出异常,不进行重试
                arguments: Arguments = None
            }
        @return:
        """
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._exchange_config = exchange_config if exchange_config else {}

    async def connect(self,
                      *,
                      channel_number: int = None,
                      publisher_confirms: bool = True,
                      on_return_raises: bool = False,
                      timeout: TimeoutType = None
                      ):
        await super().connect(
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
            timeout=timeout
        )

        if self._exchange is None:
            self._exchange = await self._channel.declare_exchange(
                self._exchange_name, self._exchange_type, **self._exchange_config
            )

    async def publish(self, message, routing_key=r"", **kwargs):
        async with self._lock:
            return await self._exchange.publish(
                message if isinstance(message, Message) else Message(message),
                routing_key,
                **kwargs
            )
