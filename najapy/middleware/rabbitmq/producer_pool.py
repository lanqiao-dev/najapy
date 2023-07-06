from typing import Optional, Union

from aio_pika import Message
from aio_pika.abc import TimeoutType, ExchangeType

from najapy.common.pool import ObjectPool
from najapy.middleware.rabbitmq.producer import Producer, ProducerWithExchange


class ProducerPool(ObjectPool):
    """RabbitMq生产者池"""

    def __init__(self, url, pool_size, *, connection_config=None):
        self._mq_url = url
        self._connection_config = connection_config if connection_config else {}

        super(ProducerPool, self).__init__(pool_size)

    async def _create_obj(self):
        return Producer(self._mq_url, **self._connection_config)

    async def _delete_obj(self, obj: Producer):
        await obj.close()

    async def connect(self,
                      *,
                      channel_number: int = None,
                      publisher_confirms: bool = True,
                      on_return_raises: bool = False,
                      timeout: TimeoutType = None
                      ):
        await self.open()

        for _ in range(self._queue.qsize()):
            with self.get_nowait() as connection:
                await connection.connect(
                    channel_number=channel_number,
                    publisher_confirms=publisher_confirms,
                    on_return_raises=on_return_raises,
                    timeout=timeout
                )

    async def publish(self, message: Union[bytes, Message], routing_key=r"", **kwargs):
        async with self.get() as connection:
            return await connection.publish(
                message,
                routing_key=routing_key,
                **kwargs
            )


class ProducerWithExchangePool(ProducerPool):
    """RabbitMq交换机生产者池"""
    def __init__(self,
                 url,
                 pool_size,
                 exchange_name,
                 *,
                 exchange_type: ExchangeType = ExchangeType.FANOUT,
                 exchange_config: Optional[dict] = None,
                 connection_config: Optional[dict] = None
                 ):
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._exchange_config = exchange_config

        super().__init__(
            url, pool_size, connection_config=connection_config
        )

    async def _create_obj(self):
        connection = ProducerWithExchange(
            self._mq_url, **self._connection_config
        )
        connection.config(
            self._exchange_name,
            exchange_type=self._exchange_type,
            exchange_config=self._exchange_config
        )

        return connection
