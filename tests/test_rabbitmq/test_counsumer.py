
import pytest
from aio_pika import IncomingMessage

from najapy.common.async_base import Utils
from najapy.middleware.rabbitmq.consumer import ConsumerForExchange
from najapy.middleware.rabbitmq.producer_pool import ProducerWithExchangePool
from tests.test_rabbitmq import RabbitMqUrl
from tests.test_rabbitmq.test_procuder_pool import exchange_pool_name1, msg

queue_name1 = "najapy_queue"


async def consume_handler(message: IncomingMessage):
    async with message.process(ignore_processed=True):
        Utils.log.info(f"Received message on {message.routing_key=}, {message.body=}")


@pytest.fixture()
async def producer_exchange_pool():
    producer_pool = ProducerWithExchangePool(
        RabbitMqUrl, pool_size=3, exchange_name=exchange_pool_name1
    )

    await producer_pool.connect()

    for i in range(100):
        await producer_pool.publish(msg + str(i).encode(), routing_key="najapy")

    await Utils.sleep(0.5)

    yield producer_pool

    await producer_pool.close()


@pytest.fixture()
async def consumer_for_exchange():
    consumer = ConsumerForExchange(RabbitMqUrl)

    consumer.config(
        exchange_pool_name1, consume_handler, routing_key="najapy", queue_config={"name": queue_name1}
    )

    await consumer.connect()

    yield consumer

    await consumer.close()


async def test_consume_for_exchange(producer_exchange_pool, consumer_for_exchange):
    res = await consumer_for_exchange.get()
    assert consumer_for_exchange.current_exchange.name == exchange_pool_name1
    assert consumer_for_exchange.current_channel.number == 1
    assert consumer_for_exchange.current_queue.name == queue_name1
    assert res is None

    await Utils.sleep(1)
