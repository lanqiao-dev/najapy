import pamqp
import pytest
from aio_pika import ExchangeType
from aiormq.abc import DeliveredMessage
from pika.spec import Basic

from najapy.middleware.rabbitmq.producer_pool import ProducerPool, ProducerWithExchangePool
from tests.test_rabbitmq import RabbitMqUrl

msg = b"hello rabbitmq producer pool"
exchange_pool_name1 = "najapy_exchange_pool"
exchange_pool_name2 = "najapy_exchange_pool_direct"
exchange_pool_name3 = "najapy_exchange_pool_topic"


@pytest.fixture()
async def producer_pool():
    producer_pool = ProducerPool(RabbitMqUrl, pool_size=3)

    await producer_pool.connect()

    yield producer_pool

    await producer_pool.close()


class TestProducerPool:
    @staticmethod
    async def test_publish(producer_pool):
        res = await producer_pool.publish(msg)
        assert isinstance(res, DeliveredMessage)
        assert res.body == msg

    @staticmethod
    async def test_batch_publish(producer_pool):
        for i in range(1000):
            res = await producer_pool.publish(msg + str(i).encode())
            assert isinstance(res, DeliveredMessage)
            assert res.body == msg + str(i).encode()
            assert producer_pool.size == 3


@pytest.fixture()
async def producer_exchange_pool():
    producer_pool = ProducerWithExchangePool(
        RabbitMqUrl, pool_size=3, exchange_name=exchange_pool_name1
    )

    await producer_pool.connect()

    yield producer_pool

    await producer_pool.close()


@pytest.fixture()
async def producer_exchange_pool_direct():
    producer_pool = ProducerWithExchangePool(
        RabbitMqUrl, pool_size=3, exchange_name=exchange_pool_name2, exchange_type=ExchangeType.DIRECT
    )

    await producer_pool.connect()

    yield producer_pool

    await producer_pool.close()


@pytest.fixture()
async def producer_exchange_pool_topic():
    producer_pool = ProducerWithExchangePool(
        RabbitMqUrl, pool_size=3, exchange_name=exchange_pool_name3, exchange_type=ExchangeType.TOPIC
    )

    await producer_pool.connect()

    yield producer_pool

    await producer_pool.close()


class TestProducerExchangePool:
    @staticmethod
    async def test_publish(producer_exchange_pool):
        res = await producer_exchange_pool.publish(msg + b'xiami', routing_key="najapy")
        if isinstance(res, pamqp.commands.Basic.Ack):
            assert res.name == 'Basic.Ack'
            return

        assert isinstance(res, DeliveredMessage)
        assert res.body == msg
        assert res.exchange == exchange_pool_name1

    @staticmethod
    async def test_publish_direct(producer_exchange_pool_direct):
        res = await producer_exchange_pool_direct.publish(msg, routing_key="najapy")
        if isinstance(res, pamqp.commands.Basic.Ack):
            assert res.name == 'Basic.Ack'
            return
        assert isinstance(res, DeliveredMessage)
        assert res.body == msg
        assert res.exchange == exchange_pool_name2

    @staticmethod
    async def test_publish_topic(producer_exchange_pool_topic):
        res = await producer_exchange_pool_topic.publish(msg, routing_key="najapy")
        if isinstance(res, pamqp.commands.Basic.Ack):
            assert res.name == 'Basic.Ack'
            return
        assert isinstance(res, DeliveredMessage)
        assert res.body == msg
        assert res.exchange == exchange_pool_name3

    @staticmethod
    async def test_batch_publish(producer_exchange_pool):
        for i in range(1000):
            res = await producer_exchange_pool.publish(msg, routing_key="najapy")
            if isinstance(res, pamqp.commands.Basic.Ack):
                assert res.name == 'Basic.Ack'
                return

            assert isinstance(res, DeliveredMessage)
            assert res.body == msg
            assert res.exchange == exchange_pool_name1
