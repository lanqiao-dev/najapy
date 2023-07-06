import aiormq
import pytest
from aio_pika import ExchangeType
from aiormq.abc import DeliveredMessage

from najapy.middleware.rabbitmq.producer import Producer, ProducerWithExchange
from tests.test_rabbitmq import RabbitMqUrl

msg = b"hello rabbitmq"
exchange_name1 = "najapy_exchange"
exchange_name2 = "najapy_exchange_direct"
exchange_name3 = "najapy_exchange_topic"


@pytest.fixture()
async def producer():
    producer = Producer(RabbitMqUrl)

    await producer.connect()

    yield producer

    await producer.close()


@pytest.fixture()
async def producer_on_return_raises():
    producer = Producer(RabbitMqUrl)

    await producer.connect(on_return_raises=True)

    yield producer

    await producer.close()


@pytest.fixture()
async def producer_publisher_confirms():
    producer = Producer(RabbitMqUrl)

    await producer.connect(publisher_confirms=False)

    yield producer

    await producer.close()


@pytest.fixture()
async def producer_with_channel_number():
    producer = Producer(RabbitMqUrl)

    await producer.connect(channel_number=110)

    yield producer

    await producer.close()


class TestProducer:

    @staticmethod
    async def test_publish(producer):
        res = await producer.publish(msg)
        assert isinstance(res, DeliveredMessage)

    @staticmethod
    async def test_batch_publish(producer):
        for i in range(100):
            res = await producer.publish(msg)
            assert isinstance(res, DeliveredMessage)

    @staticmethod
    async def test_nack(producer_on_return_raises):
        with pytest.raises(aiormq.DeliveryError):
            await producer_on_return_raises.publish(msg, routing_key="wrong")

    @staticmethod
    async def test_publisher_confirms(producer_publisher_confirms):
        res = await producer_publisher_confirms.publish(msg)
        assert res is None

    @staticmethod
    async def test_publisher_channel_number(producer_with_channel_number):
        res = await producer_with_channel_number.publish(msg)

        assert isinstance(res, DeliveredMessage)
        assert res.channel.number == 110


@pytest.fixture()
async def producer_with_exchange():
    producer = ProducerWithExchange(RabbitMqUrl)
    producer.config(exchange_name1)

    await producer.connect()

    yield producer

    await producer.close()


@pytest.fixture()
async def producer_with_exchange_direct():
    producer = ProducerWithExchange(RabbitMqUrl)
    producer.config(exchange_name2, exchange_type=ExchangeType.DIRECT)

    await producer.connect()

    yield producer

    await producer.close()


@pytest.fixture()
async def producer_with_exchange_topic():
    producer = ProducerWithExchange(RabbitMqUrl)
    producer.config(exchange_name3, exchange_type=ExchangeType.TOPIC)

    await producer.connect()

    yield producer

    await producer.close()


class TestProducerWithExchange:
    @staticmethod
    async def test_publish(producer_with_exchange):
        res = await producer_with_exchange.publish(msg)

        assert isinstance(res, DeliveredMessage)
        assert res.exchange == exchange_name1

    @staticmethod
    async def test_batch_publish(producer_with_exchange):
        for i in range(100):
            res = await producer_with_exchange.publish(msg)

            assert isinstance(res, DeliveredMessage)
            assert res.exchange == exchange_name1

    @staticmethod
    async def test_publish_routing_key(producer_with_exchange):
        res = await producer_with_exchange.publish(msg, routing_key="najapy")

        assert isinstance(res, DeliveredMessage)
        assert res.exchange == exchange_name1
        assert res.routing_key == "najapy"

    @staticmethod
    async def test_publish_exchange_type(producer_with_exchange_direct):
        res = await producer_with_exchange_direct.publish(msg, routing_key="najapy")

        assert isinstance(res, DeliveredMessage)
        assert res.routing_key == "najapy"
        assert res.exchange == exchange_name2

    @staticmethod
    async def test_publish_exchange_type2(producer_with_exchange_topic):
        res = await producer_with_exchange_topic.publish(msg, routing_key="najapy*")

        assert isinstance(res, DeliveredMessage)
        assert res.routing_key == "najapy*"
        assert res.exchange == exchange_name3
