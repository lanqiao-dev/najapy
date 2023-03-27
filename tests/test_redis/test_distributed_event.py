import pytest
from pydantic import BaseModel, Field

from najapy.cache.redis import BlockingRedisPool
from najapy.common.async_base import Utils
from najapy.enum.base_enum import BaseEnum, Enum
from najapy.event.async_event import DistributedEvent


async def test_distributed_event(p2: BlockingRedisPool):
    channel_name = "test_channel"
    event_name = "test_event"

    e = DistributedEvent(
        p2, channel_name, 1
    )

    await Utils.sleep(0.5)

    async def call_func(num1, num2):
        Utils.log.info("test_distributed_event call success.")
        assert num1 + 1 == 1
        assert num2 + 2 == 3

    e.add_listener(event_name, call_func)

    await e.dispatch(event_name, 0, 1)


async def test_distributed_event_2(p2: BlockingRedisPool):
    channel_name = "test_channel_2"
    event_name = "test_event_2"

    e = DistributedEvent(
        p2, channel_name, 1
    )

    await Utils.sleep(0.5)

    async def call_func(num1, num2):
        Utils.log.info("test_distributed_event_2 call success.")
        assert num1 + 1 == 1
        assert num2 + 2 == 3

    e.add_listener(event_name, call_func)
    await Utils.sleep(2)
    await e.dispatch(event_name, 0, 1)


class QuestionerSelectionType(BaseEnum):
    """答题器选择题类型"""
    SINGLE = Enum(1, "单选")
    MULTI = Enum(2, "多选")


class QuestionerInfo(BaseModel):
    option_num: int = Field(..., title='选项数')
    option_type: QuestionerSelectionType = Field(..., title='类型')
    is_running: bool = Field(..., title='状态：是否进行中 True-进行中 False-已停止')


class QuestionerInfoResp(QuestionerInfo):
    questioner_id: int = Field(..., title='答题器id')
    teacher_id: int = Field(...)
    teacher_sso_id: int = Field(...)
    student_num: int = Field(...)
    correct_rate: int = Field(...)


@pytest.mark.skip("需要单个执行")
async def test_distributed_event_questioner(p2: BlockingRedisPool):
    channel_name = "test_channel_questioner"
    event_name = "test_event_questioner"

    e = DistributedEvent(
        p2, channel_name, 1
    )

    await Utils.sleep(0.5)

    async def call_func(questioner):
        Utils.log.info("test_distributed_event_questioner call success.")
        assert isinstance(questioner, QuestionerInfoResp)
        assert questioner.questioner_id == 1
        assert questioner.option_type == QuestionerSelectionType.MULTI.code
        assert questioner.correct_rate == 1000

    e.add_listener(event_name, call_func)
    await Utils.sleep(1)

    questioner_info_resp = {
        "questioner_id": 1,
        "option_num": 2,
        "option_type": QuestionerSelectionType.MULTI.code,
        "is_running": False,
        "teacher_id": 1,
        "teacher_sso_id": 10,
        "student_num": 10,
        "correct_rate": 1000,
    }

    await e.dispatch(event_name, QuestionerInfoResp.parse_obj(questioner_info_resp))
