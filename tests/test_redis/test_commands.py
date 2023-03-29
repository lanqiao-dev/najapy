import asyncio
import datetime

import pytest
import pytest_asyncio
import redis
from redis.client import EMPTY_RESPONSE

from najapy.cache.redis import CacheClient
from tests.test_redis.conftest import skip_if_server_version_lt

REDIS_6_VERSION = "5.9.0"


@pytest_asyncio.fixture()
async def r_teardown(r):
    """使用client后删除私有用户名
    """
    usernames = []

    def factory(username):
        usernames.append(username)
        return r

    yield factory
    for username in usernames:
        await r.acl_deluser(username)


async def redis_server_time(client: CacheClient):
    seconds, milliseconds = await client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.datetime.fromtimestamp(timestamp)


class TestResponseCallbacks:
    """测试返回结果回调
    """

    async def test_response_callbacks(self, r):
        assert r.response_callbacks == redis.Redis.RESPONSE_CALLBACKS
        assert id(r.response_callbacks) != id(redis.Redis.RESPONSE_CALLBACKS)
        r.set_response_callback("GET", lambda x: "static")
        await r.set("a", "foo")
        assert await r.get("a") == "static"

    async def test_case_insensitive_command_names(self, r: redis.Redis):
        assert r.response_callbacks["del"] == r.response_callbacks["DEL"]


async def test_asynckills(r: CacheClient):
    """redis-py v4.5.3 中修复的bug"""
    await r.set("foo", "foo")
    await r.set("bar", "bar")

    t = asyncio.create_task(r.get("foo"))
    await asyncio.sleep(1)
    t.cancel()

    try:
        await t
    except asyncio.CancelledError:
        pytest.fail("connection left open with unread response")

    assert await r.get("bar") == b"bar"
    assert await r.ping()
    assert await r.get("foo") == b"foo"


class TestRedisCommands:
    @pytest.mark.redis_basic_comm
    async def test_get_and_set(self, r: redis.Redis):
        """get and set 不相互独立测试"""
        assert await r.get("a") is None
        string = "xiami"
        byte_string = b"value"
        integer = 5
        floater = 0.01
        unicode_string = chr(3456) + "abcd" + chr(3421)

        assert await r.set("string", string)
        assert await r.set("byte_string", byte_string)
        assert await r.set("integer", integer)
        assert await r.set("floater", floater)
        assert await r.set("unicode_string", unicode_string)

        assert await r.get("string") == string.encode("utf-8")
        assert await r.get("byte_string") == byte_string
        assert await r.get("integer") == str(integer).encode()
        assert await r.get("floater") == str(floater).encode()
        assert (await r.get("unicode_string")).decode("utf-8") == unicode_string

    @pytest.mark.redis_basic_comm
    async def test_get_and_set_obj(self, r: CacheClient):
        assert await r.get("a") is None
        string = "xiami"
        byte_string = b"value"
        integer = 5
        floater = 0.01
        unicode_string = chr(3456) + "abcd" + chr(3421)
        dict1 = {
            "a": 1,
            "b": [1, 2, 3],
            "c": 0.01,
            "d": b'value'
        }

        assert await r.set_obj("string", string)
        assert await r.set_obj("byte_string", byte_string)
        assert await r.set_obj("integer", integer)
        assert await r.set_obj("floater", floater)
        assert await r.set_obj("unicode_string", unicode_string)
        assert await r.set_obj("dict1", dict1)

        assert await r.get_obj("string") == string
        assert await r.get_obj("byte_string") == byte_string
        assert await r.get_obj("integer") == integer
        assert await r.get_obj("floater") == floater
        assert await r.get_obj("unicode_string") == unicode_string
        assert await r.get_obj("dict1") == dict1

    async def test_command_on_invalid_key_type(self, r: redis.Redis):
        await r.lpush("a", "1", "2")
        with pytest.raises(redis.ResponseError):
            await r.get("a")

        assert await r.lrange("a", 0, -1) == [b"2", b"1"]

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.redis_acl
    async def test_acl_cat_no_category(self, r: CacheClient):
        categories = await r.acl_cat()
        assert isinstance(categories, list)
        assert "read" in categories

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.redis_acl
    async def test_acl_cat_with_category(self, r: CacheClient):
        commands = await r.acl_cat("read")
        assert isinstance(commands, list)
        assert "get" in commands

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.redis_acl
    async def test_acl_deluser(self, r_teardown):
        username = "redis-py-user"
        r = r_teardown(username)

        assert await r.acl_deluser(username) == 0
        assert await r.acl_setuser(username, enabled=False, reset=True)
        assert await r.acl_deluser(username) == 1

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.redis_acl
    async def test_acl_genpass(self, r: CacheClient):
        password = await r.acl_genpass()
        assert isinstance(password, str)

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.redis_acl
    async def test_acl_users(self, r: CacheClient):
        users = await r.acl_users()
        assert isinstance(users, list)
        assert len(users) > 0

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.redis_acl
    async def test_acl_whoami(self, r: CacheClient):
        username = await r.acl_whoami()
        assert isinstance(username, str)

    async def test_client_list(self, r: CacheClient):
        clients = await r.client_list()
        assert isinstance(clients[0], dict)
        assert "addr" in clients[0]

    async def test_config_get(self, r: CacheClient):
        data = await r.config_get()
        assert "maxmemory" in data
        assert data["maxmemory"].isdigit()

    async def test_config_resetstat(self, r: CacheClient):
        await r.ping()
        prior_commands_processed = int((await r.info())["total_commands_processed"])
        assert prior_commands_processed >= 1
        await r.config_resetstat()
        reset_commands_processed = int((await r.info())["total_commands_processed"])
        assert reset_commands_processed < prior_commands_processed

    async def test_config_set(self, r: CacheClient):
        await r.config_set("timeout", 70)
        assert (await r.config_get())["timeout"] == "70"
        assert await r.config_set("timeout", 0)
        assert (await r.config_get())["timeout"] == "0"

    @pytest.mark.redis_basic_comm
    async def test_dbsize(self, r: CacheClient):
        await r.set("a", "foo")
        await r.set("b", "bar")
        assert await r.dbsize() == 2

    @pytest.mark.redis_basic_comm
    async def test_echo(self, r: CacheClient):
        assert await r.echo("foo bar") == b"foo bar"

    async def test_info(self, r: CacheClient):
        await r.set("a", "foo")
        await r.set("b", "bar")
        info = await r.info()
        assert isinstance(info, dict)
        assert "arch_bits" in info.keys()
        assert "redis_version" in info.keys()

    @pytest.mark.redis_basic_comm
    async def test_lastsave(self, r: CacheClient):
        assert isinstance(await r.lastsave(), datetime.datetime)

    async def test_object(self, r: CacheClient):
        await r.set("a", "foo")
        assert isinstance(await r.object("refcount", "a"), int)
        assert isinstance(await r.object("idletime", "a"), int)
        assert await r.object("encoding", "a") in (b"raw", b"embstr")
        assert await r.object("idletime", "invalid-key") is None

    @pytest.mark.redis_basic_comm
    async def test_ping(self, r: CacheClient):
        assert await r.ping()

    async def test_empty_response_option(self, r: CacheClient):
        opts = {EMPTY_RESPONSE: []}
        await r.delete("a")
        assert await r.execute_command("EXISTS", "a", **opts) == 0

    @pytest.mark.redis_basic_comm
    async def test_append(self, r: CacheClient):
        assert await r.append("c", "a1") == 2
        assert await r.get("c") == b"a1"
        assert await r.append("c", "a2") == 4
        assert await r.get("c") == b"a1a2"

    @pytest.mark.redis_basic_comm
    async def test_decr(self, r: redis.Redis):
        assert await r.decr("a") == -1
        assert await r.get("a") == b"-1"
        assert await r.decr("a") == -2
        assert await r.get("a") == b"-2"
        assert await r.decr("a", amount=5) == -7
        assert await r.get("a") == b"-7"

    @pytest.mark.redis_basic_comm
    async def test_decrby(self, r: redis.Redis):
        assert await r.decrby("a", amount=2) == -2
        assert await r.decrby("a", amount=3) == -5
        assert await r.get("a") == b"-5"

    @pytest.mark.redis_basic_comm
    async def test_delete(self, r: redis.Redis):
        assert await r.delete("a") == 0
        await r.set("a", "foo")
        assert await r.delete("a") == 1

    @pytest.mark.redis_basic_comm
    async def test_delete_with_multiple_keys(self, r: redis.Redis):
        await r.set("a", "foo")
        await r.set("b", "bar")
        assert await r.delete("a", "b") == 2
        assert await r.get("a") is None
        assert await r.get("b") is None

    @pytest.mark.redis_basic_comm
    async def test_delitem(self, r: CacheClient):
        await r.set_obj("a", "foo")
        await r.delete("a")
        assert await r.get_obj("a") is None

    @skip_if_server_version_lt("4.0.0")
    @pytest.mark.redis_basic_comm
    async def test_unlink(self, r: CacheClient):
        """unlink与delete的区别在于unlink非阻塞即先断开key值后续异步删除key值"""
        assert await r.unlink("a") == 0
        await r.set_obj("a", "foo")
        assert await r.unlink("a") == 1
        assert await r.get_obj("a") is None

    @skip_if_server_version_lt("4.0.0")
    @pytest.mark.redis_basic_comm
    async def test_unlink_with_multiple_keys(self, r: CacheClient):
        await r.set_obj("a", "foo")
        await r.set_obj("b", "bar")
        assert await r.unlink("a", "b") == 2
        assert await r.get_obj("a") is None
        assert await r.get_obj("b") is None

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.redis_basic_comm
    async def test_dump_and_restore(self, r: CacheClient):
        await r.set("a", "foo")
        dumped = await r.dump("a")
        await r.delete("a")
        await r.restore("a", 0, dumped)
        assert await r.get("a") == b"foo"

    @skip_if_server_version_lt("3.0.0")
    @pytest.mark.redis_basic_comm
    async def test_dump_and_restore_and_replace(self, r: redis.Redis):
        await r.set("a", "bar")
        dumped = await r.dump("a")
        with pytest.raises(redis.ResponseError):
            await r.restore("a", 0, dumped)

        await r.restore("a", 0, dumped, replace=True)
        assert await r.get("a") == b"bar"

    @pytest.mark.redis_basic_comm
    async def test_exists(self, r: CacheClient):
        assert await r.exists("a") == 0
        await r.set_obj("a", "foo")
        await r.set_obj("b", "bar")
        assert await r.exists("a") == 1
        assert await r.exists("a", "b") == 2

    @pytest.mark.redis_basic_comm
    async def test_exists_contains(self, r: CacheClient):
        assert not await r.exists("a")
        await r.set_obj("a", "foo")
        assert await r.exists("a")

    @pytest.mark.redis_basic_comm
    async def test_expire(self, r: CacheClient):
        assert not await r.expire("a", 10)
        await r.set_obj("a", "foo")
        assert await r.expire("a", 10)
        assert 0 < await r.ttl("a") <= 10
        assert await r.persist("a")  # 移除key值的过期时间
        assert await r.ttl("a") == -1

    @pytest.mark.redis_basic_comm
    async def test_expireat_datetime(self, r: CacheClient):
        expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
        await r.set("a", "foo")
        assert await r.expireat("a", expire_at)
        assert 0 < await r.ttl("a") <= 61

    @pytest.mark.redis_basic_comm
    async def test_get_set_bit(self, r: redis.Redis):
        # no value
        assert not await r.getbit("a", 5)
        # set bit 5
        assert not await r.setbit("a", 5, True)
        assert await r.getbit("a", 5)
        # unset bit 4
        assert not await r.setbit("a", 4, False)
        assert not await r.getbit("a", 4)
        # set bit 4
        assert not await r.setbit("a", 4, True)
        assert await r.getbit("a", 4)
        # set bit 5 again
        assert await r.setbit("a", 5, True)
        assert await r.getbit("a", 5)

    @pytest.mark.redis_basic_comm
    async def test_getrange(self, r: redis.Redis):
        await r.set("a", "foo")
        assert await r.getrange("a", 0, 0) == b"f"
        assert await r.getrange("a", 0, 2) == b"foo"
        assert await r.getrange("a", 3, 4) == b""

    @pytest.mark.redis_basic_comm
    async def test_getset(self, r: redis.Redis):
        """getset命令将在Redis 6.2之后废弃"""
        assert await r.getset("a", "foo") is None
        assert await r.getset("a", "bar") == b"foo"
        assert await r.get("a") == b"bar"

    @pytest.mark.redis_basic_comm
    async def test_incr(self, r: CacheClient):
        assert await r.incr("a") == 1
        assert await r.get("a") == b"1"
        assert await r.incr("a") == 2
        assert await r.get("a") == b"2"
        assert await r.incr("a", amount=5) == 7
        assert await r.get("a") == b"7"

    @pytest.mark.redis_basic_comm
    async def test_incrby(self, r: redis.Redis):
        assert await r.incrby("a") == 1
        assert await r.incrby("a", 4) == 5
        assert await r.get("a") == b"5"

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.redis_basic_comm
    async def test_incrbyfloat(self, r: redis.Redis):
        assert await r.incrbyfloat("a") == 1.0
        assert await r.get("a") == b"1"
        assert await r.incrbyfloat("a", 1.1) == 2.1
        assert float(await r.get("a")) == float(2.1)

    @pytest.mark.redis_basic_comm
    async def test_keys(self, r: redis.Redis):
        assert await r.keys() == []
        keys_with_underscores = {b"test_a", b"test_b"}
        keys = keys_with_underscores.union({b"testc"})
        for key in keys:
            await r.set(key, 1)
        assert set(await r.keys(pattern="test_*")) == keys_with_underscores
        assert set(await r.keys(pattern="test*")) == keys

    @pytest.mark.redis_basic_comm
    async def test_mget(self, r: redis.Redis):
        assert await r.mget([]) == []
        assert await r.mget(["a", "b"]) == [None, None]
        await r.set("a", "1")
        await r.set("b", "2")
        await r.set("c", "3")
        assert await r.mget("a", "other", "b", "c") == [b"1", None, b"2", b"3"]

    @pytest.mark.redis_basic_comm
    async def test_mset(self, r: redis.Redis):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        assert await r.mset(d)
        for k, v in d.items():
            assert await r.get(k) == v

    @pytest.mark.redis_basic_comm
    async def test_msetnx(self, r: redis.Redis):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        assert await r.msetnx(d)
        d2 = {"a": b"x", "d": b"4"}
        assert not await r.msetnx(d2)
        for k, v in d.items():
            assert await r.get(k) == v
        assert await r.get("d") is None

    @pytest.mark.redis_basic_comm
    async def test_rename(self, r: CacheClient):
        await r.set_obj("a", "1")
        assert await r.rename("a", "b")
        assert await r.get_obj("a") is None
        assert await r.get_obj("b") == "1"

    @pytest.mark.redis_basic_comm
    async def test_renamenx(self, r: CacheClient):
        """Renamenx 命令用于在新的 key 不存在时修改 key 的名称 """
        await r.set_obj("a", "1")
        await r.set_obj("b", "2")
        assert not await r.renamenx("a", "b")
        assert await r.get_obj("a") == "1"
        assert await r.get_obj("b") == "2"

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.redis_basic_comm
    async def test_set_nx(self, r: CacheClient):
        """nx True 键值对不存在时才能设置成功"""
        assert await r.set_obj("a", "1", nx=True)
        assert not await r.set_obj("a", "2", nx=True)
        assert await r.get_obj("a") == "1"

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.redis_basic_comm
    async def test_set_xx(self, r: CacheClient):
        """xx True 键值对存在时才能设置成功"""
        assert not await r.set_obj("a", "1", xx=True)
        assert await r.get_obj("a") is None
        await r.set_obj("a", "bar")
        assert await r.set_obj("a", "2", xx=True)
        assert await r.get_obj("a") == "2"

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.redis_basic_comm
    async def test_set_px(self, r: CacheClient):
        """px 毫秒"""
        assert await r.set("a", "1", px=10000)
        assert await r.get("a") == b"1"
        assert 0 < await r.pttl("a") <= 10000
        assert 0 < await r.ttl("a") <= 10

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.redis_basic_comm
    async def test_set_px_timedelta(self, r: CacheClient):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert await r.set("a", "1", px=expire_at)
        assert 0 < await r.pttl("a") <= 1000
        assert 0 < await r.ttl("a") <= 1

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.redis_basic_comm
    async def test_set_ex(self, r: CacheClient):
        """ex 秒"""
        assert await r.set_obj("a", "1", ex=10)
        assert 0 < await r.ttl("a") <= 10

    @pytest.mark.redis_basic_comm
    async def test_set_ex(self, r: CacheClient):
        """ex 秒"""
        assert await r.set_obj("a", "1", ex=10)
        assert 0 < await r.ttl("a") <= 10

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.redis_basic_comm
    async def test_set_keepttl(self, r: CacheClient):
        await r.set("a", "val")
        assert await r.set("a", "1", xx=True, px=10000)
        assert 0 < await r.ttl("a") <= 10
        await r.set("a", "2", keepttl=True)
        assert await r.get("a") == b"2"
        assert 0 < await r.ttl("a") <= 10

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.redis_basic_comm
    async def test_set_get_true(self, r: CacheClient):
        assert await r.set("a", "val", get=True) is None
        assert await r.set("a", "foo", get=True) == b'val'
        assert await r.get("a") == b"foo"

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.redis_basic_comm
    async def test_set_ex_timedelta(self, r: CacheClient):
        expire_at = datetime.timedelta(seconds=60)
        assert await r.set_obj("a", "1", ex=expire_at)
        assert 0 < await r.ttl("a") <= 60

    @pytest.mark.redis_basic_comm
    async def test_setex(self, r: redis.Redis):
        assert await r.setex("a", 60, "1")
        assert await r.get("a") == b"1"
        assert 0 < await r.ttl("a") <= 60

    @pytest.mark.redis_basic_comm
    async def test_setnx(self, r: redis.Redis):
        """setx 即 set命令nx参数为 True 键值对不存在时才能设置成功"""
        assert await r.setnx("a", "1")
        assert await r.get("a") == b"1"
        assert not await r.setnx("a", "2")
        assert await r.get("a") == b"1"

    @pytest.mark.redis_basic_comm
    async def test_setrange(self, r: redis.Redis):
        assert await r.setrange("a", 5, "foo") == 8
        assert await r.get("a") == b"\0\0\0\0\0foo"
        await r.set("a", "abcdefghijh")
        assert await r.setrange("a", 6, "12345") == 11
        assert await r.get("a") == b"abcdef12345"

    @pytest.mark.redis_basic_comm
    async def test_ttl(self, r: redis.Redis):
        await r.set("a", "1")
        assert await r.expire("a", 10)
        assert 0 < await r.ttl("a") <= 10
        assert await r.persist("a")
        assert await r.ttl("a") == -1

    @skip_if_server_version_lt("2.8.0")
    @pytest.mark.redis_basic_comm
    async def test_ttl_nokey(self, r: redis.Redis):
        """TTL on servers 2.8 and after return -2 when the key doesn't exist"""
        assert await r.ttl("a") == -2

    @pytest.mark.redis_basic_comm
    async def test_type(self, r: redis.Redis):
        assert await r.type("a") == b"none"
        await r.set("a", "1")
        assert await r.type("a") == b"string"
        await r.delete("a")
        await r.lpush("a", "1")
        assert await r.type("a") == b"list"
        await r.delete("a")
        await r.sadd("a", "1")
        assert await r.type("a") == b"set"
        await r.delete("a")
        await r.zadd("a", {"1": 1})
        assert await r.type("a") == b"zset"

    @pytest.mark.redis_basic_comm
    async def test_linsert(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.linsert("a", "after", "2", "2.5") == 4
        assert await r.lrange("a", 0, -1) == [b"1", b"2", b"2.5", b"3"]
        assert await r.linsert("a", "before", "2", "1.5") == 5
        assert await r.lrange("a", 0, -1) == [b"1", b"1.5", b"2", b"2.5", b"3"]

    @pytest.mark.redis_basic_comm
    async def test_llen(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.llen("a") == 3

    @pytest.mark.redis_basic_comm
    async def test_lpop(self, r: CacheClient):
        await r.rpush("a", "1", "2", "3")
        assert await r.lpop("a") == b"1"
        assert await r.lpop("a") == b"2"
        assert await r.lpop("a") == b"3"
        assert await r.lpop("a") is None

    @pytest.mark.redis_basic_comm
    async def test_lpush(self, r: redis.Redis):
        assert await r.lpush("a", "1") == 1
        assert await r.lpush("a", "2") == 2
        assert await r.lpush("a", "3", "4") == 4
        assert await r.lrange("a", 0, -1) == [b"4", b"3", b"2", b"1"]

    @pytest.mark.redis_basic_comm
    async def test_lpushx(self, r: redis.Redis):
        assert await r.lpushx("a", "1") == 0
        assert await r.lrange("a", 0, -1) == []
        await r.rpush("a", "1", "2", "3")
        assert await r.lpushx("a", "4") == 4
        assert await r.lrange("a", 0, -1) == [b"4", b"1", b"2", b"3"]

    @pytest.mark.redis_basic_comm
    async def test_lrange(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3", "4", "5")
        assert await r.lrange("a", 0, 2) == [b"1", b"2", b"3"]
        assert await r.lrange("a", 2, 10) == [b"3", b"4", b"5"]
        assert await r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4", b"5"]

    @pytest.mark.redis_basic_comm
    async def test_rpop(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.rpop("a") == b"3"
        assert await r.rpop("a") == b"2"
        assert await r.rpop("a") == b"1"
        assert await r.rpop("a") is None

    @pytest.mark.redis_basic_comm
    async def test_rpoplpush(self, r: redis.Redis):
        await r.rpush("a", "a1", "a2", "a3")
        await r.rpush("b", "b1", "b2", "b3")
        assert await r.rpoplpush("a", "b") == b"a3"
        assert await r.lrange("a", 0, -1) == [b"a1", b"a2"]
        assert await r.lrange("b", 0, -1) == [b"a3", b"b1", b"b2", b"b3"]

    @pytest.mark.redis_basic_comm
    async def test_rpush(self, r: redis.Redis):
        assert await r.rpush("a", "1") == 1
        assert await r.rpush("a", "2") == 2
        assert await r.rpush("a", "3", "4") == 4
        assert await r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4"]

    @pytest.mark.redis_basic_comm
    async def test_hget_and_hset(self, r: redis.Redis):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hget("a", "1") == b"1"
        assert await r.hget("a", "2") == b"2"
        assert await r.hget("a", "3") == b"3"

        # field was updated, redis returns 0
        assert await r.hset("a", "2", 5) == 0
        assert await r.hget("a", "2") == b"5"

        # field is new, redis returns 1
        assert await r.hset("a", "4", 4) == 1
        assert await r.hget("a", "4") == b"4"

        # key inside of hash that doesn't exist returns null value
        assert await r.hget("a", "b") is None

        # keys with bool(key) == False
        assert await r.hset("a", 0, 10) == 1
        assert await r.hset("a", "", 10) == 1

    @pytest.mark.redis_basic_comm
    async def test_hset_with_multi_key_values(self, r: redis.Redis):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hget("a", "1") == b"1"
        assert await r.hget("a", "2") == b"2"
        assert await r.hget("a", "3") == b"3"

        await r.hset("b", "foo", "bar", mapping={"1": 1, "2": 2})
        assert await r.hget("b", "1") == b"1"
        assert await r.hget("b", "2") == b"2"
        assert await r.hget("b", "foo") == b"bar"

    @pytest.mark.redis_basic_comm
    async def test_hset_without_data(self, r: redis.Redis):
        with pytest.raises(redis.exceptions.DataError):
            await r.hset("x")

    @pytest.mark.redis_basic_comm
    async def test_hdel(self, r: redis.Redis):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hdel("a", "2") == 1
        assert await r.hget("a", "2") is None
        assert await r.hdel("a", "1", "3") == 2
        assert await r.hlen("a") == 0

    @pytest.mark.redis_basic_comm
    async def test_hexists(self, r: redis.Redis):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hexists("a", "1")
        assert not await r.hexists("a", "4")

    @pytest.mark.redis_basic_comm
    async def test_hgetall(self, r: redis.Redis):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        await r.hset("a", mapping=h)
        assert await r.hgetall("a") == h

    @pytest.mark.redis_basic_comm
    async def test_hincrby(self, r: redis.Redis):
        assert await r.hincrby("a", "1") == 1
        assert await r.hincrby("a", "1", amount=2) == 3
        assert await r.hincrby("a", "1", amount=-2) == 1

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.redis_basic_comm
    async def test_hincrbyfloat(self, r: redis.Redis):
        assert await r.hincrbyfloat("a", "1") == 1.0
        assert await r.hincrbyfloat("a", "1") == 2.0
        assert await r.hincrbyfloat("a", "1", 1.2) == 3.2

    @pytest.mark.redis_basic_comm
    async def test_hkeys(self, r: redis.Redis):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        await r.hset("a", mapping=h)
        local_keys = list(h.keys())
        remote_keys = await r.hkeys("a")
        assert sorted(local_keys) == sorted(remote_keys)

    @pytest.mark.redis_basic_comm
    async def test_hlen(self, r: CacheClient):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hlen("a") == 3
