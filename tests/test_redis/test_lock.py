import asyncio

from najapy.common.async_base import Utils


class TestLock:

    async def test_lock_client(self, r):
        key = "foo1"
        lock = r.allocate_lock(key)
        lock_key = r.get_safe_key(key)

        assert await lock.acquire()
        assert await r.get(lock_key) == lock.local.token
        assert await r.ttl(lock_key) == 60
        await lock.release()
        assert await r.get(lock_key) is None

    async def test_timeout_client(self, r):
        key = "foo1"
        lock = r.allocate_lock(key)
        lock_key = r.get_safe_key(key)

        assert await lock.acquire()
        assert 8 < (await r.ttl(lock_key)) <= 60
        await lock.release()

    async def test_blocking(self, r):
        lock = r.allocate_lock("foo")
        assert not lock.blocking

        lock_2 = r.allocate_lock("foo", blocking=True)
        assert lock_2.blocking

    async def test_blocking_timeout(self, r, event_loop):
        lock1 = r.allocate_lock("foo")
        assert await lock1.acquire()
        bt = 0.2
        sleep = 0.05
        lock2 = r.allocate_lock("foo", sleep=sleep, blocking=True, blocking_timeout=bt)
        start = Utils.loop_time()
        assert not await lock2.acquire()
        # The elapsed duration should be less than the total blocking_timeout
        assert bt >= (Utils.loop_time() - start) > bt - sleep
        await lock1.release()

    async def test_lock_with_multi_tasks(self, r):
        lock1 = r.allocate_lock("foo", blocking=True)

        async def target_method():
            assert await lock1.acquire()
            await Utils.sleep(0.5)
            assert 1 == 1
            await lock1.release()

        start = Utils.loop_time()
        await asyncio.gather(target_method(), target_method())
        assert Utils.loop_time() - start >= 1

    async def test_blocking_false(self, r):
        lock1 = r.allocate_lock("foo")

        assert await lock1.acquire()

        lock2 = r.allocate_lock("foo")
        assert not await lock2.acquire()

        await lock1.release()

        assert await lock2.acquire()
