from najapy.cache.redis import CacheClient, ShareCache, PeriodCounter


async def test_period_counter(r: CacheClient):
    c = PeriodCounter(r, "counter", 6)
    for i in range(6):
        val = await c.incr()
        assert i+1 == int(val)

    assert await c.release() is None


async def test_share_cache(r: CacheClient):
    async def ex(c1: PeriodCounter, c2: PeriodCounter):
        await c1.incr()
        value = "xiami"

        share_cache = ShareCache(r, "foo2", lock_blocking_timeout=1)
        v = await share_cache.get()
        if v:
            print("Hit Cache")
            assert v == value
            return v

        await c2.incr()
        await share_cache.set(value, 2)
        await share_cache.release()

    c1 = PeriodCounter(r, "counter1", 6)
    c2 = PeriodCounter(r, "counter2", 6)

    await ex(c1, c2)
    await ex(c1, c2)

    assert await c1.incr() == 3
    assert await c2.incr() == 2
