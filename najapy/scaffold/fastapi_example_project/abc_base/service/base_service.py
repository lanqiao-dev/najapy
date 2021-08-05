from najapy.cache.base import ShareFuture, FuncCache
from najapy.cache.redis import RedisDelegate
from najapy.common.async_base import MultiTasks, Utils
from najapy.common.base import catch_error, Ignore
from najapy.common.metaclass import Singleton
from najapy.common.process import HeartbeatChecker
from najapy.database.mysql import MySQLDelegate

from najapy.scaffold.fastapi_example_project.conf import ConfigStatic, ConfigDynamic


class DataSource(Singleton, RedisDelegate, MySQLDelegate):

    def __init__(self):
        MySQLDelegate.__init__(self)
        RedisDelegate.__init__(self)

        self._heartbeat = HeartbeatChecker()

        self._event_bus = None

    @classmethod
    async def initialize(cls):

        inst = cls()

        await inst.async_init_redis(
            ConfigStatic.RedisHost, ConfigStatic.RedisPasswd,
            minsize=ConfigStatic.RedisMinConn, maxsize=ConfigStatic.RedisMaxConn,
            db=ConfigStatic.RedisBase, expire=ConfigStatic.RedisExpire,
            key_prefix=ConfigStatic.RedisKeyPrefix
        )

        # 初始化mysql连接
        await inst.async_init_mysql_rw(
            ConfigStatic.MySqlMasterServer[0], ConfigStatic.MySqlMasterServer[1], ConfigStatic.MySqlName,
            ConfigStatic.MySqlUser, ConfigStatic.MySqlPasswd,
            minsize=ConfigStatic.MySqlMasterMinConn, maxsize=ConfigStatic.MySqlMasterMaxConn,
            echo=ConfigDynamic.Debug, pool_recycle=21600, conn_life=43200
        )

        if ConfigStatic.MySqlSlaveServer:
            await inst.async_init_mysql_ro(
                ConfigStatic.MySqlSlaveServer[0], ConfigStatic.MySqlSlaveServer[1], ConfigStatic.MySqlName,
                ConfigStatic.MySqlUser, ConfigStatic.MySqlPasswd,
                minsize=ConfigStatic.MySqlSlaveMinConn, maxsize=ConfigStatic.MySqlSlaveMaxConn,
                echo=ConfigDynamic.Debug, readonly=True, pool_recycle=21600, conn_life=43200
            )

        inst._event_bus = inst.event_dispatcher(
            f'{ConfigStatic.RedisKeyPrefix}_client_event',
            ConfigDynamic.ClentEventBusChannels
        )

    async def release(self):

        self._heartbeat.release()

        await self.async_close_redis()
        await self.async_close_mysql()

    @property
    def online(self):
        return self._heartbeat.check()

    @ShareFuture()
    @FuncCache(ttl=30)
    async def health(self):
        result = False

        with catch_error():
            tasks = MultiTasks()

            tasks.append(self.cache_health())
            tasks.append(self.mysql_health())

            await tasks

            result = all(tasks)

            if result:
                self._heartbeat.refresh()

            Utils.log.info(f'Check health {result}')

        return result

    @property
    def event_bus(self):

        return self._event_bus


class ServiceBase(Utils, Singleton):
    """service基类"""

    def __init__(self):
        self._data_source = DataSource()

    @property
    def data_source(self):

        return self._data_source

    def Break(self, data=None, layers=1):

        raise Ignore(data, layers)
