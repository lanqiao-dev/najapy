# -*- coding: utf-8 -*-

import os

from pynaja.common.struct import Configure

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


class _Static(Configure):

    def _init_options(self):
        ##################################################
        # MySql数据库

        self.MySqlMasterServer = self._parser.get_split_host(r'MySql', r'MySqlMasterServer')

        self.MySqlSlaveServer = self._parser.get_split_host(r'MySql', r'MySqlSlaveServer')

        self.MySqlName = self._parser.get(r'MySql', r'MySqlName')

        self.MySqlUser = self._parser.get(r'MySql', r'MySqlUser')

        self.MySqlPasswd = self._parser.get(r'MySql', r'MySqlPasswd')

        self.MySqlMasterMinConn = self._parser.getint(r'MySql', r'MySqlMasterMinConn')

        self.MySqlMasterMaxConn = self._parser.getint(r'MySql', r'MySqlMasterMaxConn')

        self.MySqlSlaveMinConn = self._parser.getint(r'MySql', r'MySqlSlaveMinConn')

        self.MySqlSlaveMaxConn = self._parser.getint(r'MySql', r'MySqlSlaveMaxConn')

        ##################################################
        # 缓存

        self.RedisHost = self._parser.get_split_host(r'Redis', r'RedisHost')

        self.RedisBase = self._parser.getint(r'Redis', r'RedisBase')

        self.RedisPasswd = self._parser.get(r'Redis', r'RedisPasswd')

        self.RedisMinConn = self._parser.getint(r'Redis', r'RedisMinConn')

        self.RedisMaxConn = self._parser.getint(r'Redis', r'RedisMaxConn')

        self.RedisExpire = self._parser.getint(r'Redis', r'RedisExpire')

        self.RedisKeyPrefix = self._parser.get(r'Redis', r'RedisKeyPrefix')


class _Dynamic(Configure):

    def _init_options(self):
        ##################################################
        # 基本

        self.Port = self._parser.getint(r'Base', r'Port')
        self.Debug = self._parser.getboolean(r'Base', r'Debug')
        allowed_hosts = self._parser.get_split_str(r'Base', r'AllowedHosts')
        self.AllowedHosts = list(allowed_hosts) or []
        abnormal_status = self._parser.get_split_int(r'Base', r'AbnormalStatus')
        self.AbnormalStatus = list(abnormal_status) or []
        self.JWTSecret = self._parser.get(r"Base", r"JWTSecret")

        ##################################################
        # 日志

        self.LogLevel = self._parser.get(r'Log', r'LogLevel')
        self.LogFilePath = self._parser.get(r'Log', r'LogFilePath')

        ##################################################
        # 事件总线

        self.ClentEventBusChannels = self._parser.getint(r'Event', r'ClentEventBusChannels')


ConfigStatic = _Static()
ConfigDynamic = _Dynamic()

cluster = os.getenv(r'CLUSTER', None)

if cluster is None:
    ConfigStatic.read(f'{BASE_PATH}/static.conf')
    ConfigDynamic.read(f'{BASE_PATH}/dynamic.conf')
else:
    ConfigStatic.read(f'{BASE_PATH}/static.{cluster.lower()}.conf')
    ConfigDynamic.read(f'{BASE_PATH}/dynamic.{cluster.lower()}.conf')
