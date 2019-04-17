# -*- coding: utf-8 -*-

from hagworm.extend.base import Ignore
from hagworm.extend.metaclass import Singleton
from hagworm.extend.asyncio.base import Utils
from hagworm.extend.asyncio.cache import RedisDelegate
from hagworm.extend.asyncio.database import MongoDelegate, MySQLDelegate

from setting import config


class DataSource(Singleton, RedisDelegate, MongoDelegate, MySQLDelegate):

    def __init__(self):

        RedisDelegate.__init__(self)

        MongoDelegate.__init__(
            self,
            config.MongoHost, config.MongoUser, config.MongoPasswd,
            minPoolSize=config.MongoMinConn, maxPoolSize=config.MongoMaxConn
        )

        MySQLDelegate.__init__(self)

    async def initialize(self):

        await self.async_init_redis(
            config.RedisHost, config.RedisPasswd,
            minsize=config.RedisMinConn, maxsize=config.RedisMaxConn,
            db=config.RedisBase, expire=config.RedisExpire,
            key_prefix=config.RedisKeyPrefix
        )

        await self.async_init_mysql_rw(
            config.MySqlMasterServer[0], config.MySqlMasterServer[1], config.MySqlName,
            config.MySqlUser, config.MySqlPasswd,
            minsize=config.MySqlMasterMinConn, maxsize=config.MySqlMasterMaxConn
        )

        if(config.MySqlSlaveServer):

            await self.async_init_mysql_ro(
                config.MySqlSlaveServer[0], config.MySqlSlaveServer[1], config.MySqlName,
                config.MySqlUser, config.MySqlPasswd,
                minsize=config.MySqlSlaveMinConn, maxsize=config.MySqlSlaveMaxConn,
                readonly=True
            )


class _ModelBase(Singleton, Utils):

    def __init__(self):

        self._data_source = DataSource()

    def Break(self, msg=None):

        raise Ignore(msg)
