# -*- coding: utf-8 -*-

import aiomysql

from aiomysql.sa import SAConnection, Engine
from aiomysql.sa.engine import _dialect as dialect

import aiotask_context as context

from sqlalchemy.sql.selectable import Select
from sqlalchemy.sql.dml import Insert, Update, Delete

from motor.motor_asyncio import AsyncIOMotorClient

from hagworm.extend.base import Utils

from .base import AsyncContextManager


class MongoPool():

    def __init__(self, host, username=None, password=None, *, minPoolSize=8, maxPoolSize=32, **settings):

        settings[r'host'] = host
        settings[r'minPoolSize'] = minPoolSize
        settings[r'maxPoolSize'] = maxPoolSize

        if(username and password):
            settings[r'username'] = username
            settings[r'password'] = password

        self._pool = AsyncIOMotorClient(**settings)

        Utils.log.info(
            r'Mongo {0} initialized'.format(host)
        )

    def get_database(self, db_name):

        result = None

        try:
            result = self._pool[db_name]
        except Exception as err:
            Utils.log.exception(err)

        return result


class MongoDelegate():

    def __init__(self, *args, **kwargs):

        self._mongo_pool = MongoPool(*args, **kwargs)

    async def mongo_health(self):

        result = False

        try:

            result = bool(await self._mongo_pool._pool.server_info())

        except Exception as err:

            Utils.log.error(err)

        return result

    def get_mongo_database(self, db_name):

        return self._mongo_pool.get_database(db_name)

    def get_mongo_collection(self, db_name, collection):

        return self.get_mongo_database(db_name)[collection]


class MySQLPool():

    class _Connection(SAConnection):

        async def destroy(self):

            if(self._connection):
                self._connection.close()

            await self.close()

    def __init__(self, host, port, db, user, password, *, minsize=8, maxsize=32, charset=r'utf8', autocommit=True, cursorclass=aiomysql.DictCursor, readonly=False, **settings):

        self._pool = None
        self._engine = None
        self._readonly = readonly

        self._settings = settings

        self._settings[r'host'] = host
        self._settings[r'port'] = port
        self._settings[r'db'] = db

        self._settings[r'user'] = user
        self._settings[r'password'] = password

        self._settings[r'minsize'] = minsize
        self._settings[r'maxsize'] = maxsize

        self._settings[r'charset'] = charset
        self._settings[r'autocommit'] = autocommit
        self._settings[r'cursorclass'] = cursorclass

    async def initialize(self):

        self._pool = await aiomysql.create_pool(**self._settings)
        self._engine = Engine(dialect, self._pool)

        Utils.log.info(
            r'MySQL {0}:{1} {2}({3}) initialized'.format(
                self._settings[r'host'], self._settings[r'port'], self._settings[r'db'],
                r'ro' if self._readonly else r'rw'
            )
        )

    async def get_sa_conn(self):

        conn = await self._pool.acquire()

        return self._Connection(conn, self._engine)

    def get_client(self):

        result = None

        try:
            result = DBClient(self, self._readonly)
        except Exception as err:
            Utils.log.exception(err)

        return result

    def get_transaction(self):

        result = None

        try:
            result = DBTransaction(self)
        except Exception as err:
            Utils.log.exception(err)

        return result


class MySQLDelegate():

    def __init__(self):

        self._mysql_rw_pool = None
        self._mysql_ro_pool = None

    async def async_init_mysql_rw(self, *args, **kwargs):

        self._mysql_rw_pool = MySQLPool(*args, **kwargs)

        await self._mysql_rw_pool.initialize()

    async def async_init_mysql_ro(self, *args, **kwargs):

        self._mysql_ro_pool = MySQLPool(*args, **kwargs)

        await self._mysql_ro_pool.initialize()

    async def mysql_health(self):

        result = False

        try:

            res_rw_pool = res_ro_pool = False

            if(self._mysql_rw_pool):
                db_client = self._mysql_rw_pool.get_client()
                records = await db_client.execute(r'select version();')
                res_rw_pool = bool(records)

            if(self._mysql_ro_pool):
                db_client = self._mysql_rw_pool.get_client()
                records = await db_client.execute(r'select version();')
                res_ro_pool = bool(records)
            else:
                res_ro_pool = True

            result = res_rw_pool and res_ro_pool

        except Exception as err:

            Utils.log.error(err)

        return result

    def get_db_client(self, readonly=False, *, alone=False):

        if(alone):

            if(readonly):
                if(self._mysql_ro_pool is not None):
                    client = self._mysql_ro_pool.get_client()
                else:
                    client = self._mysql_rw_pool.get_client()
                    client._readonly = True
            else:
                client = self._mysql_rw_pool.get_client()

        else:

            if(readonly):

                _client = context.get(r'mysql_rw_client', None)

                if(_client is not None):
                    _client.release()
                    context.set(r'mysql_rw_client', None)

                client = context.get(r'mysql_ro_client', None)

                if(client is None):

                    if(self._mysql_ro_pool is not None):
                        client = self._mysql_ro_pool.get_client()
                    else:
                        client = self._mysql_rw_pool.get_client()
                        client._readonly = True

                    context.set(r'mysql_ro_client', client)

            else:

                _client = context.get(r'mysql_ro_client', None)

                if(_client is not None):
                    _client.release()
                    context.set(r'mysql_ro_client', None)

                client = context.get(r'mysql_rw_client', None)

                if(client is None):
                    client = self._mysql_rw_pool.get_client()
                    context.set(r'mysql_rw_client', client)

        return client

    def get_db_transaction(self):

        context.set(r'mysql_ro_client', None)
        context.set(r'mysql_rw_client', None)

        return self._mysql_rw_pool.get_transaction()


class _ClientBase():

    class ReadOnly(Exception):
        pass

    def __init__(self, readonly=False):

        self._readonly = readonly

    @staticmethod
    def safestr(val):

        cls = type(val)

        if(cls is str):
            val = aiomysql.escape_string(val)
        elif(cls is dict):
            val = aiomysql.escape_dict(val)
        else:
            val = str(val)

        return val

    @property
    def readonly(self):

        return self._readonly

    @property
    def insert_id(self):

        raise NotImplementedError()

    def _get_conn(self):

        raise NotImplementedError()

    async def execute(self, clause):

        raise NotImplementedError()

    async def select(self, clause):

        result = None

        if(not isinstance(clause, Select)):
            raise TypeError(r'Not sqlalchemy.sql.selectable.Select object')

        proxy = await self.execute(clause)
        result = await proxy.cursor.fetchall()

        if(not proxy.closed):
            await proxy.close()

        return result

    async def find(self, clause):

        result = None

        if(not isinstance(clause, Select)):
            raise TypeError(r'Not sqlalchemy.sql.selectable.Select object')

        proxy = await self.execute(clause)
        result = await proxy.cursor.fetchone()

        if(not proxy.closed):
            await proxy.close()

        return result

    async def insert(self, clause):

        if(self._readonly):
            raise self.ReadOnly()

        if(not isinstance(clause, Insert)):
            raise TypeError(r'Not sqlalchemy.sql.dml.Insert object')

        proxy = await self.execute(clause)

        if(not proxy.closed):
            await proxy.close()

        return self.insert_id

    async def update(self, clause):

        if(self._readonly):
            raise self.ReadOnly()

        if(not isinstance(clause, Update)):
            raise TypeError(r'Not sqlalchemy.sql.dml.Update object')

        proxy = await self.execute(clause)

        if(not proxy.closed):
            await proxy.close()

        return proxy.rowcount

    async def delete(self, clause):

        if(self._readonly):
            raise self.ReadOnly()

        if(not isinstance(clause, Delete)):
            raise TypeError(r'Not sqlalchemy.sql.dml.Delete object')

        proxy = await self.execute(clause)

        if(not proxy.closed):
            await proxy.close()

        return proxy.rowcount


class DBClient(_ClientBase, AsyncContextManager):

    def __init__(self, pool, readonly):

        super().__init__(readonly)

        self._running = False
        self._is_active = True

        self._pool = pool
        self._conn = None

    async def _context_release(self):

        await self._close()

    async def _close(self):

        if(self._is_active):

            self._is_active = False

            if(self._conn):
                await self._conn.close()

            self._pool = self._conn = None

    @property
    def insert_id(self):

        return self._conn.connection.insert_id() if self._conn else 0

    async def _get_conn(self):

        if(not self._conn and self._is_active and self._pool):
            self._conn = await self._pool.get_sa_conn()

        return self._conn

    async def release(self):

        if(self._conn):
            await self._conn.close()
            self._conn = None

    async def execute(self, clause):

        if(self._running):
            raise RuntimeError(r'Request blocked')

        result = None

        self._running = True

        if(self._is_active):

            for _ in range(0xff):

                try:

                    conn = await self._get_conn()

                    result = await conn.execute(clause)

                    break

                except ConnectionResetError as err:

                    await self.destroy()

                    self.log.exception(err)

                except Exception as err:

                    await self.destroy()

                    self.log.exception(err)

                    raise err

        self._running = False

        return result


class DBTransaction(DBClient):

    def __init__(self, pool):

        super().__init__(pool, False)

        self._trx = None

    async def _context_release(self):

        await self.rollback()

    async def _get_conn(self):

        if(not self._conn and self._is_active and self._pool):
            self._conn = await self._pool.get_sa_conn()
            self._trx = await self._conn.begin()

        return self._conn

    async def execute(self, clause):

        if(self._running):
            raise RuntimeError(r'Request blocked')

        result = None

        self._running = True

        if(self._is_active):

            try:

                conn = await self._get_conn()

                result = await conn.execute(clause)

            except Exception as err:

                await self.destroy()

                self.log.exception(err)

                raise err

        self._running = False

        return result

    async def commit(self):

        if(self._is_active):

            self._is_active = False

            if(self._trx):

                await self._trx.commit()

                await self._trx.close()

            self._pool = self._conn = None

    async def rollback(self):

        if(self._is_active):

            self._is_active = False

            if(self._trx):

                await self._trx.commit()

                await self._trx.close()

            self._pool = self._conn = None
