# -*- coding: utf-8 -*-

import functools
import asyncio
import aioredis

from aioredis.commands.generic import GenericCommandsMixin
from aioredis.commands.string import StringCommandsMixin
from aioredis.commands.hash import HashCommandsMixin
from aioredis.commands.hyperloglog import HyperLogLogCommandsMixin
from aioredis.commands.set import SetCommandsMixin
from aioredis.commands.sorted_set import SortedSetCommandsMixin
from aioredis.commands.transaction import TransactionsCommandsMixin
from aioredis.commands.list import ListCommandsMixin
from aioredis.commands.scripting import ScriptingCommandsMixin
from aioredis.commands.server import ServerCommandsMixin
from aioredis.commands.pubsub import PubSubCommandsMixin
from aioredis.commands.cluster import ClusterCommandsMixin
from aioredis.commands.geo import GeoCommandsMixin

import aiotask_context as context

from hagworm.extend.base import TimedCache

from .base import Utils, AsyncContextManager


_NOTSET = aioredis.util._NOTSET

_SET_IF_NOT_EXIST = aioredis.Redis.SET_IF_NOT_EXIST
_SET_IF_EXIST = aioredis.Redis.SET_IF_EXIST


class RedisPool():

    class _Connection(
        aioredis.RedisConnection,
        GenericCommandsMixin, StringCommandsMixin,
        HyperLogLogCommandsMixin, SetCommandsMixin,
        HashCommandsMixin, TransactionsCommandsMixin,
        SortedSetCommandsMixin, ListCommandsMixin,
        ScriptingCommandsMixin, ServerCommandsMixin,
        PubSubCommandsMixin, ClusterCommandsMixin,
        GeoCommandsMixin
    ):
        pass

    def __init__(self, address, password=None, *, minsize=8, maxsize=32, db=0, expire=3600, key_prefix=r'', **settings):

        self._pool = None
        self._expire = expire
        self._key_prefix = key_prefix

        self._settings = settings

        self._settings[r'address'] = address
        self._settings[r'password'] = password

        self._settings[r'minsize'] = minsize
        self._settings[r'maxsize'] = maxsize

        self._settings[r'db'] = db
        self._settings[r'connection_cls'] = self._Connection

    async def initialize(self):

        self._pool = await aioredis.create_pool(**self._settings)

        Utils.log.info(
            r'Redis {0} initialized'.format(self._settings[r'address'])
        )

    def get_client(self):

        if(self._pool is not None):
            return MCache(self._pool, self._expire, self._key_prefix)


class RedisDelegate():

    def __init__(self):

        self._redis_pool = None

    async def async_init_redis(self, *args, **kwargs):

        self._redis_pool = RedisPool(*args, **kwargs)

        await self._redis_pool.initialize()

    async def cache_health(self):

        result = False

        try:

            cache = self._redis_pool.get_client()

            result = bool(await cache.time())

        except Exception as err:

            Utils.log.error(err)

        return result

    def get_cache_client(self, *, alone=False):

        if(alone):
            return self._redis_pool.get_client()

        client = context.get(r'cache_client', None)

        if(client is None):

            client = self._redis_pool.get_client()

            if(client):
                context.get(r'cache_client', client)

        return client

    def share_cache(self, cache, ckey):

        return ShareCache(cache, ckey)


class MCache(AsyncContextManager):

    def __init__(self, pool, expire, key_prefix):

        self._pool = pool

        self._expire = expire

        self._key_prefix = key_prefix

        self._client = None

    def __del__(self):

        self.close()

    async def _context_release(self):

        if(self._client and self._pool):
            self._pool.release(self._client)

        self._pool = self._client = None

    async def _get_client(self):

        if(self._client is None and self._pool):
            self._client = await self._pool.acquire()

        return self._client

    def close(self):

        if(self._client and self._pool):
            self._pool.release(self._client)

        self._client = None

    def key(self, key, *args, **kwargs):

        if(self._key_prefix):
            key = r'{0}_{1}'.format(self._key_prefix, key)

        if(not args and not kwargs):
            return key

        sign = Utils.params_sign(*args, **kwargs)

        return r'{0}_{1}'.format(key, sign)

    def allocate_lock(self, key, expire=60):

        return MLock(self, key, expire)

    def mutex_lock(self, key, expire=60):

        return MutexLock(self, key, expire)

    def _val_encode(self, val):

        result = Utils.pickle_dumps(val)

        return result

    def _val_decode(self, val):

        result = Utils.pickle_loads(val)

        return result

    # SIMPLE COMMANDS

    async def get(self, key):

        result = await self._get(key)

        if(result is not None):
            result = self._val_decode(result)

        return result

    async def set(self, key, value, expire=0):

        if(expire <= 0):
            expire = self._expire

        value = self._val_encode(value)

        result = await self._set(key, value, expire=expire)

        return result

    async def delete(self, *keys):

        _keys = []

        for key in keys:

            if(key.find(r'*') < 0):
                _keys.append(key)
            else:
                _keys.extend((await self.keys(key)))

        result = (await self._delete(*_keys)) if(len(_keys) > 0) else 0

        return result

    # BASE COMMANDS

    async def echo(self, message, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.echo(message, encoding=encoding)

        return result

    async def ping(self, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.ping(encoding=encoding)

        return result

    # GENERIC COMMANDS

    async def _delete(self, key, *keys):

        client = await self._get_client()

        result = await client.delete(key, *keys)

        return result

    async def dump(self, key):

        client = await self._get_client()

        result = await client.dump(key)

        return result

    async def exists(self, key, *keys):

        client = await self._get_client()

        result = await client.exists(key, *keys)

        return result

    async def expire(self, key, timeout):

        client = await self._get_client()

        result = await client.expire(key, timeout)

        return result

    async def expireat(self, key, timestamp):

        client = await self._get_client()

        result = await client.expireat(key, timestamp)

        return result

    async def keys(self, pattern, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.keys(pattern, encoding=encoding)

        return result

    async def migrate(self, host, port, key, dest_db, timeout, copy=False, replace=False):

        client = await self._get_client()

        result = await client.migrate(host, port, key, dest_db, timeout, copy, replace)

        return result

    async def move(self, key, db):

        client = await self._get_client()

        result = await client.move(key, db)

        return result

    async def object_encoding(self, key):

        client = await self._get_client()

        result = await client.object_encoding(key)

        return result

    async def object_idletime(self, key):

        client = await self._get_client()

        result = await client.object_idletime(key)

        return result

    async def object_refcount(self, key):

        client = await self._get_client()

        result = await client.object_refcount(key)

        return result

    async def persist(self, key):

        client = await self._get_client()

        result = await client.persist(key)

        return result

    async def pexpire(self, key, timeout):

        client = await self._get_client()

        result = await client.pexpire(key, timeout)

        return result

    async def pexpireat(self, key, timestamp):

        client = await self._get_client()

        result = await client.pexpireat(key, timestamp)

        return result

    async def pttl(self, key):

        client = await self._get_client()

        result = await client.pttl(key)

        return result

    async def randomkey(self, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.randomkey(encoding=encoding)

        return result

    async def rename(self, key, newkey):

        client = await self._get_client()

        result = await client.rename(key, newkey)

        return result

    async def renamenx(self, key, newkey):

        client = await self._get_client()

        result = await client.renamenx(key, newkey)

        return result

    async def restore(self, key, ttl, value):

        client = await self._get_client()

        result = await client.restore(key, ttl, value)

        return result

    async def scan(self, cursor=0, match=None, count=None):

        client = await self._get_client()

        result = await client.scan(cursor, match, count)

        return result

    async def sort(self, key, *get_patterns, by=None, offset=None, count=None, asc=None, alpha=False, store=None):

        client = await self._get_client()

        result = await client.sort(key, *get_patterns, by, offset, count, asc, alpha, store)

        return result

    async def ttl(self, key):

        client = await self._get_client()

        result = await client.ttl(key)

        return result

    async def type(self, key):

        client = await self._get_client()

        result = await client.type(key)

        return result

    # GEO COMMANDS

    GeoPoint = aioredis.GeoPoint
    GeoMember = aioredis.GeoMember

    async def geoadd(self, key, longitude, latitude, member, *args, **kwargs):

        client = await self._get_client()

        result = await client.geoadd(key, longitude, latitude, member, *args, **kwargs)

        return result

    async def geodist(self, key, member1, member2, unit=r'm'):

        client = await self._get_client()

        result = await client.geodist(key, member1, member2, unit)

        return result

    async def geohash(self, key, member, *args, **kwargs):

        client = await self._get_client()

        result = await client.geohash(key, member, *args, **kwargs)

        return result

    async def geopos(self, key, member, *args, **kwargs):

        client = await self._get_client()

        result = await client.geopos(key, member, *args, **kwargs)

        return result

    async def georadius(self, key, longitude, latitude, radius, unit=r'm', *, with_dist=False, with_hash=False, with_coord=False, count=None, sort=None, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.georadius(key, longitude, latitude, radius, unit, with_dist=with_dist, with_hash=with_hash, with_coord=with_coord, count=count, sort=sort, encoding=encoding)

        return result

    async def georadiusbymember(self, key, member, radius, unit=r'm', *, with_dist=False, with_hash=False, with_coord=False, count=None, sort=None, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.georadiusbymember(key, member, radius, unit, with_dist=with_dist, with_hash=with_hash, with_coord=with_coord, count=count, sort=sort, encoding=encoding)

        return result

    # STRINGS COMMANDS

    async def append(self, key, value):

        client = await self._get_client()

        result = await client.append(key, value)

        return result

    async def bitcount(self, key, start=None, end=None):

        client = await self._get_client()

        result = await client.bitcount(key, start=None, end=None)

        return result

    async def bitop_and(self, dest, key, *keys):

        client = await self._get_client()

        result = await client.bitop_and(dest, key, *keys)

        return result

    async def bitop_not(self, dest, key):

        client = await self._get_client()

        result = await client.bitop_not(dest, key)

        return result

    async def bitop_or(self, dest, key, *keys):

        client = await self._get_client()

        result = await client.bitop_or(dest, key, *keys)

        return result

    async def bitop_xor(self, dest, key, *keys):

        client = await self._get_client()

        result = await client.bitop_xor(dest, key, *keys)

        return result

    async def bitpos(self, key, bit, start=None, end=None):

        client = await self._get_client()

        result = await client.bitpos(key, bit, start, end)

        return result

    async def decr(self, key):

        client = await self._get_client()

        result = await client.decr(key)

        return result

    async def decrby(self, key, decrement):

        client = await self._get_client()

        result = await client.decrby(key, decrement)

        return result

    async def _get(self, key, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.get(key, encoding=encoding)

        return result

    async def getbit(self, key, offset):

        client = await self._get_client()

        result = await client.getbit(key, offset)

        return result

    async def getrange(self, key, start, end, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.getrange(key, start, end, encoding=encoding)

        return result

    async def getset(self, key, value, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.getset(key, value, encoding=encoding)

        return result

    async def incr(self, key):

        client = await self._get_client()

        result = await client.incr(key)

        return result

    async def incrby(self, key, increment):

        client = await self._get_client()

        result = await client.incrby(key, increment)

        return result

    async def incrbyfloat(self, key, increment):

        client = await self._get_client()

        result = await client.incrbyfloat(key, increment)

        return result

    async def mget(self, key, *keys, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.mget(key, *keys, encoding)

        return result

    async def mset(self, key, value, *pairs):

        client = await self._get_client()

        result = await client.mset(key, value, *pairs)

        return result

    async def msetnx(self, key, value, *pairs):

        client = await self._get_client()

        result = await client.msetnx(key, value, *pairs)

        return result

    async def psetex(self, key, milliseconds, value):

        client = await self._get_client()

        result = await client.psetex(key, milliseconds, value)

        return result

    async def _set(self, key, value, *, expire=0, pexpire=0, exist=None):

        client = await self._get_client()

        result = await client.set(key, value, expire=expire, pexpire=pexpire, exist=exist)

        return result

    async def setbit(self, key, offset, value):

        client = await self._get_client()

        result = await client.setbit(key, offset, value)

        return result

    async def setex(self, key, seconds, value):

        client = await self._get_client()

        result = await client.setex(key, seconds, value)

        return result

    async def setnx(self, key, value):

        client = await self._get_client()

        result = await client.setnx(key, value)

        return result

    async def setrange(self, key, offset, value):

        client = await self._get_client()

        result = await client.setrange(key, offset, value)

        return result

    async def strlen(self, key):

        client = await self._get_client()

        result = await client.strlen(key)

        return result

    # HASH COMMANDS

    async def hdel(self, key, field, *fields):

        client = await self._get_client()

        result = await client.hdel(key, field, *fields)

        return result

    async def hexists(self, key, field):

        client = await self._get_client()

        result = await client.hexists(key, field)

        return result

    async def hget(self, key, field, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.hget(key, field, encoding=encoding)

        return result

    async def hgetall(self, key, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.hgetall(key, encoding=encoding)

        return result

    async def hincrby(self, key, field, increment=1):

        client = await self._get_client()

        result = await client.hincrby(key, field, increment)

        return result

    async def hincrbyfloat(self, key, field, increment=1.0):

        client = await self._get_client()

        result = await client.hincrbyfloat(key, field, increment)

        return result

    async def hkeys(self, key, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.hkeys(key, encoding=encoding)

        return result

    async def hlen(self, key):

        client = await self._get_client()

        result = await client.hlen(key)

        return result

    async def hmget(self, key, field, *fields, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.hmget(key, field, *fields, encoding)

        return result

    async def hmset(self, key, field, value, *pairs):

        client = await self._get_client()

        result = await client.hmset(key, field, value, *pairs)

        return result

    async def hmset_dict(self, key, *args, **kwargs):

        client = await self._get_client()

        result = await client.hmset_dict(key, *args, **kwargs)

        return result

    async def hscan(self, key, cursor=0, match=None, count=None):

        client = await self._get_client()

        result = await client.hscan(key, cursor, match, count)

        return result

    async def hset(self, key, field, value):

        client = await self._get_client()

        result = await client.hset(key, field, value)

        return result

    async def hsetnx(self, key, field, value):

        client = await self._get_client()

        result = await client.hsetnx(key, field, value)

        return result

    async def hstrlen(self, key, field):

        client = await self._get_client()

        result = await client.hstrlen(key, field)

        return result

    async def hvals(self, key, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.hvals(key, encoding=encoding)

        return result

    # LIST COMMANDS

    async def blpop(self, key, *keys, timeout=0, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.blpop(key, *keys, timeout=timeout, encoding=encoding)

        return result

    async def brpop(self, key, *keys, timeout=0, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.brpop(key, *keys, timeout=timeout, encoding=encoding)

        return result

    async def brpoplpush(self, sourcekey, destkey, timeout=0, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.brpoplpush(sourcekey, destkey, timeout, encoding)

        return result

    async def lindex(self, key, index, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.lindex(key, index, encoding=encoding)

        return result

    async def linsert(self, key, pivot, value, before=False):

        client = await self._get_client()

        result = await client.linsert(key, pivot, value, before)

        return result

    async def llen(self, key):

        client = await self._get_client()

        result = await client.llen(key)

        return result

    async def lpop(self, key, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.lpop(key, encoding=encoding)

        return result

    async def lpush(self, key, value, *values):

        client = await self._get_client()

        result = await client.lpush(key, value, *values)

        return result

    async def lpushx(self, key, value):

        client = await self._get_client()

        result = await client.lpushx(key, value)

        return result

    async def lrange(self, key, start, stop, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.lrange(key, start, stop, encoding=encoding)

        return result

    async def lrem(self, key, count, value):

        client = await self._get_client()

        result = await client.lrem(key, count, value)

        return result

    async def lset(self, key, index, value):

        client = await self._get_client()

        result = await client.lset(key, index, value)

        return result

    async def ltrim(self, key, start, stop):

        client = await self._get_client()

        result = await client.ltrim(key, start, stop)

        return result

    async def rpop(self, key, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.rpop(key, encoding=encoding)

        return result

    async def rpoplpush(self, sourcekey, destkey, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.rpoplpush(self, sourcekey, destkey, encoding=encoding)

        return result

    async def rpush(self, key, value, *values):

        client = await self._get_client()

        result = await client.rpush(key, value, *values)

        return result

    async def rpushx(self, key, value):

        client = await self._get_client()

        result = await client.rpushx(key, value)

        return result

    # SET COMMANDS

    async def sadd(self, key, member, *members):

        client = await self._get_client()

        result = await client.sadd(key, member, *members)

        return result

    async def scard(self, key):

        client = await self._get_client()

        result = await client.scard(key)

        return result

    async def sdiff(self, key, *keys):

        client = await self._get_client()

        result = await client.sdiff(key, *keys)

        return result

    async def sdiffstore(self, destkey, key, *keys):

        client = await self._get_client()

        result = await client.sdiffstore(destkey, key, *keys)

        return result

    async def sinter(self, key, *keys):

        client = await self._get_client()

        result = await client.sinter(key, *keys)

        return result

    async def sinterstore(self, destkey, key, *keys):

        client = await self._get_client()

        result = await client.sinterstore(destkey, key, *keys)

        return result

    async def sismember(self, key, member):

        client = await self._get_client()

        result = await client.sismember(key, member)

        return result

    async def smembers(self, key, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.smembers(key, encoding=encoding)

        return result

    async def smove(self, sourcekey, destkey, member):

        client = await self._get_client()

        result = await client.smove(sourcekey, destkey, member)

        return result

    async def spop(self, key, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.spop(key, encoding=encoding)

        return result

    async def srandmember(self, key, count=None, *, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.srandmember(key, count, encoding=encoding)

        return result

    async def srem(self, key, member, *members):

        client = await self._get_client()

        result = await client.srem(key, member, *members)

        return result

    async def sscan(self, key, cursor=0, match=None, count=None):

        client = await self._get_client()

        result = await client.sscan(key, cursor, match, count)

        return result

    async def sunion(self, key, *keys):

        client = await self._get_client()

        result = await client.sunion(key, *keys)

        return result

    async def sunionstore(self, destkey, key, *keys):

        client = await self._get_client()

        result = await client.sunionstore(destkey, key, *keys)

        return result

    # SORTED SET COMMANDS

    async def zadd(self, key, score, member, *pairs):

        client = await self._get_client()

        result = await client.zadd(key, score, member, *pairs)

        return result

    async def zcard(self, key):

        client = await self._get_client()

        result = await client.zcard(key)

        return result

    async def zcount(self, key, _min=float(r'-inf'), _max=float(r'inf'), *, exclude=None):

        client = await self._get_client()

        result = await client.zcount(key, _min, _max, exclude=exclude)

        return result

    async def zincrby(self, key, increment, member):

        client = await self._get_client()

        result = await client.zincrby(key, increment, member)

        return result

    async def zinterstore(self, destkey, key, *keys, with_weights=False, aggregate=None):

        client = await self._get_client()

        result = await client.zinterstore(destkey, key, *keys, with_weights=with_weights, aggregate=aggregate)

        return result

    async def zlexcount(self, key, _min=b'-', _max=b'+', include_min=True, include_max=True):

        client = await self._get_client()

        result = await client.zlexcount(key, _min, _max, include_min, include_max)

        return result

    async def zrange(self, key, start=0, stop=-1, withscores=False):

        client = await self._get_client()

        result = await client.zrange(key, start, stop, withscores)

        return result

    async def zrangebylex(self, key, _min=b'-', _max=b'+', include_min=True, include_max=True, offset=None, count=None):

        client = await self._get_client()

        result = await client.zrangebylex(key, _min, _max, include_min, include_max, offset, count)

        return result

    async def zrangebyscore(self, key, _min=float(r'-inf'), _max=float(r'inf'), withscores=False, offset=None, count=None, *, exclude=None):

        client = await self._get_client()

        result = await client.zrangebyscore(key, _min, _max, withscores, offset, count, exclude=exclude)

        return result

    async def zrank(self, key, member):

        client = await self._get_client()

        result = await client.zrank(key, member)

        return result

    async def zrem(self, key, member, *members):

        client = await self._get_client()

        result = await client.zrem(key, member, *members)

        return result

    async def zremrangebylex(self, key, _min=b'-', _max=b'+', include_min=True, include_max=True):

        client = await self._get_client()

        result = await client.zremrangebylex(key, _min, _max, include_min, include_max)

        return result

    async def zremrangebyrank(self, key, start, stop):

        client = await self._get_client()

        result = await client.zremrangebyrank(key, start, stop)

        return result

    async def zremrangebyscore(self, key, _min=float(r'-inf'), _max=float(r'inf'), *, exclude=None):

        client = await self._get_client()

        result = await client.zremrangebyscore(key, _min, _max, exclude=exclude)

        return result

    async def zrevrange(self, key, start, stop, withscores=False):

        client = await self._get_client()

        result = await client.zrevrange(key, start, stop, withscores)

        return result

    async def zrevrangebyscore(self, key, _min=float(r'-inf'), _max=float(r'inf'), *, exclude=None, withscores=False, offset=None, count=None):

        client = await self._get_client()

        result = await client.zrevrangebyscore(key, _max, _min, exclude=exclude, withscores=withscores, offset=offset, count=count)

        return result

    async def zrevrank(self, key, member):

        client = await self._get_client()

        result = await client.zrevrank(key, member)

        return result

    async def zscan(self, key, cursor=0, match=None, count=None):

        client = await self._get_client()

        result = await client.zscan(key, cursor, match, count)

        return result

    async def zscore(self, key, member):

        client = await self._get_client()

        result = await client.zscore(key, member)

        return result

    async def zunionstore(self, destkey, key, *keys, with_weights=False, aggregate=None):

        client = await self._get_client()

        result = await client.zunionstore(destkey, key, *keys, with_weights, aggregate)

        return result

    # SERVER COMMANDS

    async def bgrewriteaof(self):

        client = await self._get_client()

        result = await client.bgrewriteaof()

        return result

    async def bgsave(self):

        client = await self._get_client()

        result = await client.bgsave()

        return result

    async def client_getname(self, encoding=_NOTSET):

        client = await self._get_client()

        result = await client.client_getname(encoding)

        return result

    async def client_kill(self):

        client = await self._get_client()

        result = await client.client_kill()

        return result

    async def client_list(self):

        client = await self._get_client()

        result = await client.client_list()

        return result

    async def client_pause(self, timeout):

        client = await self._get_client()

        result = await client.client_pause(timeout)

        return result

    async def client_setname(self, name):

        client = await self._get_client()

        result = await client.client_setname(name)

        return result

    async def config_get(self, parameter=r'*'):

        client = await self._get_client()

        result = await client.config_get(parameter)

        return result

    async def config_resetstat(self):

        client = await self._get_client()

        result = await client.config_resetstat()

        return result

    async def config_rewrite(self):

        client = await self._get_client()

        result = await client.config_rewrite()

        return result

    async def config_set(self, parameter, value):

        client = await self._get_client()

        result = await client.config_set(parameter, value)

        return result

    async def dbsize(self):

        client = await self._get_client()

        result = await client.dbsize()

        return result

    async def debug_object(self, key):

        client = await self._get_client()

        result = await client.debug_object(key)

        return result

    async def debug_segfault(self, key):

        client = await self._get_client()

        result = await client.debug_segfault(key)

        return result

    async def flushall(self):

        client = await self._get_client()

        result = await client.flushall()

        return result

    async def flushdb(self):

        client = await self._get_client()

        result = await client.flushdb()

        return result

    async def info(self, section='default'):

        client = await self._get_client()

        result = await client.info(section)

        return result

    async def lastsave(self):

        client = await self._get_client()

        result = await client.lastsave()

        return result

    async def monitor(self):

        client = await self._get_client()

        result = await client.monitor()

        return result

    async def role(self):

        client = await self._get_client()

        result = await client.role()

        return result

    async def save(self):

        client = await self._get_client()

        result = await client.save()

        return result

    async def shutdown(self, save=None):

        client = await self._get_client()

        result = await client.shutdown(save)

        return result

    async def slaveof(self, host=_NOTSET, port=None):

        client = await self._get_client()

        result = await client.slaveof(host, port)

        return result

    async def slowlog_get(self, length=None):

        client = await self._get_client()

        result = await client.slowlog_get(length)

        return result

    async def slowlog_len(self, length=None):

        client = await self._get_client()

        result = await client.slowlog_len(length)

        return result

    async def slowlog_reset(self):

        client = await self._get_client()

        result = await client.slowlog_reset()

        return result

    async def sync(self):

        client = await self._get_client()

        result = await client.sync()

        return result

    async def time(self):

        client = await self._get_client()

        result = await client.time()

        return result

    # HYPERLOGLOG COMMANDS

    async def pfadd(self, key, value, *values):

        client = await self._get_client()

        result = await client.pfadd(key, value, *values)

        return result

    async def pfcount(self, key, *keys):

        client = await self._get_client()

        result = await client.pfcount(key, *keys)

        return result

    async def pfmerge(self, destkey, sourcekey, *sourcekeys):

        client = await self._get_client()

        result = await client.pfmerge(destkey, sourcekey, *sourcekeys)

        return result

    # TRANSACTION COMMANDS

    async def multi_exec(self):

        client = await self._get_client()

        result = await client.multi_exec()

        return result

    async def pipeline(self):

        client = await self._get_client()

        result = await client.pipeline()

        return result

    async def unwatch(self):

        client = await self._get_client()

        result = await client.unwatch()

        return result

    async def watch(self, key, *keys):

        client = await self._get_client()

        result = await client.watch(key, *keys)

        return result

    # SCRIPTING COMMANDS

    async def eval(self, script, keys=[], args=[]):

        client = await self._get_client()

        result = await client.eval(script, keys, args)

        return result

    async def evalsha(self, digest, keys=[], args=[]):

        client = await self._get_client()

        result = await client.evalsha(digest, keys, args)

        return result

    async def script_exists(self, digest, *digests):

        client = await self._get_client()

        result = await client.script_exists(digest, *digests)

        return result

    async def script_flush(self):

        client = await self._get_client()

        result = await client.script_flush()

        return result

    async def script_kill(self):

        client = await self._get_client()

        result = await client.script_kill()

        return result

    async def script_load(self, script):

        client = await self._get_client()

        result = await client.script_load(script)

        return result

    # PUB/SUB COMMANDS

    async def channels(self):

        client = await self._get_client()

        result = await client.channels

        return result

    async def in_pubsub(self):

        client = await self._get_client()

        result = await client.in_pubsub

        return result

    async def patterns(self):

        client = await self._get_client()

        result = await client.patterns

        return result

    async def psubscribe(self, pattern, *patterns):

        client = await self._get_client()

        result = await client.psubscribe(pattern, *patterns)

        return result

    async def publish(self, channel, message):

        client = await self._get_client()

        result = await client.publish(channel, message)

        return result

    async def publish_json(self, channel, obj):

        client = await self._get_client()

        result = await client.publish_json(channel, obj)

        return result

    async def pubsub_channels(self, pattern=None):

        client = await self._get_client()

        result = await client.pubsub_channels(pattern)

        return result

    async def pubsub_numpat(self):

        client = await self._get_client()

        result = await client.pubsub_numpat()

        return result

    async def pubsub_numsub(self, *channels):

        client = await self._get_client()

        result = await client.pubsub_numsub(*channels)

        return result

    async def punsubscribe(self, pattern, *patterns):

        client = await self._get_client()

        result = await client.punsubscribe(pattern, *patterns)

        return result

    async def subscribe(self, channel, *channels):

        client = await self._get_client()

        result = await client.subscribe(channel, *channels)

        return result

    async def unsubscribe(self, channel, *channels):

        client = await self._get_client()

        result = await client.unsubscribe(channel, *channels)

        return result


class MLock(AsyncContextManager):

    _acquire_future = {}

    _wait_unlock_future = {}

    _unlock_script = '''
if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return 0
end
'''

    def __init__(self, cache, key, expire):

        self._cache = cache
        self._expire = expire

        self._lock_tag = r'process_lock_{0}'.format(key)
        self._lock_val = Utils.uuid1().encode()

        self._locked = False

    async def _context_release(self):

        await self.release()

    def _clear_acquire_future(self):

        if(self._lock_tag in self._acquire_future):
            del self._acquire_future[self._lock_tag]

    def _clear_wait_unlock_future(self):

        if(self._lock_tag in self._wait_unlock_future):
            del self._wait_unlock_future[self._lock_tag]

    async def _do_acquire(self, always=False):

        global _SET_IF_NOT_EXIST

        params = {
            r'key': self._lock_tag,
            r'value': self._lock_val,
            r'expire': self._expire,
            r'exist': _SET_IF_NOT_EXIST,
        }

        result = await self._cache._set(**params)

        if(not result and always):

            while(not result):

                await Utils.wait_frame()

                result = await self._cache._set(**params)

        self._clear_acquire_future()

        return result

    async def _do_wait_unlock(self):

        result = await self._cache.exists(self._lock_tag)

        while(result):

            await Utils.wait_frame()

            result = await self._cache.exists(self._lock_tag)

        self._clear_wait_unlock_future()

        return False

    def _acquire(self, always=False):

        result = None

        if(self._lock_tag in self._acquire_future):

            result = asyncio.Future()

            result.set_result(False)

        else:

            result = self._do_acquire(always)

            self._acquire_future[self._lock_tag] = result

        return result

    def _wait_unlock(self):

        result = None

        if(self._lock_tag in self._wait_unlock_future):

            result = self._wait_unlock_future[self._lock_tag]

        else:

            result = self._do_wait_unlock()

            self._wait_unlock_future[self._lock_tag] = result

        return result

    async def acquire(self):

        if(self._locked):
            return False

        while(not self._locked):

            await Utils.wait_frame()

            self._locked = await self._acquire(True)

        return self._locked

    async def wait_unlock(self):

        if(self._locked):
            return False

        self._locked = await self._acquire()

        if(not self._locked):
            await self._wait_unlock()

        return self._locked

    async def release(self):

        if(self._locked):

            self._locked = False

            if(self._cache):
                await self._cache.eval(self._unlock_script, [self._lock_tag], [self._lock_val])

        self._clear_acquire_future()
        self._clear_wait_unlock_future()

        self._cache = self._lock_tag = None


class MutexLock():

    def __init__(self, cache, key, expire):

        self._cache = cache
        self._expire = expire

        self._lock_tag = r'mutex_lock_{0}'.format(key)
        self._lock_val = Utils.uuid1().encode()

        self._locked = False

    async def acquire(self):

        global _SET_IF_NOT_EXIST

        self._locked = False

        try:

            res = await self._cache._get(self._lock_tag)

            if(res):

                if(res == self._lock_val):

                    await self._cache.expire(self._lock_tag, self._expire)

                    self._locked = True

                else:

                    self._locked = False

            else:

                params = {
                    r'key': self._lock_tag,
                    r'value': self._lock_val,
                    r'expire': self._expire,
                    r'exist': _SET_IF_NOT_EXIST,
                }

                res = await self._cache._set(**params)

                if(res):
                    self._locked = True
                else:
                    self._locked = False

        except Exception as err:

            self._cache.close()

            self._cache.log.error(err)

        return self._locked


class ShareCache(AsyncContextManager):

    def __init__(self, cache, ckey):

        self._cache = cache
        self._ckey = ckey

        self._locker = None
        self._locked = False

        self.result = None

    async def _context_release(self):

        await self.release()

    async def get(self):

        result = await self._cache.get(self._ckey)

        if(result is None):

            self._locker = self._cache.allocate_lock(self._ckey)
            self._locked = await self._locker.wait_unlock()

            if(not self._locked):
                result = await self._cache.get(self._ckey)

        return result

    async def set(self, value, expire=0):

        result = await self._cache.set(self._ckey, value, expire)

        return result

    async def release(self):

        if(self._locked):

            self._locked = False

            if(self._locker):
                await self._locker.release()

        self._cache = self._locker = None
