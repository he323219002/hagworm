# -*- coding: utf-8 -*-

import os
import sys
import functools

from tornado.gen import coroutine
from tornado.concurrent import Future

from hagworm.extend.base import Utils


def func_cache(timeout=10):

    def _wrapper(func):

        __cache = TimedCache()

        @functools.wraps(func)
        def __wrapper(*args, **kwargs):

            func_sign = Utils.params_sign(func, *args, **kwargs)

            result = __cache.get(func_sign)

            if(result is None):

                result = func(*args, **kwargs)

                if(isinstance(result, Future)):
                    result = yield result

                __cache.set(func_sign, result, timeout)

            return result

        return coroutine(__wrapper)

    return _wrapper


def share_future(func):

    __future = {}

    @functools.wraps(func)
    def __wrapper(*args, **kwargs):

        result = None

        func_sign = Utils.params_sign(func, *args, **kwargs)

        if(func_sign in __future):

            result = Future()

            __future[func_sign].append(result)

        else:

            result = coroutine(func)(*args, **kwargs)

            __future[func_sign] = [result]

            result.add_done_callback(
                Utils.func_partial(__clear_future, func_sign))

        return result

    def __clear_future(func_sign, future):

        if(func_sign not in __future):
            return

        futures = __future.pop(func_sign)

        result = futures.pop(0).result()

        for future in futures:
            future.set_result(Utils.deepcopy(result))

    return __wrapper
