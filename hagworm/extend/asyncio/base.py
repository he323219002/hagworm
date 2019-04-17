# -*- coding: utf-8 -*-

import types
import asyncio
import collections

from hagworm.extend import base


class Utils(base.Utils):

    sleep = staticmethod(asyncio.sleep)

    @staticmethod
    @types.coroutine
    def wait_frame(count=10):

        for _ in range(max(1, count)):
            yield

    @staticmethod
    def call_soon(callback, *args):

        loop = asyncio.events.get_event_loop()

        return loop.call_soon(callback, *args)

    @staticmethod
    def call_later(delay, callback, *args):

        loop = asyncio.events.get_event_loop()

        return loop.call_later(delay, callback, *args)

    @staticmethod
    def call_at(when, callback, *args):

        loop = asyncio.events.get_event_loop()

        return loop.call_at(when, callback, *args)

    @staticmethod
    def run_until_complete(callback, *args):

        loop = asyncio.events.get_event_loop()

        return loop.run_until_complete(callback(*args))


class FutureWrapper(asyncio.Future):

    def __init__(self, future):

        super().__init__()

        future.add_done_callback(self._future_handle)

    def _future_handle(self, future):

        self.set_result(future.result())


class FutureWithTimeout(asyncio.Future):

    def __init__(self, delay):

        super().__init__()

        self._timeout_handle = Utils.call_later(
            delay,
            self.set_result,
            None
        )

        self.add_done_callback(self._clear_timeout)

    def _clear_timeout(self, *_):

        if(self._timeout_handle is not None):
            self._timeout_handle.cancel()
            self._timeout_handle = None


class MultiFuture(asyncio.Future):

    def __init__(self, *, loop=None):

        super().__init__()

        self._pending = False
        self._futures = []

    def append(self, coro_or_future):

        future = asyncio.ensure_future(coro_or_future)
        future.add_done_callback(self._check_futures)

        self._futures.append(future)

    def wait(self):

        self._pending = True

        self._check_futures()

        return self

    def _check_futures(self, *args):

        for _future in self._futures:

            if(not _future.done()):
                break

        else:

            if(self._pending):

                self.set_result(
                    [_future.result() for _future in self._futures]
                )


class AsyncContextManager():

    async def __aenter__(self):

        return self

    async def __aexit__(self, *args):

        await self._context_release()

        if(args[0] is base.Ignore):

            return True

        elif(args[1]):

            Utils.log.exception(args[1], exc_info=args[2])

            return True

    async def _context_release(self):

        raise RuntimeError(r'nothing to release')


class Transaction(AsyncContextManager):

    def __init__(self, *, commit_callback=None, rollback_callback=None):

        self._commit_callbacks = set()
        self._rollback_callbacks = set()

        if(commit_callback):
            self._commit_callbacks.add(commit_callback)

        if(rollback_callback):
            self._rollback_callbacks.add(rollback_callback)

    async def _context_release(self):

        await self.rollback()

    def add(self, _callable, *arg, **kwargs):

        self._callables.add(Utils.func_partial(_callable, *arg, **kwargs))

    async def commit(self):

        if(self._callables is None):
            return

        self._callables.clear()
        self._callables = None

    async def rollback(self):

        if(self._callables is None):
            return

        for _callable in self._callables:

            _res = _callable()

            if(isinstance(_res, collections.abc.Awaitable)):
                await _res

        self._callables.clear()
        self._callables = None
