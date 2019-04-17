# -*- coding: utf-8 -*-

from .base import Utils


class EventObserver():

    def __init__(self, *callables):

        self._callables = {id(_callable): _callable for _callable in callables}

    def __del__(self):

        self._callables.clear()

        self._callables = None

    def __call__(self, *args, **kwargs):

        for _callable in self._callables.values():
            _callable(*args, **kwargs)

    @property
    def is_valid(self):

        return len(self._callables) > 0

    def add(self, _callable):

        result = False

        _id = id(_callable)

        if(_id not in self._callables):
            self._callables[_id] = _callable
            result = True

        return result

    def remove(self, _callable):

        result = False

        _id = id(_callable)

        if(_id in self._callables):
            del self._callables[_id]
            result = True

        return result


class EventDispatcher():

    def __init__(self):

        self._observers = {}

    def dispatch(self, _type, *args, **kwargs):

        Utils.log.debug(
            r'dispatch event {0} {1} {2}'.format(_type, args, kwargs))

        if(_type in self._observers):
            self._observers[_type](*args, **kwargs)

    def add_listener(self, _type, _callable):

        Utils.log.debug(r'add event listener => type({0}) function({1})'.format(
            _type, id(_callable)))

        result = False

        if(_type in self._observers):
            result = self._observers[_type].add(_callable)
        else:
            self._observers[_type] = EventObserver(_callable)
            result = True

        return result

    def remove_listener(self, _type, _callable):

        Utils.log.debug(r'remove event listener => type({0}) function({1})'.format(
            _type, id(_callable)))

        result = False

        if(_type in self._observers):

            observer = self._observers[_type]

            result = observer.remove(_callable)

            if(not observer.is_valid):
                del self._observers[_type]

        return result
