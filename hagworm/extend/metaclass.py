# -*- coding: utf-8 -*-


class SingletonMetaclass(type):

    _instances = {}

    def __call__(self, *args, **kwargs):

        result = None

        instances = self.__class__._instances

        if(self in instances):
            result = instances[self]
        else:
            result = instances[self] = super().__call__(*args, **kwargs)

        return result


class Singleton(metaclass=SingletonMetaclass):
    pass


class SubclassMetaclass(type):

    _baseclasses = []

    def __new__(cls, name, bases, attrs):

        subclass = r'.'.join([attrs[r'__module__'], attrs[r'__qualname__']])

        for base in bases:

            if(base in cls._baseclasses):
                base._subclasses.append(subclass)
            else:
                cls._baseclasses.append(base)
                setattr(base, r'_subclasses', [subclass])

        return type.__new__(cls, name, bases, attrs)
