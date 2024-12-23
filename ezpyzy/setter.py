from __future__ import annotations

import typing as T
import dataclasses as dc


R: T.TypeVar = T.TypeVar('R')


def setter(f: T.Callable[[T.Any, T.Any], R], variable_name=None, default=None) -> R:
    return Setter(f, variable_name, default)  # noqa

class Setter:
    def __init__(self, f, variable_name=None, default=None):
        self.f = f
        self.name = variable_name or f.__name__
        self.__doc__ = f.__doc__
        self.default = default

    def __set__(self, obj, value):
        obj.__dict__[self.name] = self.f(obj, value)


class RawSetter:
    def __init__(self, variable_name=None):
        self.name = variable_name

    def __set__(self, obj, value):
        obj.__dict__[self.name] = value


class FieldSetter(dc.Field):
    f: callable
    name: str
    def __init__(self, f, name, *,
        default=dc.MISSING, default_factory=dc.MISSING, init=True, repr=True,
        hash=None, compare=True, metadata=None, kw_only=dc.MISSING
    ):
        super().__init__(default=default, default_factory=default_factory, init=init, repr=repr,
            hash=hash, compare=compare, metadata=metadata, kw_only=kw_only)
        self.f = f
        self.name = name
    def __set__(self, obj, value):
        obj.__dict__[self.name] = self.f(obj, value)


def setters(cls):
    for name, value in list(cls.__dict__.items()):
        if callable(value) and name.startswith('_set_'):
            attr_name = name[len('_set_'):]
            setattr(cls, attr_name, setter(value, attr_name))
            private_attr_name = '_'+attr_name
            setattr(cls, private_attr_name, RawSetter(attr_name))
    return cls


if __name__ == '__main__':

    import dataclasses as dc


    @setters
    @dc.dataclass
    class Foo:
        x: float
        y: list | T.Iterable

        def _set_y(self, value):
            return list(value)

        def _set_x(self, value):
            return value / 2

        def bar(self):
            self._x = 27.0


    foo = Foo(3, 'abc')
    print(foo)

    myvar = foo.y

    print(foo)
    foo.bar()
    print(foo)

