
from __future__ import annotations


import dataclasses as dc
import contextlib as cl
import inspect as ins
import functools as ft
import copy as cp
import re

import typing as T


ClassVar_pattern = re.compile(r'\bClassVar\b')


def default(x) -> ...:
    if callable(x) and getattr(x, '__name__', None) == "<lambda>":
        return dc.field(default_factory=x)
    else:
        return dc.field(default_factory=ft.partial(cp.deepcopy, x))


class Setter:
    def __init__(self, f, variable_name=None):
        self.f = f
        self.name = variable_name or f.__name__
        self.__doc__ = f.__doc__

    def __set__(self, obj, value):
        obj.__dict__[self.name] = self.f(obj, value)
    __call__ = __set__

class RawSetter:
    def __init__(self, variable_name=None):
        self.name = variable_name

    def __set__(self, obj, value):
        obj.__dict__[self.name] = value
    __call__ = __set__

def setters(cls):
    for name, value in list(cls.__dict__.items()):
        if callable(value) and name.startswith('_set_'):
            attr_name = name[len('_set_'):]
            setattr(cls, attr_name, Setter(value, attr_name)) # noqa
            private_attr_name = '_'+attr_name
            setattr(cls, private_attr_name, RawSetter(attr_name))
    return cls


class ImmutableField:
    def __init__(self, attr: str = None, default=dc.MISSING, setter=None):
        self.attr = attr
        self.default = default
        self.setter = setter
    def __set__(self, obj, value):
        assert not obj.configured, f"{obj} has already been initialized, so {self.attr} cannot be set to {value} because it has been marked as immutable."
        if self.setter is None:
            obj.__dict__[self.attr] = value
        else:
            self.setter(obj, value)


ImmutableDefault = T.TypeVar('ImmutableDefault')

def immutable(x: ImmutableDefault, attr:str=None) -> ImmutableDefault:
    return ImmutableField(attr=attr, default=x)


class ConfigField:
    def __init__(self, name: str, default=dc.MISSING):
        self.name = name
        self.setter = None
        self.get_default_value = lambda: self.default_value
        self.update_default(default)

    def update_default(self, default):
        self.default = default
        self.default_value: T.Any = dc.MISSING
        self.default_factory: T.Callable[[], T.Any]|type[dc.MISSING] = dc.MISSING
        if self.default is not dc.MISSING:
            if isinstance(self.default, ImmutableField):
                self.update_setter(self.default)
                self.default = self.setter.default
            if isinstance(self.default, dc.Field):
                self.default_value = self.default.default
                self.default_factory = self.default.default_factory
            elif callable(self.default) and getattr(self.default, '__name__', None) == '<lambda>':
                self.default_factory = self.default
            elif isinstance(self.default, (int, float, str, bool, bytes, frozenset, tuple)):
                self.default_value = self.default
            else:
                self.default_factory = ft.partial(cp.deepcopy, self.default)
        else:
            self.default_value = None
        if self.default_value is dc.MISSING:
            self.get_default_value = self.default_factory

    def update_setter(self, setter):
        if isinstance(setter, ImmutableField):
            if isinstance(self.setter, ImmutableField):
                if setter.setter is not None:
                    self.setter.setter = setter.setter
            else:
                previous_setter = self.setter
                self.setter = setter
                if self.setter.setter is None:
                    self.setter.setter = previous_setter
        else:
            if isinstance(self.setter, ImmutableField):
                self.setter.setter = setter
            else:
                self.setter = setter


class FieldTester:
    def __init__(self, object):
        self.__object__ = object
    def __getattr__(self, attr):
        return attr in self.__object__


default_constant = object()

O = T.TypeVar('O')

class ConfigTracker(T.Generic[O]):
    def __init__(self, object: O, fields):
        self.object: O = object
        self.fields = dict.fromkeys(fields)
        self.initialized = False
        self._state = 'default'
        self._add = False
        self._filter_flag = None

    def update(self, field, is_configured=default_constant):
        if not self._add and field not in self.fields:
            return
        if self._state == 'default':
            if is_configured is default_constant:
                self.fields[field] = False
            else:
                self.fields[field] = bool(is_configured)
        elif self._state == 'freeze':
            return
        else:
            self.fields[field] = bool(is_configured)

    def subconfigs(self):
        return {attr: subconfig for attr in self if isinstance(subconfig:=getattr(self, attr, None), Config)}

    def __iter__(self):
        return iter(field for field, is_configured in self.fields
            if self._filter_flag is None or self._filter_flag is is_configured)

    def __len__(self):
        return len(list(self))

    def __contains__(self, item):
        return item in self.fields and (self._filter_flag is None or self._filter_flag is self.fields[item])

    def has(self) -> O:
        return FieldTester(self)

    @cl.contextmanager
    def set_configured(self):
        old_state = self._state
        self._state = 'config'
        yield
        self._state = old_state

    @cl.contextmanager
    def set_defaults(self):
        old_state = self._state
        self._state = 'default'
        yield
        self._state = old_state

    @cl.contextmanager
    def freeze(self):
        old_state = self._state
        self._state = 'freeze'
        yield
        self._state = old_state

    @cl.contextmanager
    def add_fields(self):
        old_state = self._add
        self._add = True
        yield
        self._add = old_state

    def configured(self):
        config_copy = cp.copy(self)
        config_copy._filter_flag = True
        return config_copy

    def defaults(self):
        config_copy = cp.copy(self)
        config_copy._filter_flag = False
        return config_copy


class ConfigMeta(type):
    __config_fields__: dict[str, ConfigField]
    def __new__(cls, name: str, bases: tuple[type], attrs: dict[str, T.Any]):
        __config_fields__ = {}
        for base in (base for base in bases if isinstance(base, ConfigMeta)):
            for attr, base_field in base.__config_fields__.items():
                __config_fields__[attr] = base_field
                if attr in attrs and attr not in attrs.get('__annotations__', {}):
                    base_field.update_default(attrs[attr])
        for attr, annotation in attrs.get('__annotations__', {}).items():
            if ClassVar_pattern.search(str(annotation)): continue
            if attr in __config_fields__:
                config_field = cp.copy(__config_fields__[attr])
                if attr in attrs:
                    default = attrs[attr]
                    config_field.update_default(default)
            else:
                __config_fields__[attr] = ConfigField(attr, default=attrs.get(attr, dc.MISSING))
        attrs['__config_fields__'] = __config_fields__
        for attr, field in __config_fields__.items():
            attrs[attr] = field.setter
        for setter_name, value in list(attrs.items()):
            if setter_name.startswith('_set_') and callable(value) and hasattr(value, '__name__'):
                attr = setter_name[len('_set_'):]
                private_setter_name = '_'+attr
                setter = Setter(value, attr) # noqa
                if attr in __config_fields__:
                    field = __config_fields__[attr]
                    field.update_setter(setter)
                attrs[attr] = setter
                attrs[private_setter_name] = RawSetter(attr)
        def __init__(self, base=None, **kwargs):
            config_fields = self.__config_fields__
            self.config = ConfigTracker(self, config_fields)
            for attr, field in config_fields.items():
                value = kwargs[attr] if attr in kwargs else field.get_default_value()
                setattr(self, attr, value)
            if hasattr(self, '__post_init__'):
                self.__post_init__()
            self.config._state = 'config'
            return
        attrs['__init__'] = __init__
        cls = super().__new__(cls, name, bases, attrs)
        return cls


class Config(metaclass=ConfigMeta):
    config: ConfigTracker[T.Self]

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if hasattr(self, 'config'):
            self.config.update(key)

    def __imul__(self, other):
        if isinstance(other, dict):
            fields = iter((field, value) for field, value in other.items() if field != '__class__')
        else:
            fields = iter((attr, getattr(other, attr, None)) for attr in other.config)
        for field, value in fields:
            if field not in self.config.fields: continue
            old_value = getattr(self, field, None)
            if isinstance(old_value, Config) and (isinstance(value, dict) or isinstance(value, type(old_value))):
                old_value.__imul__(value)
            else:
                setattr(self, field, value)
        return self

    def __mul__(self, other):
        return cp.deepcopy(self).__imul__(other)

    def __ipow__(self, other):
        with self.config.add_fields():
            if isinstance(other, dict):
                fields = iter((field, value) for field, value in other.items() if field != '__class__')
            else:
                fields = iter((attr, getattr(other, attr, None)) for attr in other.config)
            for field, value in fields:
                old_value = getattr(self, field, None)
                if isinstance(old_value, Config) and (
                    isinstance(value, dict) or isinstance(value, type(old_value))
                ):
                    old_value.__ipow__(value)
                else:
                    setattr(self, field, value)
        return self

    def __pow__(self, other):
        return cp.deepcopy(self).__ipow__(other)

    def __ilshift__(self, other):
        if isinstance(other, dict):
            fields = iter((field, value) for field, value in other.items() if field != '__class__')
        else:
            fields = iter((attr, getattr(other, attr, None)) for attr in other.config.configured())
        for field, value in fields:
            if self.config.fields.get(field, True): continue
            old_value = getattr(self, field, None)
            if isinstance(old_value, Config) and (isinstance(value, dict) or isinstance(value, type(old_value))):
                old_value.__ilshift__(value)
            else:
                setattr(self, field, value)
        return self

    def __lshift__(self, other):
        return cp.deepcopy(self).__ilshift__(other)

    def __irshift__(self, other):
        if isinstance(other, dict):
            fields = iter((field, value) for field, value in other.items() if field != '__class__')
        else:
            fields = iter((attr, getattr(other, attr, None)) for attr in other.config.configured())
        for field, value in fields:
            if field not in self.config.fields: continue
            old_value = getattr(self, field, None)
            if isinstance(old_value, Config) and (isinstance(value, dict) or isinstance(value, type(old_value))):
                old_value.__irshift__(value)
            else:
                setattr(self, field, value)
        return self

    def __rshift__(self, other):
        return cp.deepcopy(self).__irshift__(other)

    def __ior__(self, other):
        with self.config.add_fields():
            if isinstance(other, dict):
                fields = iter((field, value) for field, value in other.items() if field != '__class__')
            else:
                fields = iter((attr, getattr(other, attr, None)) for attr in other.config.configured())
            for field, value in fields:
                old_value = getattr(self, field, None)
                if isinstance(old_value, Config) and (
                    isinstance(value, dict) or isinstance(value, type(old_value))
                ):
                    old_value.__ior__(value)
                else:
                    setattr(self, field, value)
        return self

    def __or__(self, other):
        return cp.deepcopy(self).__ior__(other)

    def __iand__(self, other):
        with self.config.add_fields():
            if isinstance(other, dict):
                fields = iter((field, value) for field, value in other.items() if field != '__class__')
            else:
                fields = iter((attr, getattr(other, attr, None)) for attr in other.config.configured())
            for field, value in fields:
                if self.config.fields.get(field, None): continue
                old_value = getattr(self, field, None)
                if isinstance(old_value, Config) and (
                    isinstance(value, dict) or isinstance(value, type(old_value))
                ):
                    old_value.__iand__(value)
                else:
                    setattr(self, field, value)
        return self

    def __and__(self, other):
        return cp.deepcopy(self).__iand__(other)



"""
Merging Configs
===============

Update base with everything including defaults (and add new fields):
base **= overrides

Update base with everything as long as it is a field of base:
base *= overrides

Update base with configured overrides (adding new fields):
base |= overrides

Update only base fields with overrides:
base >>= overrides

Update only unconfigured fields (and add missing fields) with overrides:
base &= overrides

Update only unconfigured fields with overrides:
base <<= overrides
"""



if __name__ == '__main__':

    @dc.dataclass
    class Foo(Config):
        x: int = None
        y: str = 'hello world!'

        def __post_init__(self):
            if self.x is None:
                self.x = 0

        def _set_y(self, value):
            return value.upper()


    foo = Foo()


    print(foo)