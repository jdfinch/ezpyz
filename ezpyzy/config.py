
from __future__ import annotations

import inspect
import dataclasses as dc
import pathlib as pl
import json
import contextlib as cl
import copy as cp
import functools as ft
import re

from ezpyzy.setter import RawSetter, Setter
from ezpyzy.import_path import get_import_path, import_obj_from_path

import typing as T

ClassVar_pattern = re.compile(r'\bClassVar\b')

def default(x) -> ...:
    if callable(x) and getattr(x, '__name__', None) == "<lambda>":
        return dc.field(default_factory=x)
    else:
        return dc.field(default_factory=ft.partial(cp.deepcopy, x))


class ImplementsConfig:
    __config_implemented__: T.ClassVar[Config]


CI = T.TypeVar('CI', bound='Config')
def construct_implementation_of(config: CI) -> CI:
    assert not isinstance(config, ImplementsConfig), \
        f"Tried to construct implementation of already-implemented {config}. Please pass it's config instead."
    assert hasattr(config, '__implementation__')
    implemented = config.__implementation__(config) # noqa
    return implemented

class ConfigFields(dict[str, None]):
    def __init__(self, object, which_fields_serialization_strategy):
        dict.__init__(self)
        self.object = object
        self._which_fields_serialization_strategy = which_fields_serialization_strategy
        self.has = FieldTester(self)

    def dict(self):
        encoder = ConfigJSONEncoder(which=self._which_fields_serialization_strategy)
        return encoder.default(self.object)

    def json(self, indent=2):
        encoder = ConfigJSONEncoder(which=self._which_fields_serialization_strategy, indent=indent)
        return encoder.encode(self.object)

    def save(self, path: str | pl.Path = None, indent: int | None = 2):
        serialized = self.json(indent=indent)
        if path is not None:
            path = pl.Path(path).expanduser()
        elif self.object.base:
            path = pl.Path(self.object.base)
        else:
            raise ValueError('No path provided and no base path found for saving config.')
        path.write_text(serialized)
        return serialized


class FieldTester:
    def __init__(self, object):
        self.__object = object
    def __getattr__(self, field):
        return field in self.__object
    def __deepcopy__(self, memodict={}):
        return FieldTester(self.__object)


OA = T.TypeVar('OA', bound='Config')
class Args(T.Generic[OA], dict):
    has: OA
    def __init__(self, obj: OA, args):
        dict.__init__(self, args) # noqa
        self.has: OA = FieldTester(self) # noqa
        self.__obj: OA = obj
    def __deepcopy__(self, memodict={}):
        return Args(self.__obj, self)

empty = object()


O = T.TypeVar('O', bound='Config')

class Configured(T.Generic[O]):
    def __init__(self, object, fields, args):
        self.object: O = object
        self.subconfigs: ConfigFields[str, None] = ConfigFields(object, 'subconfigs')
        self.and_unconfigured: ConfigFields[str, None] = ConfigFields(object, 'and_unconfigured')
        self.configured: ConfigFields[str, None] = ConfigFields(object, 'configured')
        self.initialized: bool = False
        self._set_fields_configured: bool = False
        self._set_fields_unconfigured: bool = False
        self._do_not_configure: bool = False
        self.args: Args[O]|None = Args(object, args)
        self.has: O = FieldTester(self) # noqa
        for field in fields:
            self.set(field, args.get(field, empty), configured=field in args)

    def __bool__(self):
        return self.initialized

    def __contains__(self, field: str):
        return field in self.configured

    def __iter__(self):
        return iter(self.configured)

    def set(self, field: str, value: T.Any = empty, configured: bool = None):
        if configured:
            configured = True
        elif configured is False:
            configured = False
        elif self._do_not_configure:
            return
        elif self._set_fields_configured:
            configured = True
        elif self._set_fields_unconfigured:
            configured = False
        elif configured is None and field not in self.and_unconfigured:
            return
        else:
            configured = self.initialized
        if isinstance(value, Config):
            self.subconfigs[field] = None
        elif value is not empty:
            self.subconfigs.pop(field, None)
        if configured:
            self.configured[field] = None
            self.and_unconfigured[field] = None
        elif not configured:
            self.configured.pop(field, None)
            self.and_unconfigured[field] = None

    def remove(self, field: str):
        self.configured.pop(field, None)
        self.and_unconfigured.pop(field, None)
        self.subconfigs.pop(field, None)

    def configuring(self):
        @cl.contextmanager
        def configuring_context():
            old_configuring_value = self._set_fields_configured
            old_do_not_config_value = self._do_not_configure
            self._do_not_configure = False
            self._set_fields_configured = True
            subconfig_contexts = []
            for subconfig_name in self.subconfigs:
                subconfig = getattr(self.object, subconfig_name, None)
                if isinstance(subconfig, Config):
                    context = subconfig.configured.configuring()
                    subconfig_contexts.append(context)
                    context.__enter__()
            yield self
            for subconfig_context in subconfig_contexts:
                subconfig_context.__exit__(None, None, None)
            self._set_fields_configured = old_configuring_value
            self._do_not_configure = old_do_not_config_value
        return configuring_context()

    def configuring_defaults(self):
        @cl.contextmanager
        def unconfigured_context():
            old_configuring_value = self._set_fields_unconfigured
            old_do_not_config_value = self._do_not_configure
            self._do_not_configure = False
            self._set_fields_unconfigured = True
            subconfig_contexts = []
            for subconfig_name in self.subconfigs:
                subconfig = getattr(self.object, subconfig_name, None)
                if isinstance(subconfig, Config):
                    context = subconfig.configured.configuring_defaults()
                    subconfig_contexts.append(context)
                    context.__enter__()
            yield self
            for subconfig_context in subconfig_contexts:
                subconfig_context.__exit__(None, None, None)
            self._set_fields_unconfigured = old_configuring_value
            self._do_not_configure = old_do_not_config_value
        return unconfigured_context()

    def not_configuring(self):
        @cl.contextmanager
        def not_configuring_context():
            old_configuring_value = self._do_not_configure
            self._do_not_configure = True
            subconfig_contexts = []
            for subconfig_name in self.subconfigs:
                subconfig = getattr(self.object, subconfig_name, None)
                if isinstance(subconfig, Config):
                    context = subconfig.configured.not_configuring()
                    subconfig_contexts.append(context)
                    context.__enter__()
            yield self
            for subconfig_context in subconfig_contexts:
                subconfig_context.__exit__(None, None, None)
            self._do_not_configure = old_configuring_value
        return not_configuring_context()

    @property
    def unconfigured(self) -> ConfigFields[str, None]:
        config_fields = ConfigFields(self.object, 'unconfigured')
        config_fields.update({k: v for k, v in self.and_unconfigured.items() if k not in self.configured})
        return config_fields

    def dict(self):
        encoder = ConfigJSONEncoder()
        return encoder.default(self.object)

    def json(self, indent=2):
        encoder = ConfigJSONEncoder(indent=indent)
        return encoder.encode(self.object)

    def save(self, path:str|pl.Path=None, indent:int|None=2):
        serialized = self.json(indent=indent)
        if path is not None:
            path = pl.Path(path).expanduser()
        elif self.object.base:
            path = pl.Path(self.object.base)
        else:
            raise ValueError('No path provided and no base path found for saving config.')
        path.write_text(serialized)
        return serialized

    def __str__(self):
        return f"{self.object.__class__.__name__}(configured={{{', '.join(self.configured)}}}, unconfigured={{{', '.join(self.unconfigured)}}}, subconfigs={{{', '.join(self.subconfigs)}}})"


class ConfigMeta(type):
    configured: Configured

    def __new__(cls, name, bases: tuple, attrs):
        if name == 'Config':
            del attrs['__getattr__']
        __default_subconfigs__ = {}
        for attr, default_value in attrs.items():
            if (attr in attrs.get('__annotations__', {})
                and default_value is not None
                and not isinstance(default_value, (str, int, float, bool, dc.Field))
                and ClassVar_pattern.search(str(attrs['__annotations__'][attr])) is None
            ):
                if isinstance(default_value, Config):
                    subconfig_stack = [default_value]
                    while subconfig_stack:
                        subconfig = subconfig_stack.pop()
                        for field in subconfig.configured.and_unconfigured:
                            subconfig.configured.set(field, configured=False)
                            if field in subconfig.configured.subconfigs:
                                subconfig_stack.append(getattr(subconfig, field))
                    __default_subconfigs__[attr] = default_value
                attrs[attr] = default(default_value)
        inherited_setters = {}
        inherited_fields = set()
        for base in bases:
            if '__default_subconfigs__' in base.__dict__:
                __default_subconfigs__.update(
                    (k,v) for k,v in base.__default_subconfigs__.items()
                    if k not in __default_subconfigs__)
            if dc.is_dataclass(base):
                for field in dc.fields(base):
                    if (isinstance(field.default, Setter) and field.name not in attrs and
                        field.name not in inherited_fields and field.name not in inherited_setters
                    ):
                        inherited_setters[field.name] = field.default
                        if callable(field.default.default):
                            attrs[field.name] = dc.field(default_factory=field.default.default)
                            attrs.setdefault('__annotations__', {})[field.name] = field.type
                        else:
                            attrs[field.name] = dc.field(default=field.default.default)
                            attrs.setdefault('__annotations__', {})[field.name] = field.type
                    else:
                        inherited_fields.add(field.name)
        attrs['__default_subconfigs__'] = __default_subconfigs__
        # if 'MultiConfig' in {base.__name__ for base in bases} and '__init__' not in attrs:
        #     def __multiconfig_subinit__(self, **subconfigs):
        #         super(type(self), self).__init__(**subconfigs)
        #     attrs['__init__'] = __multiconfig_subinit__
        cls = super().__new__(cls, name, bases, attrs)
        cls = dc.dataclass(cls) # noqa
        fields = {field.name: None for i, field in enumerate(dc.fields(cls)) if i > 0} # noqa
        if ImplementsConfig in bases:
            impl_index = bases.index(ImplementsConfig)
            implmented_config_cls = bases[impl_index + 1]
            fields = {field.name: None for i, field in enumerate(dc.fields(implmented_config_cls)) if i > 0}
            if ('__implementation__' in implmented_config_cls.__dict__
                and implmented_config_cls.__dict__['__implementation__'] != cls
            ):
                raise TypeError(f"Defined Implementation {cls} of Config {implmented_config_cls} but Config already has an implmentation: {implmented_config_cls.__implementation__}.")
            elif not issubclass(implmented_config_cls, Config):
                raise TypeError(f"Defined Config Implementation {cls} of {implmented_config_cls}, but it is not a subclass of Config.")
            implmented_config_cls.__implementation__ = cls
            cls.__config_implemented__ = implmented_config_cls
        init = getattr(cls, '__init__', lambda self: None)
        init_sig = inspect.signature(init)
        def __init__(self, *args, **kwargs):
            arguments = init_sig.bind(self, *args, **kwargs).arguments
            self.configured = Configured(object=self, fields=fields,
                args={k:v for i,(k,v) in enumerate(arguments.items()) if i > 0})
            with self.configured.not_configuring():
                init(self, *args, **kwargs) # noqa
            self.configured.initialized = True
            self.configured.args = None
        cls.__init__ = __init__
        dc_fields = {field.name: field for field in dc.fields(cls)}
        for attr, value in attrs.items():
            if attr.startswith('_set_') and callable(value):
                setter_name = attr[len('_set_'):]
                if setter_name in dc_fields:
                    field = dc_fields[setter_name]
                    default_value = field.default if field.default_factory is dc.MISSING else field.default_factory
                    setattr(cls, setter_name, Setter(value, setter_name, default=default_value))
                else:
                    setattr(cls, setter_name, Setter(value, setter_name, getattr(cls, setter_name, dc.MISSING)))
                setattr(cls, f'_{setter_name}', RawSetter(setter_name))
            elif attr in dc_fields:
                setattr(cls, attr, value)
        for setter_name, setter_descriptor in inherited_setters.items():
            setattr(cls, setter_name, setter_descriptor)
            setattr(cls, f'_{setter_name}', RawSetter(setter_name))
        return cls


@dc.dataclass
class Config(metaclass=ConfigMeta):
    configured: T.ClassVar[Configured[T.Self]]
    base: str | pl.Path | 'Config' | None = None

    def __post_init__(self):
        if self.base is not None:
            if isinstance(self.base, str) and self.base.lstrip().startswith('{'):
                '''load base from JSON str'''
                decoder = ConfigJSONDecoder()
                loaded = decoder.decode(self.base)
                base = loaded
                self.base = None
            elif isinstance(self.base, pl.Path) or isinstance(self.base, str):
                '''load base from file'''
                config_path = pl.Path(self.base)
                json_content = config_path.read_text()
                decoder = ConfigJSONDecoder()
                loaded = decoder.decode(json_content)
                base = loaded
                self.base = str(config_path)
            else:
                '''load base from Config instance, JSON dict, or other object'''
                base = self.base
                self.base = None
            self <<= base
        else:
            for subconfig_attr in self.configured.subconfigs:
                subconfig = getattr(self, subconfig_attr, None)
                default_subconfig = self.__class__.__default_subconfigs__.get(subconfig_attr, None)
                if isinstance(subconfig, Config) and isinstance(default_subconfig, Config):
                    with subconfig.configured.not_configuring():
                        subconfig <<= default_subconfig
        if isinstance(self, ImplementsConfig):
            self.__implement__() # noqa

    def __implement__(self):
        for attr, value in vars(self).items():
            if attr != 'base' and type(value).__dict__.get('__implementation__') is not None:
                implementation_cls = value.__implementation__
                implementation_obj = implementation_cls(base=value)
                for field in value.configured.configured:
                    implementation_obj.configured.set(field, configured=True)
                setattr(self, attr, implementation_obj)
                implementation_obj.__implement__()

    def __call__(self, **subconfigs: Config):
        with self.configured.configuring():
            for field, subconfig in subconfigs.items():
                setattr(self, field, subconfig)
        return self

    def __contains__(self, field: str):
        return field in self.configured.and_unconfigured

    def __iter__(self):
        return iter((field, getattr(self, field)) for field in self.configured.and_unconfigured)

    def __len__(self):
        return len(self.configured.and_unconfigured)

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        if hasattr(self, 'configured'):
            if isinstance(getattr(self, key, None), (RawSetter, Setter)):
                self.configured.set(key[1:], value, configured=None)
            else:
                self.configured.set(key, value, configured=None)
        return

    def __getattr__(self, item):
        raise AttributeError(f"Config has no attribute {item}.")

    def __setitem__(self, key, value):
        with self.configured.configuring():
            return self.__setattr__(key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __divmod__(self, other):
        """
        Check whether the configuration matches other.
        """
        if isinstance(other, Config):
            return ((type(self) == type(other)
                     or getattr(type(self), '__config_implemented__', None) is type(other)
                     or getattr(type(other), '__config_implemented__', None) is type(self)
                ) and
                set(self.configured.and_unconfigured).issuperset(
                set(other.configured.and_unconfigured)))
        elif isinstance(other, dict):
            return set(self.configured.and_unconfigured).issuperset(set(other))
        else:
            return False

    def __ilshift__(self, other):
        """
        Update ONLY UNconfigured fields with ALL fields from other.
        """
        if isinstance(other, Config):
            other = {field: getattr(other, field) for field in other.configured.and_unconfigured}
        for field, value in self:
            if (
                field in self.configured.subconfigs and
                field in other and value.__divmod__(other[field])
            ):
                value.__ilshift__(other[field])
            elif field in self.configured.and_unconfigured and field not in self.configured and field in other:
                setattr(self, field, other[field])
        return self

    def __lshift__(self, other):
        copy = cp.deepcopy(self)
        copy.__ilshift__(other)
        return copy

    def __irshift__(self, other):
        """
        Update ALL fields with CONFIGURED values from other.
        """
        if isinstance(other, Config):
            other = {field: getattr(other, field) for field in other.configured.configured}
        for field, value in self:
            if (
                field in self.configured.subconfigs and
                field in other and value.__divmod__(other[field])
            ):
                value.__irshift__(other[field])
            elif field in self.configured.and_unconfigured and field in other:
                setattr(self, field, other[field])
        return self

    def __rshift__(self, other):
        copy = cp.deepcopy(self)
        copy.__irshift__(other)
        return copy

    def __ixor__(self, other):
        """
        Update ONLY UNconfigured fields with CONFIGURED values from other.
        """
        if isinstance(other, Config):
            other = {field: getattr(other, field) for field in other.configured.configured}
        for field, value in self:
            if (
                field in self.configured.subconfigs and
                field in other and value.__divmod__(other[field])
            ):
                value.__ixor__(other[field])
            elif field in self.configured.and_unconfigured and field not in self.configured and field in other:
                setattr(self, field, other[field])
        return self

    def __xor__(self, other):
        copy = cp.deepcopy(self)
        copy.__ixor__(other)
        return copy

    def __pos__(self) -> T.Self:
        copy = cp.copy(self)
        if hasattr(copy, 'configured'):
            del copy.configured
        return copy


class ImmutableConfig(Config):
    def __setattr__(self, key, value):
        if hasattr(self, 'configured') and 'key' in self.configured.and_unconfigured:
            raise AttributeError(f'ImmutableConfig {self} is immutable after construction.')
        return super().__setattr__(key, value)


SUBCONFIGS = T.TypeVar('SUBCONFIGS')

class MultiConfig(Config, T.Generic[SUBCONFIGS]):
    def __init__(self, **subconfigs: SUBCONFIGS):
        super().__init__()
        with self.configured.configuring():
            for field, subconfig in subconfigs.items():
                setattr(self, field, subconfig)
    def __iter__(self) -> T.Iterable[tuple[str, SUBCONFIGS]]:
        return super().__iter__()


class ConfigJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        self.configs = {}
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if '__class__' in obj:
            cls = import_obj_from_path(obj.pop('__class__'))
            config_cls = getattr(cls, '__config_implemented__', None)
            if config_cls is not None:
                cls = config_cls
            fields = {field.name for field in dc.fields(cls)}
            config = cls(**{var: val for var, val in obj.items() if var in fields}) # noqa
            if isinstance(config, MultiConfig):
                for var, val in obj.items():
                    if isinstance(config, MultiConfig) and isinstance(val, Config):
                        setattr(config, var, val)
                        config.configured.set(var, val, configured=True)
            self.configs.setdefault(cls, []).append(config)
            return config
        else:
            return obj

class ConfigJSONEncoder(json.JSONEncoder):
    def __init__(self, which: T.LiteralString = "all", *args, **kwargs):
        self.which_fields = which
        super().__init__(*args, **kwargs)

    def default(self, obj):
        if isinstance(obj, Config):
            which = dict(all='and_unconfigured').get(self.which_fields, self.which_fields)
            fields_to_serialize = getattr(obj.configured, which)
            json = {field: getattr(obj, field) for field in fields_to_serialize}
            if self.which_fields == 'all':
                cls = obj.__config_implemented__ if isinstance(obj, ImplementsConfig) else obj.__class__
                json['__class__'] = get_import_path(cls)
                imported_cls = import_obj_from_path(json['__class__'])
                assert (imported_cls is cls
                        or cls.__module__ == '__main__'
                        and cls.__name__ == imported_cls.__name__
                ), f"Imported class {imported_cls} is not the same as the class {cls} being serialized."
            return json
        else:
            return super().default(obj)


if __name__ == '__main__':

    @dc.dataclass
    class Foo(Config):
        x: int = 1
        y: int = 2

    foo = Foo()

    print(f'{foo = }')