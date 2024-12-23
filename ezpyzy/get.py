
import functools as ft

import typing as T


def try_get_item(obj, item):
    try:
        return obj[item]
    except Exception:
        return None

def try_call(obj, *args, **kwargs):
    try:
        return obj(*args, **kwargs)
    except Exception:
        return None


O = T.TypeVar('O')

class Op(T.Generic[O]):

    def __init__(self, obj: O|T.Self):
        self.__obj__: O = obj
        self.__chain__ = []

    def __collapse__(self):
        obj = self.__obj__
        for op in self.__chain__:
            obj = op(obj)
        return obj

    def __getattr__(self, item) -> O|T.Self:
        self.__chain__.append(lambda obj: getattr(obj, item, None))
        return self

    def __getitem__(self, item) -> O|T.Self:
        self.__chain__.append(ft.partial(try_get_item, item=item))
        return self

    def __call__(self, *args, **kwargs):
        self.__chain__.append(lambda obj: try_call(obj, *args, **kwargs))
        return self


OBJ1 = T.TypeVar('OBJ1')

def op(obj: OBJ1) -> OBJ1:
    return Op(obj) # noqa


OBJ2 = T.TypeVar('OBJ2')

def get(obj: OBJ2) -> OBJ2:
    return obj.__collapse__()









if __name__ == '__main__':

    import dataclasses as dc

    @dc.dataclass
    class Foo:
        a: int = 2
        b: 'Foo' = None


    foo1 = Foo(a=28)
    foo2 = Foo(4, foo1)

    data = {
        'this': {
            'thing': foo2
        }
    }


    print(get(op(data)['this']['thing'].b.a))
