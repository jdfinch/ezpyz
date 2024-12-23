
from __future__ import annotations

import itertools as it

import typing as T


E = T.TypeVar('E')

def peek(iterable: T.Iterable[E]) -> tuple[E | None, T.Iterable[E]]:
    """
    Get (the first element, iterable without the first element consumed).
    """
    iterating = iter(iterable)
    e = next(iterating, None)
    if iter(iterable) is iterating:
        i = it.chain((e,), iterating)
        return e, i
    else:
        return e, iterable


if __name__ == '__main__':

    first, items = peek(range(7))
    print(first, list(items))

    def my_gen(n):
        for i in range(n):
            yield i

    first, items = peek(my_gen(7))
    print(first, list(items))



