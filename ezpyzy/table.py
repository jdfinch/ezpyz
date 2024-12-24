from __future__ import annotations


import dataclasses as dc
import copy as cp
import itertools as it
import re

from ezpyzy.peek import peek
from ezpyzy.alphanumeral import alphanumerals

import typing as T


def iter_anonymous_columns(existing_columns):
    for anonymous_column in alphanumerals():
        if anonymous_column not in existing_columns:
            yield anonymous_column


R = T.TypeVar('R', bound='Row')

class Table(T.Generic[R]):
    __cols__ = None
    __row_type__ = None
    __attrs__ = None
    __origin__ = None

    def __init__(self, *rows: T.Iterable[R], **columns):
        self.__rows__: list[R] = []
        self.__cols__: dict[str, Column[T.Any, R]]
        self.__row_type__: type[R]
        self.__origin__: R
        self += rows
        self -= columns

    def __call__(self) -> 'TableAttrs[T.Self]':
        return self.__attrs__

    def __pos__(self) -> R:
        copy = cp.copy(self)
        copy.__origin__ = copy
        copy.__attrs__ = TableAttrs(copy)
        copy.__cols__ = dict.fromkeys(copy.__cols__)
        copy.__rows__ = cp.deepcopy(copy.__rows__)
        return copy # noqa

    def __neg__(self) -> R:
        copy = cp.copy(self)
        copy.__origin__ = copy
        copy.__attrs__ = TableAttrs(copy)
        copy.__cols__ = dict.fromkeys(copy.__cols__)
        copy.__rows__ = [cp.copy(row) for row in copy.__rows__]
        return copy  # noqa

    def __invert__(self) -> R:
        copy = cp.copy(self)
        copy.__origin__ = copy
        copy.__attrs__ = TableAttrs(copy)
        copy.__cols__ = dict.fromkeys(copy.__cols__)
        return copy  # noqa

    def __iter__(self) -> T.Iterator[R]:
        return self.__rows__.__iter__()

    def __len__(self):
        return len(self.__rows__)

    def __getattr__(self, item):
        if item in self.__cols__:
            column = self.__cols__[item]
            if column is None:
                if '__cols__' not in self.__dict__:
                    self.__cols__ = dict.fromkeys(self.__cols__)
                column = Column(table=self, name=item)
                self.__cols__[item] = column
            return column
        elif item == '__attrs__':
            if self.__attrs__ is None:
                self.__attrs__ = TableAttrs(self)
            return self.__attrs__
        elif item == '__origin__':
            self.__origin__ = self
            return self
        return None

    def __setattr__(self, key, value):
        if isinstance(value, Column):
            if value.__table__ is self and value.__name__ == key:
                return
            assert len(self) == len(value), \
                f"Merged Column must be same length as table {self}. Got {len(value) = } and {len(self) = }"
            if '__cols__' not in self.__dict__:
                self.__cols__ = dict.fromkeys(self.__cols__)
            self.__cols__[key] = None
            for row, val in zip(self, value):
                setattr(row, key, val)
        else:
            super().__setattr__(key, value)

    def __delattr__(self, item):
        if item in self.__cols__:
            if '__cols__' not in self.__dict__:
                self.__cols__ = dict.fromkeys(self.__cols__)
            del self.__cols__[item]
            for row in self:
                delattr(row, item)
        else:
            super().__delattr__(item)

    def __getitem__(self, selection) -> R:
        if isinstance(selection, int):
            return self.__rows__.__getitem__(selection)
        elif isinstance(selection, slice):
            return self.__class__(self.__rows__[selection]) # noqa
        elif selection is ellipsis:
            return self.__class__(self.__rows__) # noqa
        elif isinstance(selection, tuple) and selection:
            first = selection[0]
            if isinstance(first, (Column, str)):
                col_view = cp.copy(self)
                if selection[-1] is Ellipsis:
                    selected_cols = tuple(c.__name__ if isinstance(c, Column) else c for c in selection[:-1])
                    rest_of_cols = tuple(col for col in self.__cols__ if col not in selected_cols)
                    col_view.__cols__ = dict.fromkeys(selected_cols+rest_of_cols)
                else:
                    col_view.__cols__ = dict.fromkeys(c if isinstance(c, str) else c.__name__ for c in selection)
                col_view.__attrs__ = None
                return col_view # noqa
            else:
                col_selection = selection[1:]
                if col_selection and isinstance(col_selection[0], (str, Column)):
                    rows_view = self[first]
                    return rows_view[col_selection]
        elif isinstance(selection, Column):
            col_view = cp.copy(self)
            col_view.__cols__ = dict.fromkeys((selection.__name__,))
            return col_view # noqa
        elif isinstance(selection, (Table, TableAttrs)):
            if isinstance(selection, TableAttrs):
                selection = selection.table
            col_view = cp.copy(self)
            col_view.__cols__ = dict.fromkeys(selection.__cols__)
            return col_view # noqa
        elif callable(selection):
            selection = [selection(row) for row in self.__rows__]
        if not selection:
            return self.__class__()  # noqa
        if isinstance(selection, set):
            return self.__class__(row for row in self.__rows__ if row in selection) # noqa
        else:
            if (iterator:=iter(selection)) is iter(selection):
                first, selection = peek(selection)
            else:
                first = next(iterator)
            if isinstance(first, bool):
                assert len(selection) == len(self.__rows__), \
                    f'Boolean selection must be same length as table. Got {len(selection) = } and {len(self) = }'
                return self.__class__(row for row, i in zip(self.__rows__, selection) if i) # noqa
            if isinstance(first, int):
                return self.__class__(self.__rows__[i] for i in selection) # noqa
            else:
                assert len(selection) == len(self.__rows__), \
                    f'Boolean selection must be same length as table. Got {len(selection) = } and {len(self) = }'
                return self.__class__(row for row, i in zip(self.__rows__, selection) if i)  # noqa

    def __setitem__(self, selection, data):
        if isinstance(selection, int):
            if isinstance(data, self.__row_type__):
                self.__rows__[selection] = data
            else:
                ...
        elif isinstance(selection, slice):
            ...
        elif isinstance(selection, tuple):
            ...
        elif isinstance(selection, str):
            ...
        elif isinstance(selection, set):
            ...
        else:
            if not isinstance(selection, list):
                selection = list(selection)
            first = selection[0]
            if isinstance(first, int):
                ...
            elif isinstance(first, bool):
                ...


    def __delitem__(self, selection):
        ...

    def __isub__(self, other) -> R:
        """Add new columns to this table using column data `other`"""
        if isinstance(other, Column):
            assert (col:=other.__name__) not in self.__cols__, f"Error Message"
            self.__cols__[col] = None
            if len(self) == 0: self += len(other)
            else: assert len(other) == len(self), f"Error Message"
            for row, value in zip(self, other): setattr(row, col, value)
        elif isinstance(other, Table):
            other_cols = other.__cols__
            assert all(col not in self.__cols__ for col in other_cols), f"Error Message"
            if len(self) == 0: self += len(other)
            else: assert len(other) == len(self), f"Error Message"
            self.__cols__.update(dict.fromkeys(other_cols))
            for self_row, other_row in zip(self, other):
                for col in other_cols: setattr(self_row, col, getattr(other_row, col))
        elif isinstance(other, dict):
            assert all(isinstance(col, str) for col in other), f"Error Message"
            assert all(col not in self.__cols__ for col in other), f"Error Message"
            assert len(col_lens:={len(column) for column in other}) <= 1, f"Error Message"
            other_len, = col_lens
            if len(self) == 0: self += other_len
            else: assert len(self) == other_len, f"Error Message"
            for col, column in other:
                for row, value in zip(self, column): setattr(row, col, value)
        elif isinstance(other, str):
            assert other not in self.__cols__, f"Error Message"
            self.__cols__[other] = None
        else:
            if not isinstance(other, list):
                other = list(other)
            if not other: return self # noqa
            first = other[0]
            if isinstance(first, (list, tuple)):
                assert len(row_lens:={len(other_row) for other_row in other}) == 1, f"Error Message"
                assert all(isinstance(other_row, (list, tuple)) for other_row in other), f"Error Message"
                if len(self) == 0: self += len(other)
                else: assert len(other) == len(self), f"Error Message"
                num_cols, = row_lens
                cols = it.islice(iter_anonymous_columns(self.__cols__), num_cols)
                for col in cols: self.__cols__[col] = None
                for self_row, other_row in zip(self, other):
                    for col, value in zip(cols, other_row): setattr(self_row, col, value)
            elif isinstance(first, dict):
                assert len(col_layouts:={frozenset(row) for row in other}) == 1, f"Error Message"
                assert all(isinstance(other_row, dict) for other_row in other), f"Error Message"
                cols, = col_layouts
                assert all(col not in self.__cols__ for col in cols), f"Error Message"
                for col in cols: self.__cols__[col] = None
                if len(self) == 0: self += len(other)
                else: assert len(other) == len(self), f"Error Message"
                for self_row, other_row in zip(self, other):
                    for col, value in other_row.items(): setattr(self_row, col, value)
            elif isinstance(first, Row):
                assert len(col_layouts:={row.__cols__ for row in other}) == 1, f"Error Message"
                cols, = col_layouts
                assert all(col not in self.__cols__ for col in cols), f"Error Message"
                if len(self) == 0: self += len(other)
                else: assert len(other) == len(self), f"Error Message"
                for col in cols: self.__cols__[col] = None
                for self_row, other_row in zip(self, other):
                    for col in cols: setattr(self_row, col, getattr(other_row, col))
            elif isinstance(first, Column):
                assert all(isinstance(other_col, Column) for other_col in other), f"Error Message"
                assert all(column.__name__ not in self.__cols__ for column in other), f"Error Message"
                assert len(col_lens:={len(column) for column in other}) == 1, f"Error Message"
                other_len, = col_lens
                if len(self) == 0: self += other_len
                else: assert len(self) == other_len, f"Error Message"
                for column in other:
                    for row, value in zip(self, column): setattr(row, column.__name__, value)
            elif isinstance(first, str):
                assert all(isinstance(other_col, str) for other_col in other), f"Error Message"
                assert all(col not in self.__cols__ for col in other), f"Error Message"
                for col in other: self.__cols__[col] = None
            elif first is None and list(other) == []:
                pass
            else:
                raise ValueError(f"Error Message")
        return self # noqa

    def __iadd__(self, other) -> R:
        """Add new rows to this table using row data `other`"""
        if isinstance(other, self.__row_type__):
            self.__rows__.append(other)
        elif isinstance(other, Row):
            self.__rows__.append(self.__row_type__())
            self[-1] = other
        elif isinstance(other, Table):
            if issubclass(other.__row_type__, self.__row_type__):
                self.__rows__.extend(other)
            else:
                original_len = len(self)
                self.__iadd__(len(other))
                self[original_len:] = other
        elif isinstance(other, dict):
            assert set(other) == set(self.__cols__), f"Error Message"
            assert len(col_lens:={len(column) for column in other.values()}) == 1, f"Error Message"
            original_len = len(self)
            extension_len, = col_lens
            self.__iadd__(extension_len)
            self[original_len:] = other
        elif isinstance(other, Column):
            self.__iadd__([other])
        else:
            if not isinstance(other, list):
                other = list(other)
            if not other: return self # noqa
            first = other[0]
            if isinstance(first, Row):
                assert all(isinstance(other_row, Row) for other_row in other), f"Error Message"
                update_indices = []
                update_rows = []
                for i, other_row in enumerate(other):
                    if isinstance(other_row, self.__row_type__):
                        self.__rows__.append(other_row)
                    else:
                        self.__rows__.append(self.__row_type__())
                        update_indices.append(i)
                        update_rows.append(other_row)
                if update_indices:
                    self[update_indices] = update_rows
            elif isinstance(first, Column):
                assert all(isinstance(other_col, Column) for other_col in other), f"Error Message"
                assert {other_col.__name__ for other_col in other} == set(self.__cols__), f"Error Message"
                assert len(col_lens:={len(other_col) for other_col in other}) == 1, f"Error Message"
                extension_len, = col_lens
                original_len = len(self)
                self.__iadd__(extension_len)
                self[original_len:] = other
            else:
                original_len = len(self)
                self.__iadd__(len(other))
                self[original_len:] = other
        return self # noqa



TableAttrsType  = T.TypeVar('TableAttrsType')

class TableAttrs(T.Generic[TableAttrsType]):
    def __init__(self, table: TableAttrsType):
        self.table: TableAttrsType = table

    def __iter__(self):
        return iter(self.table.__cols__.values())

    def __len__(self):
        return len(self.table.__cols__)

    def __getitem__(self, item):
        return self.table.__cols__[item]



E = T.TypeVar('E')
TR = T.TypeVar('TR')

class Column(T.Generic[E, TR]):
    __attrs__ = None
    def __init__(self, items: T.Iterable[E], name: str=None, table: Table[TR] = None):
        if table is None:
            self.__table__ = Table(**{name: items})
            self.__table__.__cols__[name] = self
        else:
            assert not items
            self.__table__: Table[TR] = table
            self.__name__ = name

    def __call__(self) -> 'ColumnAttrs[T.Self]':
        if self.__attrs__ is None:
            self.__attrs__ = ColumnAttrs(self)
        return self.__attrs__

    def __pos__(self) -> T.Self:
        table = -self.__table__
        for row in table:
            setattr(row, self.__name__, cp.deepcopy(getattr(row, self.__name__)))
        return getattr(table, self.__name__)

    def __invert__(self) -> T.Self:
        table = ~self.__table__
        return getattr(table, self.__name__)

    def __iter__(self) -> T.Iterator[E]:
        return (getattr(row, self.__name__) for row in self.__table__)

    def __len__(self):
        return len(self.__table__)

    def __getitem__(self, item) -> E|Column[E, TR]:
        return getattr(self.__table__[item], self.__name__)



ColumnAttrsType = T.TypeVar('ColumnAttrsType', bound=Column)

class ColumnAttrs(T.Generic[ColumnAttrsType]):
    def __init__(self, col: ColumnAttrsType):
        self.col: ColumnAttrsType = col

    @property
    def table(self):
        return self.col.__table__

    @property
    def name(self):
        return self.col.__name__



CellType = T.TypeVar('CellType')
RowType = T.TypeVar('RowType')
Col = T.Union[Column[CellType, RowType], CellType, None]

col_discovery_pattern = re.compile(r"\bCol\b")

class RowMeta(type):
    __cols__:tuple[str] = ()
    def __new__(mcs, name, bases, attrs):
        """Discovers Columns based on the word-pattern "Col" appearing in class field annotations."""
        bases = tuple(base for base in bases if base is not Table)
        attrs['__cols__'] = tuple(name for name, annot in attrs.get('__annotations__', {}).items()
            if col_discovery_pattern.search(annot))
        cls = super().__new__(mcs, name, bases, attrs)
        return cls

@dc.dataclass
class Row(Table[T.Self], metaclass=RowMeta):
    @classmethod
    def s(cls, *rows, **cols) -> T.Self:
        table = Table(*rows, **cols)
        table.__row_type__ = cls
        return table

    def __getattr__(self, item):
        setattr(self.__class__, item, None)
        return None

    def __hash__(self):
        return hash(id(self))

    def __eq__(self, other):
        return vars(self) == vars(other)

    def __pos__(self):
        return cp.deepcopy(self)

Table.__row_type__ = Row

"""
Add Columns:
table -= columns # set column data to existing rows in order as long as column does not already exist in table

Add Rows:
table += rows # add rows to table by reference if they are table.__row_cls__ 
    (else create new Row objects by table.__row_cls__)

Set data:
table[row_select] = rows # will add rows by reference as long as rows are Row objects (else create with __row_cls__)
table[row_select,:] = rows # will add row data to existing rows (only columns in table)
table |= rows # will add ALL row data to existing rows by mutation

Remove Columns:
del table[columns] # columns as a tuple of Column or str

Remove Rows:
del table[rows]


"""


if __name__ == '__main__':

    import dataclasses as dc

    from ezpyzy.timer import Timer


    @dc.dataclass
    class Duck(Row):
        name: Col[str, Duck] = None
        age: Col[int, Duck] = None
        children: Col[list[str], Duck] = None

        def quack(self) -> Col[str]:
            return f'{self.name} quack!'



    ducks: T.Any = [None] * 1_000_000
    other = []
    with Timer('One million rows'):
        for i in range(1_000_000):
            d = Duck('donald', 42, ['don', 'quack'])
            ducks[i] = d
            other.append(i)


    with Timer('Validate rows'):
        assert all(isinstance(duck, Duck) for duck in ducks)




    def main():

        """
        What should happen?
            - reference or copies created?
            - any error?
            - what data is moved?
        """

        @dc.dataclass
        class Duck(Row):
            name: Col[str, Duck] = None
            age: Col[int, Duck] = None
            children: Col[list[str], Duck] = None

            def quack(self) -> Col[str]:
                return f'{self.name} quack!'

        ducks = Duck.s()

        the_duck = Duck('Donald', 5, ['Huey', 'Dewey', 'Louie'])
        ducks += the_duck
        ''' add the_duck by reference '''

        ducks += [Duck(name=name) for name in ['Huey', 'Dewey', 'Louie']]
        ''' add each duck by reference '''

        ducks += [dict(name=name) for name in ['Huey', 'Dewey', 'Louie']]
        ''' create new ducks and put the new values in those rows '''

        ducks += [dict(wings=n) for n in (2, 2, 1)]
        ''' error because wings is not a column in Duck.s '''

        ducks += [dict() for _ in range(3)]
        ''' create new empty ducks '''

        @dc.dataclass
        class Elephant(Row):
            pounds: int = 'fat'

        ducks += Elephant(8392018)
        ''' error Elephant is the wrong row type '''

        ducks += Row(name='Rick') # noqa
        ''' a Duck should be created and the data moved to that duck '''

        # ducks is an existing table with 3 rows
        ducks[1] = Duck(name='Brian')
        ''' replace with reference '''

        # ducks is an existing table with 3 rows
        table2 = Duck.s([
            ['Brain', 3, []],
            ['Will', 2, []]
        ])
        ducks[1:3,'age':] = table2
        '''  '''

        # ducks is an existing table with 3 rows
        ducks[:] = [Duck(name=name) for name in ['Huey', 'Dewey', 'Louie']]
        ''' '''

        # ducks is an existing table with 4 rows
        ducks[:] = [Duck(name=name) for name in ['Huey', 'Dewey', 'Louie']]
        ''' replace all 4 rows with the 3 ducks '''

        # ducks is an existing table with 2 rows
        ducks[:] = [Duck(name=name) for name in ['Huey', 'Dewey', 'Louie']]
        ''' ducks has the new rows by reference and is now 3 rows '''

        # ducks is an existing table with 2 rows
        ducks[2:2] = [Duck(name=name) for name in ['Huey', 'Dewey', 'Louie']]
        ''' allow slicing rows to insert rows of any number '''

        # ducks is an existing table with 3 rows
        ducks[:,'name'] = [Duck(name=name) for name in ['Huey', 'Dewey', 'Louie']]
        ''' name cells replaced '''

        # ducks is an existing table with 4 rows
        ducks[:,'name'] = [Duck(name=name) for name in ['Huey', 'Dewey', 'Louie']]
        ''' error, number of rows doesn't match number in slice with cell replacement '''

        # ducks is an existing table with 3 rows
        ducks[:,:] = [Duck(name=name) for name in ['Huey', 'Dewey', 'Louie']]
        ''' '''

        # ducks is an existing table with 3 rows
        ducks[:,ducks.name,ducks.age] = [Duck(name=name, age=3) for name in ['Huey', 'Dewey', 'Louie']]
        ''' '''
