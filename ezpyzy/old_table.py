"""
Alternative to pandas and polars that doesn't make you want to kill yourself.

More for machine learning dev than data exploration. If data exploration is your goal the existing alternatives provide a swiss army knife of built-in methods; table.py focuses on api consistency and concision of the most-used/fundamental data processing features instead.
"""

from __future__ import annotations

import dataclasses
import json

import ezpyzy as ez
import dataclasses as dc
import pathlib as pl
import io
import csv
import textwrap as tw
import inspect as ins
import sys
import weakref as wr
import itertools as it
import typing as T


default = object()


def column_type_map(tabletype):
    if not isinstance(tabletype, type):
        tabletype = type(tabletype)
        for x in tabletype.__mro__:
            if Table in x.__mro__:
                tabletype = x
                break
        else:
            raise TypeError(f'Object {tabletype} is not a Table subclass or instance')
    column_types = {} # name: (type, element type)
    for field in dc.fields(tabletype):
        backup_element_type = str
        coltype = field.type
        origin = T.get_origin(coltype)
        typeargs = T.get_args(coltype)
        while origin and Column not in getattr(origin, '__mro__', ()):
            if hasattr(typeargs, '__iter__'):
                for candidate in typeargs:
                    if candidate in (str, int, float, bool):
                        backup_element_type = candidate
                        break
                coltype = typeargs[0]
                origin = T.get_origin(coltype)
                typeargs = T.get_args(coltype)
        if Column in getattr(origin, '__mro__', ()):
            column_types[field.name] = (
                origin, typeargs[0] if typeargs else backup_element_type
            )
    return column_types

def column_base_type_map(column):
    if isinstance(column, (DictColumn, DictColumnView)):
        return DictColumn
    else:
        return ListColumn


T1 = T.TypeVar('T1', bound='Table')

@dc.dataclass
class Table:

    def __post_init__(self):
        self._meta:Meta = Meta(self)
        self._name: str = self.__class__.__name__
        self._id: str | None = None
        self._view_index: list[int]|None = None
        self._origin:Table|None = None
        self._right_joined: Table|None = None
        self._columns: dict[str|tuple[[str]], Column] = {}
        self._path:pl.Path|None = None
        column_types = {k: v[0] for k, v in column_type_map(self).items()}
        assert len({len(column) for column in vars(self).values() if isinstance(column, Column)}) <= 1, \
            f'Columns must have the same number of rows, but got columns ' \
            f'{", ".join(column.name for column in vars(self).values() if isinstance(column, Column))} ' \
            f'with lengths {[len(column) for column in vars(self).values() if isinstance(column, Column)]}'
        length = max([1] + [len(c) for c in vars(self).values() if isinstance(c, Column)])
        for name, column_type in column_types.items():
            if hasattr(self, name):
                attr = getattr(self, name)
                if isinstance(attr, Column):
                    setattr(self, name, column_type(items=attr, name=name))
                else:
                    setattr(self, name, column_type(items=[attr]*length, name=name))
            else:
                setattr(self, name, column_type(name=name))

    def __str__(self):
        return self().display()

    def __repr__(self):
        return f"<{self._name} {'Table' if self._name != 'Table' else ''}: {', '.join(c.name for c in self())}>"

    @classmethod
    def of(
        cls:type[T1],
        *datas: T.Union[
            T.Collection[T.Union[T.Collection, T.Mapping, 'Table']],
            T.Mapping[str, T.Collection],
            'Table',
            ez.filelike
        ],
        fill: T.Any = default,
    ) -> T1:
        tables = []
        for data in datas:
            table = cls()
            column_types = column_type_map(table)
            if isinstance(data, (str, pl.Path, io.IOBase, ez.File)):
                if isinstance(data, str) and not pl.Path(data).exists():
                    try:
                        csv_data = ez.CSV.deserialize(data)
                    except Exception:
                        csv_data = ez.File(data).load()
                else:
                    csv_data = ez.File(data).load()
                data = {col[0]: col[1:] for col in zip(*csv_data)}
                for col_name, col_vals in list(data.items()):
                    col_elements_type = column_types.get(col_name, (None, str))[1]
                    data[col_name] = [json.loads(val) for val in col_vals]
            if isinstance(data, Table):
                for var, val in list(vars(table).items()):
                    if isinstance(val, Column):
                        table._del_column(val)
                for column in data(): # noqa
                    col = column_base_type_map(column)(items=column, name=column.name)
                    for alias in data().aliases(column): # noqa
                        table._set_attr(alias, col)
            elif isinstance(data, dict):
                for var, val in list(vars(table).items()):
                    if isinstance(val, Column):
                        table._del_column(val)
                for name, column_data in data.items():
                    col_type = column_types.get(name, (Column, None))[0]
                    setattr(table, name, col_type(items=column_data, name=name))
            elif isinstance(data, list):
                if not data:
                    del table[0]
                    return table
                first_row = next(iter(data))
                if isinstance(first_row, Table):
                    data = [row().dict() for row in data]
                    first_row = next(iter(data))
                if isinstance(first_row, (list, tuple)):
                    table_cols = list(table()) + [Column() for _ in range(len(first_row) - len(table()))]
                    for name in table().column_names:
                        delattr(table, name)
                    table._columns.clear()
                    for empty_column, *column_data in zip(table_cols, *data):
                        column_type = column_types.get(empty_column.name, (ListColumn, None))[0]
                        column = column_type(items=column_data, name=empty_column.name)
                        table._set_attr(column.name, column)
                elif isinstance(first_row, dict):
                    columns = {}
                    for i, row in enumerate(data):
                        for column in columns.values():
                            column.append(None)
                        for name, value in row.items():
                            try:
                                columns[name][-1] = value
                            except KeyError:
                                columns[name] = [None] * i
                                columns[name].append(value)
                    for column in table():
                        if column.name not in columns:
                            columns[column.name] = [None] * len(data)
                    for name in table().column_names:
                        delattr(table, name)
                    table._columns.clear()
                    for name, column_data in columns.items():
                        col_type = column_types.get(name, (Column, None))[0]
                        column = col_type(items=column_data, name=name)
                        setattr(table, name, column)
                else:
                    raise TypeError(f'Invalid data type {type(first_row)}')
            else:
                raise TypeError(f'Invalid data type {type(data)}: {data}')
            tables.append(table)
        table = tables[0]
        for other in tables[1:]:
            table += other
        if fill is not default:
            table().fill(fill)
        return table

    def _set_attr(self, key, value):
        if isinstance(value, Column) and '_columns' in vars(self):
            if key is None:
                existing_col_names = {col.name for col in self()}
                for name in it.cycle(ez.digital_iteration(chars="ABCDEFGHIJKLMNOPQRSTUVWXYZ")):
                    if name not in existing_col_names:
                        key = name
                        break
            assert not self() or len(value) == len(self), \
                f'Column {key} has {len(value)} rows, but table has {len(self())} rows'
            name, column = key, value
            if column.name is None:
                column.name = name if isinstance(name, str) else name[0]
            self._columns[name] = column
            if self._id is None and isinstance(column, DictColumn) and isinstance(column.name, str):
                self._id = column.name
            if column._origin is None:
                column._origin = self
        elif '_columns' in vars(self) and key in self._columns: # overwriting column with non-column
            del self._columns[key]
        if isinstance(key, str):
            object.__setattr__(self, key, value)

    def _del_column(self, column):
        aliases = self().aliases(column)
        for alias in aliases:
            if self._columns.get(alias) is column:
                del self._columns[alias]
                if alias == self._id:
                    self._id = None
            if self._columns.get((alias,)) is column:
                del self._columns[(alias,)]
            if vars(self).get(alias) is column:
                object.__delattr__(self, alias)

    def __getattr__(self, item):
        return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        if isinstance(value, Column) and value._origin is not self and value._origin is not None:
            raise ValueError(f'Cannot set column {key} to {value} because it belongs to Table {value._origin}')
        self._set_attr(key, value)

    def __delattr__(self, item):
        if item in self._columns:
            self._del_column(self._columns[item])
        else:
            object.__delattr__(self, item)

    def __call__(self:T1) -> 'Meta[T1]':
        return self._meta

    def __invert__(self:T1) -> T1:
        detached = type(self).of(self)
        vars(detached).update({
            var: val for var, val in vars(self).items()
            if not isinstance(val, Column)
            and var not in {
                '_origin', '_views', '_columns', '_meta', '_view_index', '_right_joined'
            }
        })
        return detached

    def __len__(self):
        columns = list(self())
        if columns:
            return len(columns[0])
        return 0

    def __iter__(self:T1) -> T.Iterator[T1]:
        yield from (self[i] for i in range(len(self)))

    def __contains__(self, item):
        if isinstance(item, Table):
            return item._origin is self
        elif isinstance(item, Column):
            return item in self()
        elif self._id is not None:
            return item in getattr(self, self._id)
        else:
            raise TypeError(f'Invalid item type {type(item)} in Table membership check: {item}')

    def __getitem__(self:T1, selects) -> T1:
        view = type(self).__new__(type(self))
        vars(view).update([(k, v) for k, v in vars(self).items() if not isinstance(v, Column)])
        if view._origin is None:
            view._origin = self
        view._columns = {}
        view._meta = Meta(view)
        rows = None
        cols = None
        if isinstance(selects, int):
            rows = [selects]
        elif isinstance(selects, str):
            rows = [dict.__getitem__(self().id._ids, selects)]
        elif isinstance(selects, tuple):
            if not selects:
                cols = []
            elif isinstance(selects[0], str):
                cols = [self().column_names[name] for name in selects]
            elif isinstance(selects[0], Column):
                cols = selects
            else:
                raise TypeError(f'Invalid column type {type(selects[0])} of {selects[0]} in selection {selects}')
        elif isinstance(selects, Column) and selects.name is not None:
            cols = [selects]
        elif isinstance(selects, slice):
            rows = list(range(*selects.indices(len(self))))
        elif isinstance(selects, DictColumn):
            rows = [dict.__getitem__(self().id._ids, key) for key in selects]
        elif isinstance(selects, list):
            if not selects:
               rows = []
            else:
                first = selects[0]
                if isinstance(first, bool):
                    assert len(selects) == len(self), \
                        f'Boolean index must have same length as table, but got {len(selects)} != {len(self)}'
                    rows = [i for i, b in enumerate(selects) if b]
                elif isinstance(first, str):
                    rows = [dict.__getitem__(self().id._ids, key) for key in selects]
                elif isinstance(first, int):
                    rows = selects
                elif isinstance(first, Column):
                    cols = selects
                elif isinstance(first, list):
                    raise NotImplementedError("Gather is not implemented")
                else:
                    raise TypeError(f'Invalid index type {type(first)} of {first} in selection {selects}')
        else:
            raise TypeError(f'Invalid index type {type(selects)} of {selects}')
        if rows is not None:
            view._origin = view
            for column in self():
                column_view = ColumnView(column, rows)
                setattr(view, column.name, column_view)
                view._view_index = list(rows)
                for alias in self().aliases(column):
                    setattr(view, alias, column_view)
        elif cols is not None:
            own_columns = {id(column) for column in self()}
            assert all(id(col) in own_columns for col in cols), \
                f'Attempted selecting columns not belonging to Table {self}: {[c.name for c in cols]}'
            for column in cols:
                view._set_attr(column.name, column)
                for alias in self().aliases(column):
                    view._set_attr(alias, column)
        return view

    def __setitem__(self, selects, values):
        if id(self) == id(values) or self._origin is values:
            return
        if isinstance(values, Table):
            values = list(zip(*values()))
            if isinstance(selects, int):
                values = values[0]
        if isinstance(selects, str):
            selects = dict.__getitem__(self().id._ids, selects) # noqa
        elif isinstance(selects, list) and selects and isinstance(selects[0], str):
            selects = [dict.__getitem__(self().id._ids, key) for key in selects] # noqa
        if isinstance(selects, int):
            assert len(self()) == len(values), \
                f'Number of batch mutation values ({len(values)}) != number of columns ({len(self())}) in Table {self}'
            for column, value in zip(self(), values):
                column[selects] = value
        elif isinstance(selects, slice):
            valcols = list(zip(*values))
            start, stop, step = selects.indices(len(self))
            num_sliced = (stop - start) // step
            assert len(self()) == len(valcols), \
                f'Number of mutation columns ({len(valcols)}) != number of columns ({len(self())}) in Table {self}:\n {values}'
            if not valcols:
                return
            assert len(valcols[0]) == num_sliced, \
                f'Number of mutation values ({len(valcols[0])}) != number of rows sliced ({num_sliced}):\n {values}'
            for column, valcol in zip(self(), valcols):
                column[selects] = valcol
        elif isinstance(selects, DictColumn):
            raise NotImplementedError("ID selection is not implemented")
        elif isinstance(selects, list):
            if not selects:
                num_value_rows = len(list(values))
                assert num_value_rows == 0, \
                    f'Number of mutation values ({num_value_rows}) != number of rows selected ({len(selects)}):\n {values}'
                return
            valcols = list(zip(*values))
            first_select = selects[0]
            if isinstance(first_select, bool):
                selects = [i for i, b in enumerate(selects) if b]
            elif isinstance(first_select, str):
                id_column = self().id
                assert id_column is not None, \
                    f'Cannot select rows by ID because Table {self} has no ID column'
                selects = [dict.__getitem__(self().id._ids, key) for key in selects]
            elif isinstance(first_select, list):
                raise NotImplementedError("Scatter is not implemented")
            assert len(selects) == len(values), \
                f'Number of mutation values ({len(values)}) != number of rows selected ({len(selects)}):\n {values}'
            if isinstance(first_select, int):
                assert len(self()) == len(valcols), \
                    f'Number of mutation values ({len(valcols)}) != number of columns ({len(self())}) in Table {self}:\n {values}'
                for column, value in zip(self(), valcols):
                    column[selects] = value
            else:
                raise TypeError(f'Invalid selection type {type(first_select)}')
        else:
            raise TypeError(f'Invalid selection type {type(selects)} of {selects}')

    def __delitem__(self, selects):
        rows = None
        cols = None
        if isinstance(selects, int):
            rows = [selects]
        elif isinstance(selects, str):
            rows = [dict.__getitem__(self().id._ids, selects)]
        elif isinstance(selects, tuple):
            if not selects:
                cols = []
            elif isinstance(selects[0], str):
                cols = [self().column_names[name] for name in selects]
            elif isinstance(selects[0], Column):
                cols = selects
            else:
                raise TypeError(f'Invalid column type {type(selects[0])} of {selects[0]} in selection {selects}')
        elif isinstance(selects, slice):
            rows = list(range(*selects.indices(len(self))))
        elif isinstance(selects, DictColumn):
            rows = [dict.__getitem__(self().id._ids, key) for key in selects]
        elif isinstance(selects, list):
            if not selects:
                rows = []
            else:
                first = selects[0]
                if isinstance(first, bool):
                    assert len(selects) == len(self), \
                        f'Boolean index must have same length as table, but got {len(selects)} != {len(self)}'
                    rows = [i for i, b in enumerate(selects) if b]
                elif isinstance(first, str):
                    rows = [dict.__getitem__(self().id._ids, key) for key in selects]
                elif isinstance(first, int):
                    rows = selects
                elif isinstance(first, Column):
                    cols = selects
                elif isinstance(first, list):
                    raise NotImplementedError("Gather is not implemented")
                else:
                    raise TypeError(f'Invalid index type {type(first)} of {first} in selection {selects}')
        else:
            raise TypeError(f'Invalid index type {type(selects)} of {selects}')
        if rows is not None:
            for column in self():
                del column[rows]
        elif cols is not None:
            own_columns = {id(column) for column in self()}
            assert all(id(col) in own_columns for col in cols), \
                f'Attempted selecting columns not belonging to Table {self}: {cols}'
            for column in cols:
                self._del_column(column)

    def __add__(self: T1, other:T.Union[
            T.Collection[T.Union[T.Collection, T.Mapping, 'Table']],
            T.Mapping[str, T.Collection],
            'Table',
            ez.filelike
        ]) -> T1: # row concatenation
        return self.of(self, other)

    def __iadd__(self: T1, other:T.Union[
            T.Collection[T.Union[T.Collection, T.Mapping, 'Table']],
            T.Mapping[str, T.Collection],
            'Table',
            ez.filelike
        ]) -> T1: # row concatenation
        assert all(not isinstance(column, ColumnView) for column in self()), \
            f'Row concatenation is not supported for views. Columns: {list(self())}'
        if not isinstance(other, Table):
            other = self.of(other)
        assert len(self()) == len(other()), \
            f'Row concatenation requires equal number of columns, but got column lengths {len(self)} != {len(other)}'
        for column, values in zip(self(), other()):
            column._extend(values)
        return self

    def __sub__(self: T1, other:T.Union[
            T.Collection[T.Union[T.Collection, T.Mapping, 'Table']],
            T.Mapping[str, T.Collection],
            'Table',
            'Column',
            ez.filelike
        ]) -> T1: # column concatenation
        if isinstance(other, Column):
            other = other.table()
        copy = ~self
        copy -= other
        return copy

    def __isub__(self: T1, other:T.Union[
            T.Collection[T.Union[T.Collection, T.Mapping, 'Table']],
            T.Mapping[str, T.Collection],
            'Table',
            'Column',
            ez.filelike
        ]) -> T1: # column concatenation
        if isinstance(other, Column):
            other = other.table()
        elif not isinstance(other, Table):
            other = Table.of(other)
        assert len(self) == len(other), \
            f'Column concatenation requires equal number of rows, but got row lengths {len(self)} != {len(other)}'
        for name, column in other._columns.items():
            if name not in self._columns:
                self._set_attr(name, Column(column, name=name))
        return self

    def __and__(self: T1, other) -> T1: # inner join
        if isinstance(other, Column):
            other = other.table()
        assert len(self()) == len(other()), \
            f"Left join received join key of different lengths: len({list(self())}) != len({list(other())})"
        ltable = self._origin or self
        rtable = other._origin or other
        cut_right_cols = set(ltable().column_names) | set(other().column_names)
        lkeys = self().items()
        rkeys = other().items()
        ldata = ltable().items()
        rcols = [col for col in rtable() if col.name not in cut_right_cols]
        rdata = zip(*rcols)
        rmap = {}
        for rkey, rrow in zip(rkeys, rdata):
            if rkey not in rmap:
                rmap[rkey] = []
            rmap[rkey].append(rrow)
        rempty = []
        result_col_names = [col.name for col in ltable()] + [col.name for col in rcols]
        result_rows = [
            lrow + rrow for lkey, lrow in zip(lkeys, ldata) for rrow in rmap.get(lkey, rempty)
        ]
        result_cols = zip(*result_rows)
        result = type(ltable).of({})
        for col_name, col in zip(result_col_names, result_cols):
            result._set_attr(col_name, Column(col))
        return result

    def __lshift__(self, other): # left join
        if isinstance(other, Column):
            other = other.table()
        assert len(self()) == len(other()), \
            f"Left join received join key of different lengths: len({list(self())}) != len({list(other())})"
        ltable = self._origin or self
        rtable = other._origin or other
        cut_right_cols = set(ltable().column_names) | set(other().column_names)
        lkeys = self().items()
        rkeys = other().items()
        ldata = ltable().items()
        rcols = [col for col in rtable() if col.name not in cut_right_cols]
        rdata = zip(*rcols)
        rmap = {}
        for rkey, rrow in zip(rkeys, rdata):
            if rkey not in rmap:
                rmap[rkey] = []
            rmap[rkey].append(rrow)
        rempty = [(None,) * len(rcols)]
        result_col_names = [col.name for col in ltable()] + [col.name for col in rcols]
        result_rows = [
            lrow + rrow for lkey, lrow in zip(lkeys, ldata) for rrow in rmap.get(lkey, rempty)
        ]
        result_cols = zip(*result_rows)
        result = type(ltable).of({})
        for col_name, col in zip(result_col_names, result_cols):
            result._set_attr(col_name, Column(col))
        return result

    def __or__(self, other): # full join
        if isinstance(other, Column):
            other = other.table()
        assert len(self()) == len(other()), \
            f"Left join received join key of different lengths: len({list(self())}) != len({list(other())})"
        ltable = self._origin or self
        rtable = other._origin or other
        cut_right_cols = set(ltable().column_names) | set(other().column_names)
        lkeys = list(self().items())
        rkeys = other().items()
        ldata = ltable().items()
        rcols = [col for col in rtable() if col.name not in cut_right_cols]
        rdata = zip(*rcols)
        rmap = {}
        for rkey, rrow in zip(rkeys, rdata):
            if rkey not in rmap:
                rmap[rkey] = []
            rmap[rkey].append(rrow)
        lempty = [None] * len(ltable())
        rempty = [(None,) * len(rcols)]
        result_col_names = [col.name for col in ltable()] + [col.name for col in rcols]
        lkeyset = set(lkeys)
        result_rows = [
            lrow + rrow for lkey, lrow in zip(lkeys, ldata) for rrow in rmap.get(lkey, rempty)
        ] + [
            lempty + rrow for rrow in [rrow for rkey, rrow in rmap.items() if rkey not in lkeyset]
        ]
        result_cols = zip(*result_rows)
        result = type(ltable).of({})
        for col_name, col in zip(result_col_names, result_cols):
            result._set_attr(col_name, Column(col))
        return result

    def __matmul__(self, other):  # cartesian product
        if isinstance(other, Column):
            other = other.table()
        ltable = self
        rtable = other
        cut_right_cols = set(ltable().column_names)
        ldata = ltable().items()
        rcols = [col for col in rtable() if col.name not in cut_right_cols]
        rdata = list(zip(*rcols))
        result_col_names = [col.name for col in ltable()] + [col.name for col in rcols]
        result_rows = [
            lrow + rrow for lrow in ldata for rrow in rdata
        ]
        result_cols = zip(*result_rows)
        result = type(ltable).of({})
        for col_name, col in zip(result_col_names, result_cols):
            result._set_attr(col_name, Column(col))
        return result

    def __rshift__(self, other): # right join
        if isinstance(other, Column):
            other = other.table()
        return other << self

    def __iand__(self: T1, other) -> T1: # inner join
        raise NotImplementedError('In-place Inner Join not implemented yet')

    def __ior__(self: T1, other) -> T1: # outer join
        raise NotImplementedError('In-place Outer Join not implemented yet')

    def __ilshift__(self, other): # left join
        raise NotImplementedError('In-place Left Join not implemented yet')

    def __irshift__(self, other): # right join
        raise NotImplementedError('In-place Right Join not implemented yet')



T2 = T.TypeVar('T2', bound=Table)
T4 = T.TypeVar('T4', bound='Table')

class Meta(T.Generic[T2]):
    def __init__(self, table:T2):
        self.table:T2 = table
    @property
    def path(self):
        return self.table._path
    @path.setter
    def path(self, path:ez.filelike):
        self.table._path = ez.File(path).path
    def save(self, path:ez.filelike=None, json_cells=True):
        col_types = {k: v[1] for k, v in column_type_map(type(self.table)).items()}
        columns = [[col.name] + [
            json.dumps(val) if json_cells else val for val in col
        ] for col in self.columns]
        rows = zip(*columns)
        if path is None:
            return ez.CSV.serialize(rows)
        else:
            file = ez.File(path or self.table._path, format='csv')
            return file.save(rows)
    @property
    def origin(self):
        return self.table._origin
    @origin.setter
    def origin(self, origin):
        self.table._origin = origin
    @property
    def is_view(self):
        return self.table._origin is not None
    @property
    def name(self):
        return self.table._name
    @name.setter
    def name(self, name):
        self.table._name = name
    @property
    def columns(self): # source of truth for all columns and their order
        ids = dict()
        for column in self.table._columns.values():
            ids[id(column)] = column
        return list(ids.values())
    @property
    def column_names(self):
        return {column.name: column for column in self.columns}
    @property
    def size(self):
        return len(self.table), len(self)
    @property
    def id(self) -> T.Optional['DictColumn']:
        return self.table._columns.get(self.table._id)
    @id.setter
    def id(self, id):
        self.table._id = id
    @property
    def index(self):
        if self.table._view_index is None:
            return list(range(len(self.table)))
        return self.table._view_index
    @property
    def L(self):
        return self.table
    @property
    def R(self):
        return self.table._right_joined
    def aliases(self, column:'Column'):
        return [
            name[0] if isinstance(name, tuple) else name
            for name, col in self.table._columns.items()
            if col is column
        ]
    def item(self):
        return [col[0] for col in self.columns]
    def items(self):
        return zip(*self.columns)
    def dict(self):
        return {name: col[0] for name, col in self.column_names.items()}
    def dicts(self):
        names, columns = zip(*self.column_names.items())
        return [dict(zip(names, row)) for row in zip(*columns)]
    def __iter__(self):
        return iter(self.columns)
    def __getitem__(self, key):
        if isinstance(key, int):
            return self.columns[key]
        elif isinstance(key, str):
            return self.column_names[key]
        else:
            raise TypeError(f'Column key must be int (index) or str (column name), not {type(key)}')
    def __len__(self):
        return len(self.columns)
    def __contains__(self, item):
        if isinstance(item, str):
            return item in self.column_names
        elif isinstance(item, Column):
            return item in self.columns
        else:
            raise TypeError(f'Invalid item type {type(item)} in Table Meta membership check: {item}')

    def display(self, max_cell_width=None, max_row_width=None, max_num_rows=None):
        columns = [[col.name]+[str(c) for c in col] for col in self.columns]
        max_col_widths = [len(max(col, key=len)) for col in columns]
        if max_cell_width is not None:
            max_col_widths = [min(mcw, max_cell_width) for mcw in max_col_widths]
        rows = list(zip(*columns))
        rows = rows[:max_num_rows+1] if max_num_rows is not None else rows
        formatted_rows = [
            f"{self.name} {'Table' if self.name != 'Table' else ''}: {len(self):,} cols x {len(self.table):,} rows"
        ]
        for row in rows:
            row = '|'.join(
                f'{cell if len(cell) <= mcw else cell[:mcw-2]+"..":{mcw}s}'
                for cell, mcw in zip(row, max_col_widths)
            )
            formatted_rows.append(row)
        if max_row_width is not None:
            formatted_rows = [tw.shorten(row, max_row_width) for row in formatted_rows]
        sep = '|'.join('-'*mcw for mcw in max_col_widths)
        if max_row_width is not None:
            sep = sep[:max_row_width]
        formatted_rows.insert(2, sep)
        return '\n'.join(formatted_rows)

    def fill(self, value=None):
        for dc_field in dc.fields(type(self.table)):
            if dc_field.name not in self.column_names:
                setattr(self.table, dc_field.name, ListColumn([value] * len(self.table), name=dc_field.name))
        return self

    def extend(self, format:type[T4]) -> T4:
        self.table.__class__ = format
        self.fill(None)
        return self.table

    def cast(self, format:type[T4]) -> T4:
        self.table.__class__ = format
        fields = {f.name for f in dataclasses.fields(format)}
        for name, attr in list(vars(self.table).items()):
            if isinstance(attr, Column) and name not in fields:
                delattr(self.table, name)
        self.fill(None)
        return self.table

    def spec(self):
        spec = {}
        column_types = column_type_map(self.table)
        for dc_field in dc.fields(type(self.table)):
            spec[dc_field.name] = column_types[dc_field.name]
        return self

    def apply(self, fn:callable, processes:int=1):
        sig = ins.signature(fn)
        if all(param in self.table._columns for param in sig.parameters):
            columnwise = [self.table._columns[param] for param in sig.parameters]
            results = [fn(*args) for args in zip(*columnwise)]
        else:
            results = [fn(row) for row in self.table]
        results = [result for result in results if result is not None]
        if all(type(result) is dict for result in results):
            output = Table.of([result for result in results if result is not None])
        else:
            output = Column(items=results, name=getattr(fn, '__name__', None))
        return output

    def sort(self, key=None, reverse=False):
        if isinstance(key, Column):
            key = key.table()
        if key is None:
            key = self.table
        if isinstance(key, Table):
            key = list(zip(*key()))
        if callable(key) and not isinstance(key, (Column, Table)):
            sig = ins.signature(key)
            if all(param in self.table._columns for param in sig.parameters):
                columnwise = [self.table._columns[param] for param in sig.parameters]
                key = [key(*args) for args in zip(*columnwise)] # noqa
            else:
                key = [key(row) for row in self.table] # noqa
        assert len(key) == len(self.table), \
            f'Key must have same length as table, but got {len(key)} != {len(self)}'
        items = sorted(zip(key, range(len(self.table))), reverse=reverse)
        keys, indices = zip(*items)
        old_to_new_indices = [None] * len(indices)
        for i, index in enumerate(indices):
            old_to_new_indices[index] = i # noqa
        for column in self.columns:
            if isinstance(column, list):
                tmp = list(list.__iter__(column))
                column.clear()
                column._extend([tmp[i] for i in indices]) # noqa
                if getattr(column, '_views', None):
                    for view in column._views.values(): # noqa
                        vtmp = list(list.__iter__(view))
                        view.clear()
                        view._extend([old_to_new_indices[i] for i in vtmp])
            else:
                raise NotImplementedError('Sort not implemented for non-list columns')
        return self.table

    default = object()

    def group(self, key=None) -> T.Dict[T.Any, T2]:
        if key is None:
            key = list(self.items())
        if isinstance(key, Column):
            key = list(key)
        elif isinstance(key, Table):
            key = list(key().items()) # noqa
        if callable(key) and not isinstance(key, (Column, Table)):
            sig = ins.signature(key)
            if all(param in self.table._columns for param in sig.parameters):
                columnwise = [self.table._columns[param] for param in sig.parameters]
                key = [key(*args) for args in zip(*columnwise)] # noqa
            else:
                key = [key(row) for row in self.table] # noqa
        group_indices = {k: [] for k in key}
        for i, group in enumerate(key):
            group_indices[group].append(i)
        groups = {key: self.table[indices] for key, indices in group_indices.items()}
        return groups


class ColumnOpsTypeHinting:
    def __and__(self, other): pass
    def __iand__(self, other): pass
    def __or__(self, other): pass
    def __ior__(self, other): pass
    def __lshift__(self, other): pass
    def __ilshift__(self, other): pass
    def __rshift__(self, other): pass
    def __irshift__(self, other): pass
    def __xor__(self, other): pass
    def __ixor__(self, other): pass
    def __matmul__(self, other): pass
    def __imatmul__(self, other): pass
    def __add__(self, other): pass
    def __iadd__(self, other): pass
    def __sub__(self, other): pass
    def __isub__(self, other): pass
    def __mul__(self, other): pass
    def __imul__(self, other): pass
    def __truediv__(self, other): pass
    def __itruediv__(self, other): pass
    def __floordiv__(self, other): pass
    def __ifloordiv__(self, other): pass
    def __mod__(self, other): pass
    def __imod__(self, other): pass
    def __pow__(self, other): pass
    def __ipow__(self, other): pass
    def __lt__(self, other): pass
    def __le__(self, other): pass
    def __eq__(self, other): pass
    def __ne__(self, other): pass
    def __gt__(self, other): pass
    def __ge__(self, other): pass
    def __neg__(self): pass
    def __pos__(self): pass
    def __abs__(self): pass



T3 = T.TypeVar('T3', bound='Column')

class ColumnOps(T.Generic[T3]):
    def __and__(self, other):
        return self.table() & other
    def __iand__(self, other):
        x = self.table()
        x &= other
        return x
    def __or__(self, other):
        return self.table() | other
    def __ior__(self, other):
        x = self.table()
        x |= other
        return x
    def __lshift__(self, other):
        return self.table() << other
    def __ilshift__(self, other):
        x = self.table()
        x <<= other
        return x
    def __rshift__(self, other):
        return self.table() >> other
    def __irshift__(self, other):
        x = self.table()
        x >>= other
        return x
    def __xor__(self, other):
        return self.table() ^ other
    def __ixor__(self, other):
        x = self.table()
        x ^= other
        return x
    def __matmul__(self, other):
        return self.table() @ other
    def __imatmul__(self, other):
        x = self.table()
        x @= other
        return x
    # Elementwise
    def __add__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a + b for a, b in zip(self, other)]
        return Column(items=results)
    def __iadd__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a + b for a, b in zip(self, other)]
        for i, result in enumerate(results):
            self[i] = result
        return self
    def __sub__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a - b for a, b in zip(self, other)]
        return Column(items=results)
    def __isub__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a - b for a, b in zip(self, other)]
        for i, result in enumerate(results):
            self[i] = result
        return self
    def __mul__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a * b for a, b in zip(self, other)]
        return Column(items=results)
    def __imul__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a * b for a, b in zip(self, other)]
        for i, result in enumerate(results):
            self[i] = result
        return self
    def __truediv__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a / b for a, b in zip(self, other)]
        return Column(items=results)
    def __itruediv__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a / b for a, b in zip(self, other)]
        for i, result in enumerate(results):
            self[i] = result
        return self
    def __floordiv__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a // b for a, b in zip(self, other)]
        return Column(items=results)
    def __ifloordiv__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a // b for a, b in zip(self, other)]
        for i, result in enumerate(results):
            self[i] = result
        return self
    def __mod__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a % b for a, b in zip(self, other)]
        return Column(items=results)
    def __imod__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a % b for a, b in zip(self, other)]
        for i, result in enumerate(results):
            self[i] = result
        return self
    def __pow__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a ** b for a, b in zip(self, other)]
        return Column(items=results)
    def __ipow__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [a ** b for a, b in zip(self, other)]
        for i, result in enumerate(results):
            self[i] = result
        return self
    def __lt__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [(a < b) for a, b in zip(self, other)]
        return Column(items=results)
    def __le__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [(a <= b) for a, b in zip(self, other)]
        return Column(items=results)
    def __eq__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [(a == b) for a, b in zip(self, other)]
        return Column(items=results)
    def __ne__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [(a != b) for a, b in zip(self, other)]
        return Column(items=results)
    def __gt__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [(a > b) for a, b in zip(self, other)]
        return Column(items=results)
    def __ge__(self, other):
        if type(other) is not list and not isinstance(other, Column):
            other = [other] * len(self)
        results = [(a >= b) for a, b in zip(self, other)]
        return Column(items=results)
    def __neg__(self):
        results = [-a for a in self]
        return Column(items=results)
    def __pos__(self):
        results = [+a for a in self]
        return Column(items=results)
    def __abs__(self):
        results = [abs(a) for a in self]
        return Column(items=results)


TC = T.TypeVar('TC')

class Column(ColumnOpsTypeHinting, T.Generic[TC]):
    def __new__(_cls, *args, **kwargs):
        if _cls is DictColumn:
            _obj = dict.__new__(_cls)
        else:
            _cls = ListColumn
            _obj = list.__new__(_cls)
        return _obj
    def __init__(self, items:T.Iterable[TC]=(), name:str=None):
        self.name:str|None = name
        self._origin = None
        self._views = wr.WeakValueDictionary()
    def __call__(self, new_value=None):
        if new_value is not None:
            self[0] = new_value
        return self[0]
    def __len__(self) -> int: ...
    def __iter__(self) -> T.Iterator[TC]: ...
    def __getitem__(self, key) -> TC: ...
    def __setitem__(self, key, value:TC|T.Iterable[TC]): ...
    def __delitem__(self, key): ...
    def base(self) -> 'Column':
        while hasattr(self, '_column'):
            self = self._column
        return self
    def table(self) -> Table:
        table = Table.of({})
        table._set_attr(self.name, self)
        table._name = self.name
        table._origin = self._origin
        return table


class ColumnView(Column, T.Generic[TC]):
    def __new__(_cls, _column, *args, **kwargs):
        if isinstance(_column, DictColumn):
            _cls = DictColumnView
            _obj = list.__new__(_cls)
        elif isinstance(_column, list):
            _cls = ListColumnView
            _obj = list.__new__(_cls)
        else:
            raise TypeError(f'Invalid column type {type(_column)}')
        return _obj
    def __init__(self, column, selection, name:str=None):
        self._column = column
        self.name = column.name if name is None else name
        self._origin = None
    def __str__(self):
        return f"[{', '.join(self)}]"
    def __repr__(self):
        return f"[{', '.join(repr(e) for e in self)}]"

class ListColumn(ColumnOps, list, Column, T.Generic[TC]):
    def __init__(self, items=(), name=None):
        list.__init__(self, items)
        Column.__init__(self, name=name)
    def __getitem__(self, selection):
        try:
            return list.__getitem__(self, selection)
        except TypeError:
            if not isinstance(selection, list):
                raise TypeError(f'Invalid index type {type(selection)} of {selection}')
            if not selection:
                return []
            first = selection[0]
            if isinstance(first, bool):
                assert len(selection) == len(self), \
                    f'Boolean index must have same length as column, but got {len(selection)} != {len(self)}'
                return [list.__getitem__(self, i) for i, b in enumerate(selection) if b]
            elif isinstance(first, int):
                return [list.__getitem__(self, i) for i in selection]
            else:
                raise TypeError(f'Invalid index type {type(first)} of {first}')
    def __delitem__(self, selection):
        if isinstance(selection, int):
            to_delete = {selection}
        elif isinstance(selection, slice):
            to_delete = set(range(*selection.indices(len(self))))
        elif isinstance(selection, list):
            if not selection:
                to_delete = set()
            elif isinstance(selection[0], bool):
                assert len(selection) == len(self), \
                    f'Boolean index must have same length as column, but got {len(selection)} != {len(self)}'
                to_delete = {i for i, b in enumerate(selection) if b}
            else:
                to_delete = set(selection)
        else:
            raise TypeError(f'Invalid index type {type(selection)} of {selection}')
        copy = list(self)
        self.clear()
        index_map = {}
        redone_elements = [e for i, e in enumerate(copy) if i not in to_delete]
        for i, e in [(i, e) for i, e in enumerate(copy) if i not in to_delete]:
            index_map[i] = len(index_map)
        self._extend(redone_elements)
        for view in self._views.values():
            viewcopy = list(list.__iter__(view))
            view.clear()
            view._extend([index_map[index] for index in viewcopy if index in index_map])
    def __setitem__(self, selection, values):
        if isinstance(values, Table):
            return
        if isinstance(selection, (int, slice)):
            list.__setitem__(self, selection, values)
        elif isinstance(selection, list):
            if selection and isinstance(selection[0], bool):
                selection = [i for i, b in enumerate(selection) if b]
            assert len(selection) == len(values), \
                f'Number of values ({len(values)}) does not match number of indices ({len(selection)})'
            for i, value in zip(selection, values):
                list.__setitem__(self, i, value)
        else:
            raise TypeError(f'Invalid index type {type(selection)} of {selection}')
    def extend(self, values:T.Iterable[TC]):
        assert self._origin is None, \
            f'Cannot extend column ({self}) attached to table ({self._origin})'
        self._extend(values)
    _extend = list.extend


class ListColumnView(ColumnOps, list, ColumnView, T.Generic[TC]):
    def __init__(self, column:Column, indices:list[int], name=None):
        if isinstance(column, ColumnView):
            indices = [list.__getitem__(column, i) for i in indices] # noqa
            column = column._column
        list.__init__(self, indices)
        ColumnView.__init__(self, column, indices, name=name)
        self._column._views[id(self)] = self
    def __getitem__(self, selection):
        try:
            return self._column[list.__getitem__(self, selection)]
        except TypeError:
            if not isinstance(selection, list):
                raise TypeError(f'Invalid index type {type(selection)}')
            if not selection:
                return []
            first = selection[0]
            if isinstance(first, bool):
                assert len(selection) == len(self), \
                    f'Boolean index must have same length as column, but got {len(selection)} != {len(self)}'
                indices = [i for i, b in zip(list.__iter__(self), selection) if b]
                return self._column[indices]
            elif isinstance(first, int):
                indices = [list.__getitem__(self, i) for i in selection]
                return self._column[indices]
            else:
                raise TypeError(f'Invalid index type {type(first)} of {first}')
    def __iter__(self):
        if isinstance(self._column, ColumnView):
            indices = list(list.__iter__(self))
            return iter(self._column[indices])
        else:
            return iter([list.__getitem__(self._column, i) for i in list.__iter__(self)])
    def __delitem__(self, selection):
        if isinstance(selection, int):
            to_delete = {selection}
        elif isinstance(selection, slice):
            to_delete = set(range(*selection.indices(len(self))))
        elif isinstance(selection, list):
            if not selection:
                to_delete = set()
            elif isinstance(selection[0], bool):
                assert len(selection) == len(self), \
                    f'Boolean index must have same length as column, but got {len(selection)} != {len(self)}'
                to_delete = {i for i, b in enumerate(selection) if b}
            else:
                to_delete = set(selection)
        else:
            raise TypeError(f'Invalid index type {type(selection)} of {selection}')
        assert all(-len(self) <= i < len(self) for i in to_delete), \
            f'Indices in {selection} are beyond the index bounds of of {self} (len {len(self)}) for deletion'
        copy = [item for i, item in enumerate(list.__iter__(self)) if i not in to_delete]
        self.clear()
        self._extend(copy)
    def clear(self):
        list.clear(self)
    def __setitem__(self, selection, values):
        if isinstance(values, Table):
            return
        if isinstance(selection, (int, slice)):
            self._column[list.__getitem__(self, selection)] = values
        elif isinstance(selection, list):
            if selection and isinstance(selection[0], bool):
                selection = [i for i, b in enumerate(selection) if b]
            assert len(selection) == len(values), \
                f'Number of values ({len(values)}) does not match number of indices ({len(selection)})'
            self._column[[list.__getitem__(self, i) for i in selection]] = values
        else:
            raise TypeError(f'Invalid index type {type(selection)} of {selection}')
    def extend(self, values:T.Iterable[TC]):
        raise TypeError(f'Column of type {type(self)} does not support extend: {self}')
    def _extend(self, indices):
        list.extend(self, indices)
    def __str__(self):
        return f"[{', '.join(self)}]"
    def __repr__(self):
        return f"[{', '.join(repr(e) for e in self)}]"


class DictColumn(ListColumn[TC]):
    def __init__(self, items=(), name=None):
        ListColumn.__init__(self, (), name=name)
        self._ids = {}
        self._extend(items)
    def clear(self):
        self._ids.clear()
        ListColumn.clear(self)
    def extend(self, values:T.Iterable[TC]):
        assert self._origin is None, \
            f'Cannot extend column ({self}) attached to table ({self._origin})'
        self._extend(values)
    def _extend(self, values:T.Iterable[TC]):
        values = [str(ez.uuid()) if value is None else value for value in values]
        extended_len = len(self) + len(values)
        union = set(self._ids) | set(values)
        assert len(union) == extended_len, \
            f'Cannot extend ID column with duplicate values in: {set(values)}'
        self._ids.update({value: i for i, value in enumerate(values)})
        ListColumn._extend(self, values)
    def append(self, value):
        raise AssertionError("Appending to IDColumn is not allowed!")
    def __setitem__(self, selection, values):
        if isinstance(values, Table):
            return
        if isinstance(selection, int):
            if values is None:
                values = str(ez.uuid())
            assert values not in self._ids, \
                f'Cannot set ID column with duplicate value: {values}'
            del self._ids[list.__getitem__(self, selection)]
            list.__setitem__(self, selection, values)
            self._ids[values] = selection
        else:
            values = [str(ez.uuid()) if value is None else value for value in values]
            assert len(set(values)) == len(values), \
                f'Cannot set ID column with duplicate values in: {values}'
            remaining = set(self._ids) - set(values)
            assert all(value not in remaining for value in values), \
                f'Cannot set ID column with duplicate values in: {values}'
            if isinstance(selection, slice):
                for key in list.__getitem__(self, selection):
                    del self._ids[key]
                list.__setitem__(self, selection, values)
                for key, index in zip(list.__getitem__(self, selection), range(*selection.indices(len(self)))):
                    self._ids[key] = index
            elif isinstance(selection, list):
                if selection and isinstance(selection[0], bool):
                    selection = [i for i, b in enumerate(selection) if b]
                assert len(selection) == len(values), \
                    f'Number of values ({len(values)}) does not match number of indices ({len(selection)})'
                for i, value in zip(selection, values):
                    del self._ids[list.__getitem__(self, i)]
                    list.__setitem__(self, i, value)
                    self._ids[value] = i
            else:
                raise TypeError(f'Invalid index type {type(selection)} of {selection}')

class DictColumnView(ListColumnView[TC], DictColumn):
    def __init__(self, column:Column, indices:list[int], name=None):
        assert isinstance(column, DictColumn), \
            f'Cannot create DictColumnView from non-DictColumn: {column}'
        if isinstance(column, ColumnView):
            indices = [list.__getitem__(column, i) for i in indices] # noqa
            column = column._column
        self._ids = {key: i for i, key in enumerate(column)}
        ListColumnView.__init__(self, column, indices, name=name)
    def _origin_index(self, key): # noqa
        return list.__getitem__(self, self._ids[key])
    def clear(self):
        self._ids.clear()
        ListColumnView.clear(self)
    def _extend(self, indices):
        extended_len = len(self) + len(indices)
        union = set(self._ids.values()) | set(indices)
        assert len(union) == extended_len, \
            f'Cannot extend ID column view with duplicate view indices: {set(self._ids) & set(indices)}'
        old_len = len(self)
        self._ids.update({self._column[index]: i for i, index in enumerate(indices, old_len)})
        list.extend(self, indices)
    def __str__(self):
        return f"[{', '.join(self)}]"
    def __repr__(self):
        return f"[{', '.join(repr(e) for e in self)}]"


IDColumn = Column
setattr(sys.modules[__name__], 'IDColumn', DictColumn)


