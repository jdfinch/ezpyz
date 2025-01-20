

import ezpyzy.table as t
import copy as cp
import typing as T


class Table(t.Tab):

    def get_column_names(self: t.Tab|T.Self, cols):
        """Get a list of strings corresponding to columns in table from a cols specification (either strings, Column objects, a slice, or a sequence with Ellipses (...) filling in unspecified columns."""
        if isinstance(cols, t.Column):
            col_names = (cols.__name__,)
        elif isinstance(cols, str):
            col_names = (cols,)
        elif isinstance(cols, ellipsis):
            return tuple(self.__cols__)
        elif isinstance(cols, slice):
            cols_list = tuple(self.__cols__)
            left_bound = cols.start
            if isinstance(left_bound, (str, t.Column)):
                if isinstance(left_bound, t.Column):
                    left_bound = left_bound.__name__
                assert left_bound in cols_list, f"Error Message"
                left_bound = cols_list.index(left_bound)
            right_bound = cols.stop
            if isinstance(right_bound, (str, t.Column)):
                if isinstance(right_bound, t.Column):
                    right_bound = right_bound.__name__
                assert right_bound in cols_list, f"Error Message"
                right_bound = cols_list.index(right_bound)
            col_names = cols_list[left_bound:right_bound:cols.step]
        elif isinstance(cols, int):
            assert 0 <= cols <= len(self.__cols__), f"Error Message"
            return (tuple(self.__cols__)[cols],)
        elif isinstance(cols, set):
            assert all(isinstance(col, (str, t.Column)) for col in cols), f"Error Message"
            cols = {col if isinstance(col, str) else col.__name__ for col in cols}
            col_names = tuple(col for col in self.__cols__ if col in cols)
            return col_names
        elif all(isinstance(col, bool) for col in cols):
            assert len(cols) == len(self.__cols__), f"Error Message"
            col_names = tuple(col for flag, col in zip(cols, self.__cols__) if flag)
            return col_names
        elif callable(cols):
            col_names = [col for col in self.__cols__ if cols(col)]
        elif all(isinstance(col, int) for col in cols):
            assert all(0 <= col < len(self.__cols__) for col in cols), f"Error Message"
            cols_list = tuple(self.__cols__)
            col_names = tuple(cols_list[col] for col in cols)
        elif all(isinstance(col, (str, t.Column, ellipsis)) for col in cols):
            ellipsis_idxs = [i for i, col in enumerate(cols) if col is Ellipsis]
            assert len(ellipsis_idxs) <= 1, f"Error Message"
            if ellipsis_idxs:
                ellipsis_idx, = ellipsis_idxs
                col_names = tuple(col.__name__ if isinstance(col, t.Column) else col
                    for col in cols if col is not Ellipsis)
                specified_cols = set(col_names)
                other_columns = tuple(col for col in self.__cols__ if col not in specified_cols)
                col_names = col_names[:ellipsis_idx] + other_columns + col_names[ellipsis_idx:]
            else:
                col_names = tuple(col.__name__ if isinstance(col, t.Column) else col for col in cols)
        else:
            raise TypeError(f"Error Message")
        assert all(col in self.__cols__ for col in col_names), f"Error Message"
        assert len(set(col_names)) == len(col_names), f"Error Message"
        return col_names

    def replace_columns(self: t.Tab | T.Self, data, cols=None):
        """Replace cols in all rows with a block of cell data (mutates existing rows)"""
        if cols is None:
            cols = self.__cols__
        else:
            cols = Table.get_column_names(self, cols)
        Table._replace_columns(self, cols, data)
        return self

    def _replace_columns(rows: T.Sequence[t.Row], cols: T.Collection[str], data):
        """Replace cols in rows with a block of cell data (mutates existing rows)"""
        if isinstance(data, t.Tab):
            assert len(data) == len(rows), f"Error Message"
            assert set(cols).issubset(data.__cols__), f"Error Message"
            for row, vals in zip(rows, data):
                for col in cols:
                    setattr(row, col, getattr(vals, col))
            return rows
        elif isinstance(data, t.Row):
            assert len(rows) == 1, f"Error Message"
            assert all(hasattr(data, col) for col in cols), f"Error Message"
            row, = rows
            for col in cols:
                setattr(row, col, getattr(data, col))
        elif isinstance(data, dict):
            assert set(cols).issubset(data), f"Error Message"
            if not data: return rows
            assert len(col_lens:={len(col) for col in data.values()}) == 1, f"Error Message"
            data_len, = col_lens
            assert data_len == len(rows), f"Error Message"
            for col in cols:
                column = data[col]
                for row, val in zip(rows, column):
                    setattr(row, col, val)
            return rows
        elif isinstance(data, t.Column):
            assert len(cols) == 1, f"Error Message"
            col, = cols
            assert col == data.__name__, f"Error Message"
            assert len(data) == len(rows), f"Error Message"
            for row, val in zip(rows, data):
                setattr(row, col, val)
            return rows
        elif len(data) and all(isinstance(col, t.Column) for col in data):
            col_map = {col.__name__: col for col in data}
            assert set(cols).issubset(set(col_map)), f"Error Message"
            assert all(len(column) == len(rows) for column in data), f"Error Message"
            for col in cols:
                for row, val in zip(rows, col_map[col]):
                    setattr(row, col, val)
            return rows
        else:
            assert len(data) == len(rows), f"Error Message"
            cols_set = set(cols)
            for row, vals in zip(rows, data):
                if isinstance(vals, dict):
                    assert cols_set.issubset(vals), f"Error Message"
                    for col in cols:
                        setattr(row, col, vals[col])
                elif isinstance(vals, (list, tuple)):
                    assert len(vals) == len(cols), f"Error Message"
                    for col, val in zip(cols, vals):
                        setattr(row, col, val)
                else:
                    assert all(hasattr(vals, col) for col in cols), f"Error Message"
                    for col in cols:
                        setattr(row, col, getattr(vals, col))
            return rows

    def create_rows(self: T.Self | type[t.Row], data):
        """Create new rows using row_type and transfer cell data to them along columns"""
        cols = self.__cols__
        if isinstance(self, t.Tab):
            row_type = self.__row_type__
        else:
            row_type = self
        if isinstance(data, t.Row):
            num_rows = 1
        elif isinstance(data, dict):
            if not data:
                return ()
            num_rows = len(next(iter(data.values())))
        elif len(data) and all(isinstance(col, t.Column) for col in data):
            num_rows = len(next(iter(data)))
        else:
            num_rows = len(data)
        rows = tuple(row_type() for _ in range(num_rows))
        rows = Table._replace_columns(rows, cols, data)
        return rows

    def cast_rows(self: t.Tab|T.Self, data):
        """Prepare rows for insertion into table. Rows that are raw data or not the correct row type result in new rows created and cell data transfered to the new rows that match the table Row type."""
        if isinstance(data, self.__row_type__):
            return (data,)
        elif isinstance(data, (t.Row, t.Column, dict)) or all(isinstance(e, t.Column) for e in data):
            return Table.create_rows(self, data)
        else:
            rows = data
            new_row_indices, new_row_data = zip(*(i, row
                for i, row in enumerate(data) if not isinstance(row, self.__row_type__)))
            if new_row_data:
                new_rows = Table.create_rows(self.__row_type__, new_row_data)
                rows = list(data)
                for i, new_row in zip(new_row_indices, new_rows):
                    rows[i] = new_row
            return rows

    def replace_row_indices(self: t.Tab|T.Self, indices: T.Collection[int], data):
        """Replace rows at specific indices with new row data. Must match number of inserted rows to number of old rows at the specified indices."""
        rows = Table.cast_rows(self, data)
        assert all(-len(self) <= i < len(self) for i in indices), f"Error Message"
        assert len(rows) == len(indices), f"Error Message"
        for i, row in zip(indices, rows):
            self.__rows__[i] = row
        return self

    def replace_row_mask(self: t.Tab|T.Self, mask: T.Collection[bool], data):
        rows = Table.cast_rows(self, data)
        assert len(self) == len(mask), f"Error Message"
        indices = [i for i, flag in mask if flag is True]
        assert len(indices) == len(rows), f"Error Message"
        for i, row in zip(indices, rows):
            self.__rows__[i] = row
        return self

    def replace_row_set(self: t.Tab|T.Self, ids: set[int], data):
        """Replace rows in table based on membership of their ids (using id(row)) in the given set"""
        rows = Table.cast_rows(self, data)
        indices = [i for i, row in enumerate(self) if id(row) in ids]
        assert len(indices) == len(rows), f"Error Message"
        for i, row in zip(indices, rows):
            self.__rows__[i] = row
        return self

    def replace_row_slice(self: t.Tab|T.Self, indices: slice, data):
        """Replaces rows using slicing notation with new row data. Must match number of inserted rows to number of old rows at the specified indices."""
        return Table.replace_row_indices(self, tuple(range(*indices.indices(len(self)))), data)

    def replace_row_block(self: t.Tab|T.Self, indices: slice, data):
        """Replace rows using slicing notation with new row data. Slice must be contiguous specifying a stride of either 1 or -1. Allows replacement of any number of old rows to any number of new rows."""
        rows = Table.cast_rows(self, data)
        start, stop, stride = indices.indices(len(self))
        assert stride in (1, -1), f"Error Message"
        if stride == -1:
            rows = reversed(rows)
            start, stop = stop, start
        if stop >= len(self):
            del self.__rows__[start:]
            self.__rows__.extend(rows)
        else:
            suffix = self.__rows__[stop:]
            del self.__rows__[start:]
            self.__rows__.extend(rows)
            self.__rows__.extend(suffix)
        return self

    def replace_row_by_predicate(self: t.Tab|T.Self, selector: T.Callable[[t.Row], T.Any], rows):
        """Replaces rows where the predicate selector returns a true value"""
        rows = Table.cast_rows(self, rows)
        indices = [i for i, row in enumerate(self) if selector(row)]
        assert len(indices) == len(rows), f"Error Message"
        for i, row in zip(indices, rows):
            self.__rows__[i] = row
        return self

    def replace(self: t.Tab|T.Self, selection, data):
        if isinstance(selection, int):
            Table.replace_row_indices(self, (selection,), (data,))
        elif isinstance(selection, slice):
            if isinstance(selection.start, (str, t.Column)) or isinstance(selection.stop, (str, t.Column)):
                Table.replace_columns(self, data, selection)
            else:
                if selection.step is None:
                    Table.replace_row_block(self, selection, data)
                else:
                    Table.replace_row_slice(self, selection, data)
        elif isinstance(selection, tuple):
            if not selection:
                Table.replace_row_indices(self, selection, data)
            elif all(isinstance(selector, (str, t.Column, ellipsis)) for selector in selection):
                Table.replace_columns(self, data, selection)
            elif all(isinstance(selector, bool) for selector in selection):
                Table.replace_row_mask(self, selection, data)
            elif all(isinstance(selector, int) for selector in selection):
                Table.replace_row_indices(self, selection, data)
            elif len(selection) == 1:
                Table.replace(self, selection[0], data)
            else:
                row_select, *col_select = selection
                row_view = Table.select(self, row_select)
                cols = Table.get_column_names(self, col_select)
                Table._replace_columns(row_view, cols, data)
        elif isinstance(selection, (str, t.Column, ellipsis)):
            Table.replace_columns(self, data, selection)
        elif isinstance(selection, set):
            if all(isinstance(selector, (str, t.Column)) for selector in selection):
                Table.replace_columns(self, data, selection)
            else:
                Table.replace_row_set(self, selection, data)
        elif callable(selection):
            Table.replace_row_by_predicate(self, selection, data)
        else:
            if all(isinstance(selector, bool) for selector in selection):
                Table.replace_row_mask(self, selection, data)
            elif all(isinstance(selector, int) for selector in selection):
                Table.replace_row_indices(self, selection, data)
            elif all(isinstance(selector, (str, t.Column, Ellipsis)) for selector in selection):
                Table.replace_columns(self, data, selection)
            else:
                raise TypeError(f"Error Message")

    def delete(self: t.Tab|T.Self, selection):
        if isinstance(selection, int):
            assert -len(self.__rows__) <= selection < len(self.__rows__), f"Error Message"
            del self.__rows__[selection]
        elif isinstance(selection, slice):
            if isinstance(selection.start, (str, t.Column)) or isinstance(selection.stop, (str, t.Column)):
                return Table.delete_cols(self, selection)
            else:
                return Table.delete_rows_slice(self, selection)
        elif isinstance(selection, tuple):
            if not selection:
                return self
            elif all(isinstance(selector, (str, t.Column, ellipsis)) for selector in selection):
                return Table.delete_cols(self, selection)
            elif all(isinstance(selector, bool) for selector in selection):
                return Table.delete_rows_mask(self, selection)
            elif all(isinstance(selector, int) for selector in selection):
                return Table.delete_rows_indices(self, selection)
            elif len(selection) == 1:
                return Table.delete(self, selection[0])
            else:
                row_select, *col_select = selection
                row_view = Table.select(self, row_select)
                return Table.delete_cols(row_view, col_select)
        elif isinstance(selection, (str, t.Column, ellipsis)):
            Table.delete_cols(self, selection)
        elif isinstance(selection, set):
            if all(isinstance(selector, (str, t.Column)) for selector in selection):
                Table.delete_cols(self, selection)
            else:
                Table.delete_rows_set(self, selection)
        elif callable(selection):
            Table.delete_rows_predicate(self, selection)
        else:
            if all(isinstance(selector, bool) for selector in selection):
                Table.delete_rows_mask(self, selection)
            elif all(isinstance(selector, int) for selector in selection):
                Table.delete_rows_indices(self, selection)
            elif all(isinstance(selector, (str, t.Column, ellipsis)) for selector in selection):
                Table.delete_cols(self, selection)
            else:
                raise TypeError(f"Error Message")

    def delete_cols(self: t.Tab|T.Self, cols):
        cols = Table.get_column_names(self, cols)
        cols = Table._delete_cols(self, cols)
        return cols

    def _delete_cols(self: t.Tab|T.Self, cols: T.Collection[str]):
        cols = set(cols)
        assert all(col in self.__cols__ for col in cols), f"Error Message"
        for col in cols:
            del self.__cols__[col]
        for col in cols:
            for row in self.__rows__:
                setattr(row, col, None)
        return self

    def delete_rows_slice(self: t.Tab|T.Self, selection: slice):
        del self.__rows__[selection]
        return self

    def delete_rows_indices(self: t.Tab|T.Self, selection: T.Collection[int]):
        assert all(-len(self.__rows__) <= index < len(self.__rows__) for index in selection), f"Error Message"
        deleted_indices = {i if i >= 0 else i+len(self) for i in selection}
        deleted_rows = {id(row) for i, row in enumerate(self.__rows__) if i in deleted_indices}
        kept_rows = [row for row in self.__rows__ if id(row) not in deleted_rows]
        self.__rows__.clear()
        self.__rows__.extend(kept_rows)
        return self

    def delete_rows_mask(self: t.Tab|T.Self, selection: T.Collection[bool]):
        assert len(selection) == len(self), f"Error Message"
        deleted_rows = {id(row) for is_deleted, row in zip(selection, self.__rows__) if not is_deleted}
        kept_rows = [row for row in self.__rows__ if id(row) not in deleted_rows]
        self.__rows__.clear()
        self.__rows__.extend(kept_rows)
        return self

    def delete_rows_set(self: t.Tab|T.Self, selection: set[int]):
        kept_rows = [row for row in self.__rows__ if id(row) not in selection]
        self.__rows__.clear()
        self.__rows__.extend(kept_rows)
        return self

    def delete_rows_predicate(self: t.Tab|T.Self,
        selector: T.Callable[[t.Row], T.Any]
    ):
        kept_rows = [row for row in self.__rows__ if not selector(row)]
        self.__rows__.clear()
        self.__rows__.extend(kept_rows)
        return self

    def select(self: t.Tab|T.Self, selection):
        if isinstance(selection, int):
            assert 0 <= selection < len(self.__rows__), f"Error Message"
            return self.__rows__[selection]
        elif isinstance(selection, slice):
            if isinstance(selection.start, (str, t.Column)) or isinstance(selection.stop, (str, t.Column)):
                return Table.select_cols(self, selection)
            else:
                return Table.select_rows_slice(self, selection)
        elif isinstance(selection, tuple):
            if not selection:
                return Table.select_rows_indices(self, ())
            elif all(isinstance(selector, (str, t.Column, ellipsis)) for selector in selection):
                return Table.select_cols(self, selection)
            elif all(isinstance(selector, bool) for selector in selection):
                return Table.select_rows_mask(self, selection)
            elif all(isinstance(selector, int) for selector in selection):
                return Table.select_rows_indices(self, selection)
            elif len(selection) == 1:
                return Table.select(self, selection[0])
            else:
                row_select, *col_select = selection
                row_view = Table.select(self, row_select)
                return Table.select_cols(row_view, col_select)
        elif isinstance(selection, (str, t.Column, ellipsis)):
            Table.select_cols(self, selection)
        elif isinstance(selection, set):
            if all(isinstance(selector, (str, t.Column)) for selector in selection):
                Table.select_cols(self, selection)
            else:
                Table.select_rows_set(self, selection)
        elif callable(selection):
            Table.select_rows_predicate(self, selection)
        else:
            if all(isinstance(selector, bool) for selector in selection):
                Table.select_rows_mask(self, selection)
            elif all(isinstance(selector, int) for selector in selection):
                Table.select_rows_indices(self, selection)
            elif all(isinstance(selector, (str, t.Column, ellipsis)) for selector in selection):
                Table.select_cols(self, selection)
            else:
                raise TypeError(f"Error Message")

    def select_cols(self: t.Tab|T.Self, cols):
        cols = Table.get_column_names(self, cols)
        cols = Table._select_cols(self, cols)
        return cols

    def _select_cols(self: t.Tab|T.Self, cols: T.Collection[str]):
        view = cp.copy(self)
        view.__cols__ = dict.fromkeys(cols)
        view.__rows__ = list(self.__rows__)
        return view

    def select_rows_slice(self: t.Tab|T.Self, selection: slice, cols:T.Collection[str]|None=None):
        view = cp.copy(self)
        view.__cols__ = dict.fromkeys(self.__cols__ if cols is None else cols)
        view.__rows__ = self.__rows__[selection]
        return view

    def select_rows_indices(self: t.Tab|T.Self, selection: T.Collection[int], cols:T.Collection[str]|None=None):
        view = cp.copy(self)
        view.__cols__ = dict.fromkeys(self.__cols__ if cols is None else cols)
        view.__rows__ = [self.__rows__[i] for i in selection]
        return view

    def select_rows_mask(self: t.Tab|T.Self, selection: T.Collection[bool], cols:T.Collection[str]|None=None):
        assert len(selection) == len(self), f"Error Message"
        view = cp.copy(self)
        view.__cols__ = dict.fromkeys(self.__cols__ if cols is None else cols)
        view.__rows__ = [self.__rows__[flag] for flag in selection if flag is True]
        return view

    def select_rows_set(self: t.Tab|T.Self, selection: set[int], cols:T.Collection[str]|None=None):
        view = cp.copy(self)
        view.__cols__ = dict.fromkeys(self.__cols__ if cols is None else cols)
        view.__rows__ = [row for row in self.__rows__ if id(row) in selection]
        return view

    def select_rows_predicate(self: t.Tab|T.Self,
        selector: T.Callable[[t.Row], T.Any], cols:T.Collection[str]|None=None
    ):
        view = cp.copy(self)
        view.__cols__ = dict.fromkeys(self.__cols__ if cols is None else cols)
        view.__rows__ = [row for row in self.__rows__ if selector(row)]
        return view
