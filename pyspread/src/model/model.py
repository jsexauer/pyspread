#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright Martin Manns
# Distributed under the terms of the GNU General Public License

# --------------------------------------------------------------------
# pyspread is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# pyspread is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with pyspread.  If not, see <http://www.gnu.org/licenses/>.
# --------------------------------------------------------------------

"""

Model
=====

The model contains the core data structures of pyspread.
It is divided into layers.

Layer 3: CodeArray
Layer 2: DataArray
Layer 1: DictGrid
Layer 0: KeyValueStore

"""

import ast
import base64
import bz2
from copy import copy
import cStringIO
import datetime
from itertools import imap, ifilter, product
import re
import sys
from types import SliceType, IntType

import numpy

import wx

from src.config import config

from src.lib.typechecks import is_slice_like, is_string_like, is_generator_like
from src.lib.selection import Selection

import src.lib.charts as charts

from src.sysvars import get_color, get_font_string

from unredo import UnRedo
import time


class KeyValueStore(dict):
    """Key-Value store in memory. Currently a dict with default value None.

    This class represents layer 0 of the model.

    """

    def __missing__(self, value):
        """Returns the default value None"""

        return

# End of class KeyValueStore

# -----------------------------------------------------------------------------


class CellAttributes(list):
    """Stores cell formatting attributes in a list of 3 - tuples

    The first element of each tuple is a Selection.
    The second element is the table
    The third element is a dict of attributes that are altered.

    The class provides attribute read access to single cells via __getitem__
    Otherwise it behaves similar to a list.

    Note that for the method undoable_append to work, unredo has to be
    defined as class attribute.

    """

    default_cell_attributes = {
        "borderwidth_bottom": 1,
        "borderwidth_right": 1,
        "bordercolor_bottom": get_color(config["grid_color"]).GetRGB(),
        "bordercolor_right": get_color(config["grid_color"]).GetRGB(),
        "bgcolor": get_color(config["background_color"]).GetRGB(),
        "textfont": get_font_string(config["font"]),
        "pointsize": 10,
        "fontweight": wx.NORMAL,
        "fontstyle": wx.NORMAL,
        "textcolor": get_color(config["text_color"]).GetRGB(),
        "underline": False,
        "strikethrough": False,
        "angle": 0.0,
        "column-width": 150,
        "row-height": 26,
        "vertical_align": "top",
        "justification": "left",
        "frozen": False,
        "merge_area": None,
    }

    # Cache for __getattr__ maps key to tuple of len and attr_dict

    _attr_cache = {}

    def undoable_append(self, value, mark_unredo=True):
        """Appends item to list and provides undo and redo functionality"""

        undo_operation = (self.pop, [])
        redo_operation = (self.undoable_append, [value, mark_unredo])

        self.unredo.append(undo_operation, redo_operation)

        if mark_unredo:
            self.unredo.mark()

        self.append(value)
        self._attr_cache.clear()

    def __getitem__(self, key):
        """Returns attribute dict for a single key"""

        assert not any(type(key_ele) is SliceType for key_ele in key)

        if key in self._attr_cache:
            cache_len, cache_dict = self._attr_cache[key]

            # Use cache result only if no new attrs have been defined
            if cache_len == len(self):
                return cache_dict

        row, col, tab = key

        result_dict = copy(self.default_cell_attributes)

        for selection, table, attr_dict in self:
            if tab == table and (row, col) in selection:
                result_dict.update(attr_dict)

        # Upddate cache with current length and dict
        self._attr_cache[key] = (len(self), result_dict)

        return result_dict

    def get_merging_cell(self, key):
        """Returns key of cell that merges the cell key

        or None if cell key not merged

        Parameters
        ----------
        key: 3-tuple of Integer
        \tThe key of the cell that is merged

        """

        row, col, tab = key

        merging_cell = None

        def is_in_merge_area(row, col, merge_area):
            top, left, bottom, right = merge_area
            return top <= row <= bottom and left <= col <= right

        for selection, table, attr_dict in self:
            try:
                merge_area = attr_dict["merge_area"]
                if table == tab and merge_area is not None:
                    # We have a merge area in the cell's table
                    if is_in_merge_area(row, col, merge_area):
                        merging_cell = merge_area[0], merge_area[1], tab
            except KeyError:
                pass

        return merging_cell

    # Allow getting and setting elements in list
    get_item = list.__getitem__
    set_item = list.__setitem__

# End of class CellAttributes


class DictGrid(KeyValueStore):
    """The core data class with all information that is stored in a pys file.

    Besides grid code access via standard dict operations, it provides
    the following attributes:

    * cell_attributes: Stores cell formatting attributes
    * macros:          String of all macros

    This class represents layer 1 of the model.

    Parameters
    ----------
    shape: n-tuple of integer
    \tShape of the grid

    """

    def __init__(self, shape):
        KeyValueStore.__init__(self)

        self.shape = shape

        self.cell_attributes = CellAttributes()

        self.macros = u""

        self.row_heights = {}  # Keys have the format (row, table)
        self.col_widths = {}  # Keys have the format (col, table)

    def __getitem__(self, key):

        shape = self.shape

        for axis, key_ele in enumerate(key):
            if shape[axis] <= key_ele or key_ele < -shape[axis]:
                msg = "Grid index {key} outside grid shape {shape}."
                msg = msg.format(key=key, shape=shape)
                raise IndexError(msg)

        return KeyValueStore.__getitem__(self, key)

# End of class DictGrid

# -----------------------------------------------------------------------------


class DataArray(object):
    """DataArray provides enhanced grid read/write access.

    Enhancements comprise:
     * Slicing
     * Multi-dimensional operations such as insertion and deletion along 1 axis
     * Undo/redo operations

    This class represents layer 2 of the model.

    Parameters
    ----------
    shape: n-tuple of integer
    \tShape of the grid

    """

    def __init__(self, shape):
        self.dict_grid = DictGrid(shape)

        # Undo and redo management
        self.unredo = UnRedo()
        self.dict_grid.cell_attributes.unredo = self.unredo

        # Safe mode
        self.safe_mode = False

    # Data is the central content interface for loading / saving data.
    # It shall be used for loading and saving from and to pys and other files.
    # It shall be used for loading and saving macros.
    # It is not used for importinf and exporting data because these operations
    # are partial to the grid.

    def _get_data(self):
        """Returns dict of data content.

        Keys
        ----

        shape: 3-tuple of Integer
        \tGrid shape
        grid: Dict of 3-tuples to strings
        \tCell content
        attributes: List of 3-tuples
        \tCell attributes
        row_heights: Dict of 2-tuples to float
        \t(row, tab): row_height
        col_widths: Dict of 2-tuples to float
        \t(col, tab): col_width
        macros: String
        \tMacros from macro list

        """

        data = {}

        data["shape"] = self.shape
        data["grid"] = {}.update(self.dict_grid)
        data["attributes"] = [ca for ca in self.cell_attributes]
        data["row_heights"] = self.row_heights
        data["col_widths"] = self.col_widths
        data["macros"] = self.macros

        return data

    def _set_data(self, **kwargs):
        """Sets data from given parameters

        Old values are deleted.
        If a paremeter is not given, nothing is changed.

        Parameters
        ----------

        shape: 3-tuple of Integer
        \tGrid shape
        grid: Dict of 3-tuples to strings
        \tCell content
        attributes: List of 3-tuples
        \tCell attributes
        row_heights: Dict of 2-tuples to float
        \t(row, tab): row_height
        col_widths: Dict of 2-tuples to float
        \t(col, tab): col_width
        macros: String
        \tMacros from macro list

        """

        if "shape" in kwargs:
            self.shape = kwargs["shape"]

        if "grid" in kwargs:
            self.dict_grid.clear()
            self.dict_grid.update(kwargs["grid"])

        if "attributes" in kwargs:
            self.attributes[:] = kwargs["attributes"]

        if "row_heights" in kwargs:
            self.row_heights = kwargs["row_heights"]

        if "col_widths" in kwargs:
            self.col_widths = kwargs["col_widths"]

        if "macros" in kwargs:
            self.macros = kwargs["macros"]

    data = property(_get_data, _set_data)

    # Row and column attributes mask
    # Keys have the format (row, table)

    def _get_row_heights(self):
        """Returns row_heights dict"""

        return self.dict_grid.row_heights

    def _set_row_heights(self, row_heights):
        """Sets  macros string"""

        self.dict_grid.row_heights = row_heights

    row_heights = property(_get_row_heights, _set_row_heights)

    def _get_col_widths(self):
        """Returns col_widths dict"""

        return self.dict_grid.col_widths

    def _set_col_widths(self, col_widths):
        """Sets  macros string"""

        self.dict_grid.col_widths = col_widths

    col_widths = property(_get_col_widths, _set_col_widths)

    # Cell attributes mask
    def _get_cell_attributes(self):
        """Returns cell_attributes list"""

        return self.dict_grid.cell_attributes

    def _set_cell_attributes(self, value):
        """Setter for cell_atributes"""

        # Empty cell_attributes first
        self.cell_attributes[:] = []
        self.cell_attributes.extend(value)

    cell_attributes = attributes = \
        property(_get_cell_attributes, _set_cell_attributes)

    def __iter__(self):
        """Returns iterator over self.dict_grid"""

        return iter(self.dict_grid)

    def _get_macros(self):
        """Returns macros string"""

        return self.dict_grid.macros

    def _set_macros(self, macros):
        """Sets  macros string"""

        self.dict_grid.macros = macros

    macros = property(_get_macros, _set_macros)

    def keys(self):
        """Returns keys in self.dict_grid"""

        return self.dict_grid.keys()

    def pop(self, key, mark_unredo=True):
        """Pops dict_grid with undo and redo support

        Parameters
        ----------
        key: 3-tuple of Integer
        \tCell key that shall be popped
        mark_unredo: Boolean, defaults to True
        \tIf True then an unredo marker is set after the operation

        """

        result = self.dict_grid.pop(key)

        # UnRedo support

        if mark_unredo:
            self.unredo.mark()

        undo_operation = (self.__setitem__, [key, result, mark_unredo])
        redo_operation = (self.pop, [key, mark_unredo])

        self.unredo.append(undo_operation, redo_operation)

        if mark_unredo:
            self.unredo.mark()

        # End UnRedo support

        return result

    # Shape mask

    def _get_shape(self):
        """Returns dict_grid shape"""

        return self.dict_grid.shape

    def _set_shape(self, shape, mark_unredo=True):
        """Deletes all cells beyond new shape and sets dict_grid shape

        Parameters
        ----------
        shape: 3-tuple of Integer
        \tTarget shape for grid
        mark_unredo: Boolean, defaults to True
        \tIf True then an unredo marker is set after the operation

        """

        # Delete each cell that is beyond new borders

        old_shape = self.shape

        if any(new_axis < old_axis
               for new_axis, old_axis in zip(shape, old_shape)):
            for key in self.dict_grid.keys():
                if any(key_ele >= new_axis
                       for key_ele, new_axis in zip(key, shape)):
                    self.pop(key)

        # Set dict_grid shape attribute

        self.dict_grid.shape = shape

        # UnRedo support

        undo_operation = (self._set_shape, [old_shape, mark_unredo])
        redo_operation = (self._set_shape, [shape, mark_unredo])

        self.unredo.append(undo_operation, redo_operation)

        if mark_unredo:
            self.unredo.mark()

        # End UnRedo support

    shape = property(_get_shape, _set_shape)

    def get_last_filled_cell(self, table=None):
        """Returns key for the bottommost rightmost cell with content

        Parameters
        ----------
        table: Integer, defaults to None
        \tLimit search to this table

        """

        maxrow = 0
        maxcol = 0

        for row, col, tab in self.dict_grid:
            if table is None or tab == table:
                maxrow = max(row, maxrow)
                maxcol = max(col, maxcol)

        return maxrow, maxcol, table

    # Pickle support

    def __getstate__(self):
        """Returns dict_grid for pickling

        Note that all persistent data is contained in the DictGrid class

        """

        return {"dict_grid": self.dict_grid}

    # Slice support

    def __getitem__(self, key):
        """Adds slicing access to cell code retrieval

        The cells are returned as a generator of generators, of ... of unicode.

        Parameters
        ----------
        key: n-tuple of integer or slice
        \tKeys of the cell code that is returned

        Note
        ----
        Classical Excel type addressing (A$1, ...) may be added here

        """

        for key_ele in key:
            if is_slice_like(key_ele):
                # We have something slice-like here

                return self.cell_array_generator(key)

            elif is_string_like(key_ele):
                # We have something string-like here
                msg = "Cell string based access not implemented"
                raise NotImplementedError(msg)

        # key_ele should be a single cell

        return self.dict_grid[key]

    def __setitem__(self, key, value, mark_unredo=True):
        """Accepts index and slice keys

        Parameters
        ----------
        key: 3-tuple of Integer or Slice object
        \tCell key(s) that shall be set
        value: Object (should be Unicode or similar)
        \tCode for cell(s) to be set
        mark_unredo: Boolean, defaults to True
        \tIf True then an unredo marker is set after the operation

        """

        single_keys_per_dim = []

        for axis, key_ele in enumerate(key):
            if is_slice_like(key_ele):
                # We have something slice-like here

                length = key[axis]
                slice_range = xrange(*key_ele.indices(length))
                single_keys_per_dim.append(slice_range)

            elif is_string_like(key_ele):
                # We have something string-like here

                raise NotImplementedError

            else:
                # key_ele is a single cell

                single_keys_per_dim.append((key_ele, ))

        single_keys = product(*single_keys_per_dim)

        unredo_mark = False

        for single_key in single_keys:
            if value:
                # UnRedo support

                old_value = self(key)

                try:
                    old_value = unicode(old_value, encoding="utf-8")
                except TypeError:
                    pass

                # We seem to have double calls on __setitem__
                # This hack catches them

                if old_value != value:

                    unredo_mark = True

                    undo_operation = (self.__setitem__,
                                      [key, old_value, mark_unredo])
                    redo_operation = (self.__setitem__,
                                      [key, value, mark_unredo])

                    self.unredo.append(undo_operation, redo_operation)

                    # End UnRedo support

                self.dict_grid[single_key] = value
            else:
                # Value is empty --> delete cell
                try:
                    self.dict_grid.pop(key)

                except (KeyError, TypeError):
                    pass

        if mark_unredo and unredo_mark:
            self.unredo.mark()

    def cell_array_generator(self, key):
        """Generator traversing cells specified in key

        Parameters
        ----------
        key: Iterable of Integer or slice
        \tThe key specifies the cell keys of the generator

        """

        for i, key_ele in enumerate(key):

            # Get first element of key that is a slice
            if type(key_ele) is SliceType:
                slc_keys = xrange(*key_ele.indices(self.dict_grid.shape[i]))
                key_list = list(key)

                key_list[i] = None

                has_subslice = any(type(ele) is SliceType for ele in key_list)

                for slc_key in slc_keys:
                    key_list[i] = slc_key

                    if has_subslice:
                        # If there is a slice left yield generator
                        yield self.cell_array_generator(key_list)

                    else:
                        # No slices? Yield value
                        yield self[tuple(key_list)]

                break

    def _shift_rowcol(self, insertion_point, no_to_insert, mark_unredo):
        """Shifts row and column sizes when a table is inserted or deleted"""

        if mark_unredo:
            self.unredo.mark()

        # Shift row heights

        new_row_heights = {}
        del_row_heights = []

        for row, tab in self.row_heights:
            if tab > insertion_point:
                new_row_heights[(row, tab + no_to_insert)] = \
                    self.row_heights[(row, tab)]
                del_row_heights.append((row, tab))

        for row, tab in new_row_heights:
            self.set_row_height(row, tab, new_row_heights[(row, tab)],
                                mark_unredo=False)

        for row, tab in del_row_heights:
            if (row, tab) not in new_row_heights:
                self.set_row_height(row, tab, None, mark_unredo=False)

        # Shift column widths

        new_col_widths = {}
        del_col_widths = []

        for col, tab in self.col_widths:
            if tab > insertion_point:
                new_col_widths[(col, tab + no_to_insert)] = \
                    self.col_widths[(col, tab)]
                del_col_widths.append((col, tab))

        for col, tab in new_col_widths:
            self.set_col_width(col, tab, new_col_widths[(col, tab)],
                               mark_unredo=False)

        for col, tab in del_col_widths:
            if (col, tab) not in new_col_widths:
                self.set_col_width(col, tab, None, mark_unredo=False)

        if mark_unredo:
            self.unredo.mark()

    def _adjust_rowcol(self, insertion_point, no_to_insert, axis, tab=None,
                       mark_unredo=True):
        """Adjusts row and column sizes on insertion/deletion"""

        if axis == 2:
            self._shift_rowcol(insertion_point, no_to_insert, mark_unredo)
            return

        assert axis in (0, 1)

        if mark_unredo:
            self.unredo.mark()

        cell_sizes = self.col_widths if axis else self.row_heights
        set_cell_size = self.set_col_width if axis else self.set_row_height

        new_sizes = {}
        del_sizes = []

        for pos, table in cell_sizes:
            if pos > insertion_point and (tab is None or tab == table):
                if 0 <= pos + no_to_insert < self.shape[axis]:
                    new_sizes[(pos + no_to_insert, table)] = \
                        cell_sizes[(pos, table)]
                del_sizes.append((pos, table))

        for pos, table in new_sizes:
            set_cell_size(pos, table, new_sizes[(pos, table)],
                          mark_unredo=False)

        for pos, table in del_sizes:
            if (pos, table) not in new_sizes:
                set_cell_size(pos, table, None, mark_unredo=False)

        if mark_unredo:
            self.unredo.mark()

    def _adjust_cell_attributes(self, insertion_point, no_to_insert, axis,
                                tab=None, cell_attrs=None, mark_unredo=True):
        """Adjusts cell attributes on insertion/deletion"""

        if mark_unredo:
            self.unredo.mark()

        old_cell_attrs = self.cell_attributes[:]

        if axis < 2:
            # Adjust selections

            if cell_attrs is None:
                cell_attrs = []

                for key in self.cell_attributes:
                    selection, table, value = key
                    if tab is None or tab == table:
                        new_sel = copy(selection)
                        new_val = copy(value)
                        new_sel.insert(insertion_point, no_to_insert, axis)
                        # Update merge area if present
                        if "merge_area" in value:
                            top, left, bottom, right = value["merge_area"]
                            ma_sel = Selection([(top, left)],
                                               [(bottom, right)], [], [], [])
                            ma_sel.insert(insertion_point, no_to_insert, axis)
                            __top, __left = ma_sel.block_tl[0]
                            __bottom, __right = ma_sel.block_br[0]

                            new_val["merge_area"] = \
                                __top, __left, __bottom, __right

                        cell_attrs.append((new_sel, table, new_val))

            self.cell_attributes[:] = cell_attrs

            self.cell_attributes._attr_cache.clear()

        elif axis == 2:
            # Adjust tabs
            new_tabs = []
            for selection, old_tab, value in self.cell_attributes:
                if old_tab > insertion_point and \
                   (tab is None or tab == old_tab):
                    new_tabs.append((selection, old_tab + no_to_insert, value))
                else:
                    new_tabs.append(None)

            for i, sel_tab_val in enumerate(new_tabs):
                if sel_tab_val is not None:
                    self.dict_grid.cell_attributes.set_item(i, sel_tab_val)

            self.cell_attributes._attr_cache.clear()

        else:
            raise ValueError("Axis must be in [0, 1, 2]")

        undo_operation = (self._adjust_cell_attributes,
                          [insertion_point, -no_to_insert, axis, tab,
                           old_cell_attrs, mark_unredo])
        redo_operation = (self._adjust_cell_attributes,
                          [insertion_point, no_to_insert, axis, tab,
                           cell_attrs, mark_unredo])

        self.unredo.append(undo_operation, redo_operation)

        if mark_unredo:
            self.unredo.mark()

    def insert(self, insertion_point, no_to_insert, axis, tab=None):
        """Inserts no_to_insert rows/cols/tabs/... before insertion_point

        Parameters
        ----------

        insertion_point: Integer
        \tPont on axis, before which insertion takes place
        no_to_insert: Integer >= 0
        \tNumber of rows/cols/tabs that shall be inserted
        axis: Integer
        \tSpecifies number of dimension, i.e. 0 == row, 1 == col, ...
        tab: Integer, defaults to None
        \tIf given then insertion is limited to this tab for axis < 2

        """

        self.unredo.mark()

        if not 0 <= axis <= len(self.shape):
            raise ValueError("Axis not in grid dimensions")

        if insertion_point > self.shape[axis] or \
           insertion_point < -self.shape[axis]:
            raise IndexError("Insertion point not in grid")

        new_keys = {}
        del_keys = []

        for key in self.dict_grid.keys():
            if key[axis] > insertion_point and (tab is None or tab == key[2]):
                new_key = list(key)
                new_key[axis] += no_to_insert
                if 0 <= new_key[axis] < self.shape[axis]:
                    new_keys[tuple(new_key)] = self(key)
                del_keys.append(key)

        # Now re-insert moved keys

        for key in new_keys:
            self.__setitem__(key, new_keys[key], mark_unredo=False)

        for key in del_keys:
            if key not in new_keys and self(key) is not None:
                self.pop(key, mark_unredo=False)

        self._adjust_rowcol(insertion_point, no_to_insert, axis, tab=tab,
                            mark_unredo=False)
        self._adjust_cell_attributes(insertion_point, no_to_insert, axis,
                                     tab=tab, mark_unredo=False)

        self.unredo.mark()

    def delete(self, deletion_point, no_to_delete, axis, tab=None):
        """Deletes no_to_delete rows/cols/... starting with deletion_point

        Axis specifies number of dimension, i.e. 0 == row, 1 == col, ...

        """

        self.unredo.mark()

        if not 0 <= axis < len(self.shape):
            raise ValueError("Axis not in grid dimensions")

        if no_to_delete < 0:
            raise ValueError("Cannot delete negative number of rows/cols/...")

        elif no_to_delete >= self.shape[axis]:
            raise ValueError("Last row/column/table must not be deleted")

        if deletion_point > self.shape[axis] or \
           deletion_point <= -self.shape[axis]:
            raise IndexError("Deletion point not in grid")

        new_keys = {}
        del_keys = []

        # Note that the loop goes over a list that copies all dict keys
        for key in self.dict_grid.keys():
            if tab is None or tab == key[2]:
                if deletion_point <= key[axis] < deletion_point + no_to_delete:
                    del_keys.append(key)

                elif key[axis] >= deletion_point + no_to_delete:
                    new_key = list(key)
                    new_key[axis] -= no_to_delete

                    new_keys[tuple(new_key)] = self(key)
                    del_keys.append(key)

        # Now re-insert moved keys

        for key in new_keys:
            self.__setitem__(key, new_keys[key], mark_unredo=False)

        for key in del_keys:
            if key not in new_keys and self(key) is not None:
                self.pop(key, mark_unredo=False)

        if axis in (0, 1):
            self._adjust_rowcol(deletion_point, -no_to_delete, axis, tab=tab,
                                mark_unredo=False)
        self._adjust_cell_attributes(deletion_point, -no_to_delete, axis,
                                     tab=tab, mark_unredo=False)

        self.unredo.mark()

    def set_row_height(self, row, tab, height, mark_unredo=True):
        """Sets row height"""

        if mark_unredo:
            self.unredo.mark()

        try:
            old_height = self.row_heights.pop((row, tab))

        except KeyError:
            old_height = None

        if height is not None:
            self.row_heights[(row, tab)] = float(height)

        # Make undoable

        undo_operation = (self.set_row_height,
                          [row, tab, old_height, mark_unredo])
        redo_operation = (self.set_row_height, [row, tab, height, mark_unredo])

        self.unredo.append(undo_operation, redo_operation)

        if mark_unredo:
            self.unredo.mark()

    def set_col_width(self, col, tab, width, mark_unredo=True):
        """Sets column width"""

        if mark_unredo:
            self.unredo.mark()

        try:
            old_width = self.col_widths.pop((col, tab))

        except KeyError:
            old_width = None

        if width is not None:
            self.col_widths[(col, tab)] = float(width)

        # Make undoable

        undo_operation = (self.set_col_width,
                          [col, tab, old_width, mark_unredo])
        redo_operation = (self.set_col_width, [col, tab, width, mark_unredo])

        self.unredo.append(undo_operation, redo_operation)

        if mark_unredo:
            self.unredo.mark()

    # Element access via call

    __call__ = __getitem__

# End of class DataArray

# -----------------------------------------------------------------------------


class CodeArray(DataArray):
    """CodeArray provides objects when accessing cells via __getitem__

    Cell code can be accessed via function call

    This class represents layer 3 of the model.

    """

    # Cache for results from __getitem__ calls
    result_cache = {}

    # Cache for frozen objects
    frozen_cache = {}

    def __setitem__(self, key, value, mark_unredo=True):
        """Sets cell code and resets result cache"""

        # Prevent unchanged cells from being recalculated on cursor movement

        repr_key = repr(key)

        unchanged = (repr_key in self.result_cache and
                     value == self(key)) or \
                    ((value is None or value == "") and
                     repr_key not in self.result_cache)

        DataArray.__setitem__(self, key, value, mark_unredo=mark_unredo)

        if not unchanged:
            # Reset result cache
            self.result_cache = {}

    def __getitem__(self, key):
        """Returns _eval_cell"""

        # Frozen cell handling
        if all(type(k) is not SliceType for k in key):
            frozen_res = self.cell_attributes[key]["frozen"]
            if frozen_res:
                if repr(key) in self.frozen_cache:
                    return self.frozen_cache[repr(key)]
                else:
                    # Frozen cache is empty.
                    # Maybe we have a reload without the frozen cache
                    result = self._eval_cell(key, self(key))
                    self.frozen_cache[repr(key)] = result
                    return result

        # Normal cell handling

        if repr(key) in self.result_cache:
            return self.result_cache[repr(key)]

        elif self(key) is not None:
            result = self._eval_cell(key, self(key))
            self.result_cache[repr(key)] = result

            return result

    def _make_nested_list(self, gen):
        """Makes nested list from generator for creating numpy.array"""

        res = []

        for ele in gen:
            if ele is None:
                res.append(None)

            elif not is_string_like(ele) and is_generator_like(ele):
                # Nested generator
                res.append(self._make_nested_list(ele))

            else:
                res.append(ele)

        return res

    def _get_assignment_target_end(self, ast_module):
        """Returns position of 1st char after assignment traget.

        If there is no assignment, -1 is returned

        If there are more than one of any ( expressions or assigments)
        then a ValueError is raised.

        """

        if len(ast_module.body) > 1:
            raise ValueError("More than one expression or assignment.")

        elif len(ast_module.body) > 0 and \
                type(ast_module.body[0]) is ast.Assign:
            if len(ast_module.body[0].targets) != 1:
                raise ValueError("More than one assignment target.")
            else:
                return len(ast_module.body[0].targets[0].id)

        return -1

    def _get_updated_environment(self, env_dict=None):
        """Returns globals environment with 'magic' variable

        Parameters
        ----------
        env_dict: Dict, defaults to {'S': self}
        \tDict that maps global variable name to value

        """

        if env_dict is None:
            env_dict = {'S': self}

        env = globals().copy()
        env.update(env_dict)

        return env

    def _eval_cell(self, key, code):
        """Evaluates one cell and returns its result"""

        # Set up environment for evaluation

        env_dict = {'X': key[0], 'Y': key[1], 'Z': key[2], 'bz2': bz2,
                    'base64': base64, 'charts': charts,
                    'R': key[0], 'C': key[1], 'T': key[2], 'S': self}
        env = self._get_updated_environment(env_dict=env_dict)

        _old_code = self(key)

        # Return cell value if in safe mode

        if self.safe_mode:
            return code

        # If cell is not present return None

        if code is None:
            return

        elif is_generator_like(code):
            # We have a generator object

            return numpy.array(self._make_nested_list(code), dtype="O")

        # If only 1 term in front of the "=" --> global

        try:
            assignment_target_error = None
            module = ast.parse(code)
            assignment_target_end = self._get_assignment_target_end(module)

        except ValueError, err:
            assignment_target_error = ValueError(err)

        except AttributeError, err:
            # Attribute Error includes RunTimeError
            assignment_target_error = AttributeError(err)

        except Exception, err:
            assignment_target_error = Exception(err)

        if assignment_target_error is None and assignment_target_end != -1:
            glob_var = code[:assignment_target_end]
            expression = code.split("=", 1)[1]
            expression = expression.strip()

            # Delete result cache because assignment changes results
            self.result_cache.clear()

        else:
            glob_var = None
            expression = code

        if assignment_target_error is not None:
            result = assignment_target_error

        else:
            import time
            try:
                #result = eval(expression, env, {})
                self.eval_manager.add_task(expression, None, key)
                self.eval_manager.main_window.log.append(
                        "%s\t Adding task to queue: %s on %s" %
                        (time.time(), expression, key))
                result = '...'

            except AttributeError, err:
                # Attribute Error includes RunTimeError
                result = AttributeError(err)

            except Exception, err:
                result = Exception(err)

        # Change back cell value for evaluation from other cells
        self.dict_grid[key] = _old_code

        if glob_var is not None:
            globals().update({glob_var: result})

        return result

    def pop(self, key, mark_unredo=True):
        """Pops dict_grid with undo and redo support

        Parameters
        ----------
        key: 3-tuple of Integer
        \tCell key that shall be popped
        mark_unredo: Boolean, defaults to True
        \tIf True then an unredo marker is set after the operation

        """

        try:
            self.result_cache.pop(repr(key))

        except KeyError:
            pass

        return DataArray.pop(self, key, mark_unredo=mark_unredo)

    def reload_modules(self):
        """Reloads modules that are available in cells"""

        import src.lib.charts as charts
        modules = [charts, bz2, base64, re, ast, sys, wx, numpy, datetime]

        for module in modules:
            reload(module)

    def clear_globals(self):
        """Clears all newly assigned globals"""

        base_keys = ['cStringIO', 'IntType', 'KeyValueStore', 'UnRedo',
                     'is_generator_like', 'is_string_like', 'bz2', 'base64',
                     '__package__', 're', 'config', '__doc__', 'SliceType',
                     'CellAttributes', 'product', 'ast', '__builtins__',
                     '__file__', 'charts', 'sys', 'is_slice_like', '__name__',
                     'copy', 'imap', 'wx', 'ifilter', 'Selection', 'DictGrid',
                     'numpy', 'CodeArray', 'DataArray', 'datetime']

        for key in globals().keys():
            if key not in base_keys:
                globals().pop(key)

    def execute_macros(self):
        """Executes all macros and returns result string

        Executes macros only when not in safe_mode

        """

        if self.safe_mode:
            return '', "Safe mode activated. Code not executed."

        # Windows exec does not like Windows newline
        self.macros = self.macros.replace('\r\n', '\n')

        # Set up environment for evaluation
        globals().update(self._get_updated_environment())

        # Create file-like string to capture output
        code_out = cStringIO.StringIO()
        code_err = cStringIO.StringIO()
        err_msg = cStringIO.StringIO()

        # Capture output and errors
        sys.stdout = code_out
        sys.stderr = code_err

        try:
            exec(self.macros, globals())

        except Exception, err:
            # Print exception
            # (Because of how the globals are handled during execution we must import modules here)
            from traceback import print_exception
            from src.lib.exception_handeling import get_user_codeframe
            exc_info = sys.exc_info()
            user_tb = get_user_codeframe(exc_info[2]) or exc_info[2]
            print_exception(exc_info[0],exc_info[1],user_tb,None,err_msg)
        # Restore stdout and stderr
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

        results = code_out.getvalue()
        errs = code_err.getvalue() + err_msg.getvalue()

        code_out.close()
        code_err.close()

        # Reset result cache
        self.result_cache.clear()

        # Reset frozen cache
        self.frozen_cache.clear()

        return results, errs

    def _sorted_keys(self, keys, startkey, reverse=False):
        """Generator that yields sorted keys starting with startkey

        Parameters
        ----------

        keys: Iterable of tuple/list
        \tKey sequence that is sorted
        startkey: Tuple/list
        \tFirst key to be yielded
        reverse: Bool
        \tSort direction reversed if True

        """

        tuple_key = lambda t: t[::-1]
        if reverse:
            tuple_cmp = lambda t: t[::-1] > startkey[::-1]
        else:
            tuple_cmp = lambda t: t[::-1] < startkey[::-1]

        searchkeys = sorted(keys, key=tuple_key, reverse=reverse)
        searchpos = sum(1 for _ in ifilter(tuple_cmp, searchkeys))

        searchkeys = searchkeys[searchpos:] + searchkeys[:searchpos]

        for key in searchkeys:
            yield key

    def string_match(self, datastring, findstring, flags=None):
        """
        Returns position of findstring in datastring or None if not found.
        Flags is a list of strings. Supported strings are:
         * "MATCH_CASE": The case has to match for valid find
         * "WHOLE_WORD": The word has to be surrounded by whitespace characters
                         if in the middle of the string
         * "REG_EXP":    A regular expression is evaluated.

        """

        if type(datastring) is IntType:  # Empty cell
            return None

        if flags is None:
            flags = []

        if "REG_EXP" in flags:
            match = re.search(findstring, datastring)
            if match is None:
                pos = -1
            else:
                pos = match.start()
        else:
            if "MATCH_CASE" not in flags:
                datastring = datastring.lower()
                findstring = findstring.lower()

            if "WHOLE_WORD" in flags:
                pos = -1
                matchstring = r'\b' + findstring + r'+\b'
                for match in re.finditer(matchstring, datastring):
                    pos = match.start()
                    break  # find 1st occurrance
            else:
                pos = datastring.find(findstring)

        if pos == -1:
            return None
        else:
            return pos

    def findnextmatch(self, startkey, find_string, flags, search_result=True):
        """ Returns a tuple with the position of the next match of find_string

        Returns None if string not found.

        Parameters:
        -----------
        startkey:   Start position of search
        find_string:String to be searched for
        flags:      List of strings, out of
                    ["UP" xor "DOWN", "WHOLE_WORD", "MATCH_CASE", "REG_EXP"]
        search_result: Bool, defaults to True
        \tIf True then the search includes the result string (slower)

        """

        assert "UP" in flags or "DOWN" in flags
        assert not ("UP" in flags and "DOWN" in flags)

        if search_result:
            def is_matching(key, find_string, flags):
                code = self(key)
                if self.string_match(code, find_string, flags) is not None:
                    return True
                else:
                    res_str = unicode(self[key])
                    return self.string_match(res_str, find_string, flags) \
                        is not None

        else:
            def is_matching(code, find_string, flags):
                code = self(key)
                return self.string_match(code, find_string, flags) is not None

        # List of keys in sgrid in search order

        reverse = "UP" in flags

        for key in self._sorted_keys(self.keys(), startkey, reverse=reverse):
            if is_matching(key, find_string, flags):
                return key

    def set_eval_manager(self, eval_manager):
        self.eval_manager = eval_manager

# End of class CodeArray
