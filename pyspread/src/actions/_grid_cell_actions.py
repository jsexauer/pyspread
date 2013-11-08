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

import wx

import src.lib.i18n as i18n
from src.lib.selection import Selection
from src.actions._main_window_actions import Actions
from src.gui._events import post_command_event

#use ugettext instead of getttext to avoid unicode errors
_ = i18n.language.ugettext


"""
_grid_cell_actions.py
=======================

Module for cell level main grid level actions.

Provides:
---------
  1. CellActions: Changes to cell code

"""


class CellActions(Actions):
    """Mixin class that supplies Cell code additions, changes and deletion"""

    def set_code(self, key, code):
        """Sets code of cell key, marks grid as changed"""

        old_code = self.grid.code_array(key)

        try:
            old_code = unicode(old_code, encoding="utf-8")

        except TypeError:
            pass

        if not (old_code is None and not code) and code != old_code:
            # Mark content as changed
            post_command_event(self.main_window, self.ContentChangedMsg,
                               changed=True)

        # Set cell code
        self.grid.code_array[key] = code

    def delete_cell(self,  key, mark_unredo=True):
        """Deletes key cell"""

        try:
            self.code_array.pop(key, mark_unredo=mark_unredo)

        except KeyError:
            pass

        self.grid.code_array.result_cache.clear()

    def _get_absolute_reference(self, ref_key):
        """Returns absolute reference code for key."""

        key_str = u", ".join(map(str, ref_key))
        return u"S[" + key_str + u"]"

    def _get_relative_reference(self, cursor, ref_key):
        """Returns absolute reference code for key.

        Parameters
        ----------

        cursor: 3-tuple of Integer
        \tCurrent cursor position
        ref_key: 3-tuple of Integer
        \tAbsolute reference key

        """

        magics = ["X", "Y", "Z"]

        # mapper takes magic, key, ref_key to build string
        def get_rel_key_ele(cursor_ele, ref_key_ele):
            """Returns relative key suffix for given key and reference key"""

            # cursor is current cursor position
            # ref_key is absolute target position

            diff_key_ele = ref_key_ele - cursor_ele

            if diff_key_ele == 0:
                return u""

            elif diff_key_ele < 0:
                return u"-" + str(abs(diff_key_ele))

            elif diff_key_ele > 0:
                return u"+" + str(diff_key_ele)

            else:
                msg = _("{key} seems to be no Integer")
                msg = msg.format(key=diff_key_ele)
                raise ValueError(msg)

        key_strings = []

        for magic, cursor_ele, ref_key_ele in zip(magics, cursor, ref_key):
            key_strings.append(magic +
                               get_rel_key_ele(cursor_ele, ref_key_ele))

        key_string = u", ".join(key_strings)

        return u"S[" + key_string + u"]"

    def append_reference_code(self, key, ref_key, ref_type="absolute"):
        """Appends reference code to cell code.

        Replaces existing reference.

        Parameters
        ----------
        key: 3-tuple of Integer
        \tKey of cell that gets the reference
        ref_key: 3-tuple of Integer
        \tKey of cell that is referenced
        ref_type: Sting in ["absolute", "relative"]
        \tAn absolute or a relative reference is added

        """

        if ref_type == "absolute":
            code = self._get_absolute_reference(ref_key)

        elif ref_type == "relative":
            code = self._get_relative_reference(key, ref_key)

        else:
            raise ValueError(_('ref_type has to be "absolute" or "relative".'))

        old_code = self.grid.code_array(key)

        if old_code is None:
            old_code = u""

        if "S" in old_code and old_code[-1] == "]":
            old_code_left, __ = old_code.rsplit("S", 1)
            new_code = old_code_left + code
        else:
            new_code = old_code + code

        post_command_event(self.grid.main_window, self.EntryLineMsg,
                           text=new_code)

        return new_code  # For unit tests

    def _set_cell_attr(self, selection, table, attr):
        """Sets cell attr for key cell and mark grid content as changed

        Parameters
        ----------

        attr: dict
        \tContains cell attribute keys
        \tkeys in ["borderwidth_bottom", "borderwidth_right",
        \t"bordercolor_bottom", "bordercolor_right",
        \t"bgcolor", "textfont",
        \t"pointsize", "fontweight", "fontstyle", "textcolor", "underline",
        \t"strikethrough", "angle", "column-width", "row-height",
        \t"vertical_align", "justification", "frozen", "merge_area"]

        """

        # Mark content as changed
        post_command_event(self.main_window, self.ContentChangedMsg,
                           changed=True)

        if selection is not None:
            cell_attributes = self.code_array.cell_attributes
            cell_attributes.undoable_append((selection, table, attr),
                                            mark_unredo=False)

    def set_attr(self, attr, value, selection=None, mark_unredo=True):
        """Sets attr of current selection to value"""

        if selection is None:
            selection = self.grid.selection

        if not selection:
            # Add current cell to selection so that it gets changed
            selection.cells.append(self.grid.actions.cursor[:2])

        attrs = {attr: value}

        table = self.grid.current_table

        # Change model
        self._set_cell_attr(selection, table, attrs)

        if mark_unredo:
            self.code_array.unredo.mark()

    def set_border_attr(self, attr, value, borders):
        """Sets border attribute by adjusting selection to borders

        Parameters
        ----------
        attr: String in ["borderwidth", "bordercolor"]
        \tBorder attribute that shall be changed
        value: wx.Colour or Integer
        \tAttribute value dependent on attribute type
        borders: Iterable over "top", "bottom", "left", "right", "inner"
        \tSpecifies to which borders of the selection the attr is applied

        """

        selection = self.grid.selection
        if not selection:
            selection.cells.append(self.grid.actions.cursor[:2])

        # determine selection for core cells and selection for border cells
        # Then apply according to inner and outer
        # A cell is inner iif it is not at the edge of the selection bbox

        if "inner" in borders:
            if "top" in borders:
                adj_selection = selection + (-1, 0)
                self.set_attr(attr + "_bottom", value, adj_selection,
                              mark_unredo=False)

            if "bottom" in borders:
                self.set_attr(attr + "_bottom", value, mark_unredo=False)

            if "left" in borders:
                adj_selection = selection + (0, -1)
                self.set_attr(attr + "_right", value, adj_selection,
                              mark_unredo=False)

            if "right" in borders:
                self.set_attr(attr + "_right", value, mark_unredo=False)

        else:
            # Adjust selection so that only bounding box edge is in selection
            bbox_tl, bbox_lr = selection.get_bbox()
            if "top" in borders:
                adj_selection = Selection([bbox_tl],
                                          [(bbox_tl[0], bbox_lr[1])],
                                          [], [], []) + (-1, 0)
                self.set_attr(attr + "_bottom", value, adj_selection,
                              mark_unredo=False)

            if "bottom" in borders:
                adj_selection = Selection([(bbox_lr[0], bbox_tl[1])],
                                          [bbox_lr], [], [], [])
                self.set_attr(attr + "_bottom", value, adj_selection,
                              mark_unredo=False)

            if "left" in borders:
                adj_selection = Selection([bbox_tl],
                                          [(bbox_lr[0], bbox_tl[1])],
                                          [], [], []) + (0, -1)
                self.set_attr(attr + "_right", value, adj_selection,
                              mark_unredo=False)

            if "right" in borders:
                adj_selection = Selection([(bbox_tl[0], bbox_lr[1])],
                                          [bbox_lr], [], [], [])
                self.set_attr(attr + "_right", value, adj_selection,
                              mark_unredo=False)

        self.code_array.unredo.mark()

    def toggle_attr(self, attr):
        """Toggles an attribute attr for current selection"""

        selection = self.grid.selection

        # Selection or single cell access?

        if selection:
            value = self.get_new_selection_attr_state(selection, attr)

        else:
            value = self.get_new_cell_attr_state(self.grid.actions.cursor,
                                                 attr)

        # Set the toggled value

        self.set_attr(attr, value, mark_unredo=False)

        self.code_array.unredo.mark()

    # Only cell attributes that can be toggled are contained

    def change_frozen_attr(self):
        """Changes frozen state of cell if there is no selection"""

        if self.grid.selection:
            boxes = zip(self.grid.selection.block_tl,
                        self.grid.selection.block_br)
        else:
            cursor = self.grid.actions.cursor[:2]
            boxes = [( cursor, cursor )]
        tab = self.grid.actions.cursor[2]

        alreadyToggled = []    # Store cells we've already toggled
        for box in boxes:
            for x in xrange(box[0][0], box[1][0]+1):
                for y in xrange(box[0][1], box[1][1]+1):
                    cell = (x,y,tab)
                    if cell not in alreadyToggled:
                        alreadyToggled.append(cell)
                        self._change_frozen_attr_single(cell)
        self.code_array.unredo.mark()

    def _change_frozen_attr_single(self, cursor):
        """Helper for change_frozen_attr.  Changes a single cell at cursor."""

        frozen = self.grid.code_array.cell_attributes[cursor]["frozen"]

        if frozen:
            # We have an frozen cell that has to be unfrozen

            # Delete frozen cache content
            self.grid.code_array.frozen_cache.pop(repr(cursor))

        else:
            # We have an non-frozen cell that has to be frozen

            # Add frozen cache content
            res_obj = self.grid.code_array[cursor]
            self.grid.code_array.frozen_cache[repr(cursor)] = res_obj

        # Set the new frozen state / code
        selection = Selection([], [], [], [], [cursor[:2]])
        self.set_attr("frozen", not frozen, selection=selection,
                      mark_unredo=False)

    def unmerge(self, unmerge_area, tab):
        """Unmerges all cells in unmerge_area"""

        top, left, bottom, right = unmerge_area
        selection = Selection([(top, left)], [(bottom, right)], [], [], [])
        attr = {"merge_area": None}

        self._set_cell_attr(selection, tab, attr)

    def merge(self, merge_area, tab):
        """Merges top left cell with all cells until bottom_right"""

        top, left, bottom, right = merge_area
        selection = Selection([(top, left)], [(bottom, right)], [], [], [])
        attr = {"merge_area": merge_area}

        self._set_cell_attr(selection, tab, attr)

    def merge_selected_cells(self, selection):
        """Merges or unmerges cells that are in the selection bounding box

        Parameters
        ----------
        selection: Selection object
        \tSelection for which attr toggle shall be returned

        """

        tab = self.grid.current_table

        # Get the selection bounding box
        bbox = selection.get_bbox()
        if bbox is None:
            row, col, tab = self.grid.actions.cursor
            (bb_top, bb_left), (bb_bottom, bb_right) = (row, col), (row, col)
        else:
            (bb_top, bb_left), (bb_bottom, bb_right) = bbox
        merge_area = bb_top, bb_left, bb_bottom, bb_right

        # Check if top-left cell is already merged
        cell_attributes = self.grid.code_array.cell_attributes
        tl_merge_area = cell_attributes[(bb_top, bb_left, tab)]["merge_area"]

        if tl_merge_area is None:
            self.merge(merge_area, tab)
        else:
            self.unmerge(tl_merge_area, tab)

    attr_toggle_values = {
        "fontweight": [wx.NORMAL, wx.BOLD],
        "fontstyle": [wx.NORMAL, wx.ITALIC],
        "underline": [False, True],
        "strikethrough": [False, True],
        "vertical_align": ["top", "middle", "bottom"],
        "justification": ["left", "center", "right"],
        "frozen": [True, False],
    }

    def get_new_cell_attr_state(self, key, attr_key):
        """Returns new attr cell state for toggles

        Parameters
        ----------
        key: 3-Tuple
        \tCell for which attr toggle shall be returned
        attr_key: Hashable
        \tAttribute key

        """

        cell_attributes = self.grid.code_array.cell_attributes
        attr_values = self.attr_toggle_values[attr_key]

        # Map attr_value to next attr_value
        attr_map = dict(zip(attr_values, attr_values[1:] + attr_values[:1]))

        # Return next value from attr_toggle_values value list

        return attr_map[cell_attributes[key][attr_key]]

    def get_new_selection_attr_state(self, selection, attr_key):
        """Toggles new attr selection state and returns it

        Parameters
        ----------
        selection: Selection object
        \tSelection for which attr toggle shall be returned
        attr_key: Hashable
        \tAttribute key

        """

        cell_attributes = self.grid.code_array.cell_attributes
        attr_values = self.attr_toggle_values[attr_key]

        # Map attr_value to next attr_value
        attr_map = dict(zip(attr_values, attr_values[1:] + attr_values[:1]))

        selection_attrs = \
            (attr for attr in cell_attributes if attr[0] == selection)

        attrs = {}
        for selection_attr in selection_attrs:
            attrs.update(selection_attr[2])

        if attr_key in attrs:
            return attr_map[attrs[attr_key]]

        else:
            # Default next value
            return self.attr_toggle_values[attr_key][1]

    def refresh_selected_frozen_cells(self, selection=None):
        """Refreshes content of frozen cells that are currently selected

        If there is no selection, the cell at the cursor is updated.

        Parameters
        ----------
        selection: Selection, defaults to None
        \tIf not None then use this selection instead of the grid selection

        """

        if selection is None:
            selection = self.grid.selection

        # Add cursor to empty selection

        if not selection:
            selection.cells.append(self.grid.actions.cursor[:2])

        cell_attributes = self.grid.code_array.cell_attributes

        for attr_selection, tab, attr_dict in cell_attributes:
            if tab == self.grid.actions.cursor[2] and \
               "frozen" in attr_dict and attr_dict["frozen"]:
                # Only single cells are allowed for freezing
                skey = attr_selection.cells[0]
                if skey in selection:
                    key = tuple(list(skey) + [tab])
                    code = self.grid.code_array(key)
                    result = self.grid.code_array._eval_cell(key, code)
                    self.grid.code_array.frozen_cache[repr(key)] = result

        cell_attributes._attr_cache.clear()
