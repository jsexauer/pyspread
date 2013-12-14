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
_grid
=====

Provides
--------
 1. Grid: The main grid of pyspread
 2. MainWindowEventHandlers: Event handlers for Grid

"""

import wx.grid

from _events import post_command_event, EventMixin

from _grid_table import GridTable
from _grid_renderer import GridRenderer
from _gui_interfaces import GuiInterfaces
from _menubars import ContextMenu
from _chart_dialog import ChartDialog

import src.lib.i18n as i18n
from src.sysvars import is_gtk

import src.lib.xrect as xrect
from src.model.model import CodeArray

from src.actions._grid_actions import AllGridActions
from src.gui._grid_cell_editor import GridCellEditor

#use ugettext instead of getttext to avoid unicode errors
_ = i18n.language.ugettext


class Grid(wx.grid.Grid, EventMixin):
    """Pyspread's main grid"""

    def __init__(self, main_window, *args, **kwargs):
        S = kwargs.pop("S")

        self.main_window = main_window

        self._states()

        self.interfaces = GuiInterfaces(self.main_window)

        if S is None:
            dimensions = kwargs.pop("dimensions")
        else:
            dimensions = S.shape
            kwargs.pop("dimensions")

        wx.grid.Grid.__init__(self, main_window, *args, **kwargs)

        self.SetDefaultCellBackgroundColour(wx.Colour(255, 255, 255, 255))

        # Cursor position on entering selection mode
        self.sel_mode_cursor = None

        # Set multi line editor
        self.SetDefaultEditor(GridCellEditor(main_window))

        # Create new grid
        if S is None:
            self.code_array = CodeArray(dimensions)
            post_command_event(self, self.GridActionNewMsg, shape=dimensions)
        else:
            self.code_array = S

        _grid_table = GridTable(self, self.code_array)
        self.SetTable(_grid_table, True)

        # Grid renderer draws the grid
        self.grid_renderer = GridRenderer(self.code_array)
        self.SetDefaultRenderer(self.grid_renderer)

        # Context menu for quick access of important functions
        self.contextmenu = ContextMenu(parent=self)

        # Handler classes contain event handler methods
        self.handlers = GridEventHandlers(self)
        self.cell_handlers = GridCellEventHandlers(self)

        # Grid actions
        self.actions = AllGridActions(self)

        # Layout and bindings
        self._layout()
        self._bind()

        # Update toolbars
        self.update_entry_line()
        self.update_attribute_toolbar()

        # Focus on grid so that typing can start immediately
        self.SetFocus()

    def _states(self):
        """Sets grid states"""

        # The currently visible table
        self.current_table = 0

        # The cell that has been selected before the latest selection
        self._last_selected_cell = 0, 0, 0

        # If we are viewing cells based on their frozen status or normally
        #  (When true, a cross-hatch is displayed for frozen cells)
        self._view_frozen = False

    def _layout(self):
        """Initial layout of grid"""

        self.EnableGridLines(False)

        # Standard row and col sizes for zooming
        self.std_row_size = self.GetRowSize(0)
        self.std_col_size = self.GetColSize(0)

        # Standard row and col label sizes for zooming
        self.col_label_size = self.GetColLabelSize()
        self.row_label_size = self.GetRowLabelSize()

        self.SetRowMinimalAcceptableHeight(1)
        self.SetColMinimalAcceptableWidth(1)

        self.SetCellHighlightPenWidth(0)

    def _bind(self):
        """Bind events to handlers"""

        main_window = self.main_window

        handlers = self.handlers
        c_handlers = self.cell_handlers

        # Non wx.Grid events

        self.Bind(wx.EVT_MOUSEWHEEL, handlers.OnMouseWheel)
        self.Bind(wx.EVT_KEY_DOWN, handlers.OnKey)

        # Grid events

        self.GetGridWindow().Bind(wx.EVT_MOTION, handlers.OnMouseMotion)
        self.Bind(wx.EVT_SCROLLWIN, handlers.OnScroll)
        self.Bind(wx.grid.EVT_GRID_RANGE_SELECT, handlers.OnRangeSelected)

        # Context menu

        self.Bind(wx.grid.EVT_GRID_CELL_RIGHT_CLICK, handlers.OnContextMenu)

        # Cell code events

        main_window.Bind(self.EVT_CMD_CODE_ENTRY, c_handlers.OnCellText)

        main_window.Bind(self.EVT_CMD_INSERT_BMP, c_handlers.OnInsertBitmap)
        main_window.Bind(self.EVT_CMD_LINK_BMP, c_handlers.OnLinkBitmap)
        main_window.Bind(self.EVT_CMD_INSERT_CHART,
                         c_handlers.OnInsertChartDialog)

        # Cell attribute events

        main_window.Bind(self.EVT_CMD_FONT, c_handlers.OnCellFont)
        main_window.Bind(self.EVT_CMD_FONTSIZE, c_handlers.OnCellFontSize)
        main_window.Bind(self.EVT_CMD_FONTBOLD, c_handlers.OnCellFontBold)
        main_window.Bind(self.EVT_CMD_FONTITALICS,
                         c_handlers.OnCellFontItalics)
        main_window.Bind(self.EVT_CMD_FONTUNDERLINE,
                         c_handlers.OnCellFontUnderline)
        main_window.Bind(self.EVT_CMD_FONTSTRIKETHROUGH,
                         c_handlers.OnCellFontStrikethrough)
        main_window.Bind(self.EVT_CMD_FROZEN, c_handlers.OnCellFrozen)
        main_window.Bind(self.EVT_CMD_MERGE, c_handlers.OnMerge)
        main_window.Bind(self.EVT_CMD_JUSTIFICATION,
                         c_handlers.OnCellJustification)
        main_window.Bind(self.EVT_CMD_ALIGNMENT, c_handlers.OnCellAlignment)
        main_window.Bind(self.EVT_CMD_BORDERWIDTH,
                         c_handlers.OnCellBorderWidth)
        main_window.Bind(self.EVT_CMD_BORDERCOLOR,
                         c_handlers.OnCellBorderColor)
        main_window.Bind(self.EVT_CMD_BACKGROUNDCOLOR,
                         c_handlers.OnCellBackgroundColor)
        main_window.Bind(self.EVT_CMD_TEXTCOLOR, c_handlers.OnCellTextColor)
        main_window.Bind(self.EVT_CMD_ROTATIONDIALOG,
                         c_handlers.OnTextRotationDialog)
        main_window.Bind(self.EVT_CMD_TEXTROTATATION,
                         c_handlers.OnCellTextRotation)

        # Cell selection events

        self.Bind(wx.grid.EVT_GRID_CMD_SELECT_CELL, c_handlers.OnCellSelected)

        # Grid edit mode events

        main_window.Bind(self.EVT_CMD_ENTER_SELECTION_MODE,
                         handlers.OnEnterSelectionMode)
        main_window.Bind(self.EVT_CMD_EXIT_SELECTION_MODE,
                         handlers.OnExitSelectionMode)

        # Grid view events

        main_window.Bind(self.EVT_CMD_VIEW_FROZEN, handlers.OnViewFrozen)
        main_window.Bind(self.EVT_CMD_REFRESH_SELECTION,
                         handlers.OnRefreshSelectedCells)
        main_window.Bind(self.EVT_CMD_DISPLAY_GOTO_CELL_DIALOG,
                         handlers.OnDisplayGoToCellDialog)
        main_window.Bind(self.EVT_CMD_GOTO_CELL, handlers.OnGoToCell)
        main_window.Bind(self.EVT_CMD_ZOOM_IN, handlers.OnZoomIn)
        main_window.Bind(self.EVT_CMD_ZOOM_OUT, handlers.OnZoomOut)
        main_window.Bind(self.EVT_CMD_ZOOM_STANDARD, handlers.OnZoomStandard)

        # Find events
        main_window.Bind(self.EVT_CMD_FIND, handlers.OnFind)
        main_window.Bind(self.EVT_CMD_REPLACE, handlers.OnShowFindReplace)
        main_window.Bind(wx.EVT_FIND, handlers.OnReplaceFind)
        main_window.Bind(wx.EVT_FIND_NEXT, handlers.OnReplaceFind)
        main_window.Bind(wx.EVT_FIND_REPLACE, handlers.OnReplace)
        main_window.Bind(wx.EVT_FIND_REPLACE_ALL, handlers.OnReplaceAll)
        main_window.Bind(wx.EVT_FIND_CLOSE, handlers.OnCloseFindReplace)

        # Grid change events

        main_window.Bind(self.EVT_CMD_INSERT_ROWS, handlers.OnInsertRows)
        main_window.Bind(self.EVT_CMD_INSERT_COLS, handlers.OnInsertCols)
        main_window.Bind(self.EVT_CMD_INSERT_TABS, handlers.OnInsertTabs)

        main_window.Bind(self.EVT_CMD_DELETE_ROWS, handlers.OnDeleteRows)
        main_window.Bind(self.EVT_CMD_DELETE_COLS, handlers.OnDeleteCols)
        main_window.Bind(self.EVT_CMD_DELETE_TABS, handlers.OnDeleteTabs)

        main_window.Bind(self.EVT_CMD_SHOW_RESIZE_GRID_DIALOG,
                         handlers.OnResizeGridDialog)

        main_window.Bind(wx.grid.EVT_GRID_ROW_SIZE, handlers.OnRowSize)
        main_window.Bind(wx.grid.EVT_GRID_COL_SIZE, handlers.OnColSize)

        # Undo/Redo events

        main_window.Bind(self.EVT_CMD_UNDO, handlers.OnUndo)
        main_window.Bind(self.EVT_CMD_REDO, handlers.OnRedo)

    _get_selection = lambda self: self.actions.get_selection()
    selection = property(_get_selection, doc="Grid selection")

    # Collison helper functions for grid drawing
    # ------------------------------------------

    def get_visiblecell_slice(self):
        """Returns a tuple of 3 slices that contanins the visible cells"""

        topleft_x = self.YToRow(self.GetViewStart()[1] * self.ScrollLineX)
        topleft_y = self.XToCol(self.GetViewStart()[0] * self.ScrollLineY)
        topleft = (topleft_x, topleft_y)
        row, col = topleft_x + 1, topleft_y + 1  # Ensures visibility

        while self.IsVisible(row, topleft[1], wholeCellVisible=False):
            row += 1
        while self.IsVisible(topleft[0], col, wholeCellVisible=False):
            col += 1
        lowerright = (row, col)  # This cell is *not* visible
        return (slice(topleft[0], lowerright[0]),
                slice(topleft[1], lowerright[1]),
                slice(self.current_table, self.current_table + 1))

    def colliding_cells(self, row, col, textbox):
        """Generates distance, row, col tuples of colliding cells

        Parameters
        ----------
        row: Integer
        \tRow of cell that is tested for collision
        col: Integer
        \tColumn of cell that is tested for collision
        """

        def l1_radius_cells(dist):
            """Generator of cell index tuples with distance dist to o"""

            if not dist:
                yield 0, 0

            else:
                for pos in xrange(-dist, dist + 1):
                    yield pos, dist
                    yield pos, -dist
                    yield dist, pos
                    yield -dist, pos

        vis_cell_slice = self.get_visiblecell_slice()
        vis_row_min = vis_cell_slice[0].start
        vis_row_max = vis_cell_slice[0].stop
        vis_col_min = vis_cell_slice[1].start
        vis_col_max = vis_cell_slice[1].stop

        max_visible_distance = max(vis_row_max - row, vis_col_max - col,
                                   row - vis_row_min, col - vis_col_min)

        for dist in xrange(max_visible_distance):
            all_empty = True

            for radius_row, radius_col in l1_radius_cells(dist + 1):
                __row = radius_row + row
                __col = radius_col + col

                if vis_row_min <= __row <= vis_row_max and \
                   vis_col_min <= __col <= vis_col_max and \
                   self.IsVisible(__row, __col, wholeCellVisible=False):
                    cell_rect = self.CellToRect(__row, __col)
                    cell_rect = xrect.Rect(cell_rect.x, cell_rect.y,
                                           cell_rect.width, cell_rect.height)
                    if textbox.collides_axisaligned_rect(cell_rect):
                        all_empty = False
                        yield dist + 1, __row, __col

            # If there are no collisions in a circle, we break

            if all_empty:
                break

    def get_block_direction(self, rect_row, rect_col, block_row, block_col):
        """Returns a blocking direction string from UP DOWN RIGHT LEFT"""

        diff_row = rect_row - block_row
        diff_col = rect_col - block_col

        assert not diff_row == diff_col == 0

        if abs(diff_row) <= abs(diff_col):
            # Columns are dominant
            if rect_col < block_col:
                return "RIGHT"
            else:
                return "LEFT"
        else:
            # Rows are dominant
            if rect_row < block_row:
                return "DOWN"
            else:
                return "UP"

    def is_merged_cell_drawn(self, key):
        """True if key in merged area shall be drawn

        This is the case if it is the top left most visible key of the merge
        area on the screen.

        """

        row, col, tab = key

        # Is key not merged? --> False
        cell_attributes = self.code_array.cell_attributes

        top, left, __ = cell_attributes.get_merging_cell(key)

        # Case 1: Top left cell of merge is visible
        # --> Only top left cell returns True
        top_left_drawn = \
            row == top and col == left and \
            self.IsVisible(row, col, wholeCellVisible=False)

        # Case 2: Leftmost column is visible
        # --> Only top visible leftmost cell returns True

        left_drawn = \
            col == left and \
            self.IsVisible(row, col, wholeCellVisible=False) and \
            not self.IsVisible(row-1, col, wholeCellVisible=False)

        # Case 3: Top row is visible
        # --> Only left visible top cell returns True

        top_drawn = \
            row == top and \
            self.IsVisible(row, col, wholeCellVisible=False) and \
            not self.IsVisible(row, col-1, wholeCellVisible=False)

        # Case 4: Top row and leftmost column are invisible
        # --> Only top left visible cell returns True

        middle_drawn = \
            self.IsVisible(row, col, wholeCellVisible=False) and \
            not self.IsVisible(row-1, col, wholeCellVisible=False) and \
            not self.IsVisible(row, col-1, wholeCellVisible=False)

        return top_left_drawn or left_drawn or top_drawn or middle_drawn

    def update_entry_line(self, key=None):
        """Updates the entry line

        Parameters
        ----------
        key: 3-tuple of Integer, defaults to current cell
        \tCell to which code the entry line is updated

        """

        if key is None:
            key = self.actions.cursor

        cell_code = self.GetTable().GetValue(*key)

        post_command_event(self, self.EntryLineMsg, text=cell_code)

    def update_attribute_toolbar(self, key=None):
        """Updates the attribute toolbar

        Parameters
        ----------
        key: 3-tuple of Integer, defaults to current cell
        \tCell to which attributes the attributes toolbar is updated

        """

        if key is None:
            key = self.actions.cursor

        post_command_event(self, self.ToolbarUpdateMsg, key=key,
                           attr=self.code_array.cell_attributes[key])

# End of class Grid


class GridCellEventHandlers(object):
    """Contains grid cell event handlers incl. attribute events"""

    def __init__(self, grid):
        self.grid = grid

    # Cell code entry events

    def OnCellText(self, event):
        """Text entry event handler"""

        row, col, _ = self.grid.actions.cursor

        self.grid.GetTable().SetValue(row, col, event.code)

        event.Skip()

    def OnInsertBitmap(self, event):
        """Insert bitmap event handler"""

        # Get file name
        wildcard = "*"
        message = _("Select bitmap for current cell")
        style = wx.OPEN | wx.CHANGE_DIR
        filepath, __ = \
            self.grid.interfaces.get_filepath_findex_from_user(wildcard,
                                                               message, style)

        try:
            img = wx.EmptyImage(1, 1)
            img.LoadFile(filepath)
        except TypeError:
            return

        if img.GetSize() == (-1, -1):
            # Bitmap could not be read
            return

        key = self.grid.actions.cursor
        code = self.grid.main_window.actions.img2code(key, img)

        self.grid.actions.set_code(key, code)

    def OnLinkBitmap(self, event):
        """Link bitmap event handler"""

        # Get file name
        wildcard = "*"
        message = _("Select bitmap for current cell")
        style = wx.OPEN | wx.CHANGE_DIR
        filepath, __ = \
            self.grid.interfaces.get_filepath_findex_from_user(wildcard,
                                                               message, style)
        try:
            bmp = wx.Bitmap(filepath)
        except TypeError:
            return

        if bmp.Size == (-1, -1):
            # Bitmap could not be read
            return

        code = "wx.Bitmap(r'{filepath}')".format(filepath=filepath)

        key = self.grid.actions.cursor
        self.grid.actions.set_code(key, code)

    def OnInsertChartDialog(self, event):
        """Chart dialog event handler"""

        key = self.grid.actions.cursor

        cell_code = self.grid.code_array(key)

        if cell_code is None:
            cell_code = u""

        chart_dialog = ChartDialog(self.grid.main_window, key, cell_code)

        if chart_dialog.ShowModal() == wx.ID_OK:
            code = chart_dialog.get_code()
            key = self.grid.actions.cursor
            self.grid.actions.set_code(key, code)

    # Cell attribute events

    def OnCellFont(self, event):
        """Cell font event handler"""

        self.grid.actions.set_attr("textfont", event.font)

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellFontSize(self, event):
        """Cell font size event handler"""

        self.grid.actions.set_attr("pointsize", event.size)

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellFontBold(self, event):
        """Cell font bold event handler"""

        try:
            try:
                weight = getattr(wx, event.weight[2:])

            except AttributeError:
                msg = _("Weight {weight} unknown").format(weight=event.weight)
                raise ValueError(msg)

            self.grid.actions.set_attr("fontweight", weight)

        except AttributeError:
            self.grid.actions.toggle_attr("fontweight")

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellFontItalics(self, event):
        """Cell font italics event handler"""

        try:
            try:
                style = getattr(wx, event.style[2:])

            except AttributeError:
                msg = _("Style {style} unknown").format(style=event.style)
                raise ValueError(msg)

            self.grid.actions.set_attr("fontstyle", style)

        except AttributeError:
            self.grid.actions.toggle_attr("fontstyle")

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellFontUnderline(self, event):
        """Cell font underline event handler"""

        self.grid.actions.toggle_attr("underline")

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellFontStrikethrough(self, event):
        """Cell font strike through event handler"""

        self.grid.actions.toggle_attr("strikethrough")

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellFrozen(self, event):
        """Cell frozen event handler"""

        self.grid.actions.change_frozen_attr()

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnMerge(self, event):
        """Merge cells event handler"""

        self.grid.actions.merge_selected_cells(self.grid.selection)

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

    def OnCellJustification(self, event):
        """Horizontal cell justification event handler"""

        self.grid.actions.toggle_attr("justification")

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellAlignment(self, event):
        """Vertical cell alignment event handler"""

        self.grid.actions.toggle_attr("vertical_align")

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellBorderWidth(self, event):
        """Cell border width event handler"""

        self.grid.actions.set_border_attr("borderwidth",
                                          event.width, event.borders)

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellBorderColor(self, event):
        """Cell border color event handler"""

        self.grid.actions.set_border_attr("bordercolor",
                                          event.color, event.borders)

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellBackgroundColor(self, event):
        """Cell background color event handler"""

        self.grid.actions.set_attr("bgcolor", event.color)

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnCellTextColor(self, event):
        """Cell text color event handler"""

        self.grid.actions.set_attr("textcolor", event.color)

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        event.Skip()

    def OnTextRotationDialog(self, event):
        """Text rotation dialog event handler"""

        cond_func = lambda i: 0 <= i <= 359
        get_int = self.grid.interfaces.get_int_from_user
        angle = get_int(_("Enter text angle in degrees."), cond_func)

        if angle is not None:
            post_command_event(self.grid.main_window,
                               self.grid.TextRotationMsg, angle=angle)

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

    def OnCellTextRotation(self, event):
        """Cell text rotation event handler"""

        self.grid.actions.set_attr("angle", event.angle, mark_unredo=True)

        self.grid.ForceRefresh()

        self.grid.update_attribute_toolbar()

        if is_gtk():
            wx.Yield()

        event.Skip()

    def OnCellSelected(self, event):
        """Cell selection event handler"""

        # If in selection mode do nothing
        # This prevents the current cell from changing
        if not self.grid.IsEditable():
            return

        key = row, col, tab = event.Row, event.Col, self.grid.current_table

        # Is the cell merged then go to merging cell
        merge_area = self.grid.code_array.cell_attributes[key]["merge_area"]

        if merge_area is not None:
            top, left, bottom, right = merge_area
            if self.grid._last_selected_cell == (top, left, tab):
                if row == top + 1:
                    self.grid.actions.set_cursor((bottom + 1, left, tab))
                    return
                elif col == left + 1:
                    self.grid.actions.set_cursor((top, right + 1, tab))
                    return
            elif (row, col) != (top, left):
                self.grid.actions.set_cursor((top, left, tab))
                return

        # Redraw cursor
        self.grid.ForceRefresh()

        # Update entry line
        self.grid.update_entry_line(key)

        # Update attribute toolbar
        self.grid.update_attribute_toolbar(key)

        self.grid._last_selected_cell = key

        event.Skip()


class GridEventHandlers(object):
    """Contains grid event handlers"""

    def __init__(self, grid):
        self.grid = grid
        self.interfaces = grid.interfaces
        self.main_window = grid.main_window

    def OnMouseMotion(self, event):
        """Mouse motion event handler"""

        grid = self.grid

        pos_x, pos_y = grid.CalcUnscrolledPosition(event.GetPosition())

        row = grid.YToRow(pos_y)
        col = grid.XToCol(pos_x)
        tab = grid.current_table

        key = row, col, tab

        merge_area = self.grid.code_array.cell_attributes[key]["merge_area"]
        if merge_area is not None:
            top, left, bottom, right = merge_area
            row, col = top, left

        grid.actions.on_mouse_over((row, col, tab))

        event.Skip()

    def OnKey(self, event):
        """Handles non-standard shortcut events"""

        keycode = event.GetKeyCode()

        if keycode == 13 and event.ControlDown():
            # <Ctrl> + <Enter>
            grid = self.grid
            tab = grid.current_table
            grid.DisableCellEditControl()

            cells = grid.selection.get_grid_cells(grid.code_array.shape)
            if cells == []:
                # No selection, do cursor instead
                cells = [(self.grid.GetGridCursorRow(),
                          self.grid.GetGridCursorCol(),
                          tab)]

            for key in cells:
                key = (key[0], key[1], tab)
                val = grid.code_array(key)
                if val is not None:
                    if not (val[0] in ('"', "'") and val[-1] in ('"', "'")):
                        grid.actions.set_code(key, '"' + val + '"')

            grid.MoveCursorDown(False)

        # If in selection mode and <Enter> is pressed end it
        elif not self.grid.IsEditable() and keycode == 13:
            ## TODO!
            pass

        elif event.ControlDown():
            if keycode == 388:
                # Ctrl + + pressed
                post_command_event(self.grid, self.grid.ZoomInMsg)

            elif keycode == 390:
                # Ctrl + - pressed
                post_command_event(self.grid, self.grid.ZoomOutMsg)

        else:
            # No Ctrl pressed

            if keycode == 127:
                # Del pressed

                # Is grid selection present? --> Delete current cell
                if self.grid.IsSelection():
                    # Delete selection
                    self.grid.actions.delete_selection()

                else:
                    # Delete cell at cursor
                    cursor = self.grid.actions.cursor
                    self.grid.actions.delete_cell(cursor)

                # Update grid
                self.grid.ForceRefresh()

                # Do not enter cell
                return

            elif keycode == 27:
                # Esc pressed
                self.grid.actions.need_abort = True

        event.Skip()

    def OnScroll(self, event):
        """Event handler for grid scroll event"""

        event.Skip()

    def OnRangeSelected(self, event):
        """Event handler for grid selection"""

        # If grid editing is disabled then pyspread is in selection mode
        if not self.grid.IsEditable():
            selection = self.grid.selection
            row, col, __ = self.grid.sel_mode_cursor
            if (row, col) in selection:
                self.grid.ClearSelection()
            else:
                self.grid.SetGridCursor(row, col)
                post_command_event(self.grid, self.grid.SelectionMsg,
                                   selection=selection)

    # Grid view events

    def OnViewFrozen(self, event):
        """Show cells as frozen status"""

        self.grid._view_frozen = not self.grid._view_frozen

        self.grid.ForceRefresh()

        event.Skip()

    def OnDisplayGoToCellDialog(self, event):
        """Shift a given cell into view"""

        self.interfaces.display_gotocell()

        event.Skip()

    def OnGoToCell(self, event):
        """Shift a given cell into view"""

        row, col, tab = event.key

        self.grid.actions.cursor = row, col, tab
        self.grid.MakeCellVisible(row, col)

        event.Skip()

    def OnEnterSelectionMode(self, event):
        """Event handler for entering selection mode, disables cell edits"""

        self.grid.sel_mode_cursor = list(self.grid.actions.cursor)
        self.grid.EnableDragGridSize(False)
        self.grid.EnableEditing(False)

    def OnExitSelectionMode(self, event):
        """Event handler for leaving selection mode, enables cell edits"""

        self.grid.sel_mode_cursor = None
        self.grid.EnableDragGridSize(True)
        self.grid.EnableEditing(True)

    def OnRefreshSelectedCells(self, event):
        """Event handler for refreshing the selected cells via menu"""

        self.grid.actions.refresh_selected_frozen_cells()
        self.grid.ForceRefresh()

        event.Skip()

    def OnZoomIn(self, event):
        """Event handler for increasing grid zoom"""

        self.grid.actions.zoom_in()

        event.Skip()

    def OnZoomOut(self, event):
        """Event handler for decreasing grid zoom"""

        self.grid.actions.zoom_out()

        event.Skip()

    def OnZoomStandard(self, event):
        """Event handler for resetting grid zoom"""

        self.grid.actions.zoom(zoom=1.0)

        event.Skip()

    def OnContextMenu(self, event):
        """Context menu event handler"""

        self.grid.PopupMenu(self.grid.contextmenu)

        event.Skip()

    def OnMouseWheel(self, event):
        """Event handler for mouse wheel actions

        Invokes zoom when mouse when Ctrl is also pressed

        """

        if event.ControlDown():
            if event.WheelRotation > 0:
                post_command_event(self.grid, self.grid.ZoomInMsg)
            else:
                post_command_event(self.grid, self.grid.ZoomOutMsg)
        else:
            event.Skip()

    # Find events

    def OnFind(self, event):
        """Find functionality, called from toolbar, returns find position"""

        # Search starts in next cell after the current one
        gridpos = list(self.grid.actions.cursor)
        text, flags = event.text, event.flags
        findpos = self.grid.actions.find(gridpos, text, flags)

        if findpos is None:
            # If nothing is found mention it in the statusbar and return

            statustext = _("'{text}' not found.").format(text=text)

        else:
            # Otherwise select cell with next occurrence if successful
            self.grid.actions.cursor = findpos

            # Update statusbar
            statustext = _(u"Found '{text}' in cell {key}.")
            statustext = statustext.format(text=text, key=findpos)

        post_command_event(self.grid.main_window, self.grid.StatusBarMsg,
                           text=statustext)

        event.Skip()

    def OnShowFindReplace(self, event):
        """Calls the find-replace dialog"""

        data = wx.FindReplaceData(wx.FR_DOWN)
        dlg = wx.FindReplaceDialog(self.grid, data, "Find & Replace",
                                   wx.FR_REPLACEDIALOG)
        dlg.data = data  # save a reference to data
        dlg.Show(True)

    def _wxflag2flag(self, wxflag):
        """Converts wxPython integer flag to pyspread flag list"""

        wx_flags = {
            0: ["UP", ],
            1: ["DOWN"],
            2: ["UP", "WHOLE_WORD"],
            3: ["DOWN", "WHOLE_WORD"],
            4: ["UP", "MATCH_CASE"],
            5: ["DOWN", "MATCH_CASE"],
            6: ["UP", "WHOLE_WORD", "MATCH_CASE"],
            7: ["DOWN", "WHOLE_WORD", "MATCH_CASE"],
        }

        return wx_flags[wxflag]

    def OnReplaceFind(self, event):
        """Called when a find operation is started from F&R dialog"""

        event.text = event.GetFindString()
        event.flags = self._wxflag2flag(event.GetFlags())

        self.OnFind(event)

    def OnReplace(self, event):
        """Called when a replace operation is started, returns find position"""

        find_string = event.GetFindString()
        flags = self._wxflag2flag(event.GetFlags())
        replace_string = event.GetReplaceString()

        gridpos = list(self.grid.actions.cursor)

        findpos = self.grid.actions.find(gridpos, find_string, flags,
                                         search_result=False)

        if findpos is None:
            statustext = _(u"'{find_string}' not found.")
            statustext = statustext.format(find_string=find_string)

        else:
            self.grid.actions.replace(findpos, find_string, replace_string)
            self.grid.actions.cursor = findpos

            # Update statusbar
            statustext = _(u"Replaced '{find_string}' in cell {key} with "
                           u"{replace_string}.")
            statustext = statustext.format(find_string=find_string,
                                           key=findpos,
                                           replace_string=replace_string)

        post_command_event(self.grid.main_window, self.grid.StatusBarMsg,
                           text=statustext)

        event.Skip()

    def OnReplaceAll(self, event):
        """Called when a replace all operation is started"""

        find_string = event.GetFindString()
        flags = self._wxflag2flag(event.GetFlags())
        replace_string = event.GetReplaceString()

        findpositions = self.grid.actions.find_all(find_string, flags)

        self.grid.actions.replace_all(findpositions, find_string,
                                      replace_string)

        event.Skip()

    def OnCloseFindReplace(self, event):
        """Called when the find&replace dialog is closed"""

        event.GetDialog().Destroy()

        event.Skip()

    # Grid change events

    def _get_no_rowscols(self, bbox):
        """Returns tuple of number of rows and cols from bbox"""

        if bbox is None:
            return 1, 1
        else:
            (bb_top, bb_left), (bb_bottom, bb_right) = bbox
            if bb_top is None:
                bb_top = 0
            if bb_left is None:
                bb_left = 0
            if bb_bottom is None:
                bb_bottom = self.grid.code_array.shape[0] - 1
            if bb_right is None:
                bb_right = self.grid.code_array.shape[1] - 1

            return bb_bottom - bb_top + 1, bb_right - bb_left + 1

    def OnInsertRows(self, event):
        """Insert the maximum of 1 and the number of selected rows"""

        bbox = self.grid.selection.get_bbox()

        if bbox is None or bbox[1][0] is None:
            # Insert rows at cursor
            ins_point = self.grid.actions.cursor[0] - 1
            no_rows = 1
        else:
            # Insert at lower edge of bounding box
            ins_point = bbox[0][0] - 1
            no_rows = self._get_no_rowscols(bbox)[0]

        self.grid.actions.insert_rows(ins_point, no_rows)

        self.grid.GetTable().ResetView()

        # Update the default sized cell sizes
        self.grid.actions.zoom()

        event.Skip()

    def OnInsertCols(self, event):
        """Inserts the maximum of 1 and the number of selected columns"""

        bbox = self.grid.selection.get_bbox()

        if bbox is None or bbox[1][1] is None:
            # Insert rows at cursor
            ins_point = self.grid.actions.cursor[1] - 1
            no_cols = 1
        else:
            # Insert at right edge of bounding box
            ins_point = bbox[0][1] - 1
            no_cols = self._get_no_rowscols(bbox)[1]

        self.grid.actions.insert_cols(ins_point, no_cols)

        self.grid.GetTable().ResetView()

        # Update the default sized cell sizes
        self.grid.actions.zoom()

        event.Skip()

    def OnInsertTabs(self, event):
        """Insert one table into grid"""

        self.grid.actions.insert_tabs(self.grid.current_table - 1, 1)
        self.grid.GetTable().ResetView()
        self.grid.actions.zoom()

        event.Skip()

    def OnDeleteRows(self, event):
        """Deletes rows from all tables of the grid"""

        bbox = self.grid.selection.get_bbox()

        if bbox is None or bbox[1][0] is None:
            # Insert rows at cursor
            del_point = self.grid.actions.cursor[0]
            no_rows = 1
        else:
            # Insert at lower edge of bounding box
            del_point = bbox[0][0]
            no_rows = self._get_no_rowscols(bbox)[0]

        self.grid.actions.delete_rows(del_point, no_rows)

        self.grid.GetTable().ResetView()

        # Update the default sized cell sizes
        self.grid.actions.zoom()

        event.Skip()

    def OnDeleteCols(self, event):
        """Deletes columns from all tables of the grid"""

        bbox = self.grid.selection.get_bbox()

        if bbox is None or bbox[1][1] is None:
            # Insert rows at cursor
            del_point = self.grid.actions.cursor[1]
            no_cols = 1
        else:
            # Insert at right edge of bounding box
            del_point = bbox[0][1]
            no_cols = self._get_no_rowscols(bbox)[1]

        self.grid.actions.delete_cols(del_point, no_cols)

        self.grid.GetTable().ResetView()

        # Update the default sized cell sizes
        self.grid.actions.zoom()

        event.Skip()

    def OnDeleteTabs(self, event):
        """Deletes tables"""

        self.grid.actions.delete_tabs(self.grid.current_table, 1)
        self.grid.GetTable().ResetView()
        self.grid.actions.zoom()

        event.Skip()

    def OnResizeGridDialog(self, event):
        """Resizes current grid by appending/deleting rows, cols and tables"""

        # Get grid dimensions

        new_shape = self.interfaces.get_dimensions_from_user(no_dim=3)

        if new_shape is None:
            return

        self.grid.actions.change_grid_shape(new_shape)

        self.grid.GetTable().ResetView()

        statustext = _("Grid dimensions changed to {shape}.")
        statustext = statustext.format(shape=new_shape)
        post_command_event(self.grid.main_window, self.grid.StatusBarMsg,
                           text=statustext)

        event.Skip()

    # Grid attribute events

    def OnRowSize(self, event):
        """Row size event handler"""

        row = event.GetRowOrCol()
        tab = self.grid.current_table
        rowsize = self.grid.GetRowSize(row) / self.grid.grid_renderer.zoom

        # Detect for resizing group of rows
        rows = self.grid.GetSelectedRows()
        if len(rows) == 0:
            rows = [row,]

        # Detect for selection of rows spanning all columns
        selection = self.grid.selection
        num_cols = self.grid.code_array.shape[1]-1
        for box in zip(selection.block_tl, selection.block_br):
            leftmost_col = box[0][1]
            rightmost_col = box[1][1]
            if leftmost_col == 0 and rightmost_col == num_cols:
                rows += range(box[0][0], box[1][0]+1)

        for row in rows:
            self.grid.code_array.set_row_height(row, tab, rowsize,
                                                mark_unredo=False)
            self.grid.SetRowSize(row, rowsize)
        self.grid.code_array.unredo.mark()
        
        event.Skip()
        self.grid.Refresh()


    def OnColSize(self, event):
        """Column size event handler"""

        col = event.GetRowOrCol()
        tab = self.grid.current_table
        colsize = self.grid.GetColSize(col) / self.grid.grid_renderer.zoom

        # Detect for resizing group of cols
        cols = self.grid.GetSelectedCols()
        if len(cols) == 0:
            cols = [col,]

        # Detect for selection of rows spanning all columns
        selection = self.grid.selection
        num_rows = self.grid.code_array.shape[0]-1
        for box in zip(selection.block_tl, selection.block_br):
            top_row = box[0][0]
            bottom_row = box[1][0]
            if top_row == 0 and bottom_row == num_rows:
                cols += range(box[0][1], box[1][1]+1)

        for col in cols:
            self.grid.code_array.set_col_width(col, tab, colsize,
                                               mark_unredo=False)
            self.grid.SetColSize(col, colsize)
        self.grid.code_array.unredo.mark()

        event.Skip()
        self.grid.Refresh()

    # Undo and redo events

    def OnUndo(self, event):
        """Calls the grid undo method"""

        self.grid.actions.undo()
        self.grid.GetTable().ResetView()
        self.grid.Refresh()
        # Reset row heights and column widths by zooming
        self.grid.actions.zoom()
        # Update toolbars
        self.grid.update_entry_line()
        self.grid.update_attribute_toolbar()

    def OnRedo(self, event):
        """Calls the grid redo method"""

        self.grid.actions.redo()
        self.grid.GetTable().ResetView()
        self.grid.Refresh()
        # Reset row heights and column widths by zooming
        self.grid.actions.zoom()
        # Update toolbars
        self.grid.update_entry_line()
        self.grid.update_attribute_toolbar()

# End of class GridEventHandlers