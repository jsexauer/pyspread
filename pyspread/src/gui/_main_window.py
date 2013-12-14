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
_main_window
============

Provides:
---------
  1) MainWindow: Main window of the application pyspread

"""

import ast
import os

import wx
import wx.lib.agw.aui as aui

from matplotlib.figure import Figure

import src.lib.i18n as i18n
from src.config import config
from src.sysvars import get_python_tutorial_path, is_gtk

from _menubars import MainMenu
from _toolbars import MainToolbar, MacroToolbar, FindToolbar, AttributesToolbar
from _widgets import EntryLineToolbarPanel, StatusBar

from src.lib.clipboard import Clipboard

from _gui_interfaces import GuiInterfaces
from src.gui.icons import icons

from _grid import Grid

from _events import post_command_event, EventMixin

from src.actions._main_window_actions import AllMainWindowActions
import time

#use ugettext instead of getttext to avoid unicode errors
_ = i18n.language.ugettext


class MainWindow(wx.Frame, EventMixin):
    """Main window of pyspread"""

    def __init__(self, parent, *args, **kwargs):
        try:
            S = kwargs.pop("S")

        except KeyError:
            S = None

        wx.Frame.__init__(self, parent, *args, **kwargs)

        self.interfaces = GuiInterfaces(self)

        try:
            self._mgr = aui.AuiManager(self)

        except Exception:
            # This may fail if py.testv runs under Windows
            # Therefore, we set up a basic framework for the unit tests
            self.grid = Grid(self, -1, S=S, dimensions=(1000, 100, 3))
            self.clipboard = Clipboard()
            self.actions = AllMainWindowActions(self.grid)

        self.parent = parent

        self.handlers = MainWindowEventHandlers(self)

        # Program states
        # --------------

        self._states()

        # GUI elements
        # ------------

        # Menu Bar
        self.menubar = wx.MenuBar()
        self.main_menu = MainMenu(parent=self, menubar=self.menubar)
        self.SetMenuBar(self.menubar)

        # Disable menu item for leaving safe mode
        post_command_event(self, self.SafeModeExitMsg)

        # Status bar
        statusbar = StatusBar(self)
        self.SetStatusBar(statusbar)

        welcome_text = _("Welcome to pyspread.")
        post_command_event(self, self.StatusBarMsg, text=welcome_text)

        # Toolbars
        self.main_toolbar = MainToolbar(self, -1)
        self.macro_toolbar = MacroToolbar(self, -1)
        self.find_toolbar = FindToolbar(self, -1)
        self.attributes_toolbar = AttributesToolbar(self, -1)

        # Entry line
        self.entry_line_panel = EntryLineToolbarPanel(self, -1)

        # Main grid

        dimensions = (
            config["grid_rows"],
            config["grid_columns"],
            config["grid_tables"],
        )

        self.grid = Grid(self, -1, S=S, dimensions=dimensions)

        # Clipboard
        self.clipboard = Clipboard()

        # Main window actions

        self.actions = AllMainWindowActions(self.grid)

        # Layout and bindings

        self._set_properties()
        self._do_layout()
        self._bind()

        self.log = []

    def _states(self):
        """Sets main window states"""

        # Has the current file been changed since the last save?
        self.changed_since_save = False
        self.filepath = None

        # Print data

        self.print_data = wx.PrintData()
        # wx.PrintData properties setup from
        # http://aspn.activestate.com/ASPN/Mail/Message/wxpython-users/3471083

    def _set_properties(self):
        """Setup title, icon, size, scale, statusbar, main grid"""

        self.set_icon(icons["PyspreadLogo"])

        # Set initial size to 90% of screen
        self.SetInitialSize(config["window_size"])
        self.SetPosition(config["window_position"])

        # Without minimum size, initial size is minimum size in wxGTK
        self.SetMinSize((2, 2))

        # Leave save mode
        post_command_event(self, self.SafeModeExitMsg)

    def _set_menu_toggles(self):
        """Enable menu bar view item checkmarks"""

        toggles = [
            (self.main_toolbar, "main_window_toolbar", _("Main toolbar")),
            (self.macro_toolbar, "macro_toolbar", _("Macro toolbar")),
            (self.attributes_toolbar, "attributes_toolbar",
             _("Format toolbar")),
            (self.find_toolbar, "find_toolbar", _("Find toolbar")),
            (self.entry_line_panel, "entry_line_panel", _("Entry line")),
        ]

        for toolbar, pane_name, toggle_label in toggles:
            # Get pane from aui manager
            pane = self._mgr.GetPane(pane_name)

            # Get menu item to toggle
            toggle_id = self.menubar.FindMenuItem(_("View"), toggle_label)

            toggle_item = self.menubar.FindItemById(toggle_id)

            # Adjust toggle to pane visibility
            toggle_item.Check(pane.IsShown())

    def _do_layout(self):
        """Adds widgets to the aui manager and controls the layout"""

        # Add the toolbars to the manager

        self._mgr.AddPane(self.main_toolbar, aui.AuiPaneInfo().
                          Name("main_window_toolbar").
                          Caption(_("Main toolbar")).
                          ToolbarPane().Top().Row(0))

        self._mgr.AddPane(self.find_toolbar, aui.AuiPaneInfo().
                          Name("find_toolbar").Caption(_("Find toolbar")).
                          ToolbarPane().Top().Row(0))

        self._mgr.AddPane(self.attributes_toolbar, aui.AuiPaneInfo().
                          Name("attributes_toolbar").
                          Caption(_("Format toolbar")).
                          ToolbarPane().Top().Row(1))

        self._mgr.AddPane(self.macro_toolbar, aui.AuiPaneInfo().
                          Name("macro_toolbar").Caption(_("Macro toolbar")).
                          Gripper(True).ToolbarPane().Top().Row(1))

        self._mgr.AddPane(self.entry_line_panel, aui.AuiPaneInfo().
                          Name("entry_line_panel").Caption(_("Entry line")).
                          MinSize((10, 20)).Row(2).CaptionVisible(False).
                          Top().CloseButton(True).Gripper(True).
                          MaximizeButton(True))

        # Load perspective from config
        window_layout = config["window_layout"]

        if window_layout:
            self._mgr.LoadPerspective(window_layout)

        # Add the main grid
        self._mgr.AddPane(self.grid, aui.AuiPaneInfo().
                          Name("grid").Caption(_("Main grid")).CentrePane())

        # Tell the manager to 'commit' all the changes just made
        self._mgr.Update()
        self._mgr.GetPane("attributes_toolbar")
        self._mgr.Update()

        self._set_menu_toggles()

    def _bind(self):
        """Bind events to handlers"""

        handlers = self.handlers

        # Main window events
        self.Bind(wx.EVT_MOVE, handlers.OnMove)
        self.Bind(wx.EVT_SIZE, handlers.OnSize)

        # Content changed event, adjusts title bar with star
        self.Bind(self.EVT_CONTENT_CHANGED, handlers.OnContentChanged)

        # Program state events

        self.Bind(self.EVT_CMD_TITLE, handlers.OnTitle)
        self.Bind(self.EVT_CMD_SAFE_MODE_ENTRY, handlers.OnSafeModeEntry)
        self.Bind(self.EVT_CMD_SAFE_MODE_EXIT, handlers.OnSafeModeExit)
        self.Bind(wx.EVT_CLOSE, handlers.OnClose)
        self.Bind(self.EVT_CMD_CLOSE, handlers.OnClose)
        self.Bind(wx.EVT_IDLE, self.grid.code_array.eval_manager.process_queues)

        # Preferences events

        self.Bind(self.EVT_CMD_PREFERENCES, handlers.OnPreferences)

        # Toolbar toggle events

        self.Bind(self.EVT_CMD_MAINTOOLBAR_TOGGLE,
                  handlers.OnMainToolbarToggle)
        self.Bind(self.EVT_CMD_MACROTOOLBAR_TOGGLE,
                  handlers.OnMacroToolbarToggle)
        self.Bind(self.EVT_CMD_ATTRIBUTESTOOLBAR_TOGGLE,
                  handlers.OnAttributesToolbarToggle)
        self.Bind(self.EVT_CMD_FIND_TOOLBAR_TOGGLE,
                  handlers.OnFindToolbarToggle)
        self.Bind(self.EVT_CMD_ENTRYLINE_TOGGLE,
                  handlers.OnEntryLineToggle)
        self.Bind(aui.EVT_AUI_PANE_CLOSE, handlers.OnPaneClose)

        # File events

        self.Bind(self.EVT_CMD_NEW, handlers.OnNew)
        self.Bind(self.EVT_CMD_OPEN, handlers.OnOpen)
        self.Bind(self.EVT_CMD_SAVE, handlers.OnSave)
        self.Bind(self.EVT_CMD_SAVEAS, handlers.OnSaveAs)
        self.Bind(self.EVT_CMD_IMPORT, handlers.OnImport)
        self.Bind(self.EVT_CMD_EXPORT, handlers.OnExport)
        self.Bind(self.EVT_CMD_APPROVE, handlers.OnApprove)
        self.Bind(self.EVT_CMD_CLEAR_GLOBALS, handlers.OnClearGlobals)

        # Find events
        self.Bind(self.EVT_CMD_FOCUSFIND, handlers.OnFocusFind)

        # Format events
        self.Bind(self.EVT_CMD_FONTDIALOG, handlers.OnFontDialog)
        self.Bind(self.EVT_CMD_TEXTCOLORDIALOG, handlers.OnTextColorDialog)
        self.Bind(self.EVT_CMD_BGCOLORDIALOG, handlers.OnBgColorDialog)

        # Print events

        self.Bind(self.EVT_CMD_PAGE_SETUP, handlers.OnPageSetup)
        self.Bind(self.EVT_CMD_PRINT_PREVIEW, handlers.OnPrintPreview)
        self.Bind(self.EVT_CMD_PRINT, handlers.OnPrint)

        # Edit Events
        self.Bind(self.EVT_CMD_SELECT_ALL, handlers.OnSelectAll)

        # Clipboard events

        self.Bind(self.EVT_CMD_CUT, handlers.OnCut)
        self.Bind(self.EVT_CMD_COPY, handlers.OnCopy)
        self.Bind(self.EVT_CMD_COPY_RESULT, handlers.OnCopyResult)
        self.Bind(self.EVT_CMD_PASTE, handlers.OnPaste)
        self.Bind(self.EVT_CMD_PASTE_AS, handlers.OnPasteAs)

        # Help events

        self.Bind(self.EVT_CMD_MANUAL, handlers.OnManual)
        self.Bind(self.EVT_CMD_TUTORIAL, handlers.OnTutorial)
        self.Bind(self.EVT_CMD_FAQ, handlers.OnFaq)
        self.Bind(self.EVT_CMD_PYTHON_TURORIAL, handlers.OnPythonTutorial)
        self.Bind(self.EVT_CMD_ABOUT, handlers.OnAbout)

        self.Bind(self.EVT_CMD_MACROLIST, handlers.OnMacroList)
        self.Bind(self.EVT_CMD_MACROREPLACE, handlers.OnMacroReplace)
        self.Bind(self.EVT_CMD_MACROEXECUTE, handlers.OnMacroExecute)
        self.Bind(self.EVT_CMD_MACROLOAD, handlers.OnMacroListLoad)
        self.Bind(self.EVT_CMD_MACROSAVE, handlers.OnMacroListSave)

    def set_icon(self, bmp):
        """Sets main window icon to given wx.Bitmap"""

        _icon = wx.EmptyIcon()
        _icon.CopyFromBitmap(bmp)
        self.SetIcon(_icon)

    def get_safe_mode(self):
        """Returns safe_mode state from code_array"""

        return self.grid.code_array.safe_mode

    safe_mode = property(get_safe_mode)

# End of class MainWindow


class MainWindowEventHandlers(EventMixin):
    """Contains main window event handlers"""

    def __init__(self, parent):
        self.main_window = parent
        self.interfaces = parent.interfaces

    # Main window events

    def OnMove(self, event):
        """Main window move event"""

        # Store window position in config

        position = event.GetPosition()

        config["window_position"] = repr((position.x, position.y))

    def OnSize(self, event):
        """Main window move event"""

        # Store window size in config

        size = event.GetSize()

        config["window_size"] = repr((size.width, size.height))

    def OnContentChanged(self, event):
        """Titlebar star adjustment event handler"""

        self.main_window.changed_since_save = event.changed

        title = self.main_window.GetTitle()

        if event.changed:
            # Put * in front of title
            if title[:2] != "* ":
                new_title = "* " + title
                post_command_event(self.main_window, self.main_window.TitleMsg,
                                   text=new_title)

        elif title[:2] == "* ":
            # Remove * in front of title
            new_title = title[2:]
            post_command_event(self.main_window, self.main_window.TitleMsg,
                               text=new_title)

    def OnTitle(self, event):
        """Title change event handler"""

        self.main_window.SetTitle(event.text)

    def OnSafeModeEntry(self, event):
        """Safe mode entry event handler"""

        # Enable menu item for leaving safe mode

        self.main_window.main_menu.enable_file_approve(True)

        self.main_window.grid.Refresh()

        event.Skip()

    def OnSafeModeExit(self, event):
        """Safe mode exit event handler"""

        # Run macros

        ##self.MainGrid.model.pysgrid.sgrid.execute_macros(safe_mode=False)

        # Disable menu item for leaving safe mode

        self.main_window.main_menu.enable_file_approve(False)

        self.main_window.grid.Refresh()

        event.Skip()

    def OnClose(self, event):
        """Program exit event handler"""

        # If changes have taken place save of old grid

        if self.main_window.changed_since_save:
            save_choice = self.interfaces.get_save_request_from_user()

            if save_choice is None:
                # Cancelled close operation
                return

            elif save_choice:
                # User wants to save content
                post_command_event(self.main_window, self.main_window.SaveMsg)

        # Save the AUI state

        config["window_layout"] = repr(self.main_window._mgr.SavePerspective())

        # Uninit the AUI stuff

        self.main_window._mgr.UnInit()

        # Save config
        config.save()

        # Close main_window

        self.main_window.Destroy()

        # Set file mode to 600 to protect GPG passwd a bit
        sp = wx.StandardPaths.Get()
        pyspreadrc_path = sp.GetUserConfigDir() + "/." + config.config_filename
        try:
            os.chmod(pyspreadrc_path, 0600)
        except OSError:
            dummyfile = open(pyspreadrc_path, "w")
            dummyfile.close()
            os.chmod(pyspreadrc_path, 0600)

        # Terminate the evaluation manager
        self.main_window.log.append("%s\t MAIN_WINDOW Writing log" % time.time())
        f = open(r"C:\log.txt", 'a')
        f.write('\n'+'\n'.join(self.main_window.log)+'\n')
        f.close()
        self.main_window.grid.code_array.eval_manager.terminate(no_restart=True)

    # Preferences events

    def OnPreferences(self, event):
        """Preferences event handler that launches preferences dialog"""

        preferences = self.interfaces.get_preferences_from_user()

        if preferences:
            for key in preferences:
                if type(config[key]) in (type(u""), type("")):
                    config[key] = preferences[key]
                else:
                    config[key] = ast.literal_eval(preferences[key])

    # Toolbar events

    def _toggle_pane(self, pane):
        """Toggles visibility of given aui pane

        Parameters
        ----------

        pane: String
        \tPane name

        """

        if pane.IsShown():
            pane.Hide()
        else:
            pane.Show()

        self.main_window._mgr.Update()

    def OnMainToolbarToggle(self, event):
        """Main window toolbar toggle event handler"""

        self.main_window.main_toolbar.SetGripperVisible(True)
        main_toolbar_info = \
            self.main_window._mgr.GetPane("main_window_toolbar")

        self._toggle_pane(main_toolbar_info)

        event.Skip()

    def OnMacroToolbarToggle(self, event):
        """Macro toolbar toggle event handler"""

        self.main_window.macro_toolbar.SetGripperVisible(True)
        macro_toolbar_info = self.main_window._mgr.GetPane("macro_toolbar")

        self._toggle_pane(macro_toolbar_info)

        event.Skip()

    def OnAttributesToolbarToggle(self, event):
        """Format toolbar toggle event handler"""

        self.main_window.attributes_toolbar.SetGripperVisible(True)
        attributes_toolbar_info = \
            self.main_window._mgr.GetPane("attributes_toolbar")

        self._toggle_pane(attributes_toolbar_info)

        event.Skip()

    def OnFindToolbarToggle(self, event):
        """Search toolbar toggle event handler"""

        self.main_window.find_toolbar.SetGripperVisible(True)

        find_toolbar_info = self.main_window._mgr.GetPane("find_toolbar")

        self._toggle_pane(find_toolbar_info)

        event.Skip()

    def OnEntryLineToggle(self, event):
        """Entry line toggle event handler"""

        entry_line_panel_info = \
            self.main_window._mgr.GetPane("entry_line_panel")

        self._toggle_pane(entry_line_panel_info)

        event.Skip()

    def OnPaneClose(self, event):
        """Pane close toggle event handler (via close button)"""

        toggle_label = event.GetPane().caption

        # Get menu item to toggle
        menubar = self.main_window.menubar
        toggle_id = menubar.FindMenuItem(_("View"), toggle_label)
        toggle_item = menubar.FindItemById(toggle_id)

        # Adjust toggle to pane visibility
        toggle_item.Check(False)

        menubar.UpdateMenus()

        self.main_window._mgr.Update()

        event.Skip()

    # File events

    def OnNew(self, event):
        """New grid event handler"""

        # If changes have taken place save of old grid

        if self.main_window.changed_since_save:
            save_choice = self.interfaces.get_save_request_from_user()

            if save_choice is None:
                # Cancelled close operation
                return

            elif save_choice:
                # User wants to save content
                post_command_event(self.main_window, self.main_window.SaveMsg)

        # Get grid dimensions

        shape = self.interfaces.get_dimensions_from_user(no_dim=3)

        if shape is None:
            return

        # Set new filepath and post it to the title bar

        self.main_window.filepath = None
        post_command_event(self.main_window, self.main_window.TitleMsg,
                           text="pyspread")

        # Clear globals
        self.main_window.grid.actions.clear_globals_reload_modules()

        # Create new grid
        post_command_event(self.main_window, self.main_window.GridActionNewMsg,
                           shape=shape)

        # Update TableChoiceIntCtrl
        post_command_event(self.main_window, self.main_window.ResizeGridMsg,
                           shape=shape)

        if is_gtk():
            wx.Yield()

        self.main_window.grid.actions.change_grid_shape(shape)

        self.main_window.grid.GetTable().ResetView()
        self.main_window.grid.ForceRefresh()

        # Display grid creation in status bar
        msg = _("New grid with dimensions {dim} created.").format(dim=shape)
        post_command_event(self.main_window, self.main_window.StatusBarMsg,
                           text=msg)

        self.main_window.grid.ForceRefresh()

        if is_gtk():
            wx.Yield()

        # Mark content as unchanged
        try:
            post_command_event(self.main_window, self.ContentChangedMsg,
                               changed=False)
        except TypeError:
            # The main window does not exist any more
            pass

    def OnOpen(self, event):
        """File open event handler"""

        # If changes have taken place save of old grid

        if self.main_window.changed_since_save:
            save_choice = self.interfaces.get_save_request_from_user()

            if save_choice is None:
                # Cancelled close operation
                return

            elif save_choice:
                # User wants to save content
                post_command_event(self.main_window, self.main_window.SaveMsg)

        # Get filepath from user

        try:
            import xlrd
            wildcard = \
                _("Pyspread file") + " (*.pys)|*.pys|" + \
                _("Excel file") + " (*.xls)|*.xls|" + \
                _("All files") + " (*.*)|*.*"

        except ImportError:
            wildcard = \
                _("Pyspread file") + " (*.pys)|*.pys|" + \
                _("Excel file") + " (*.xls)|*.xls|" + \
                _("All files") + " (*.*)|*.*"
            xlrd = None

        message = _("Choose file to open.")
        style = wx.OPEN
        filepath, filterindex = \
            self.interfaces.get_filepath_findex_from_user(wildcard, message,
                                                          style)

        if filepath is None:
            return

        filetype = "pys" if xlrd is None or filterindex != 1 else "xls"

        # Change the main window filepath state

        self.main_window.filepath = filepath

        # Load file into grid
        post_command_event(self.main_window,
                           self.main_window.GridActionOpenMsg,
                           attr={"filepath": filepath, "filetype": filetype})

        # Set Window title to new filepath

        title_text = filepath.split("/")[-1] + " - pyspread"
        post_command_event(self.main_window,
                           self.main_window.TitleMsg, text=title_text)

        self.main_window.grid.ForceRefresh()

        if is_gtk():
            wx.Yield()

        # Mark content as unchanged
        try:
            post_command_event(self.main_window, self.ContentChangedMsg,
                               changed=False)
        except TypeError:
            # The main window does not exist any more
            pass

    def OnSave(self, event):
        """File save event handler"""

        # If there is no filepath then jump to save as

        if self.main_window.filepath is None:
            post_command_event(self.main_window,
                               self.main_window.SaveAsMsg)
            return

        # Save the grid

        post_command_event(self.main_window,
                           self.main_window.GridActionSaveMsg,
                           attr={"filepath": self.main_window.filepath})

        # Display file save in status bar

        statustext = self.main_window.filepath.split("/")[-1] + " saved."
        post_command_event(self.main_window,
                           self.main_window.StatusBarMsg,
                           text=statustext)

    def OnSaveAs(self, event):
        """File save as event handler"""

        # Get filepath from user

        wildcard = \
            _("Pyspread file") + " (*.pys)|*.pys|" + \
            _("All files") + " (*.*)|*.*"
        message = _("Choose filename for saving.")
        style = wx.SAVE
        filepath, filterindex = \
            self.interfaces.get_filepath_findex_from_user(wildcard, message,
                                                          style)

        if filepath is None:
            return 0

        # Look if path is already present
        if os.path.exists(filepath):
            if not os.path.isfile(filepath):
                # There is a directory with the same path
                statustext = _("Directory present. Save aborted.")
                post_command_event(self.main_window,
                                   self.main_window.StatusBarMsg,
                                   text=statustext)
                return 0

            # There is a file with the same path
            message = \
                _("The file {filepath} is already present.\nOverwrite?")\
                .format(filepath=filepath)
            short_msg = _("File collision")

            if not self.main_window.interfaces.get_warning_choice(message,
                                                                  short_msg):

                statustext = _("File present. Save aborted by user.")
                post_command_event(self.main_window,
                                   self.main_window.StatusBarMsg,
                                   text=statustext)
                return 0

        # Put pys suffix if wildcard choice is 0
        if filterindex == 0 and filepath[-4:] != ".pys":
            filepath += ".pys"

        # Set the filepath state
        self.main_window.filepath = filepath

        # Set Window title to new filepath
        title_text = filepath.split("/")[-1] + " - pyspread"
        post_command_event(self.main_window, self.main_window.TitleMsg,
                           text=title_text)

        # Now jump to save
        post_command_event(self.main_window, self.main_window.SaveMsg)

    def OnImport(self, event):
        """File import event handler"""

        # Get filepath from user

        wildcard = \
            _("CSV file") + " (*.*)|*.*|" + \
            _("Tab delimited text file") + " (*.*)|*.*"
        message = _("Choose file to import.")
        style = wx.OPEN
        filepath, filterindex = \
            self.interfaces.get_filepath_findex_from_user(wildcard, message,
                                                          style)

        if filepath is None:
            return

        # Get generator of import data
        import_data = self.main_window.actions.import_file(filepath,
                                                           filterindex)

        if import_data is None:
            return

        # Paste import data to grid
        grid = self.main_window.grid
        tl_cell = grid.GetGridCursorRow(), grid.GetGridCursorCol()

        grid.actions.paste(tl_cell, import_data)

        self.main_window.grid.ForceRefresh()

    def OnExport(self, event):
        """File export event handler

        Currently, only CSV export is supported

        """

        code_array = self.main_window.grid.code_array
        tab = self.main_window.grid.current_table

        selection = self.main_window.grid.selection

        # Check if no selection is present

        selection_bbox = selection.get_bbox()

        wildcard = _("CSV file") + " (*.*)|*.*"

        if selection_bbox is None:
            # No selection --> Use smallest filled area for bottom right edge
            maxrow, maxcol, __ = code_array.get_last_filled_cell(tab)

            (top, left), (bottom, right) = (0, 0), (maxrow, maxcol)

        else:
            (top, left), (bottom, right) = selection_bbox

        # Generator of row and column keys in correct order

        __top = 0 if top is None else top
        __bottom = code_array.shape[0] if bottom is None else bottom + 1
        __left = 0 if left is None else left
        __right = code_array.shape[1] if right is None else right + 1

        def data_gen(top, bottom, left, right):
            for row in xrange(top, bottom):
                yield (code_array[row, col, tab]
                       for col in xrange(left, right))

        data = data_gen(__top, __bottom, __left, __right)
        preview_data = data_gen(__top, __bottom, __left, __right)

        # Get target filepath from user

        # No selection --> Provide svg export of current cell
        # if current cell is a matplotlib figure
        if selection_bbox is None:
            cursor = self.main_window.grid.actions.cursor
            figure = code_array[cursor]
            if isinstance(figure, Figure):
                wildcard += \
                    "|" + _("SVG file") + " (*.svg)|*.svg" + \
                    "|" + _("EPS file") + " (*.eps)|*.eps" + \
                    "|" + _("PS file") + " (*.ps)|*.ps" + \
                    "|" + _("PDF file") + " (*.pdf)|*.pdf" + \
                    "|" + _("PNG file") + " (*.png)|*.png"

        message = _("Choose filename for export.")
        style = wx.SAVE
        path, filterindex = \
            self.interfaces.get_filepath_findex_from_user(wildcard, message,
                                                          style)

        # If an svg is exported then the selection bbox
        # has to be changed to the current cell
        if filterindex >= 1:
            data = figure

        # Export file
        # -----------

        self.main_window.actions.export_file(path, filterindex, data,
                                             preview_data)

    def OnApprove(self, event):
        """File approve event handler"""

        if not self.main_window.safe_mode:
            return

        msg = _(u"You are going to approve and trust a file that\n"
                u"you have not created yourself.\n"
                u"After proceeding, the file is executed.\n \n"
                u"It may harm your system as any program can.\n"
                u"Please check all cells thoroughly before\nproceeding.\n \n"
                u"Proceed and sign this file as trusted?")

        short_msg = _("Security warning")

        if self.main_window.interfaces.get_warning_choice(msg, short_msg):
            # Leave safe mode
            self.main_window.grid.actions.leave_safe_mode()

            # Display safe mode end in status bar

            statustext = _("Safe mode deactivated.")
            post_command_event(self.main_window, self.main_window.StatusBarMsg,
                               text=statustext)

    def OnClearGlobals(self, event):
        """Clear globals event handler"""

        msg = _("Deleting globals and reloading modules cannot be undone."
                " Proceed?")
        short_msg = _("Really delete globals and modules?")

        choice = self.main_window.interfaces.get_warning_choice(msg, short_msg)

        if choice:
            self.main_window.grid.actions.clear_globals_reload_modules()

            statustext = _("Globals cleared and base modules reloaded.")
            post_command_event(self.main_window, self.main_window.StatusBarMsg,
                               text=statustext)

    # Print events

    def OnPageSetup(self, event):
        """Page setup handler for printing framework"""

        print_data = self.main_window.print_data
        new_print_data = \
            self.main_window.interfaces.get_print_setup(print_data)
        self.main_window.print_data = new_print_data

    def _get_print_area(self):
        """Returns selection bounding box or visible area"""

        # Get print area from current selection
        selection = self.main_window.grid.selection
        print_area = selection.get_bbox()

        # If there is no selection use the visible area on the screen
        if print_area is None:
            print_area = self.main_window.grid.actions.get_visible_area()

        return print_area

    def OnPrintPreview(self, event):
        """Print preview handler"""

        print_area = self._get_print_area()
        print_data = self.main_window.print_data

        self.main_window.actions.print_preview(print_area, print_data)

    def OnPrint(self, event):
        """Print event handler"""

        print_area = self._get_print_area()
        print_data = self.main_window.print_data

        self.main_window.actions.printout(print_area, print_data)

    # Clipboard events

    def OnCut(self, event):
        """Clipboard cut event handler"""

        data = self.main_window.actions.cut(self.main_window.grid.selection)
        self.main_window.clipboard.set_clipboard(data)

        self.main_window.grid.ForceRefresh()

        event.Skip()

    def OnCopy(self, event):
        """Clipboard copy event handler"""

        focus = self.main_window.FindFocus()

        if isinstance(focus, wx.TextCtrl):
            # Copy selection from TextCtrl if in focus
            focus.Copy()

        else:
            selection = self.main_window.grid.selection
            data = self.main_window.actions.copy(selection)
            self.main_window.clipboard.set_clipboard(data)

        event.Skip()

    def OnCopyResult(self, event):
        """Clipboard copy results event handler"""

        selection = self.main_window.grid.selection
        data = self.main_window.actions.copy_result(selection)

        # Check if result is a bitmap
        if type(data) is wx._gdi.Bitmap:
            # Copy bitmap to clipboard
            self.main_window.clipboard.set_clipboard(data, datatype="bitmap")

        else:
            # Copy string representation of result to clipboard

            self.main_window.clipboard.set_clipboard(data, datatype="text")

        event.Skip()

    def OnPaste(self, event):
        """Clipboard paste event handler"""

        data = self.main_window.clipboard.get_clipboard()

        focus = self.main_window.FindFocus()

        if isinstance(focus, wx.TextCtrl):
            # Paste into TextCtrl if in focus
            focus.WriteText(data)

        else:
            # We got a grid selection
            key = self.main_window.grid.actions.cursor

            self.main_window.actions.paste(key, data)

        self.main_window.grid.ForceRefresh()

        event.Skip()

    def OnPasteAs(self, event):
        """Clipboard paste as event handler"""

        data = self.main_window.clipboard.get_clipboard()
        key = self.main_window.grid.actions.cursor

        self.main_window.actions.paste_as(key, data)

        self.main_window.grid.ForceRefresh()

        event.Skip()

    # Edit events
    def OnSelectAll(self, event):
        """Select all cells event handler"""
        self.main_window.grid.SelectAll()

    # View events

    def OnFocusFind(self, event):
        """Event handler for focusing find toolbar text widget"""

        self.main_window.find_toolbar.search.SetFocus()

    # Format events

    def OnFontDialog(self, event):
        """Event handler for launching font dialog"""

        # Get current font data from current cell
        cursor = self.main_window.grid.actions.cursor
        attr = self.main_window.grid.code_array.cell_attributes[cursor]

        size, style, weight, font = \
            [attr[name] for name in ["pointsize", "fontstyle", "fontweight",
                                     "textfont"]]
        current_font = wx.Font(int(size), -1, style, weight, 0, font)

        # Get Font from dialog

        fontdata = wx.FontData()
        fontdata.EnableEffects(True)
        fontdata.SetInitialFont(current_font)

        dlg = wx.FontDialog(self.main_window, fontdata)

        if dlg.ShowModal() == wx.ID_OK:
            fontdata = dlg.GetFontData()
            font = fontdata.GetChosenFont()

            post_command_event(self.main_window, self.main_window.FontMsg,
                               font=font.FaceName)
            post_command_event(self.main_window, self.main_window.FontSizeMsg,
                               size=font.GetPointSize())

            post_command_event(self.main_window, self.main_window.FontBoldMsg,
                               weight=font.GetWeightString())
            post_command_event(self.main_window,
                               self.main_window.FontItalicsMsg,
                               style=font.GetStyleString())

            if is_gtk():
                wx.Yield()

            self.main_window.grid.update_attribute_toolbar()

    def OnTextColorDialog(self, event):
        """Event handler for launching text color dialog"""

        dlg = wx.ColourDialog(self.main_window)

        # Ensure the full colour dialog is displayed,
        # not the abbreviated version.
        dlg.GetColourData().SetChooseFull(True)

        if dlg.ShowModal() == wx.ID_OK:

            # Fetch color data
            data = dlg.GetColourData()
            color = data.GetColour().GetRGB()

            post_command_event(self.main_window, self.main_window.TextColorMsg,
                               color=color)

        dlg.Destroy()

    def OnBgColorDialog(self, event):
        """Event handler for launching background color dialog"""

        dlg = wx.ColourDialog(self.main_window)

        # Ensure the full colour dialog is displayed,
        # not the abbreviated version.
        dlg.GetColourData().SetChooseFull(True)

        if dlg.ShowModal() == wx.ID_OK:

            # Fetch color data
            data = dlg.GetColourData()
            color = data.GetColour().GetRGB()

            post_command_event(self.main_window,
                               self.main_window.BackgroundColorMsg,
                               color=color)

        dlg.Destroy()

    # Macro events

    def OnMacroList(self, event):
        """Macro list dialog event handler"""

        self.main_window.interfaces.display_macros()

        event.Skip()

    def OnMacroReplace(self, event):
        """Macro change event handler"""

        self.main_window.actions.replace_macros(event.macros)

    def OnMacroExecute(self, event):
        """Macro execution event handler"""

        self.main_window.actions.execute_macros()

    def OnMacroListLoad(self, event):
        """Macro list load event handler"""

        # Get filepath from user

        wildcard = \
            _("Macro file") + " (*.py)|*.py|" + \
            _("All files") + " (*.*)|*.*"
        message = _("Choose macro file.")

        style = wx.OPEN
        filepath, filterindex = \
            self.interfaces.get_filepath_findex_from_user(wildcard, message,
                                                          style)

        if filepath is None:
            return

        # Enter safe mode because macro file could be harmful

        post_command_event(self.main_window, self.main_window.SafeModeEntryMsg)

        # Load macros from file

        self.main_window.actions.open_macros(filepath)

        event.Skip()

    def OnMacroListSave(self, event):
        """Macro list save event handler"""

        # Get filepath from user

        wildcard = \
            _("Macro file") + " (*.py)|*.py|" + \
            _("All files") + " (*.*)|*.*"
        message = _("Choose macro file.")

        style = wx.SAVE
        filepath, filterindex = \
            self.interfaces.get_filepath_findex_from_user(wildcard, message,
                                                          style)

        # Save macros to file

        macros = self.main_window.grid.code_array.macros
        self.main_window.actions.save_macros(filepath, macros)

        event.Skip()

    # Help events

    def OnManual(self, event):
        """Manual launch event handler"""

        self.main_window.actions.launch_help("First steps in pyspread",
                                             "First steps.html")

    def OnTutorial(self, event):
        """Tutorial launch event handler"""

        self.main_window.actions.launch_help("Pyspread tutorial",
                                             "tutorial.html")

    def OnFaq(self, event):
        """FAQ launch event handler"""

        self.main_window.actions.launch_help("Pyspread tutorial", "faq.html")

    def OnPythonTutorial(self, event):
        """Python tutorial launch event handler"""

        self.main_window.actions.launch_help("Python tutorial",
                                             get_python_tutorial_path())

    def OnAbout(self, event):
        """About dialog event handler"""

        self.main_window.interfaces.display_about(self.main_window)

# End of class MainWindowEventHandlers