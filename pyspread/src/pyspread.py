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

========
pyspread
========

Python spreadsheet application

Run this script to start the application.

Provides
--------

* Commandlineparser: Gets command line options and parameters
* MainApplication: Initial command line operations and application launch

"""

import sys
import optparse

import wx
__ = wx.App(False)  # Windows Hack

from sysvars import get_program_path
import lib.i18n as i18n


#use ugettext instead of getttext to avoid unicode errors
_ = i18n.language.ugettext

sys.setrecursionlimit(10000)
sys.path.insert(0, get_program_path())

# Patch for using with PyScripter thanks to Colin J. Williams
# If wx exists in sys,modules, we dont need to import wx version.
# wx is already imported if the PyScripter wx engine is used.

try:
    sys.modules['wx']
except KeyError:
    # Select wx version 2.8 if possible
    try:
        import wxversion
        wxversion.select(['2.8', '2.9'])

    except ImportError:
        pass

from src.gui._events import post_command_event, GridActionEventMixin

DEBUG = False


class Commandlineparser(object):
    """
    Command line handling

    Methods:
    --------

    parse: Returns command line options and arguments as 2-tuple

    """

    def __init__(self):
        from src.config import config
        self.config = config

        usage_str = _("usage: %prog [options] [filename]")
        version = config["version"]
        version_str = _("%prog {version}").format(version=version)
        self.parser = optparse.OptionParser(usage=usage_str,
                                            version=version_str)

        grid_shape = (
            config["grid_rows"],
            config["grid_columns"],
            config["grid_tables"],
        )

        self.parser.add_option(
            "-d", "--dimensions", type="int", nargs=3,
            dest="dimensions", default=grid_shape,
            help=_("Dimensions of empty grid (works only without filename) "
                   "rows, cols, tables [default: %default]")
        )

    def parse(self):
        """
        Returns a a tuple (options, filename)

        options: The command line options
        filename: String (defaults to None)
        \tThe name of the file that is loaded on start-up

        """
        options, args = self.parser.parse_args()

        # If one dimension is 0 then the grid has no cells
        if min(options.dimensions) < 1:
            print _("Cell dimension must be > 0.")
            sys.exit()

        # No MDI yet, pyspread can be started several times though
        if len(args) > 1:
            print _("Only one file may be opened at a time.")
            sys.exit()

        filename = None
        if len(args) == 1:
            # A filename is provided and hence opened
            filename = args[0]

        return options, filename

# end of class Commandlineparser


class MainApplication(wx.App, GridActionEventMixin):
    """Main application class for pyspread."""

    dimensions = (1, 1, 1)  # Will be overridden anyways
    options = {}
    filename = None

    def __init__(self, *args, **kwargs):

        try:
            self.S = kwargs.pop("S")
        except KeyError:
            self.S = None

        # call parent class initializer
        wx.App.__init__(self, *args, **kwargs)

    def OnInit(self):
        """Init class that is automatically run on __init__"""

        # Get command line options and arguments
        self.get_cmd_args()

        # Initialize the prerequisitions to construct the main window
        wx.InitAllImageHandlers()

        # Main window creation
        from src.gui._main_window import MainWindow

        self.main_window = MainWindow(None, title="pyspread", S=self.S)

        ## Initialize file loading via event

        # Create GPG key if not present

        try:
            from src.lib.gpg import genkey
            genkey()

        except ImportError:
            pass

        except ValueError:
            # python-gnupg is installed but gnupg is not insatlled

            pass

        # Show application window
        self.SetTopWindow(self.main_window)
        self.main_window.Show()

        # Load filename if provided
        if self.filepath is not None:
            post_command_event(self.main_window, self.GridActionOpenMsg,
                               attr={"filepath": self.filepath})
            self.main_window.filepath = self.filepath


        return True

    def get_cmd_args(self):
        """Returns command line arguments

        Created attributes
        ------------------

        options: dict
        \tCommand line options
        dimensions: Three tuple of Int
        \tGrid dimensions, default value (1,1,1).
        filename: String
        \tFile name that is loaded on start

        """

        cmdp = Commandlineparser()
        self.options, self.filepath = cmdp.parse()

        if self.filename is None:
            rows, columns, tables = self.options.dimensions
            cmdp.config["grid_rows"] = str(rows)
            cmdp.config["grid_columns"] = str(columns)
            cmdp.config["grid_tables"] = str(tables)


def pyspread(S=None):
    """Parses command line and starts pyspread"""

    # Initialize main application
    app = MainApplication(S=S, redirect=False)

    app.MainLoop()


if __name__ == "__main__":
    if 'unicode' not in wx.PlatformInfo:
        print _("You need a unicode build of wxPython to run pyspread.")

    else:
        if DEBUG:
            import cProfile
            cProfile.run('pyspread()')
        else:
            pyspread()
