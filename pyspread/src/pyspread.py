#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011 Martin Manns
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

pyspread
========

Python spreadsheet application

Run this script to start the application.

Provides
--------

* Commandlineparser: Gets command line options and parameters
* MainApplication: Initial command line operations and application launch

"""

# Patch for using with PyScripter thanks to Colin J. Williams
# If wx exists in sys,modules, we dont need to import wx version.
# wx is already imported if the PyScripter wx engine is used.

from sys import path, modules
from sysvars import get_program_path

import optparse

path.insert(0, get_program_path())

try:
    modules['wx']
except KeyError:
    # End of patch

    # Select wx version 2.8 if possible

    try:
        import wxversion
        wxversion.select(['2.8', '2.9'])

    except ImportError:
        pass

from wx import App
from wx import InitAllImageHandlers


from src.gui._events import post_command_event, GridActionOpenMsg

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
        usage = "usage: %prog [options] [filename]"
        version = "%prog " + unicode(config["version"])
        self.parser = optparse.OptionParser(usage=usage, version=version)

        self.parser.add_option("-d", "--dimensions", type="int", nargs=3,
            dest="dimensions", default=config["grid_shape"], \
            help="Dimensions of empty grid (works only without filename) "
                 "rows, cols, tables [default: %default]")

    def parse(self):
        """
        Returns a a tuple (options, filename)

        options: The command line options
        filename: String (defaults to None)
        \tThe name of the file that is loaded on start-up

        """
        options, args = self.parser.parse_args()

        if min(options.dimensions) < 1:
            raise ValueError, "The number of cells in each dimension " + \
                              "has to be greater than 0"

        if len(args) > 1:
            raise ValueError, "Only one file may be opened at a time"
        elif len(args) == 1:
            filename = args[0]
        else:
            filename = None

        return options, filename

# end of class Commandlineparser


class MainApplication(App):
    """Main application class for pyspread."""

    dimensions = (1, 1, 1)  # Will be overridden anyways
    options = {}
    filename = None

    def OnInit(self):
        """Init class that is automatically run on __init__"""

        # Get command line options and arguments
        self.get_cmd_args()

        # Initialize the prerequisitions to construct the main window
        InitAllImageHandlers()

        # Main window creation
        from src.gui._main_window import MainWindow

        self.main_window = MainWindow(None, title="pyspread")

        ## Set dimensions

        ## Initialize file loading via event

        # Create GPG key if not present

        from src.lib.gpg import genkey

        genkey()

        # Show application window
        self.SetTopWindow(self.main_window)
        self.main_window.Show()

        # Load filename if provided
        if self.filepath is not None:
            post_command_event(self.main_window, GridActionOpenMsg,
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
            self.dimensions = self.options.dimensions


def __main__():
    """Compatibility hack"""

    pass


def main():
    """Parses command line and starts pyspread"""

    # Initialize main application
    app = MainApplication(0)

    app.MainLoop()


if __name__ == "__main__":
    if DEBUG:
        import cProfile
        cProfile.run('main()')
    else:
        main()